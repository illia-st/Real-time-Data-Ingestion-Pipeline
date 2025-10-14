"""
    Defining class for producer
"""
from datetime import datetime, timezone
import json
import uuid
from typing import Any, Dict
import time
import boto3
from confluent_kafka import Consumer, KafkaException
from src.utils import logger

class KafkaConsumer:
    """ Kafka Consumer class. """
    def __init__(self, consumer_config: Dict[str, Any]):
        """ Initializes the Kafka Consumer. """
        consumer_conf = {
            'bootstrap.servers': consumer_config['kafka']['bootstrap_servers'],
            'group.id': consumer_config['kafka']['consumer_group_id'],
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'false',
        }
        self.__consumer = Consumer(**consumer_conf)
        self.__topic = consumer_config['kafka']['topic_name']

        s3_config = consumer_config['s3_config']
        self.__s3_client = boto3.client('s3')
        self.__s3_bucket_name = s3_config['s3_bucket_name']
        self.__batch_size = int(s3_config['batch_size'])
        self.__batch_interval_time_secs = int(s3_config['batch_interval_time_secs'])

        self.__batch_events = [None] * self.__batch_size # Preallocate size
        self.__current_batch_size = 0
        self.__last_received_event_time = time.perf_counter()

    def __add_event(self, event: str):
        if self.__current_batch_size == self.__batch_size:
            raise IndexError("Batch events are full, something went wrong")
        self.__batch_events[self.__current_batch_size] = event
        self.__current_batch_size += 1
        self.__last_received_event_time = time.perf_counter()

    def __clear_batch_events(self):
        self.__current_batch_size = 0
        self.__last_received_event_time = time.perf_counter()  # Reset timer too

    def __should_upload_batch(self):
        """
        Determines if the current batch of events should be uploaded to S3.

        An upload is triggered if the batch size is full, or if the batch
        is not empty and the time since the last event was received has
        exceeded the configured interval.
        """
        is_batch_full = self.__current_batch_size >= self.__batch_size

        time_since_last_event = time.perf_counter() - self.__last_received_event_time
        time_threshold_reached = time_since_last_event >= self.__batch_interval_time_secs

        is_time_to_upload = self.__current_batch_size > 0 and time_threshold_reached

        return is_batch_full or is_time_to_upload

    def consume_messages(self):
        """ Subscribes to the topic and starts the consuming loop. """
        self.__consumer.subscribe([self.__topic])
        logger.info('Subscribed to topic \'%s\'. Waiting for messages...', self.__topic)

        try:
            while True:
                msg = self.__consumer.poll(timeout=1.0)

                if self.__should_upload_batch():
                    try:
                        self.__upload_batch_to_s3()
                        self.__consumer.commit(asynchronous=False)
                        logger.info("Kafka offset committed successfully.")
                        self.__clear_batch_events()
                    except Exception as e:
                        logger.error('Failed to upload batch to S3: %s', e)
                        time.sleep(5) # adding simple retry delay, this mechanism can be improved
                        continue

                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                event_data = json.loads(msg.value().decode('utf-8'))
                self.__add_event(json.dumps(event_data))
        except KeyboardInterrupt:
            logger.info('User interrupted the process.')
        finally:
            if self.__current_batch_size > 0:
                try:
                    self.__upload_batch_to_s3()
                    self.__consumer.commit(asynchronous=False)
                except Exception as e:
                    logger.error('Failed to upload final batch: %s', e)
            self.close()

    def __upload_batch_to_s3(self):
        if not self.__batch_events:
            return

        file_content = '\n'.join(self.__batch_events[:self.__current_batch_size])
        now = datetime.now(timezone.utc)
        s3_key = (
            f"events/year={now.strftime('%Y')}"
            f"/month={now.strftime('%m')}"
            f"/day={now.strftime('%d')}"
            f"/hour={now.strftime('%H')}"
            f"/{now.strftime('%M-%S')}-{uuid.uuid4()}.jsonl"
        )

        self.__s3_client.put_object(
            Bucket=self.__s3_bucket_name,
            Key=s3_key,
            Body=file_content.encode('utf-8')
        )
        logger.info(
            'Successfully uploaded batch of %s events to %s', self.__current_batch_size, s3_key
        )

    def close(self):
        """ Closes the consumer connection. """
        logger.info("Closing consumer...")
        self.__consumer.close()
