"""
    Defining class for producer
"""
import json
from typing import Any, Dict
from confluent_kafka import Consumer, KafkaException
from src.utils import logger

class KafkaConsumer:
    """ Kafka Consumer class. """
    def __init__(self, consumer_config: Dict[str, Any]):
        """ Initializes the Kafka Consumer. """
        consumer_conf = {
            'bootstrap.servers': consumer_config['kafka']['bootstrap_servers'],
            'group.id': consumer_config['kafka']['consumer_group_id'],
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(**consumer_conf)
        self.topic = consumer_config['kafka']['topic_name']

    def consume_messages(self):
        """ Subscribes to the topic and starts the consuming loop. """
        self.consumer.subscribe([self.topic])
        logger.info('Subscribed to topic \'%s\'. Waiting for messages...', self.topic)

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                event_data = json.loads(msg.value().decode('utf-8'))
                logger.info(
                    'Consumed record: key=%s value=%s', msg.key().decode('utf-8'), event_data
                )
        except KeyboardInterrupt:
            logger.info('User interrupted the process.')
        finally:
            self.close()

    def close(self):
        """ Closes the consumer connection. """
        logger.info("Closing consumer...")
        self.consumer.close()
