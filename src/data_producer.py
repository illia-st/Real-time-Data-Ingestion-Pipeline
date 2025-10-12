"""
    Kafka Producer class.
"""
import json
from typing import Dict, Any
from confluent_kafka import Producer
from utils import logger

class KafkaProducer:
    """ Kafka Producer class. """
    def __init__(self, producer_config: Dict[str, Any]):
        """ Initializes the Kafka Producer with the given configuration. """
        producer_conf = {'bootstrap.servers': producer_config['kafka']['bootstrap_servers']}
        self.producer: Producer = Producer(**producer_conf)
        self.topic = producer_config['kafka']['topic_name']

    def _delivery_report(self, err, msg):
        """ Private callback function for message delivery reports. """
        if err is not None:
            logger.error("Message delivery failed: %s", err)
        else:
            logger.info("Message delivered to %s [%s]", msg.topic(), msg.partition())

    def send_message(self, key: str, value: str):
        """ Produces a single message to the Kafka topic. """

        self.producer.produce(
            self.topic,
            key=key.encode('utf-8'),
            value=value.encode('utf-8'),
            callback=self._delivery_report
        )
        # Poll to trigger the delivery report callback
        self.producer.poll(0)

    def close(self):
        """ Flushes any outstanding messages and closes the producer. """
        logger.info("Flushing and closing producer...")
        self.producer.flush()
