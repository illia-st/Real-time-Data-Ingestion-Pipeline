"""
    Execution script for consumer
"""
from data_consumer import KafkaConsumer
from utils import load_config, logger

# --- Main execution block ---
if __name__ == '__main__':
    logger.info('Starting data cosnumer script')
    config = load_config()
    if config:
        consumer = KafkaConsumer(config)
        consumer.consume_messages()
