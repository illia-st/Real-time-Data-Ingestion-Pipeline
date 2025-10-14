"""
    Utils file with helper functions.
"""
import logging
import os
from dotenv import load_dotenv
import polars as pl

load_dotenv('config/.env')

# Set up a basic logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config(config_path='config/.env'):
    """
    Loads the .ENV configuration file.
    """
    if load_dotenv(config_path):
        logger.info("Configuration file loaded successfully from: %s", config_path)
        # Build up the config dictionary
        env_config = {
            'kafka': {
                'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                'consumer_group_id': os.getenv('KAFKA_CONSUMER_GROUP_ID'),
                'topic_name': os.getenv('KAFKA_TOPIC_NAME')
            },
            'test_data_config': {
                'file_path': os.getenv('TEST_DATA_FILE_PATH'),
                'events_to_produce_amount': os.getenv('TEST_DATA_EVENTS_TO_PRODUCE_AMOUNT'),
                'chunk_size': os.getenv('TEST_DATA_CHUNK_SIZE'),
                'sleep_interval': os.getenv('TEST_DATA_SLEEP_INTERVAL'),
            },
            's3_config': {
                's3_bucket_name': os.getenv('S3_BUCKET_NAME'),
                'batch_size': os.getenv('S3_BATCH_SIZE'),
                'batch_interval_time_secs': os.getenv('S3_BATCH_INTERVAL_TIME_SECS'),
            },
        }
        logger.info(str(env_config))
        return env_config
    logger.error("Configuration file not found at: %s", config_path)
    return None

def load_test_data(
    file_path: str = '',
    offset: int = 0,
    read_items_amount: int = 10000
) -> pl.DataFrame:
    """
    Loads test data from dataset using offsets and chunks.
    """
    reader = pl.read_csv(source=file_path, skip_rows_after_header=offset, n_rows=read_items_amount)
    return reader
