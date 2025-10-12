"""
    Execution script for producer
"""
import time
import json
from typing import Dict
from data_producer import KafkaProducer
from utils import load_test_data, load_config

# --- Main execution block ---
if __name__ == '__main__':
    config = load_config()
    if config:
        producer = KafkaProducer(config)
        test_data_config: Dict = config['test_data_config']
        test_data_path: str = test_data_config['file_path']
        test_data_events_to_produce_amount: int = int(test_data_config['events_to_produce_amount'])
        test_data_chunk_size: int = int(test_data_config['chunk_size'])
        test_data_sleep_interval: int = int(test_data_config['sleep_interval'])

        try:
            for i in range(int(test_data_events_to_produce_amount / test_data_chunk_size)):
                events = load_test_data(
                    file_path=test_data_path,
                    offset=i * test_data_chunk_size,
                    read_items_amount=test_data_chunk_size,
                )
                for index, row in enumerate(events.rows(named=True)):
                    event_data = json.dumps(row)
                    producer.send_message(key=str(row['user']), value=event_data)
                time.sleep(test_data_sleep_interval)
        finally:
            producer.close()
