"""
Step 3: Kafka Producer - Stream transformed data to Kafka
"""

import json
import logging
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from sqlalchemy import create_engine
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'jne-kafka:29092'  # NEW - internal Docker port
TOPIC_NAME = 'jne-shipments'
DB_CONN = "postgresql://jne_user:jne_secure_password_2024@jne-postgres:5432/jne_dashboard"

def create_producer(max_retries=5, retry_delay=5):
    """Create Kafka producer with retry logic"""
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                api_version=(0, 10, 1)
            )
            logger.info("✓ Kafka producer created successfully")
            return producer
        except NoBrokersAvailable as e:
            if attempt < max_retries - 1:
                logger.warning(f"Kafka not available yet. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to Kafka after all retries")
                raise

def stream_to_kafka():
    """Stream transformed data to Kafka"""
    logger.info("Starting Kafka streaming...")
    
    producer = create_producer()
    engine = create_engine(DB_CONN)
    
    try:
        query = "SELECT * FROM transformed.cms_cnote LIMIT 1000"
        df = pd.read_sql(query, engine)
        logger.info(f"Loaded {len(df)} records from transformed.cms_cnote")
    except Exception as e:
        logger.error(f"Failed to read from transformed.cms_cnote: {e}")
        raise
    
    if len(df) == 0:
        logger.warning("No data to stream - table is empty")
        return
    
    logger.info(f"Streaming {len(df)} records to Kafka topic: {TOPIC_NAME}")
    for idx, row in df.iterrows():
        message = row.to_dict()
        producer.send(TOPIC_NAME, value=message)
        
        if (idx + 1) % 100 == 0:
            logger.info(f"  Sent {idx + 1} messages...")
    
    producer.flush()
    producer.close()
    logger.info(f"✓ Successfully streamed {len(df)} records to topic: {TOPIC_NAME}")

if __name__ == '__main__':
    stream_to_kafka()