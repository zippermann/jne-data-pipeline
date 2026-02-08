"""
JNE Kafka Producer
Streams transformed data to Kafka with audit logging.
"""

import json
import logging
import time
import sys
from pathlib import Path
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from sqlalchemy import create_engine, text
import pandas as pd

# Add project paths
sys.path.append(str(Path(__file__).parent.parent / 'audit'))
sys.path.append(str(Path(__file__).parent.parent.parent))

# Import audit logger (non-fatal if unavailable)
try:
    from audit_logger import AuditLogger
except ImportError:
    try:
        sys.path.insert(0, '/opt/airflow/scripts/audit')
        from audit_logger import AuditLogger
    except ImportError:
        AuditLogger = None

# Import config
try:
    from pipeline_config import DB_CONN
except ImportError:
    sys.path.insert(0, '/opt/airflow')
    from pipeline_config import DB_CONN

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = 'jne-kafka:29092'
DEFAULT_TOPIC = 'jne-shipments'


def get_audit_logger(engine):
    """Get an AuditLogger instance, or None if unavailable."""
    if AuditLogger is None:
        return None
    try:
        return AuditLogger(engine)
    except Exception as e:
        logger.warning(f"Could not initialize AuditLogger: {e}")
        return None


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
            logger.info("Kafka producer created successfully")
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                logger.warning(f"Kafka not available yet. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to Kafka after all retries")
                raise


def stream_to_kafka(engine, audit, topic_name: str, limit: int = 1000):
    """
    Stream transformed data to Kafka with audit logging.

    Args:
        engine: SQLAlchemy engine
        audit: AuditLogger instance (or None)
        topic_name: Kafka topic name
        limit: Number of records to stream (default 1000)
    """
    batch_id = audit.start_batch(
        'Airflow (Kafka Stream)', 'DATA_STREAM'
    ) if audit else None

    try:
        logger.info("Connecting to Kafka...")
        producer = create_producer()

        # Check if transformed schema exists
        try:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = 'transformed'
                    AND table_name = 'cms_cnote'
                """))
                table_exists = result.fetchone()[0] > 0

            source_table = "transformed.cms_cnote" if table_exists else "raw.cms_cnote"
        except Exception as e:
            logger.warning(f"Could not check for transformed table: {e}")
            source_table = "raw.cms_cnote"

        # Load data from database
        query = f"SELECT * FROM {source_table} LIMIT {limit}"
        logger.info(f"Loading data from {source_table}...")
        df = pd.read_sql(query, engine)
        logger.info(f"Loaded {len(df)} records")

        if len(df) == 0:
            logger.warning("No data to stream - table is empty")
            if audit:
                audit.complete_batch(batch_id, 0, 'PARTIAL', 'No data to stream')
            return

        # Stream records to Kafka
        logger.info(f"Streaming {len(df)} records to Kafka topic: {topic_name}")

        success_count = 0
        failed_count = 0

        for idx, row in df.iterrows():
            try:
                message = row.to_dict()
                producer.send(topic_name, value=message)
                success_count += 1

                if (idx + 1) % 100 == 0:
                    logger.info(f"  Sent {idx + 1} messages...")

            except Exception as e:
                logger.error(f"Failed to send record {idx}: {e}")
                failed_count += 1

        # Flush and close producer
        producer.flush()
        producer.close()

        # Log traceability
        if audit:
            audit.log_trace(
                source_stage=f'PostgreSQL ({source_table})',
                target_stage=f'Kafka ({topic_name})',
                transformation_logic='Pandas row-by-row JSON serialization to Kafka',
                batch_id=batch_id,
                record_count=success_count,
            )

        status = 'SUCCESS' if failed_count == 0 else 'PARTIAL'
        if audit:
            audit.complete_batch(batch_id, success_count, status,
                                 f"{failed_count} records failed" if failed_count else None)

        logger.info(f"Successfully streamed {success_count} records to topic: {topic_name}")
        if failed_count > 0:
            logger.warning(f"  {failed_count} records failed to stream")

    except Exception as e:
        logger.error(f"Streaming failed: {e}")
        if audit:
            audit.complete_batch(batch_id, 0, 'FAILED', str(e))
        raise


def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description='Stream JNE data to Kafka')
    parser.add_argument('--limit', type=int, default=1000, help='Number of records to stream')
    parser.add_argument('--topic', type=str, default=DEFAULT_TOPIC, help='Kafka topic name')

    args = parser.parse_args()

    engine = create_engine(DB_CONN)
    audit = get_audit_logger(engine)

    try:
        stream_to_kafka(engine, audit, topic_name=args.topic, limit=args.limit)
        logger.info("Streaming completed successfully")
    except Exception as e:
        logger.error(f"Streaming failed: {e}")
        raise


if __name__ == '__main__':
    main()
