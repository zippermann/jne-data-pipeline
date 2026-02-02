"""
JNE Kafka Producer with Audit Trail
Streams transformed data to Kafka with comprehensive audit logging
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

# Add audit module to path
sys.path.append(str(Path(__file__).parent.parent / 'audit'))
from audit_logger import AuditLogger, AuditedJob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = 'jne-kafka:29092'
DEFAULT_TOPIC = 'jne-shipments'
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


def stream_to_kafka(audit_logger: AuditLogger, topic_name: str, limit: int = 1000):
    """
    Stream transformed data to Kafka with audit logging
    
    Args:
        audit_logger: AuditLogger instance
        topic_name: Kafka topic name
        limit: Number of records to stream (default 1000)
    """
    
    with AuditedJob(
        audit_logger,
        "Stream to Kafka",
        "STREAM",
        "DATA_REALTIME",
        parameters={
            'kafka_broker': KAFKA_BROKER,
            'topic_name': topic_name,
            'record_limit': limit
        }
    ) as job:
        
        logger.info("Connecting to Kafka...")
        producer = create_producer()
        
        logger.info("Connecting to database...")
        engine = create_engine(DB_CONN)
        
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
                
            if not table_exists:
                logger.warning("transformed.cms_cnote table does not exist, using raw.cms_cnote")
                source_table = "raw.cms_cnote"
            else:
                source_table = "transformed.cms_cnote"
                
        except Exception as e:
            logger.warning(f"Could not check for transformed table: {e}")
            source_table = "raw.cms_cnote"
        
        # Load data from database
        try:
            query = f"SELECT * FROM {source_table} LIMIT {limit}"
            logger.info(f"Loading data from {source_table}...")
            df = pd.read_sql(query, engine)
            logger.info(f"Loaded {len(df)} records")
            
            job.increment_processed(len(df))
            
        except Exception as e:
            logger.error(f"Failed to read from {source_table}: {e}")
            job.set_status('FAILED')
            raise
        
        if len(df) == 0:
            logger.warning("No data to stream - table is empty")
            job.set_status('PARTIAL')
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
                job.increment_success(1)
                
                if (idx + 1) % 100 == 0:
                    logger.info(f"  Sent {idx + 1} messages...")
                    
            except Exception as e:
                logger.error(f"Failed to send record {idx}: {e}")
                failed_count += 1
                job.increment_failed(1)
        
        # Flush and close producer
        producer.flush()
        producer.close()
        
        # Log data lineage
        audit_logger.log_lineage(
            source_table=source_table,
            target_table=f"KAFKA:{topic_name}",
            operation_type="STREAM",
            record_count=success_count,
            transformation_name="Database_To_Kafka_Stream",
            metadata={
                'kafka_broker': KAFKA_BROKER,
                'topic_name': topic_name,
                'success_count': success_count,
                'failed_count': failed_count
            }
        )
        
        # Log quality check
        audit_logger.log_quality_check(
            job_log_id=job.job_log_id,
            check_name="Kafka Stream Success Rate",
            check_type="ACCURACY",
            table_name=source_table,
            records_checked=len(df),
            records_passed=success_count,
            records_failed=failed_count,
            status='PASS' if failed_count == 0 else 'PARTIAL',
            actual_value=f"{(success_count/len(df)*100):.2f}% success rate"
        )
        
        logger.info(f"✓ Successfully streamed {success_count} records to topic: {topic_name}")
        if failed_count > 0:
            logger.warning(f"  {failed_count} records failed to stream")
            job.set_status('PARTIAL')


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Stream JNE data to Kafka with audit trail')
    parser.add_argument('--limit', type=int, default=1000, help='Number of records to stream')
    parser.add_argument('--topic', type=str, default=DEFAULT_TOPIC, help='Kafka topic name')
    
    args = parser.parse_args()
    
    # Initialize audit logger
    audit_logger = AuditLogger(DB_CONN)
    logger.info("Audit logger initialized")
    
    try:
        stream_to_kafka(audit_logger, topic_name=args.topic, limit=args.limit)
        logger.info("✓ Streaming completed successfully")
        
        # Print audit summary
        logger.info("\nAudit Trail Summary:")
        health = audit_logger.get_pipeline_health()
        logger.info(f"Pipeline Health: {health}")
        
    except Exception as e:
        logger.error(f"✗ Streaming failed: {e}")
        raise


if __name__ == '__main__':
    main()
