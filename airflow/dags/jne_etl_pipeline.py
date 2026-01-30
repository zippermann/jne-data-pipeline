"""
JNE Data Pipeline - Airflow DAG
Orchestrates the complete ETL pipeline
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'jne-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'jne_etl_pipeline',
    default_args=default_args,
    description='Complete JNE data pipeline: Load → Transform → Stream',
    schedule_interval='@daily',  # Run daily at midnight
    catchup=False,
    tags=['jne', 'etl', 'production'],
)

# Step 1: Load raw data to PostgreSQL
load_data = BashOperator(
    task_id='load_raw_data',
    bash_command='python /opt/airflow/scripts/etl/load_data.py --file /opt/airflow/data/raw/JNE_RAW_COMBINED.xlsx',
    dag=dag,
)

# Step 2: Transform data with Pandas
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='python /opt/airflow/scripts/transformations/transform_tables.py',
    dag=dag,
)

# Step 3: Stream to Kafka (optional, for real-time)
stream_to_kafka = BashOperator(
    task_id='stream_to_kafka',
    bash_command='python /opt/airflow/scripts/kafka/kafka_producer.py',
    dag=dag,
)

# Step 4: Data quality checks
quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command='echo "Running data quality checks..." && python /opt/airflow/scripts/etl/load_data.py --verify',
    dag=dag,
)

# Define pipeline flow
load_data >> transform_data >> stream_to_kafka >> quality_check