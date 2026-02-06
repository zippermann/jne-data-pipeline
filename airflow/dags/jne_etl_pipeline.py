"""
JNE Data Pipeline - Airflow DAG with Audit Trail Integration
=============================================================
Orchestrates the complete ETL pipeline:
  load_raw_data → unify_tables → transform_data → stream_to_kafka

Updated: 2026-02-06
Changes:
  - load_raw_data now reads CSV files + Excel lookup tables (not a single .xlsx)
  - unify_tables is a new step that creates staging.unified_shipments
  - transform_data runs pandas transformations on raw tables
  - All paths updated to match new data layout
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add audit module to path
sys.path.append('/opt/airflow/scripts/audit')

default_args = {
    'owner': 'jne-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'jne_etl_pipeline',
    default_args=default_args,
    description='Complete JNE data pipeline: Load → Unify → Transform → Stream with Audit Trail',
    schedule_interval='@daily',
    catchup=False,
    tags=['jne', 'etl', 'production', 'audit'],
)

# ============================================================
# STEP 1: LOAD RAW DATA
# ============================================================
# Loads CSV files from data/raw/csv/ into raw schema tables
# Loads Excel lookup tables (ORA_ZONE, ORA_USER, T_MDT_CITY_ORIGIN) from the .xlsx file
# Configuration: pipeline_config.py

load_data = BashOperator(
    task_id='load_raw_data',
    bash_command=(
        'python /opt/airflow/scripts/etl/load_data.py '
        '--csv-dir /opt/airflow/data/raw/csv '
        '--excel-file /opt/airflow/data/raw/JNE_RAW_COMBINED.xlsx'
    ),
    dag=dag,
)

# ============================================================
# STEP 2: UNIFY TABLES → staging.unified_shipments
# ============================================================
# Runs the unification SQL that joins all 36 raw tables into one.
# The SQL file is configurable in pipeline_config.py.
# To change unification logic, edit the SQL file — no DAG change needed.

unify_tables = BashOperator(
    task_id='unify_tables',
    bash_command=(
        'python /opt/airflow/scripts/transformations/transform_tables.py '
        '--unification-only'
    ),
    dag=dag,
)

# ============================================================
# STEP 3: TRANSFORM DATA (Pandas)
# ============================================================
# Runs registered transformations from TRANSFORMATION_REGISTRY.
# Skips unification (already done in step 2) and only runs pandas transforms.

transform_data = BashOperator(
    task_id='transform_data',
    bash_command=(
        'python /opt/airflow/scripts/transformations/transform_tables.py '
        '--skip-unification'
    ),
    dag=dag,
)

# ============================================================
# STEP 4: STREAM TO KAFKA
# ============================================================
# Streams processed data to Kafka for real-time consumers.

stream_to_kafka = BashOperator(
    task_id='stream_to_kafka',
    bash_command='python /opt/airflow/scripts/kafka/kafka_producer.py --limit 10000',
    dag=dag,
)

# ============================================================
# STEP 5: AUDIT HEALTH CHECK
# ============================================================
# Runs audit trail health checks after the pipeline completes.

audit_check = BashOperator(
    task_id='audit_health_check',
    bash_command=(
        'python /opt/airflow/scripts/etl/load_data.py --verify'
    ),
    dag=dag,
)

# ============================================================
# TASK DEPENDENCIES
# ============================================================
# load_raw_data → unify_tables → transform_data → stream_to_kafka → audit_check

load_data >> unify_tables >> transform_data >> stream_to_kafka >> audit_check
