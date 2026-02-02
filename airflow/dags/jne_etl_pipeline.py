"""
JNE Data Pipeline - Airflow DAG with Audit Trail Integration
Orchestrates the complete ETL pipeline with automatic audit logging
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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
    description='Complete JNE data pipeline: Load → Transform → Stream with Audit Trail',
    schedule_interval='@daily',  # Run daily at midnight
    catchup=False,
    tags=['jne', 'etl', 'production', 'audit'],
)

# ============================================================
# STEP 1: DATA UNIFICATION - Load raw data to PostgreSQL
# ============================================================
# This automatically logs to audit trail via audit_logger.py

load_data = BashOperator(
    task_id='load_raw_data',
    bash_command='python /opt/airflow/scripts/etl/load_data.py --file /opt/airflow/data/raw/JNE_RAW_COMBINED.xlsx',
    dag=dag,
)

# ============================================================
# STEP 2: DATA STANDARDIZATION - Transform data with Pandas
# ============================================================
# TODO: Update transform_tables.py to use audit_logger

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='python /opt/airflow/scripts/transformations/transform_tables.py',
    dag=dag,
)

# ============================================================
# STEP 3: DATA REALTIME - Stream to Kafka
# ============================================================
# This automatically logs to audit trail via audit_logger.py

stream_to_kafka = BashOperator(
    task_id='stream_to_kafka',
    bash_command='python /opt/airflow/scripts/kafka/kafka_producer.py --limit 10000',
    dag=dag,
)

# ============================================================
# STEP 4: DATA QUALITY CHECK - Verify data integrity
# ============================================================

quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command='python /opt/airflow/scripts/etl/load_data.py --verify',
    dag=dag,
)

# ============================================================
# STEP 5: AUDIT HEALTH CHECK - Monitor pipeline status
# ============================================================

def check_audit_health(**context):
    """Check if any jobs failed in this DAG run"""
    from audit_logger import AuditLogger
    
    DB_CONN = "postgresql://jne_user:jne_secure_password_2024@jne-postgres:5432/jne_dashboard"
    audit = AuditLogger(DB_CONN)
    
    # Get recent job health
    health = audit.get_pipeline_health()
    
    # Check for failures in last hour
    failed_jobs = sum(
        stage.get('failed_jobs', 0) 
        for stage in health.get('pipeline_health', [])
    )
    
    if failed_jobs > 0:
        print(f"⚠️ WARNING: {failed_jobs} jobs failed in this pipeline run")
        # In production, send alert here (Slack, email, etc.)
    else:
        print("✓ All pipeline jobs completed successfully")
    
    # Push metrics to XCom for downstream tasks
    context['task_instance'].xcom_push(key='failed_jobs', value=failed_jobs)
    context['task_instance'].xcom_push(key='health_data', value=health)
    
    return health

audit_health = PythonOperator(
    task_id='check_audit_health',
    python_callable=check_audit_health,
    dag=dag,
)

# ============================================================
# STEP 6: GENERATE DAILY REPORT
# ============================================================

def generate_daily_report(**context):
    """Generate summary report from audit trail"""
    from sqlalchemy import create_engine, text
    
    DB_CONN = "postgresql://jne_user:jne_secure_password_2024@jne-postgres:5432/jne_dashboard"
    engine = create_engine(DB_CONN)
    
    report_lines = []
    report_lines.append("=" * 60)
    report_lines.append(f"JNE PIPELINE DAILY REPORT - {datetime.now().strftime('%Y-%m-%d')}")
    report_lines.append("=" * 60)
    
    # Get job statistics
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                pipeline_stage,
                COUNT(*) as total_jobs,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
                SUM(records_processed) as total_records
            FROM audit.etl_job_log
            WHERE DATE(start_time) = CURRENT_DATE
            GROUP BY pipeline_stage
            ORDER BY pipeline_stage
        """))
        
        report_lines.append("\nJOB SUMMARY BY STAGE:")
        for row in result:
            report_lines.append(f"  {row[0]}: {row[1]} jobs, {row[2]} success, {row[3]} failed, {row[4]} records")
    
    # Get quality summary
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_checks,
                SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
                SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed,
                ROUND(AVG(pass_percentage), 2) as avg_quality
            FROM audit.data_quality_log
            WHERE DATE(checked_at) = CURRENT_DATE
        """))
        
        row = result.fetchone()
        if row and row[0] > 0:
            report_lines.append("\nQUALITY CHECKS:")
            report_lines.append(f"  Total checks: {row[0]}")
            report_lines.append(f"  Passed: {row[1]}")
            report_lines.append(f"  Failed: {row[2]}")
            report_lines.append(f"  Average quality: {row[3]}%")
    
    report_lines.append("=" * 60)
    
    report = "\n".join(report_lines)
    print(report)
    
    # Save report to XCom
    context['task_instance'].xcom_push(key='daily_report', value=report)
    
    # TODO: In production, send this report via email/Slack
    
    return report

daily_report = PythonOperator(
    task_id='generate_daily_report',
    python_callable=generate_daily_report,
    dag=dag,
)

# ============================================================
# DEFINE PIPELINE FLOW
# ============================================================
# Sequential execution with audit checks

load_data >> transform_data >> stream_to_kafka >> quality_check >> audit_health >> daily_report

# Alternative: Parallel execution (if transform and stream are independent)
# load_data >> [transform_data, stream_to_kafka] >> quality_check >> audit_health >> daily_report
