"""
JNE Audit Trail Monitoring DAG
Monitors pipeline health, data quality, and generates audit reports
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import sys
from pathlib import Path

# Add audit module to path
sys.path.append('/opt/airflow/scripts/audit')

default_args = {
    'owner': 'jne-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'jne_audit_monitoring',
    default_args=default_args,
    description='Monitor audit trail and generate reports',
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
    tags=['jne', 'audit', 'monitoring'],
)


def check_pipeline_health(**context):
    """Check overall pipeline health"""
    from audit_logger import AuditLogger
    
    DB_CONN = "postgresql://jne_user:jne_secure_password_2024@jne-postgres:5432/jne_dashboard"
    audit = AuditLogger(DB_CONN)
    
    health = audit.get_pipeline_health()
    
    # Push to XCom for downstream tasks
    context['task_instance'].xcom_push(key='pipeline_health', value=health)
    
    # Check for failures
    for stage in health.get('pipeline_health', []):
        if stage['failed_jobs'] > 0:
            print(f"⚠️ WARNING: {stage['pipeline_stage']} has {stage['failed_jobs']} failed jobs")
    
    return health


def check_data_quality(**context):
    """Check data quality scorecard"""
    from audit_logger import AuditLogger
    
    DB_CONN = "postgresql://jne_user:jne_secure_password_2024@jne-postgres:5432/jne_dashboard"
    audit = AuditLogger(DB_CONN)
    
    scorecard = audit.get_data_quality_scorecard()
    
    # Push to XCom
    context['task_instance'].xcom_push(key='quality_scorecard', value=scorecard)
    
    # Check for quality issues
    for table_check in scorecard.get('quality_scorecard', []):
        if table_check['failed_checks'] > 0:
            print(f"⚠️ WARNING: {table_check['table_name']} has {table_check['failed_checks']} failed quality checks")
        
        if table_check['avg_pass_percentage'] and table_check['avg_pass_percentage'] < 95.0:
            print(f"⚠️ WARNING: {table_check['table_name']} quality below 95% ({table_check['avg_pass_percentage']:.2f}%)")
    
    return scorecard


def generate_audit_report(**context):
    """Generate comprehensive audit report"""
    from sqlalchemy import create_engine, text
    
    DB_CONN = "postgresql://jne_user:jne_secure_password_2024@jne-postgres:5432/jne_dashboard"
    engine = create_engine(DB_CONN)
    
    report = {
        'report_timestamp': datetime.now().isoformat(),
        'report_period': '24_hours'
    }
    
    # Get job statistics for last 24 hours
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_jobs,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
                SUM(records_processed) as total_records,
                AVG(duration_seconds) as avg_duration
            FROM audit.etl_job_log
            WHERE start_time >= NOW() - INTERVAL '24 hours'
        """))
        
        row = result.fetchone()
        report['job_statistics'] = {
            'total_jobs': row[0],
            'successful': row[1],
            'failed': row[2],
            'total_records': row[3],
            'avg_duration_seconds': float(row[4]) if row[4] else 0
        }
    
    # Get quality check statistics
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_checks,
                SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
                SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed,
                AVG(pass_percentage) as avg_pass_rate
            FROM audit.data_quality_log
            WHERE checked_at >= NOW() - INTERVAL '24 hours'
        """))
        
        row = result.fetchone()
        report['quality_statistics'] = {
            'total_checks': row[0],
            'passed': row[1],
            'failed': row[2],
            'avg_pass_rate': float(row[3]) if row[3] else 0
        }
    
    # Save report
    report_json = json.dumps(report, indent=2)
    print("=" * 60)
    print("AUDIT REPORT - Last 24 Hours")
    print("=" * 60)
    print(report_json)
    print("=" * 60)
    
    # Push to XCom
    context['task_instance'].xcom_push(key='audit_report', value=report)
    
    return report


def alert_on_failures(**context):
    """Send alerts if critical failures detected"""
    pipeline_health = context['task_instance'].xcom_pull(
        key='pipeline_health',
        task_ids='check_pipeline_health'
    )
    
    quality_scorecard = context['task_instance'].xcom_pull(
        key='quality_scorecard',
        task_ids='check_data_quality'
    )
    
    alerts = []
    
    # Check for job failures
    for stage in pipeline_health.get('pipeline_health', []):
        if stage['failed_jobs'] > 0:
            alerts.append({
                'severity': 'HIGH',
                'type': 'JOB_FAILURE',
                'message': f"{stage['pipeline_stage']} has {stage['failed_jobs']} failed jobs"
            })
    
    # Check for quality issues
    for table_check in quality_scorecard.get('quality_scorecard', []):
        if table_check['avg_pass_percentage'] and table_check['avg_pass_percentage'] < 90.0:
            alerts.append({
                'severity': 'MEDIUM',
                'type': 'DATA_QUALITY',
                'message': f"{table_check['table_name']} quality at {table_check['avg_pass_percentage']:.2f}%"
            })
    
    if alerts:
        print("🚨 ALERTS DETECTED:")
        for alert in alerts:
            print(f"  [{alert['severity']}] {alert['type']}: {alert['message']}")
        
        # In production, send to Slack/email/etc
        # For now, just log
        context['task_instance'].xcom_push(key='alerts', value=alerts)
    else:
        print("✓ No alerts - all systems healthy")
    
    return alerts


# Define tasks
task_check_health = PythonOperator(
    task_id='check_pipeline_health',
    python_callable=check_pipeline_health,
    dag=dag,
)

task_check_quality = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag,
)

task_generate_report = PythonOperator(
    task_id='generate_audit_report',
    python_callable=generate_audit_report,
    dag=dag,
)

task_alert = PythonOperator(
    task_id='alert_on_failures',
    python_callable=alert_on_failures,
    dag=dag,
)

# Clean up old audit logs (keep last 90 days)
task_cleanup = BashOperator(
    task_id='cleanup_old_logs',
    bash_command="""
    psql -h jne-postgres -U jne_user -d jne_dashboard -c "
        DELETE FROM audit.etl_job_log WHERE start_time < NOW() - INTERVAL '90 days';
        DELETE FROM audit.data_quality_log WHERE checked_at < NOW() - INTERVAL '90 days';
        DELETE FROM audit.data_lineage WHERE processed_at < NOW() - INTERVAL '90 days';
    "
    """,
    env={'PGPASSWORD': 'jne_secure_password_2024'},
    dag=dag,
)

# Define task dependencies
[task_check_health, task_check_quality] >> task_generate_report >> task_alert >> task_cleanup
