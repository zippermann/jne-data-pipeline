"""
JNE Audit Trail Monitoring DAG
Monitors pipeline health and recent failures using the audit schema.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import sys

sys.path.append('/opt/airflow/scripts/audit')
sys.path.append('/opt/airflow')

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
    schedule_interval='@hourly',
    catchup=False,
    tags=['jne', 'audit', 'monitoring'],
)


def check_pipeline_health(**context):
    """Check overall pipeline health from audit.v_pipeline_health."""
    from sqlalchemy import create_engine
    from audit_logger import AuditLogger

    from pipeline_config import DB_CONN
    engine = create_engine(DB_CONN)
    audit = AuditLogger(engine)

    health = audit.get_pipeline_health()
    context['task_instance'].xcom_push(key='pipeline_health', value=health)

    for stage in health:
        if stage.get('failed', 0) > 0:
            print(f"WARNING: {stage['pipeline_stage']} has {stage['failed']} failed batches")

    return health


def check_recent_failures(**context):
    """Check recent batch failures from audit.v_recent_failures."""
    from sqlalchemy import create_engine
    from audit_logger import AuditLogger

    from pipeline_config import DB_CONN
    engine = create_engine(DB_CONN)
    audit = AuditLogger(engine)

    failures = audit.get_recent_failures()
    context['task_instance'].xcom_push(key='recent_failures', value=failures)

    if failures:
        print(f"WARNING: {len(failures)} recent failures detected")
        for f in failures[:5]:
            print(f"  {f.get('batch_id')}: {f.get('application')} — {f.get('error_details', 'no details')}")
    else:
        print("No recent failures")

    return failures


def generate_audit_report(**context):
    """Generate audit report from the 3 deliverable views."""
    from sqlalchemy import create_engine, text
    from pipeline_config import DB_CONN

    engine = create_engine(DB_CONN)
    report = {'report_timestamp': datetime.now().isoformat()}

    with engine.connect() as conn:
        # Batch summary
        result = conn.execute(text("""
            SELECT
                COUNT(*) AS total_batches,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
                SUM(record_count_in) AS total_records_in,
                SUM(record_count_out) AS total_records_out,
                ROUND(AVG(duration_seconds)::NUMERIC, 2) AS avg_duration
            FROM audit.batch_log
            WHERE started_at >= NOW() - INTERVAL '24 hours'
        """))
        row = result.fetchone()
        report['batch_statistics'] = {
            'total_batches': row[0],
            'successful': row[1],
            'failed': row[2],
            'total_records_in': row[3],
            'total_records_out': row[4],
            'avg_duration_seconds': float(row[5]) if row[5] else 0,
        }

        # Traceability count
        result = conn.execute(text("""
            SELECT COUNT(*) FROM audit.data_traceability
            WHERE event_timestamp >= NOW() - INTERVAL '24 hours'
        """))
        report['traceability_entries_24h'] = result.fetchone()[0]

        # Change log count
        result = conn.execute(text("""
            SELECT COUNT(*) FROM audit.change_log
            WHERE captured_at >= NOW() - INTERVAL '24 hours'
        """))
        report['change_log_entries_24h'] = result.fetchone()[0]

    report_json = json.dumps(report, indent=2, default=str)
    print("=" * 60)
    print("AUDIT REPORT — Last 24 Hours")
    print("=" * 60)
    print(report_json)
    print("=" * 60)

    context['task_instance'].xcom_push(key='audit_report', value=report)
    return report


def alert_on_failures(**context):
    """Alert if critical failures detected."""
    health = context['task_instance'].xcom_pull(
        key='pipeline_health', task_ids='check_pipeline_health') or []
    failures = context['task_instance'].xcom_pull(
        key='recent_failures', task_ids='check_recent_failures') or []

    alerts = []

    for stage in health:
        failed = stage.get('failed', 0)
        if failed and failed > 0:
            alerts.append({
                'severity': 'HIGH',
                'type': 'BATCH_FAILURE',
                'message': f"{stage['pipeline_stage']}: {failed} failed batches",
            })

    if len(failures) > 5:
        alerts.append({
            'severity': 'MEDIUM',
            'type': 'FAILURE_SPIKE',
            'message': f"{len(failures)} recent failures in the last batch window",
        })

    if alerts:
        print("ALERTS DETECTED:")
        for alert in alerts:
            print(f"  [{alert['severity']}] {alert['type']}: {alert['message']}")
        context['task_instance'].xcom_push(key='alerts', value=alerts)
    else:
        print("No alerts — all systems healthy")

    return alerts


# Define tasks
task_check_health = PythonOperator(
    task_id='check_pipeline_health',
    python_callable=check_pipeline_health,
    dag=dag,
)

task_check_failures = PythonOperator(
    task_id='check_recent_failures',
    python_callable=check_recent_failures,
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
        DELETE FROM audit.data_traceability WHERE event_timestamp < NOW() - INTERVAL '90 days';
        DELETE FROM audit.batch_log WHERE started_at < NOW() - INTERVAL '90 days';
        DELETE FROM audit.transformation_log WHERE timestamp < NOW() - INTERVAL '90 days';
    "
    """,
    env={'PGPASSWORD': 'jne_secure_password_2024'},
    dag=dag,
)

# Task dependencies
[task_check_health, task_check_failures] >> task_generate_report >> task_alert >> task_cleanup
