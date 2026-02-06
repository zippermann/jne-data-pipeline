"""
JNE Data Pipeline - Airflow DAG with Audit Trail & Shipment Tracking
Orchestrates the complete ETL pipeline with automatic audit logging and shipment position tracking

Pipeline Flow:
  load_raw_data → transform_data → track_shipments → stream_to_kafka → quality_check → audit_health → daily_report
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
    description='Complete JNE data pipeline: Load → Transform → Track → Stream with Audit Trail',
    schedule_interval='@daily',
    catchup=False,
    tags=['jne', 'etl', 'production', 'audit', 'tracking'],
)

# ============================================================
# STEP 1: DATA UNIFICATION - Load raw data to PostgreSQL
# ============================================================

load_data = BashOperator(
    task_id='load_raw_data',
    bash_command='python /opt/airflow/scripts/etl/load_data.py --file /opt/airflow/data/raw/JNE_RAW_COMBINED.xlsx',
    dag=dag,
)

# ============================================================
# STEP 2: DATA STANDARDIZATION - Transform data with Pandas
# ============================================================

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='python /opt/airflow/scripts/transformations/transform_tables.py',
    dag=dag,
)

# ============================================================
# STEP 3: SHIPMENT TRACKING - Track package positions
# ============================================================
# Based on JNE Package Journey: Shipper → Receiving → Manifest → Gateway → Destination → Runsheet → POD

def track_shipments_from_tables(**context):
    """
    Extract and log shipment status changes from various source tables.
    Maps to JNE Package Journey stages:
    - CMS_APICUST: Order Created
    - CMS_CNOTE: Cnote Created (Pickup/Drop-off)
    - CMS_MRCNOTE/DRCNOTE: Received at Facility
    - CMS_MANIFEST/MFCNOTE: Manifested (Transit)
    - CMS_MHOCNOTE/DHOCNOTE: Handover at Destination
    - CMS_MRSHEET/DRSHEET: On Runsheet (Out for Delivery)
    - CMS_CNOTE_POD: Delivered (POD)
    - CMS_DSTATUS: Status Updates
    """
    from sqlalchemy import create_engine, text
    import json
    
    DB_CONN = "postgresql://jne_user:jne_secure_password_2024@jne-postgres:5432/jne_dashboard"
    engine = create_engine(DB_CONN)
    
    stats = {
        'total_tracked': 0,
        'by_stage': {},
        'errors': []
    }
    
    # Define tracking sources based on JNE Package Journey
    # Format: (source_table, awb_column, status_value, location_column, date_column)
    # Flow: Origin → Outbound → Transit → Inbound → Destination → Delivery
    TRACKING_SOURCES = [
        # 1. Order Created (from Marketplace)
        ('cms_apicust', 'apicust_cnote_no', 'Order Created', 'apicust_branch_id', 'apicust_cdate'),
        
        # 2. Cnote Created (Pickup/Drop-off happened)
        ('cms_cnote', 'cnote_no', 'Cnote Created', 'cnote_branch_id', 'cnote_cdate'),
        
        # 3. Received at Origin Facility
        ('cms_drcnote', 'drcnote_cnote_no', 'Received at Facility', 'drcnote_branch_id', 'drcnote_rdate'),
        
        # 4. Handover Outbound (Leaving Origin Hub)
        ('cms_dhocnote', 'dhocnote_cnote_no', 'Handover Outbound', 'dhocnote_branch_id', 'dhocnote_tdate'),
        
        # 5. Manifested (In Transit)
        ('cms_mfcnote', 'mfcnote_cnote_no', 'Manifested', 'mfcnote_branch_id', 'mfcnote_man_date'),
        
        # 6. Handover Inbound (Arrived at Destination Hub)
        ('cms_dhicnote', 'dhicnote_cnote_no', 'Handover Inbound', 'dhicnote_branch_id', 'dhicnote_tdate'),
        
        # 7. On Delivery Runsheet (Out for Delivery)
        ('cms_drsheet', 'drsheet_cnote_no', 'On Delivery Runsheet', 'drsheet_branch_id', 'drsheet_date'),
        
        # 8. Proof of Delivery (Delivered)
        ('cms_cnote_pod', 'pod_cnote_no', 'Delivered (POD)', 'pod_branch', 'pod_date'),
    ]
    
    with engine.begin() as conn:
        # Ensure tracking table exists
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS audit.shipment_tracking (
                log_id SERIAL PRIMARY KEY,
                sequence_lsn BIGINT GENERATED ALWAYS AS IDENTITY,
                awb_number VARCHAR(50) NOT NULL,
                status_before VARCHAR(100),
                status_after VARCHAR(100) NOT NULL,
                system_action VARCHAR(100) NOT NULL,
                source_table VARCHAR(100),
                source_record_id VARCHAR(100),
                location_code VARCHAR(50),
                location_name VARCHAR(200),
                captured_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                captured_by VARCHAR(100) DEFAULT CURRENT_USER,
                metadata JSONB
            )
        """))
        
        # Create index if not exists
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_shipment_tracking_awb 
            ON audit.shipment_tracking(awb_number)
        """))
        
        for source_table, awb_col, status, loc_col, date_col in TRACKING_SOURCES:
            try:
                # Check if table exists
                check_query = text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'raw' AND table_name = '{source_table}'
                    )
                """)
                if not conn.execute(check_query).scalar():
                    print(f"  ⚠ Table raw.{source_table} does not exist, skipping")
                    continue
                
                # Check if columns exist
                col_check = text(f"""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'raw' AND table_name = '{source_table}'
                    AND column_name IN ('{awb_col}', '{loc_col}')
                """)
                existing_cols = [row[0] for row in conn.execute(col_check)]
                
                if awb_col not in existing_cols:
                    print(f"  ⚠ Column {awb_col} not found in {source_table}, skipping")
                    continue
                
                # Build dynamic query based on available columns
                loc_select = f"src.{loc_col}" if loc_col in existing_cols else "NULL"
                
                # Insert new tracking records (only for AWBs not already tracked with this status)
                insert_query = text(f"""
                    INSERT INTO audit.shipment_tracking 
                        (awb_number, status_after, system_action, source_table, location_code, metadata)
                    SELECT DISTINCT
                        src.{awb_col}::VARCHAR,
                        '{status}',
                        'Airflow: Batch Tracking',
                        'raw.{source_table}',
                        {loc_select}::VARCHAR,
                        jsonb_build_object(
                            'airflow_run', :run_id,
                            'processed_at', NOW()::TEXT
                        )
                    FROM raw.{source_table} src
                    WHERE src.{awb_col} IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM audit.shipment_tracking st
                        WHERE st.awb_number = src.{awb_col}::VARCHAR
                        AND st.status_after = '{status}'
                        AND st.source_table = 'raw.{source_table}'
                    )
                    LIMIT 5000
                """)
                
                result = conn.execute(insert_query, {
                    'run_id': context.get('run_id', 'manual')
                })
                
                count = result.rowcount
                stats['by_stage'][status] = count
                stats['total_tracked'] += count
                
                if count > 0:
                    print(f"  ✓ {source_table}: Tracked {count} shipments as '{status}'")
                    
            except Exception as e:
                error_msg = f"{source_table}: {str(e)}"
                stats['errors'].append(error_msg)
                print(f"  ✗ Error tracking from {source_table}: {e}")
    
    # Push stats to XCom
    context['task_instance'].xcom_push(key='tracking_stats', value=stats)
    
    print(f"\n📦 Shipment Tracking Complete:")
    print(f"  Total tracked: {stats['total_tracked']}")
    for stage, count in stats['by_stage'].items():
        print(f"    - {stage}: {count}")
    if stats['errors']:
        print(f"  Errors: {len(stats['errors'])}")
    
    return stats


track_shipments = PythonOperator(
    task_id='track_shipments',
    python_callable=track_shipments_from_tables,
    dag=dag,
)

# ============================================================
# STEP 4: DATA REALTIME - Stream to Kafka
# ============================================================

stream_to_kafka = BashOperator(
    task_id='stream_to_kafka',
    bash_command='python /opt/airflow/scripts/kafka/kafka_producer.py --limit 10000',
    dag=dag,
)

# ============================================================
# STEP 5: DATA QUALITY CHECK - Verify data integrity
# ============================================================

quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command='python /opt/airflow/scripts/etl/load_data.py --verify',
    dag=dag,
)

# ============================================================
# STEP 6: AUDIT HEALTH CHECK - Monitor pipeline status
# ============================================================

def check_audit_health(**context):
    """Check if any jobs failed in this DAG run"""
    from sqlalchemy import create_engine, text
    
    DB_CONN = "postgresql://jne_user:jne_secure_password_2024@jne-postgres:5432/jne_dashboard"
    engine = create_engine(DB_CONN)
    
    health_data = {'pipeline_health': [], 'tracking_summary': {}}
    
    # Get job health
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    pipeline_stage,
                    COUNT(*) as total_jobs,
                    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
                    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed
                FROM audit.job_log
                WHERE start_time >= CURRENT_DATE
                GROUP BY pipeline_stage
            """))
            
            for row in result:
                health_data['pipeline_health'].append({
                    'stage': row[0],
                    'total': row[1],
                    'success': row[2],
                    'failed': row[3]
                })
    except Exception as e:
        print(f"Could not get job health: {e}")
    
    # Get tracking summary
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    status_after,
                    COUNT(*) as count,
                    COUNT(DISTINCT awb_number) as unique_awbs
                FROM audit.shipment_tracking
                WHERE captured_at >= CURRENT_DATE
                GROUP BY status_after
                ORDER BY count DESC
            """))
            
            for row in result:
                health_data['tracking_summary'][row[0]] = {
                    'events': row[1],
                    'unique_awbs': row[2]
                }
    except Exception as e:
        print(f"Could not get tracking summary: {e}")
    
    # Check for failures
    failed_jobs = sum(
        stage.get('failed', 0) 
        for stage in health_data.get('pipeline_health', [])
    )
    
    if failed_jobs > 0:
        print(f"⚠️ WARNING: {failed_jobs} jobs failed in this pipeline run")
    else:
        print("✓ All pipeline jobs completed successfully")
    
    print("\n📊 Tracking Summary (Today):")
    for status, data in health_data.get('tracking_summary', {}).items():
        print(f"  {status}: {data['events']} events, {data['unique_awbs']} AWBs")
    
    context['task_instance'].xcom_push(key='health_data', value=health_data)
    
    return health_data


audit_health = PythonOperator(
    task_id='check_audit_health',
    python_callable=check_audit_health,
    dag=dag,
)

# ============================================================
# STEP 7: GENERATE DAILY REPORT
# ============================================================

def generate_daily_report(**context):
    """Generate summary report from audit trail including shipment tracking"""
    from sqlalchemy import create_engine, text
    
    DB_CONN = "postgresql://jne_user:jne_secure_password_2024@jne-postgres:5432/jne_dashboard"
    engine = create_engine(DB_CONN)
    
    report_lines = []
    report_lines.append("=" * 60)
    report_lines.append(f"JNE PIPELINE DAILY REPORT - {datetime.now().strftime('%Y-%m-%d')}")
    report_lines.append("=" * 60)
    
    # Get job statistics
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    pipeline_stage,
                    COUNT(*) as total_jobs,
                    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
                    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
                    COALESCE(SUM(records_processed), 0) as total_records
                FROM audit.job_log
                WHERE DATE(start_time) = CURRENT_DATE
                GROUP BY pipeline_stage
                ORDER BY pipeline_stage
            """))
            
            report_lines.append("\nJOB SUMMARY BY STAGE:")
            for row in result:
                report_lines.append(f"  {row[0]}: {row[1]} jobs, {row[2]} success, {row[3]} failed, {row[4]} records")
    except Exception as e:
        report_lines.append(f"\nJob summary unavailable: {e}")
    
    # Get shipment tracking summary
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    status_after,
                    COUNT(*) as events,
                    COUNT(DISTINCT awb_number) as unique_shipments
                FROM audit.shipment_tracking
                WHERE DATE(captured_at) = CURRENT_DATE
                GROUP BY status_after
                ORDER BY events DESC
            """))
            
            report_lines.append("\nSHIPMENT TRACKING SUMMARY (Today):")
            total_events = 0
            total_shipments = set()
            for row in result:
                report_lines.append(f"  {row[0]}: {row[1]} events, {row[2]} shipments")
                total_events += row[1]
            
            # Get total unique shipments tracked today
            result = conn.execute(text("""
                SELECT COUNT(DISTINCT awb_number) 
                FROM audit.shipment_tracking 
                WHERE DATE(captured_at) = CURRENT_DATE
            """))
            unique_today = result.scalar()
            report_lines.append(f"  TOTAL: {total_events} events, {unique_today} unique shipments")
            
    except Exception as e:
        report_lines.append(f"\nTracking summary unavailable: {e}")
    
    # Get quality summary
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_checks,
                    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
                    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed
                FROM audit.data_quality_log
                WHERE DATE(check_time) = CURRENT_DATE
            """))
            
            row = result.fetchone()
            if row and row[0] > 0:
                report_lines.append("\nQUALITY CHECKS:")
                report_lines.append(f"  Total checks: {row[0]}")
                report_lines.append(f"  Passed: {row[1]}")
                report_lines.append(f"  Failed: {row[2]}")
    except Exception as e:
        pass  # Quality table may not exist
    
    report_lines.append("\n" + "=" * 60)
    
    report = "\n".join(report_lines)
    print(report)
    
    context['task_instance'].xcom_push(key='daily_report', value=report)
    
    return report


daily_report = PythonOperator(
    task_id='generate_daily_report',
    python_callable=generate_daily_report,
    dag=dag,
)

# ============================================================
# DEFINE PIPELINE FLOW
# ============================================================
# load → transform → track shipments → stream → quality → audit → report

load_data >> transform_data >> track_shipments >> stream_to_kafka >> quality_check >> audit_health >> daily_report
