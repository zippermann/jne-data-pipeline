"""
JNE CSV Data Loader with Full Audit Trail Integration - v3
Loads multiple CSV files to PostgreSQL with comprehensive audit logging

Features:
- Integrates with existing AuditLogger and AuditedJob
- Drops FK constraints before loading
- Handles malformed CSV rows
- Preserves reference tables
- Logs shipment status changes for tracking

Usage:
    python load_csv_data_v3.py --csv-dir /path/to/csv/files
    python load_csv_data_v3.py --csv-dir /path/to/csv/files --host localhost
"""

import sys
import os
import argparse
import glob
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime
from pathlib import Path
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Tables to preserve (reference tables not in new data)
PRESERVE_TABLES = ['ora_zone', 'ora_user', 'mdt_city_origin', 'crossdock']

# Default connection (Docker internal)
DEFAULT_DB_CONN = "postgresql://jne_user:jne_secure_password_2024@jne-postgres:5432/jne_dashboard"


# ============================================================
# AUDIT LOGGER CLASSES (embedded for standalone use)
# ============================================================

class AuditLogger:
    """Audit logging system for CSV loader"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.engine = create_engine(connection_string)
        logger.info("AuditLogger initialized")
    
    def start_job(self, job_name: str, job_type: str, pipeline_stage: str, 
                  parameters: dict = None) -> int:
        """Start a new job and return job_log_id"""
        try:
            query = text("""
                INSERT INTO audit.job_log (
                    job_name, job_type, pipeline_stage, status, 
                    start_time, parameters
                ) VALUES (
                    :job_name, :job_type, :pipeline_stage, 'RUNNING',
                    CURRENT_TIMESTAMP, :parameters
                ) RETURNING job_log_id
            """)
            
            with self.engine.begin() as conn:
                result = conn.execute(query, {
                    'job_name': job_name,
                    'job_type': job_type,
                    'pipeline_stage': pipeline_stage,
                    'parameters': json.dumps(parameters) if parameters else None
                })
                job_log_id = result.fetchone()[0]
                logger.info(f"Started job {job_log_id}: {job_name}")
                return job_log_id
        except Exception as e:
            logger.warning(f"Could not log job start (audit schema may not exist): {e}")
            return None
    
    def complete_job(self, job_log_id: int, status: str, records_processed: int,
                     records_success: int, records_failed: int,
                     error_message: str = None, error_details: dict = None):
        """Complete a job with final statistics"""
        if job_log_id is None:
            return
        
        try:
            query = text("""
                UPDATE audit.job_log SET
                    status = :status,
                    end_time = CURRENT_TIMESTAMP,
                    records_processed = :records_processed,
                    records_success = :records_success,
                    records_failed = :records_failed,
                    error_message = :error_message,
                    error_details = :error_details
                WHERE job_log_id = :job_log_id
            """)
            
            with self.engine.begin() as conn:
                conn.execute(query, {
                    'job_log_id': job_log_id,
                    'status': status,
                    'records_processed': records_processed,
                    'records_success': records_success,
                    'records_failed': records_failed,
                    'error_message': error_message,
                    'error_details': json.dumps(error_details) if error_details else None
                })
                logger.info(f"Completed job {job_log_id} with status: {status}")
        except Exception as e:
            logger.warning(f"Could not log job completion: {e}")
    
    def log_lineage(self, source_table: str, target_table: str, operation_type: str,
                    record_count: int = None, transformation_name: str = None,
                    metadata: dict = None) -> int:
        """Log data lineage"""
        try:
            query = text("""
                INSERT INTO audit.data_lineage (
                    source_table, target_table, operation_type,
                    record_count, transformation_name, metadata
                ) VALUES (
                    :source_table, :target_table, :operation_type,
                    :record_count, :transformation_name, :metadata
                ) RETURNING lineage_id
            """)
            
            with self.engine.begin() as conn:
                result = conn.execute(query, {
                    'source_table': source_table,
                    'target_table': target_table,
                    'operation_type': operation_type,
                    'record_count': record_count,
                    'transformation_name': transformation_name,
                    'metadata': json.dumps(metadata) if metadata else None
                })
                return result.fetchone()[0]
        except Exception as e:
            logger.warning(f"Could not log lineage: {e}")
            return None
    
    def log_quality_check(self, job_log_id: int, check_name: str, check_type: str,
                          table_name: str, records_checked: int, records_passed: int,
                          records_failed: int, status: str, actual_value: str = None):
        """Log data quality check results"""
        if job_log_id is None:
            return
        
        try:
            query = text("""
                INSERT INTO audit.data_quality_log (
                    job_log_id, check_name, check_type, table_name,
                    records_checked, records_passed, records_failed,
                    status, actual_value
                ) VALUES (
                    :job_log_id, :check_name, :check_type, :table_name,
                    :records_checked, :records_passed, :records_failed,
                    :status, :actual_value
                )
            """)
            
            with self.engine.begin() as conn:
                conn.execute(query, {
                    'job_log_id': job_log_id,
                    'check_name': check_name,
                    'check_type': check_type,
                    'table_name': table_name,
                    'records_checked': records_checked,
                    'records_passed': records_passed,
                    'records_failed': records_failed,
                    'status': status,
                    'actual_value': actual_value
                })
        except Exception as e:
            logger.warning(f"Could not log quality check: {e}")
    
    def log_shipment_status(self, awb_number: str, status_after: str, 
                            system_action: str, source_table: str = None,
                            location_code: str = None, location_name: str = None,
                            metadata: dict = None) -> int:
        """
        Log shipment position/status change for tracking
        
        Args:
            awb_number: JNE AWB/CNOTE number
            status_after: New status (e.g., 'Arrived at Hub', 'Sorted', 'Delivered')
            system_action: What triggered this (e.g., 'Kafka CDC', 'Airflow', 'CSV Load')
            source_table: Source table name
            location_code: Hub/branch code
            location_name: Hub/branch name
            metadata: Additional data
        
        Returns:
            log_id of the tracking record
        """
        try:
            query = text("""
                SELECT audit.log_shipment_status(
                    :awb_number, :status_after, :system_action,
                    :source_table, NULL, :location_code, :location_name, :metadata
                )
            """)
            
            with self.engine.begin() as conn:
                result = conn.execute(query, {
                    'awb_number': awb_number,
                    'status_after': status_after,
                    'system_action': system_action,
                    'source_table': source_table,
                    'location_code': location_code,
                    'location_name': location_name,
                    'metadata': json.dumps(metadata) if metadata else None
                })
                log_id = result.fetchone()[0]
                return log_id
        except Exception as e:
            logger.warning(f"Could not log shipment status: {e}")
            return None
    
    def get_pipeline_health(self) -> dict:
        """Get pipeline health summary"""
        try:
            query = text("""
                SELECT pipeline_stage, 
                       COUNT(*) as total_jobs,
                       SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success,
                       SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed
                FROM audit.job_log
                WHERE start_time >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY pipeline_stage
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query)
                rows = result.fetchall()
                return {
                    'pipeline_health': [
                        {'stage': row[0], 'total': row[1], 'success': row[2], 'failed': row[3]}
                        for row in rows
                    ]
                }
        except Exception as e:
            logger.warning(f"Could not get pipeline health: {e}")
            return {'pipeline_health': []}


class AuditedJob:
    """Context manager for audited jobs"""
    
    def __init__(self, audit_logger: AuditLogger, job_name: str, job_type: str,
                 pipeline_stage: str, parameters: dict = None):
        self.audit_logger = audit_logger
        self.job_name = job_name
        self.job_type = job_type
        self.pipeline_stage = pipeline_stage
        self.parameters = parameters
        self.job_log_id = None
        self.records_processed = 0
        self.records_success = 0
        self.records_failed = 0
        self.status = 'SUCCESS'
        self.error_message = None
        self.error_details = None
    
    def __enter__(self):
        self.job_log_id = self.audit_logger.start_job(
            self.job_name, self.job_type, self.pipeline_stage, self.parameters
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.status = 'FAILED'
            self.error_message = str(exc_val)
            self.error_details = {'exception_type': exc_type.__name__}
        
        self.audit_logger.complete_job(
            self.job_log_id, self.status, self.records_processed,
            self.records_success, self.records_failed,
            self.error_message, self.error_details
        )
        return False
    
    def increment_processed(self, count: int = 1):
        self.records_processed += count
    
    def increment_success(self, count: int = 1):
        self.records_success += count
    
    def increment_failed(self, count: int = 1):
        self.records_failed += count
    
    def set_status(self, status: str):
        self.status = status


# ============================================================
# CSV LOADING FUNCTIONS
# ============================================================

def get_connection_string(host='localhost', port='5432', database='jne_dashboard', 
                          user='jne_user', password='jne_secure_password_2024'):
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def drop_all_fk_constraints(engine):
    """Drop all FK constraints in raw schema before loading"""
    logger.info("Dropping foreign key constraints in raw schema...")
    
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT tc.constraint_name, tc.table_name
            FROM information_schema.table_constraints tc
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'raw'
        """))
        constraints = [(row[0], row[1]) for row in result]
        
        if not constraints:
            logger.info("  No FK constraints found")
            return []
        
        logger.info(f"  Found {len(constraints)} FK constraints to drop")
        
        dropped = []
        for constraint_name, table_name in constraints:
            try:
                conn.execute(text(f'ALTER TABLE raw."{table_name}" DROP CONSTRAINT "{constraint_name}"'))
                logger.info(f"  ✓ Dropped {constraint_name} on {table_name}")
                dropped.append((constraint_name, table_name))
            except Exception as e:
                logger.warning(f"  ⚠ Could not drop {constraint_name}: {e}")
        
        return dropped


def backup_reference_tables(engine):
    """Backup reference tables before loading new data"""
    logger.info("Backing up reference tables...")
    backed_up = []
    
    with engine.begin() as conn:
        for table in PRESERVE_TABLES:
            try:
                result = conn.execute(text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'raw' AND table_name = '{table}'
                    )
                """))
                exists = result.scalar()
                
                if exists:
                    conn.execute(text(f"DROP TABLE IF EXISTS raw.{table}_backup"))
                    conn.execute(text(f"CREATE TABLE raw.{table}_backup AS SELECT * FROM raw.{table}"))
                    result = conn.execute(text(f"SELECT COUNT(*) FROM raw.{table}_backup"))
                    count = result.scalar()
                    logger.info(f"  ✓ Backed up raw.{table} ({count} rows)")
                    backed_up.append(table)
                else:
                    logger.warning(f"  ⚠ raw.{table} does not exist, skipping backup")
            except Exception as e:
                logger.error(f"  ✗ Failed to backup {table}: {e}")
    
    return backed_up


def restore_reference_tables(engine, backed_up_tables):
    """Restore reference tables from backup"""
    logger.info("Restoring reference tables...")
    
    with engine.begin() as conn:
        for table in backed_up_tables:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS raw.{table}"))
                conn.execute(text(f"CREATE TABLE raw.{table} AS SELECT * FROM raw.{table}_backup"))
                conn.execute(text(f"DROP TABLE raw.{table}_backup"))
                result = conn.execute(text(f"SELECT COUNT(*) FROM raw.{table}"))
                count = result.scalar()
                logger.info(f"  ✓ Restored raw.{table} ({count} rows)")
            except Exception as e:
                logger.error(f"  ✗ Failed to restore {table}: {e}")


def read_csv_safely(csv_file):
    """Read CSV with multiple fallback strategies"""
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            df = pd.read_csv(csv_file, encoding=encoding, low_memory=False)
            return df, None
        except UnicodeDecodeError:
            continue
        except pd.errors.ParserError as e:
            try:
                logger.warning(f"  ⚠ Malformed rows detected, skipping bad lines...")
                df = pd.read_csv(csv_file, encoding=encoding, low_memory=False, on_bad_lines='warn')
                return df, f"Skipped malformed rows"
            except Exception:
                continue
    
    # Last resort: Python engine
    try:
        logger.warning(f"  ⚠ Using Python CSV engine as fallback...")
        df = pd.read_csv(csv_file, encoding='latin-1', low_memory=False, 
                         engine='python', on_bad_lines='skip')
        return df, "Used Python engine with skipped bad lines"
    except Exception as e:
        return None, str(e)


def load_csv_files(csv_dir, engine, audit_logger: AuditLogger, preserve_refs=True):
    """Load all CSV files from directory to PostgreSQL with audit trail"""
    
    # Create raw schema
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
    logger.info("✓ Raw schema ready")
    
    # Drop FK constraints
    drop_all_fk_constraints(engine)
    
    # Backup reference tables
    backed_up = []
    if preserve_refs:
        backed_up = backup_reference_tables(engine)
    
    # Find CSV files
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    
    if not csv_files:
        logger.error(f"No CSV files found in {csv_dir}")
        return None
    
    logger.info(f"Found {len(csv_files)} CSV files to load")
    
    # Use AuditedJob for tracking
    with AuditedJob(
        audit_logger,
        job_name="Load CSV to Raw Schema",
        job_type="LOAD",
        pipeline_stage="DATA_UNIFICATION",
        parameters={
            'source_dir': csv_dir,
            'total_files': len(csv_files),
            'load_timestamp': datetime.now().isoformat(),
            'preserve_refs': preserve_refs
        }
    ) as job:
        
        total_files = len(csv_files)
        files_loaded = 0
        files_failed = 0
        total_records = 0
        warnings_list = []
        
        for csv_file in csv_files:
            filename = os.path.basename(csv_file)
            table_name = os.path.splitext(filename)[0].lower()
            
            try:
                logger.info(f"Loading: {filename}")
                
                # Read CSV
                df, warning = read_csv_safely(csv_file)
                
                if df is None:
                    raise Exception(f"Could not read file: {warning}")
                
                if warning:
                    logger.warning(f"  ⚠ {warning}")
                    warnings_list.append(f"{filename}: {warning}")
                
                record_count = len(df)
                
                # Clean column names
                df.columns = [col.lower().strip().replace(' ', '_').replace('-', '_') 
                             for col in df.columns]
                
                # Load to database
                df.to_sql(
                    name=table_name,
                    schema='raw',
                    con=engine,
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                
                logger.info(f"  ✓ Loaded {record_count:,} records to raw.{table_name}")
                
                # Log lineage
                audit_logger.log_lineage(
                    source_table=f"CSV:{filename}",
                    target_table=f"raw.{table_name}",
                    operation_type="INSERT",
                    record_count=record_count,
                    transformation_name="CSV_To_PostgreSQL",
                    metadata={
                        'source_file': csv_file,
                        'columns': list(df.columns),
                        'warning': warning
                    }
                )
                
                # Update job stats
                job.increment_processed(record_count)
                job.increment_success(record_count)
                files_loaded += 1
                total_records += record_count
                
            except Exception as e:
                logger.error(f"  ✗ Failed to load {filename}: {e}")
                job.increment_failed(1)
                files_failed += 1
                
                # Log failed lineage
                audit_logger.log_lineage(
                    source_table=f"CSV:{filename}",
                    target_table=f"raw.{table_name}",
                    operation_type="INSERT",
                    record_count=0,
                    metadata={'error': str(e), 'status': 'FAILED'}
                )
        
        # Log quality check
        audit_logger.log_quality_check(
            job_log_id=job.job_log_id,
            check_name="CSV Load Completeness",
            check_type="COMPLETENESS",
            table_name="raw.*",
            records_checked=total_files,
            records_passed=files_loaded,
            records_failed=files_failed,
            status='PASS' if files_failed == 0 else 'PARTIAL',
            actual_value=f"{files_loaded}/{total_files} files loaded, {total_records:,} records"
        )
        
        # Set final status
        if files_failed > 0 and files_loaded > 0:
            job.set_status('PARTIAL')
        elif files_failed > 0:
            job.set_status('FAILED')
    
    # Restore reference tables
    if preserve_refs and backed_up:
        restore_reference_tables(engine, backed_up)
    
    # Summary
    logger.info("=" * 60)
    logger.info("LOAD SUMMARY")
    logger.info(f"Total CSV files: {total_files}")
    logger.info(f"Successfully loaded: {files_loaded}")
    logger.info(f"Failed: {files_failed}")
    logger.info(f"Total records loaded: {total_records:,}")
    if preserve_refs:
        logger.info(f"Reference tables preserved: {', '.join(backed_up) if backed_up else 'None'}")
    if warnings_list:
        logger.info(f"Warnings ({len(warnings_list)}):")
        for w in warnings_list:
            logger.info(f"  - {w}")
    logger.info("=" * 60)
    
    return {
        'total_files': total_files,
        'files_loaded': files_loaded,
        'files_failed': files_failed,
        'total_records': total_records
    }


def verify_load(engine):
    """Verify all tables in raw schema"""
    logger.info("Verifying loaded tables...")
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'raw' ORDER BY table_name
        """))
        tables = [row[0] for row in result]
    
    logger.info(f"Found {len(tables)} tables in raw schema:")
    
    total_records = 0
    with engine.connect() as conn:
        for table in tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM raw.{table}"))
            count = result.scalar()
            total_records += count
            logger.info(f"  raw.{table}: {count:,} records")
    
    logger.info(f"Total records: {total_records:,}")


def main():
    parser = argparse.ArgumentParser(description='Load CSV files with audit trail (v3)')
    parser.add_argument('--csv-dir', type=str, required=True, help='Directory containing CSV files')
    parser.add_argument('--host', type=str, default='localhost', help='PostgreSQL host')
    parser.add_argument('--port', type=str, default='5432', help='PostgreSQL port')
    parser.add_argument('--database', type=str, default='jne_dashboard', help='Database name')
    parser.add_argument('--user', type=str, default='jne_user', help='Database user')
    parser.add_argument('--password', type=str, default='jne_secure_password_2024', help='Database password')
    parser.add_argument('--preserve-refs', action='store_true', default=True, help='Preserve reference tables')
    parser.add_argument('--no-preserve-refs', action='store_false', dest='preserve_refs')
    parser.add_argument('--verify', action='store_true', help='Only verify existing data')
    
    args = parser.parse_args()
    
    conn_string = get_connection_string(
        host=args.host, port=args.port, database=args.database,
        user=args.user, password=args.password
    )
    
    logger.info(f"Connecting to PostgreSQL at {args.host}:{args.port}/{args.database}")
    
    try:
        engine = create_engine(conn_string)
        
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✓ Database connection successful")
        
        # Initialize audit logger
        audit_logger = AuditLogger(conn_string)
        
        if args.verify:
            verify_load(engine)
        else:
            load_csv_files(args.csv_dir, engine, audit_logger, preserve_refs=args.preserve_refs)
            verify_load(engine)
            
            # Print audit summary
            logger.info("\nAudit Trail Summary:")
            health = audit_logger.get_pipeline_health()
            logger.info(f"Pipeline Health: {health}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
