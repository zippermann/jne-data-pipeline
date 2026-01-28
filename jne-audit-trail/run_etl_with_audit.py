"""
JNE Data Pipeline - ETL Runner with Audit Trail
================================================
Executes unify_jne_tables_v2.sql with full audit logging.

Usage:
    python run_etl_with_audit.py --triggered-by AIRFLOW
    python run_etl_with_audit.py --dry-run
    
Requirements:
    pip install psycopg2-binary python-dotenv
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'jne_dashboard'),
    'user': os.getenv('POSTGRES_USER', 'jne_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'jne_secure_password_2024')
}

# Source tables with their dedup configuration
SOURCE_TABLES = [
    # (table_name, join_key, dedup_column, needs_dedup)
    ('CMS_CNOTE', 'CNOTE_NO', None, False),
    ('CMS_CNOTE_POD', 'CNOTE_POD_NO', None, False),
    ('CMS_DRCNOTE', 'DRCNOTE_CNOTE_NO', None, False),
    ('CMS_MRCNOTE', 'MRCNOTE_NO', 'MRCNOTE_DATE', True),
    ('CMS_APICUST', 'APICUST_CNOTE_NO', None, False),
    ('CMS_DROURATE', 'DROURATE_CODE', 'DROURATE_UDATE', True),
    ('CMS_DRSHEET', 'DRSHEET_CNOTE_NO', 'DRSHEET_DATE', True),
    ('CMS_MRSHEET', 'MRSHEET_NO', 'MRSHEET_DATE', True),
    ('CMS_DRSHEET_PRA', 'DRSHEET_CNOTE_NO', 'DRSHEET_DATE', True),
    ('CMS_DHICNOTE', 'DHICNOTE_CNOTE_NO', 'DHICNOTE_TDATE', True),
    ('CMS_MHICNOTE', 'MHICNOTE_NO', 'MHICNOTE_DATE', True),
    ('CMS_DHOCNOTE', 'DHOCNOTE_CNOTE_NO', 'DHOCNOTE_TDATE', True),
    ('CMS_MHOCNOTE', 'MHOCNOTE_NO', 'MHOCNOTE_DATE', True),
    ('CMS_DHOUNDEL_POD', 'DHOUNDEL_CNOTE_NO', 'CREATE_DATE', True),
    ('CMS_MHOUNDEL_POD', 'MHOUNDEL_NO', 'MHOUNDEL_DATE', True),
    ('CMS_DHI_HOC', 'DHI_CNOTE_NO', 'CDATE', True),
    ('CMS_MHI_HOC', 'MHI_NO', 'MHI_DATE', True),
    ('CMS_MFCNOTE', 'MFCNOTE_NO', 'MFCNOTE_MAN_DATE', True),
    ('CMS_MANIFEST', 'MANIFEST_NO', 'MANIFEST_DATE', True),
    ('CMS_DBAG_HO', 'DBAG_CNOTE_NO', 'CDATE', True),
    ('CMS_DMBAG', 'DMBAG_NO', 'ESB_TIME', True),
    ('CMS_DHOV_RSHEET', 'DHOV_RSHEET_CNOTE', 'CREATE_DATE', True),
    ('CMS_DSTATUS', 'DSTATUS_CNOTE_NO', 'CREATE_DATE', True),
    ('CMS_COST_DTRANSIT_AGEN', 'CNOTE_NO', 'ESB_TIME', True),
    ('CMS_COST_MTRANSIT_AGEN', 'MANIFEST_NO', 'MANIFEST_DATE', True),
    ('T_CROSSDOCK_AWD', 'AWB_CHILD', None, False),
    ('T_GOTO', 'AWB', None, False),
    ('CMS_RDSJ', 'RDSJ_HVI_NO', 'RDSJ_CDATE', True),
    ('CMS_DSJ', 'DSJ_HVO_NO', 'DSJ_CDATE', True),
    ('CMS_MSJ', 'MSJ_NO', 'MSJ_DATE', True),
    ('CMS_DSMU', 'DSMU_NO', 'ESB_TIME', True),
    ('CMS_MSMU', 'MSMU_NO', 'MSMU_DATE', True),
    ('T_MDT_CITY_ORIGIN', 'CITY_CODE', 'CREATE_DATE', True),
    ('LASTMILE_COURIER', 'COURIER_ID', 'COURIER_UPDATED_AT', True),
    ('ORA_ZONE', 'ZONE_CODE', 'LASTUPDDTM', True),
]


class AuditedETL:
    """ETL runner with audit trail logging."""
    
    def __init__(self, conn, triggered_by='MANUAL'):
        self.conn = conn
        self.triggered_by = triggered_by
        self.job_id = None
        self.stats = {}
        
    def start_job(self, job_name='JNE_DAILY_UNIFICATION'):
        """Start an ETL job and return job_id."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT audit.start_etl_job(%s, 'UNIFICATION', %s)",
                (job_name, self.triggered_by)
            )
            self.job_id = cur.fetchone()[0]
            self.conn.commit()
        logger.info(f"Started ETL job {self.job_id}")
        return self.job_id
    
    def log_source_stats(self, table_name, rows_total, rows_after_dedup, 
                         duplicates, join_key, dedup_column):
        """Log statistics for a source table."""
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT audit.log_source_stats(%s, %s, %s, %s, %s, %s, %s, %s)""",
                (self.job_id, table_name, rows_total, rows_after_dedup,
                 None, duplicates, join_key, dedup_column)
            )
            self.conn.commit()
        self.stats[table_name] = {
            'total': rows_total,
            'after_dedup': rows_after_dedup,
            'duplicates': duplicates
        }
    
    def log_error(self, severity, message, source_table=None, 
                  affected_record=None, detail=None):
        """Log an error or warning."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT audit.log_error(%s, %s, %s, %s, %s, %s)",
                (self.job_id, severity, message, source_table, 
                 affected_record, detail)
            )
            self.conn.commit()
        if severity in ('ERROR', 'CRITICAL'):
            logger.error(f"[{source_table}] {message}")
        else:
            logger.warning(f"[{source_table}] {message}")
    
    def complete_job(self, target_rows, rows_inserted=0, rows_updated=0,
                     duplicates_removed=0, notes=None):
        """Mark job as completed successfully."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT audit.complete_etl_job(%s, %s, %s, %s, %s, %s)",
                (self.job_id, target_rows, rows_inserted, rows_updated,
                 duplicates_removed, notes)
            )
            self.conn.commit()
        logger.info(f"Completed ETL job {self.job_id} - {target_rows} rows")
    
    def fail_job(self, error_message, error_code=None):
        """Mark job as failed."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT audit.fail_etl_job(%s, %s, %s)",
                (self.job_id, error_message, error_code)
            )
            self.conn.commit()
        logger.error(f"ETL job {self.job_id} FAILED: {error_message}")
    
    def collect_source_stats(self):
        """Collect row counts from all source tables."""
        logger.info("Collecting source table statistics...")
        total_duplicates = 0
        
        with self.conn.cursor() as cur:
            for table_name, join_key, dedup_col, needs_dedup in SOURCE_TABLES:
                try:
                    # Get total count
                    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                    rows_total = cur.fetchone()[0]
                    
                    # Get deduped count if needed
                    if needs_dedup and dedup_col:
                        cur.execute(f"SELECT COUNT(DISTINCT {join_key}) FROM {table_name}")
                        rows_after_dedup = cur.fetchone()[0]
                    else:
                        rows_after_dedup = rows_total
                    
                    duplicates = rows_total - rows_after_dedup
                    total_duplicates += duplicates
                    
                    self.log_source_stats(
                        table_name, rows_total, rows_after_dedup,
                        duplicates, join_key, dedup_col
                    )
                    
                    logger.info(f"  {table_name}: {rows_total} rows, {duplicates} duplicates")
                    
                except Exception as e:
                    self.log_error('WARNING', f"Could not count {table_name}: {str(e)}", 
                                   table_name)
        
        return total_duplicates
    
    def run_unification(self, sql_file_path):
        """Execute the unification SQL and create the unified table."""
        logger.info(f"Running unification from {sql_file_path}...")
        
        # Read SQL file
        with open(sql_file_path, 'r') as f:
            unification_sql = f.read()
        
        # Wrap in CREATE TABLE
        create_sql = f"""
        DROP TABLE IF EXISTS staging.unified_shipments CASCADE;
        CREATE TABLE staging.unified_shipments AS
        {unification_sql}
        """
        
        with self.conn.cursor() as cur:
            cur.execute(create_sql)
            self.conn.commit()
            
            # Add audit columns
            cur.execute("""
                ALTER TABLE staging.unified_shipments 
                ADD COLUMN etl_job_id INTEGER DEFAULT %s,
                ADD COLUMN etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """, (self.job_id,))
            self.conn.commit()
            
            # Get row count
            cur.execute("SELECT COUNT(*) FROM staging.unified_shipments")
            target_rows = cur.fetchone()[0]
        
        logger.info(f"Created unified_shipments with {target_rows} rows")
        return target_rows
    
    def calculate_dq_metrics(self):
        """Calculate data quality metrics for the unified table."""
        logger.info("Calculating data quality metrics...")
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Basic counts
            cur.execute("""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(*) - COUNT(CNOTE_NO) as null_cnote,
                    COUNT(*) - COUNT(CNOTE_ORIGIN) as null_origin,
                    COUNT(*) - COUNT(CNOTE_DESTINATION) as null_dest,
                    COUNT(*) - COUNT(DISTINCT CNOTE_NO) as duplicates
                FROM staging.unified_shipments
            """)
            metrics = cur.fetchone()
            
            total = metrics['total_rows']
            if total > 0:
                completeness = (1 - (metrics['null_cnote'] + metrics['null_origin'] + 
                                     metrics['null_dest']) / (total * 3)) * 100
                uniqueness = (1 - metrics['duplicates'] / total) * 100
            else:
                completeness = 0
                uniqueness = 0
            
            # Insert metrics
            cur.execute("""
                INSERT INTO audit.data_quality_metrics (
                    job_id, table_name, total_rows,
                    null_cnote_no, null_origin, null_destination,
                    duplicate_cnotes,
                    completeness_score, uniqueness_score, overall_dq_score
                ) VALUES (%s, 'staging.unified_shipments', %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                self.job_id, total,
                metrics['null_cnote'], metrics['null_origin'], metrics['null_dest'],
                metrics['duplicates'],
                round(completeness, 2), round(uniqueness, 2), 
                round((completeness + uniqueness) / 2, 2)
            ))
            self.conn.commit()
            
        logger.info(f"DQ Score: Completeness={completeness:.1f}%, Uniqueness={uniqueness:.1f}%")
        return {'completeness': completeness, 'uniqueness': uniqueness}


def run_etl(sql_file_path, triggered_by='MANUAL', dry_run=False):
    """Main ETL execution function."""
    
    logger.info("=" * 60)
    logger.info("JNE Data Pipeline - Starting ETL with Audit Trail")
    logger.info("=" * 60)
    
    conn = psycopg2.connect(**DB_CONFIG)
    etl = AuditedETL(conn, triggered_by)
    
    try:
        # Step 1: Start job
        job_id = etl.start_job()
        
        # Step 2: Collect source statistics
        total_duplicates = etl.collect_source_stats()
        logger.info(f"Total duplicates to remove: {total_duplicates}")
        
        if dry_run:
            logger.info("DRY RUN - Skipping unification")
            etl.complete_job(0, 0, 0, total_duplicates, "Dry run completed")
            return
        
        # Step 3: Run unification
        target_rows = etl.run_unification(sql_file_path)
        
        # Step 4: Calculate DQ metrics
        etl.calculate_dq_metrics()
        
        # Step 5: Complete job
        etl.complete_job(
            target_rows=target_rows,
            rows_inserted=target_rows,
            duplicates_removed=total_duplicates,
            notes=f"Unified {len(SOURCE_TABLES)} tables successfully"
        )
        
        logger.info("=" * 60)
        logger.info(f"ETL Job {job_id} completed successfully!")
        logger.info(f"Target rows: {target_rows}")
        logger.info(f"Duplicates removed: {total_duplicates}")
        logger.info("=" * 60)
        
    except Exception as e:
        etl.fail_job(str(e))
        logger.exception("ETL failed")
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='JNE ETL with Audit Trail')
    parser.add_argument('--sql-file', type=str, 
                        default='unify_jne_tables_v2.sql',
                        help='Path to unification SQL file')
    parser.add_argument('--triggered-by', type=str, default='MANUAL',
                        choices=['MANUAL', 'AIRFLOW', 'SCHEDULER', 'CRON'],
                        help='Who/what triggered this run')
    parser.add_argument('--dry-run', action='store_true',
                        help='Collect stats only, skip unification')
    
    args = parser.parse_args()
    
    if not Path(args.sql_file).exists():
        logger.error(f"SQL file not found: {args.sql_file}")
        sys.exit(1)
    
    run_etl(args.sql_file, args.triggered_by, args.dry_run)


if __name__ == '__main__':
    main()
