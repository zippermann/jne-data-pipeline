"""
JNE Data Pipeline - ETL Runner with Audit Trail (v3)
=====================================================
Updated for 37 tables including ORA_USER.

Usage:
    python run_etl_with_audit_v3.py --dry-run
    python run_etl_with_audit_v3.py --sql-file unify_jne_tables_v3.sql
    
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

# ============================================================
# SOURCE TABLES CONFIGURATION (37 tables - v3)
# ============================================================
# Format: (table_name, join_key, dedup_column, needs_dedup)
# ============================================================
SOURCE_TABLES = [
    # Core (5)
    ('cms_cnote', 'cnote_no', None, False),
    ('cms_cnote_pod', 'cnote_pod_no', None, False),
    ('cms_cnote_amo', 'cnote_no', 'cdate', True),
    ('cms_apicust', 'apicust_cnote_no', None, False),
    ('cms_drourate', 'drourate_code', 'drourate_udate', True),
    
    # Runsheet (6)
    ('cms_drcnote', 'drcnote_cnote_no', None, False),
    ('cms_mrcnote', 'mrcnote_no', 'mrcnote_date', True),
    ('cms_drsheet', 'drsheet_cnote_no', 'drsheet_date', True),
    ('cms_mrsheet', 'mrsheet_no', 'mrsheet_date', True),
    ('cms_drsheet_pra', 'drsheet_cnote_no', 'drsheet_date', True),
    ('cms_dhov_rsheet', 'dhov_rsheet_cnote', 'create_date', True),
    
    # Inbound HO (2)
    ('cms_dhicnote', 'dhicnote_cnote_no', 'dhicnote_tdate', True),
    ('cms_mhicnote', 'mhicnote_no', 'mhicnote_date', True),
    
    # Outbound HO (2)
    ('cms_dhocnote', 'dhocnote_cnote_no', 'dhocnote_tdate', True),
    ('cms_mhocnote', 'mhocnote_no', 'mhocnote_date', True),
    
    # Undelivered (2)
    ('cms_dhoundel_pod', 'dhoundel_cnote_no', 'create_date', True),
    ('cms_mhoundel_pod', 'mhoundel_no', 'mhoundel_date', True),
    
    # Hub Ops (2)
    ('cms_dhi_hoc', 'dhi_cnote_no', 'cdate', True),
    ('cms_mhi_hoc', 'mhi_no', 'mhi_date', True),
    
    # Manifest (2)
    ('cms_mfcnote', 'mfcnote_no', 'mfcnote_man_date', True),
    ('cms_manifest', 'manifest_no', 'manifest_date', True),
    
    # Bags (2)
    ('cms_dbag_ho', 'dbag_cnote_no', 'cdate', True),
    ('cms_dmbag', 'dmbag_no', 'esb_time', True),
    
    # SJ Chain (3)
    ('cms_rdsj', 'rdsj_hvi_no', 'rdsj_cdate', True),
    ('cms_dsj', 'dsj_hvo_no', 'dsj_cdate', True),
    ('cms_msj', 'msj_no', 'msj_date', True),
    
    # SMU (2)
    ('cms_dsmu', 'dsmu_no', 'esb_time', True),
    ('cms_msmu', 'msmu_no', 'msmu_date', True),
    
    # Status/Cost (3)
    ('cms_dstatus', 'dstatus_cnote_no', 'create_date', True),
    ('cms_cost_dtransit_agen', 'cnote_no', 'esb_time', True),
    ('cms_cost_mtransit_agen', 'manifest_no', 'manifest_date', True),
    
    # Crossdock (2)
    ('t_crossdock_awd', 'awb_child', None, False),  # Note: awd not awb
    ('t_goto', 'awb', None, False),
    
    # Reference (5) - includes ORA_USER (NEW)
    ('t_mdt_city_origin', 'city_code', 'create_date', True),
    ('lastmile_courier', 'courier_id', 'courier_updated_at', True),
    ('ora_zone', 'zone_code', 'lastupddtm', True),
    ('ora_user', 'user_id', 'user_date', True),  # NEW - Table #37
]


class AuditedETL:
    """ETL runner with audit trail logging."""
    
    def __init__(self, conn, triggered_by='MANUAL', schema='raw'):
        self.conn = conn
        self.triggered_by = triggered_by
        self.schema = schema
        self.job_id = None
        self.stats = {}
        self.tables_found = 0
        self.tables_missing = 0
        self.audit_enabled = self._check_audit_schema()
        
    def _check_audit_schema(self):
        """Check if audit schema and functions exist."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.schemata 
                        WHERE schema_name = 'audit'
                    )
                """)
                exists = cur.fetchone()[0]
                if not exists:
                    logger.warning("Audit schema not found - stats logging disabled")
                    logger.warning("Run 01-create-audit-schema.sql to enable full audit trail")
                return exists
        except Exception as e:
            logger.warning(f"Could not check audit schema: {e}")
            self.conn.rollback()
            return False
        
    def start_job(self, job_name='JNE_DAILY_UNIFICATION_V3'):
        """Start an ETL job and return job_id."""
        if not self.audit_enabled:
            self.job_id = 0  # Dummy job_id
            logger.info(f"Started ETL job (audit disabled)")
            return self.job_id
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT audit.start_etl_job(%s, 'UNIFICATION', %s)",
                    (job_name, self.triggered_by)
                )
                self.job_id = cur.fetchone()[0]
                self.conn.commit()
            logger.info(f"Started ETL job {self.job_id}")
        except Exception as e:
            self.conn.rollback()
            self.job_id = 0
            self.audit_enabled = False
            logger.warning(f"Could not start audit job: {e}")
        return self.job_id
    
    def table_exists(self, table_name):
        """Check if a table exists in the specified schema."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = %s
                )
            """, (self.schema, table_name.lower()))
            return cur.fetchone()[0]
    
    def log_source_stats(self, table_name, rows_total, rows_after_dedup, 
                         duplicates, join_key, dedup_column):
        """Log statistics for a source table (if audit schema exists)."""
        if not self.audit_enabled:
            return  # Skip if audit not set up
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """SELECT audit.log_source_stats(%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (self.job_id, table_name, rows_total, rows_after_dedup,
                     None, duplicates, join_key, dedup_column)
                )
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.debug(f"Could not log stats (audit schema may not exist): {e}")
        self.stats[table_name] = {
            'total': rows_total,
            'after_dedup': rows_after_dedup,
            'duplicates': duplicates
        }
    
    def log_error(self, severity, message, source_table=None, 
                  affected_record=None, detail=None):
        """Log an error or warning."""
        if self.audit_enabled:
            try:
                with self.conn.cursor() as cur:
                    cur.execute(
                        "SELECT audit.log_error(%s, %s, %s, %s, %s, %s)",
                        (self.job_id, severity, message, source_table, 
                         affected_record, detail)
                    )
                    self.conn.commit()
            except Exception as e:
                self.conn.rollback()
        if severity in ('ERROR', 'CRITICAL'):
            logger.error(f"[{source_table}] {message}")
        elif severity == 'WARNING':
            logger.warning(f"[{source_table}] {message}")
    
    def complete_job(self, target_rows, rows_inserted=0, rows_updated=0,
                     duplicates_removed=0, notes=None):
        """Mark job as completed successfully."""
        if self.audit_enabled:
            try:
                with self.conn.cursor() as cur:
                    cur.execute(
                        "SELECT audit.complete_etl_job(%s, %s, %s, %s, %s, %s)",
                        (self.job_id, target_rows, rows_inserted, rows_updated,
                         duplicates_removed, notes)
                    )
                    self.conn.commit()
            except Exception as e:
                self.conn.rollback()
        logger.info(f"Completed ETL job {self.job_id} - {target_rows} rows")
    
    def fail_job(self, error_message, error_code=None):
        """Mark job as failed."""
        if self.audit_enabled:
            try:
                with self.conn.cursor() as cur:
                    cur.execute(
                        "SELECT audit.fail_etl_job(%s, %s, %s)",
                        (self.job_id, error_message, error_code)
                    )
                    self.conn.commit()
            except Exception as e:
                self.conn.rollback()
        logger.error(f"ETL job {self.job_id} FAILED: {error_message}")
    
    def collect_source_stats(self):
        """Collect row counts from all 37 source tables."""
        logger.info(f"Collecting source table statistics from schema '{self.schema}'...")
        logger.info(f"Checking {len(SOURCE_TABLES)} tables...")
        total_duplicates = 0
        
        for table_name, join_key, dedup_col, needs_dedup in SOURCE_TABLES:
            full_table = f"{self.schema}.{table_name}"
            
            # Reset connection state before each table
            try:
                self.conn.rollback()
            except:
                pass
            
            # Check if table exists first
            if not self.table_exists(table_name):
                logger.warning(f"  ✗ {table_name}: NOT FOUND")
                self.tables_missing += 1
                continue
            
            try:
                with self.conn.cursor() as cur:
                    # Get total count
                    cur.execute(f"SELECT COUNT(*) FROM {full_table}")
                    rows_total = cur.fetchone()[0]
                    
                    # Get deduped count if needed
                    rows_after_dedup = rows_total
                    if needs_dedup and join_key:
                        try:
                            cur.execute(f"SELECT COUNT(DISTINCT {join_key}) FROM {full_table}")
                            rows_after_dedup = cur.fetchone()[0]
                        except Exception as e:
                            logger.debug(f"  Could not count distinct {join_key}: {e}")
                            self.conn.rollback()
                    
                    duplicates = rows_total - rows_after_dedup
                    total_duplicates += duplicates
                
                # Log stats in separate try block
                try:
                    self.log_source_stats(
                        table_name, rows_total, rows_after_dedup,
                        duplicates, join_key, dedup_col
                    )
                except Exception as e:
                    logger.debug(f"  Could not log stats for {table_name}: {e}")
                    self.conn.rollback()
                
                self.tables_found += 1
                dup_str = f", {duplicates} duplicates" if duplicates > 0 else ""
                logger.info(f"  ✓ {table_name}: {rows_total:,} rows{dup_str}")
                    
            except Exception as e:
                self.conn.rollback()
                logger.warning(f"  ✗ {table_name}: ERROR - {str(e)}")
                self.tables_missing += 1
        
        logger.info(f"\n{'='*50}")
        logger.info(f"Tables found: {self.tables_found}/{len(SOURCE_TABLES)}")
        logger.info(f"Tables missing: {self.tables_missing}/{len(SOURCE_TABLES)}")
        logger.info(f"Total duplicates: {total_duplicates:,}")
        logger.info(f"{'='*50}")
        return total_duplicates
    
    def run_unification(self, sql_file_path):
        """Execute the unification SQL and create the unified table."""
        logger.info(f"Running unification from {sql_file_path}...")
        
        with open(sql_file_path, 'r') as f:
            unification_sql = f.read()
        
        create_sql = f"""
        DROP TABLE IF EXISTS staging.unified_shipments CASCADE;
        CREATE TABLE staging.unified_shipments AS
        {unification_sql}
        """
        
        with self.conn.cursor() as cur:
            # Set search_path to include the raw schema
            cur.execute(f"SET search_path TO {self.schema}, public, staging")
            
            cur.execute(create_sql)
            self.conn.commit()
            
            cur.execute("""
                ALTER TABLE staging.unified_shipments 
                ADD COLUMN etl_job_id INTEGER DEFAULT %s,
                ADD COLUMN etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """, (self.job_id,))
            self.conn.commit()
            
            cur.execute("SELECT COUNT(*) FROM staging.unified_shipments")
            target_rows = cur.fetchone()[0]
        
        logger.info(f"Created unified_shipments with {target_rows:,} rows")
        return target_rows


def check_audit_schema(conn):
    """Check if audit schema and functions exist."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.schemata 
                WHERE schema_name = 'audit'
            )
        """)
        schema_exists = cur.fetchone()[0]
        
        if not schema_exists:
            logger.error("=" * 60)
            logger.error("AUDIT SCHEMA NOT FOUND!")
            logger.error("Please run 01-create-audit-schema.sql first")
            logger.error("=" * 60)
            return False
        
        return True


def list_schemas_and_tables(conn):
    """List all schemas and their table counts."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_schema, COUNT(*) as table_count
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            GROUP BY table_schema
            ORDER BY table_schema
        """)
        return cur.fetchall()


def run_etl(sql_file_path=None, triggered_by='MANUAL', dry_run=False, schema='raw'):
    """Main ETL execution function."""
    
    logger.info("=" * 60)
    logger.info("JNE Data Pipeline - ETL with Audit Trail (v3)")
    logger.info(f"Total source tables: {len(SOURCE_TABLES)}")
    logger.info("=" * 60)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        logger.error(f"Could not connect to database: {e}")
        sys.exit(1)
    
    logger.info(f"Connected to {DB_CONFIG['database']} on {DB_CONFIG['host']}")
    
    # Check audit schema exists
    if not check_audit_schema(conn):
        conn.close()
        sys.exit(1)
    
    # List available schemas
    schemas = list_schemas_and_tables(conn)
    logger.info("Database schemas:")
    for s, count in schemas:
        marker = " <-- using this" if s == schema else ""
        logger.info(f"  {s}: {count} tables{marker}")
    
    etl = AuditedETL(conn, triggered_by, schema)
    
    try:
        # Step 1: Start job
        job_id = etl.start_job()
        
        # Step 2: Collect source statistics
        total_duplicates = etl.collect_source_stats()
        
        if etl.tables_found == 0:
            logger.error("NO SOURCE TABLES FOUND!")
            etl.fail_job(f"No source tables found in schema '{schema}'")
            conn.close()
            sys.exit(1)
        
        if dry_run:
            logger.info("\n" + "=" * 60)
            logger.info("DRY RUN COMPLETE")
            logger.info("=" * 60)
            etl.complete_job(0, 0, 0, total_duplicates, 
                           f"Dry run - found {etl.tables_found}/{len(SOURCE_TABLES)} tables")
            
            logger.info("\nView results with:")
            logger.info("  SELECT * FROM audit.v_latest_jobs;")
            logger.info("  SELECT * FROM audit.v_latest_source_stats;")
            conn.close()
            return
        
        if not sql_file_path:
            logger.error("No SQL file specified (use --sql-file)")
            etl.fail_job("No SQL file specified")
            conn.close()
            sys.exit(1)
        
        # Step 3: Run unification
        target_rows = etl.run_unification(sql_file_path)
        
        # Step 4: Complete job
        etl.complete_job(
            target_rows=target_rows,
            rows_inserted=target_rows,
            duplicates_removed=total_duplicates,
            notes=f"Unified {etl.tables_found} tables (v3 with ORA_USER)"
        )
        
        logger.info("=" * 60)
        logger.info(f"ETL Job {job_id} completed successfully!")
        logger.info(f"Target rows: {target_rows:,}")
        logger.info(f"Duplicates removed: {total_duplicates:,}")
        logger.info("=" * 60)
        
    except Exception as e:
        if etl.job_id:
            etl.fail_job(str(e))
        logger.exception("ETL failed")
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='JNE ETL with Audit Trail (v3 - 37 tables)')
    parser.add_argument('--sql-file', type=str, default=None,
                        help='Path to unification SQL file')
    parser.add_argument('--triggered-by', type=str, default='MANUAL',
                        choices=['MANUAL', 'AIRFLOW', 'SCHEDULER', 'CRON'],
                        help='Who/what triggered this run')
    parser.add_argument('--dry-run', action='store_true',
                        help='Collect stats only, skip unification')
    parser.add_argument('--schema', type=str, default='raw',
                        help='Schema where source tables are located (default: raw)')
    
    args = parser.parse_args()
    
    if args.sql_file and not Path(args.sql_file).exists():
        logger.error(f"SQL file not found: {args.sql_file}")
        sys.exit(1)
    
    run_etl(args.sql_file, args.triggered_by, args.dry_run, args.schema)


if __name__ == '__main__':
    main()
