"""
JNE Data Loader with Integrated Audit Trail
Loads raw Excel data to PostgreSQL with comprehensive audit logging
"""

import sys
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import logging
from datetime import datetime

# Add audit module to path
sys.path.append(str(Path(__file__).parent.parent / 'audit'))
from audit_logger import AuditLogger, AuditedJob

# Configuration
DB_CONN = "postgresql://jne_user:jne_secure_password_2024@jne-postgres:5432/jne_dashboard"
DEFAULT_EXCEL_FILE = "data/raw/JNE_RAW_COMBINED.xlsx"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_raw_schema(engine):
    with engine.begin() as conn:  # ← Change connect() to begin()
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
    logger.info("✓ Raw schema ready")


def load_excel_to_postgres(audit_logger: AuditLogger, excel_file: str):
    """
    Load all sheets from Excel file to PostgreSQL with audit logging
    
    Args:
        audit_logger: AuditLogger instance
        excel_file: Path to Excel file
    """
    # Initialize database engine
    engine = create_engine(DB_CONN)
    create_raw_schema(engine)
    
    # Read Excel file
    logger.info(f"Reading Excel file: {excel_file}")
    excel_file_obj = pd.ExcelFile(excel_file)
    sheet_names = excel_file_obj.sheet_names
    
    logger.info(f"Found {len(sheet_names)} sheets to load")
    
    total_tables = len(sheet_names)
    tables_loaded = 0
    tables_failed = 0
    total_records = 0
    
    # Use audited job context manager
    with AuditedJob(
        audit_logger,
        job_name="Load Excel to Raw Schema",
        job_type="LOAD",
        pipeline_stage="DATA_UNIFICATION",
        parameters={
            'source_file': excel_file,
            'total_sheets': total_tables,
            'load_timestamp': datetime.now().isoformat()
        }
    ) as job:
        
        for sheet_name in sheet_names:
            try:
                logger.info(f"Loading sheet: {sheet_name}")
                
                # Read sheet
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                
                # Clean table name (lowercase, replace spaces)
                table_name = sheet_name.lower().replace(' ', '_').replace('-', '_')
                record_count = len(df)
                
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
                
                # Log successful load
                logger.info(f"  ✓ Loaded {record_count} records to raw.{table_name}")
                
                # Log data lineage
                audit_logger.log_lineage(
                    source_table=f"EXCEL:{sheet_name}",
                    target_table=f"raw.{table_name}",
                    operation_type="INSERT",
                    record_count=record_count,
                    transformation_name="Excel_To_PostgreSQL",
                    metadata={
                        'source_file': excel_file,
                        'sheet_name': sheet_name,
                        'columns': list(df.columns),
                        'load_timestamp': datetime.now().isoformat()
                    }
                )
                
                # Perform basic data quality checks
                perform_quality_checks(audit_logger, job.job_log_id, table_name, df, engine)
                
                # Update job metrics
                job.increment_processed(record_count)
                job.increment_success(record_count)
                tables_loaded += 1
                total_records += record_count
                
            except Exception as e:
                logger.error(f"  ✗ Failed to load {sheet_name}: {e}")
                job.increment_failed(1)
                tables_failed += 1
                
                # Still log lineage for failed attempts
                audit_logger.log_lineage(
                    source_table=f"EXCEL:{sheet_name}",
                    target_table=f"raw.{table_name}",
                    operation_type="INSERT",
                    record_count=0,
                    metadata={'error': str(e), 'status': 'FAILED'}
                )
        
        # Set final status
        if tables_failed > 0 and tables_loaded > 0:
            job.set_status('PARTIAL')
        elif tables_failed > 0:
            job.set_status('FAILED')
    
    # Summary
    logger.info("=" * 60)
    logger.info("LOAD SUMMARY")
    logger.info(f"Total sheets: {total_tables}")
    logger.info(f"Successfully loaded: {tables_loaded}")
    logger.info(f"Failed: {tables_failed}")
    logger.info(f"Total records loaded: {total_records}")
    logger.info("=" * 60)
    
    return {
        'total_tables': total_tables,
        'tables_loaded': tables_loaded,
        'tables_failed': tables_failed,
        'total_records': total_records
    }


def perform_quality_checks(audit_logger: AuditLogger, job_log_id: int, table_name: str, df: pd.DataFrame, engine):
    """
    Perform basic data quality checks and log results
    """
    
    # Check 1: Completeness - Null values
    null_counts = df.isnull().sum()
    total_cells = len(df) * len(df.columns)
    null_cells = null_counts.sum()
    non_null_cells = total_cells - null_cells
    
    audit_logger.log_quality_check(
        job_log_id=job_log_id,
        check_name="Null Value Check",
        check_type="COMPLETENESS",
        table_name=f"raw.{table_name}",
        records_checked=total_cells,
        records_passed=non_null_cells,
        records_failed=null_cells,
        status='PASS' if null_cells < total_cells * 0.5 else 'WARNING',
        actual_value=f"{(null_cells/total_cells*100):.2f}% null"
    )
    
    # Check 2: Row count
    row_count = len(df)
    audit_logger.log_quality_check(
        job_log_id=job_log_id,
        check_name="Row Count Check",
        check_type="VALIDITY",
        table_name=f"raw.{table_name}",
        records_checked=row_count,
        records_passed=row_count,
        records_failed=0,
        status='PASS' if row_count > 0 else 'FAIL',
        actual_value=str(row_count)
    )
    
    # Check 3: Duplicate rows
    duplicate_count = df.duplicated().sum()
    unique_count = len(df) - duplicate_count
    
    audit_logger.log_quality_check(
        job_log_id=job_log_id,
        check_name="Duplicate Row Check",
        check_type="UNIQUENESS",
        table_name=f"raw.{table_name}",
        records_checked=len(df),
        records_passed=unique_count,
        records_failed=duplicate_count,
        status='PASS' if duplicate_count == 0 else 'WARNING',
        actual_value=f"{duplicate_count} duplicates found"
    )


def verify_load(audit_logger: AuditLogger):
    """
    Verify that data was loaded correctly
    """
    engine = create_engine(DB_CONN)
    inspector = inspect(engine)
    
    with AuditedJob(
        audit_logger,
        job_name="Verify Data Load",
        job_type="QUALITY_CHECK",
        pipeline_stage="DATA_UNIFICATION"
    ) as job:
        
        # Check if raw schema exists
        if 'raw' not in inspector.get_schema_names():
            logger.error("✗ Raw schema does not exist")
            job.set_status('FAILED')
            return False
        
        # Get all tables in raw schema
        tables = inspector.get_table_names(schema='raw')
        logger.info(f"Found {len(tables)} tables in raw schema")
        
        total_records = 0
        for table in tables:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM raw.{table}"))
                count = result.fetchone()[0]
                total_records += count
                logger.info(f"  {table}: {count} records")
                job.increment_success(count)
        
        logger.info(f"Total records across all tables: {total_records}")
        
        # Log verification
        audit_logger.log_quality_check(
            job_log_id=job.job_log_id,
            check_name="Table Count Verification",
            check_type="CONSISTENCY",
            table_name="raw.*",
            records_checked=len(tables),
            records_passed=len(tables),
            records_failed=0,
            status='PASS',
            actual_value=f"{len(tables)} tables verified"
        )
    
    return True


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Load JNE data with audit trail')
    parser.add_argument('--verify', action='store_true', help='Verify existing data load')
    parser.add_argument('--file', type=str, default=DEFAULT_EXCEL_FILE, help='Excel file path')
    
    args = parser.parse_args()
    
    # Initialize audit logger
    audit_logger = AuditLogger(DB_CONN)
    logger.info("Audit logger initialized")
    
    try:
        if args.verify:
            logger.info("Running verification mode")
            verify_load(audit_logger)
        else:
            logger.info("Starting data load process")
            load_excel_to_postgres(audit_logger, args.file)
            
        logger.info("✓ Process completed successfully")
        
        # Print audit summary
        logger.info("\nAudit Trail Summary:")
        health = audit_logger.get_pipeline_health()
        logger.info(f"Pipeline Health: {health}")
        
    except Exception as e:
        logger.error(f"✗ Process failed: {e}")
        raise


if __name__ == '__main__':
    main()
