"""
JNE Data Loader with Integrated Audit Trail
=============================================
Loads raw data to PostgreSQL from two sources:
  1. CSV files (one per table) from the CSV directory
  2. Excel sheets for static lookup/reference tables

Configuration is centralized in pipeline_config.py.
"""

import sys
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime

# Add project paths
sys.path.append(str(Path(__file__).parent.parent / 'audit'))
sys.path.append(str(Path(__file__).parent.parent.parent))

from audit_logger import AuditLogger, AuditedJob

# Import config
try:
    from pipeline_config import (
        DB_CONN, CSV_DIR, EXCEL_FILE,
        CSV_TABLES, EXCEL_LOOKUP_TABLES, EXCLUDED_TABLES,
        SCHEMA_RAW,
    )
except ImportError:
    sys.path.insert(0, '/opt/airflow')
    from pipeline_config import (
        DB_CONN, CSV_DIR, EXCEL_FILE,
        CSV_TABLES, EXCEL_LOOKUP_TABLES, EXCLUDED_TABLES,
        SCHEMA_RAW,
    )

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_raw_schema(engine):
    """Create the raw schema if it doesn't exist."""
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RAW}"))
    logger.info(f"✓ Schema '{SCHEMA_RAW}' ready")


def find_csv_file(csv_dir: str, table_name: str) -> str | None:
    """
    Find a CSV file for a given table name (case-insensitive).
    Returns the full path if found, None otherwise.
    """
    if not os.path.isdir(csv_dir):
        logger.error(f"CSV directory not found: {csv_dir}")
        return None

    target = table_name.lower() + ".csv"
    for filename in os.listdir(csv_dir):
        if filename.lower() == target:
            return os.path.join(csv_dir, filename)
    return None


def load_csv_tables(engine, audit_logger: AuditLogger, csv_dir: str):
    """
    Load all CSV tables listed in CSV_TABLES from the csv_dir.
    Each CSV becomes a table in the raw schema.
    """
    logger.info("=" * 60)
    logger.info(f"Loading CSV tables from: {csv_dir}")
    logger.info(f"Expected tables: {len(CSV_TABLES)}")
    logger.info("=" * 60)

    tables_loaded = 0
    tables_failed = 0
    tables_skipped = 0
    total_records = 0

    with AuditedJob(
        audit_logger,
        job_name="Load CSV Tables to Raw Schema",
        job_type="LOAD",
        pipeline_stage="DATA_UNIFICATION",
        parameters={
            'source_dir': csv_dir,
            'source_format': 'CSV',
            'expected_tables': len(CSV_TABLES),
            'load_timestamp': datetime.now().isoformat()
        }
    ) as job:

        for table_name in CSV_TABLES:
            csv_path = find_csv_file(csv_dir, table_name)

            if csv_path is None:
                logger.warning(f"  ⚠ CSV not found for: {table_name} — skipping")
                tables_skipped += 1
                continue

            try:
                logger.info(f"Loading CSV: {os.path.basename(csv_path)} → raw.{table_name.lower()}")

                df = pd.read_csv(csv_path, low_memory=False, on_bad_lines='warn')
                record_count = len(df)

                # Clean column names (lowercase for PostgreSQL)
                df.columns = [col.lower().strip() for col in df.columns]

                # Write to raw schema
                pg_table = table_name.lower()
                df.to_sql(
                    name=pg_table,
                    schema=SCHEMA_RAW,
                    con=engine,
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=1000
                )

                logger.info(f"  ✓ {record_count} records → raw.{pg_table}")
                total_records += record_count
                tables_loaded += 1

                # Track counts via AuditedJob
                job.increment_processed(record_count)
                job.increment_success(record_count)

                # Log lineage: CSV file → raw table
                audit_logger.log_lineage(
                    source_table=os.path.basename(csv_path),
                    target_table=f"{SCHEMA_RAW}.{pg_table}",
                    operation_type='LOAD',
                    record_count=record_count,
                    transformation_name='csv_to_raw',
                    metadata={'source_format': 'CSV'}
                )

            except Exception as e:
                logger.error(f"  ✗ Failed to load {table_name}: {e}")
                tables_failed += 1
                job.increment_failed(1)

    logger.info(f"\nCSV Load Summary: {tables_loaded} loaded, {tables_skipped} skipped, {tables_failed} failed, {total_records} total records")
    return tables_loaded, tables_failed, tables_skipped


def load_excel_lookup_tables(engine, audit_logger: AuditLogger, excel_file: str):
    """
    Load static lookup/reference tables from the original Excel file.
    Only loads sheets listed in EXCEL_LOOKUP_TABLES.
    """
    logger.info("=" * 60)
    logger.info(f"Loading Excel lookup tables from: {excel_file}")
    logger.info(f"Tables: {EXCEL_LOOKUP_TABLES}")
    logger.info("=" * 60)

    if not os.path.exists(excel_file):
        logger.error(f"Excel file not found: {excel_file}")
        logger.error("Lookup tables will NOT be loaded. Unification may fail.")
        return 0, len(EXCEL_LOOKUP_TABLES), 0

    tables_loaded = 0
    tables_failed = 0
    total_records = 0

    with AuditedJob(
        audit_logger,
        job_name="Load Excel Lookup Tables to Raw Schema",
        job_type="LOAD",
        pipeline_stage="DATA_UNIFICATION",
        parameters={
            'source_file': excel_file,
            'source_format': 'EXCEL',
            'tables': EXCEL_LOOKUP_TABLES,
            'load_timestamp': datetime.now().isoformat()
        }
    ) as job:

        excel_file_obj = pd.ExcelFile(excel_file)
        available_sheets = excel_file_obj.sheet_names

        for table_name in EXCEL_LOOKUP_TABLES:
            # Case-insensitive sheet match
            matched_sheet = None
            for sheet in available_sheets:
                if sheet.lower() == table_name.lower():
                    matched_sheet = sheet
                    break

            if matched_sheet is None:
                logger.warning(f"  ⚠ Sheet '{table_name}' not found in Excel — skipping")
                tables_failed += 1
                job.increment_failed(1)
                continue

            try:
                logger.info(f"Loading sheet: {matched_sheet} → raw.{table_name.lower()}")

                df = pd.read_excel(excel_file, sheet_name=matched_sheet)
                record_count = len(df)

                # Clean column names
                df.columns = [col.lower().strip() for col in df.columns]

                pg_table = table_name.lower()
                df.to_sql(
                    name=pg_table,
                    schema=SCHEMA_RAW,
                    con=engine,
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=1000
                )

                logger.info(f"  ✓ {record_count} records → raw.{pg_table}")
                total_records += record_count
                tables_loaded += 1

                job.increment_processed(record_count)
                job.increment_success(record_count)

                # Log lineage: Excel sheet → raw table
                audit_logger.log_lineage(
                    source_table=f"EXCEL:{matched_sheet}",
                    target_table=f"{SCHEMA_RAW}.{pg_table}",
                    operation_type='LOAD',
                    record_count=record_count,
                    transformation_name='excel_to_raw',
                    metadata={'source_format': 'EXCEL', 'sheet_name': matched_sheet}
                )

            except Exception as e:
                logger.error(f"  ✗ Failed to load {table_name}: {e}")
                tables_failed += 1
                job.increment_failed(1)

    logger.info(f"\nExcel Lookup Summary: {tables_loaded} loaded, {tables_failed} failed, {total_records} total records")
    return tables_loaded, tables_failed, 0


def verify_load(audit_logger: AuditLogger):
    """Verify that all expected tables exist in the raw schema with data."""
    engine = create_engine(DB_CONN)
    all_expected = [t.lower() for t in CSV_TABLES + EXCEL_LOOKUP_TABLES]

    logger.info(f"\nVerifying {len(all_expected)} tables in raw schema...")

    tables_ok = 0
    tables_missing = []

    with engine.connect() as conn:
        total_records = 0
        for table in all_expected:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA_RAW}.{table}"))
                count = result.fetchone()[0]
                total_records += count
                logger.info(f"  {table}: {count} records")
                tables_ok += 1
            except Exception:
                logger.warning(f"  {table}: MISSING")
                tables_missing.append(table)

    if tables_missing:
        logger.warning(f"\n⚠ Missing tables: {tables_missing}")
    else:
        logger.info(f"\n✓ All {len(all_expected)} tables verified. Total records: {total_records}")

    # Log a quality check for the verification
    try:
        with AuditedJob(
            audit_logger,
            job_name="Post-Load Verification",
            job_type="VERIFY",
            pipeline_stage="DATA_UNIFICATION",
        ) as job:
            audit_logger.log_quality_check(
                job_log_id=job.job_log_id,
                check_name="Raw Table Completeness",
                check_type="COMPLETENESS",
                table_name=f"{SCHEMA_RAW}.*",
                records_checked=len(all_expected),
                records_passed=tables_ok,
                records_failed=len(tables_missing),
                status='PASS' if not tables_missing else 'FAIL',
                actual_value=f"{tables_ok}/{len(all_expected)} tables present"
            )
    except Exception as e:
        logger.warning(f"Could not log verification quality check: {e}")

    return len(tables_missing) == 0


def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description='Load JNE data with audit trail')
    parser.add_argument('--verify', action='store_true', help='Verify existing data load')
    parser.add_argument('--csv-dir', type=str, default=CSV_DIR, help='Directory containing CSV files')
    parser.add_argument('--excel-file', type=str, default=EXCEL_FILE, help='Excel file for lookup tables')
    parser.add_argument('--csv-only', action='store_true', help='Only load CSV tables (skip Excel)')
    parser.add_argument('--excel-only', action='store_true', help='Only load Excel lookup tables (skip CSV)')

    args = parser.parse_args()

    # Initialize
    audit_logger = AuditLogger(DB_CONN)
    engine = create_engine(DB_CONN)
    create_raw_schema(engine)

    logger.info("=" * 60)
    logger.info("JNE Data Loader")
    logger.info(f"CSV dir:    {args.csv_dir}")
    logger.info(f"Excel file: {args.excel_file}")
    logger.info(f"CSV tables expected:    {len(CSV_TABLES)}")
    logger.info(f"Excel lookup tables:    {len(EXCEL_LOOKUP_TABLES)}")
    logger.info(f"Excluded tables:        {EXCLUDED_TABLES}")
    logger.info("=" * 60)

    try:
        if args.verify:
            verify_load(audit_logger)
        else:
            # Load CSV tables
            if not args.excel_only:
                load_csv_tables(engine, audit_logger, args.csv_dir)

            # Load Excel lookup tables
            if not args.csv_only:
                load_excel_lookup_tables(engine, audit_logger, args.excel_file)

            # Verify
            logger.info("\nRunning post-load verification...")
            verify_load(audit_logger)

        logger.info("\n✓ Load process completed successfully")

        # Print audit summary
        health = audit_logger.get_pipeline_health()
        logger.info(f"Pipeline Health: {health}")

    except Exception as e:
        logger.error(f"✗ Process failed: {e}")
        raise


if __name__ == '__main__':
    main()
