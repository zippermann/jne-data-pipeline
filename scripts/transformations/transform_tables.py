"""
JNE Data Transformation Pipeline
==================================
Step 2 of the ETL pipeline.
  Phase 1: Unification — joins all raw tables into staging.unified_shipments
  Phase 2: Transformation — applies pandas transformations (date standardization, DQ flags, etc.)

The unification logic lives in an external SQL file (configured in pipeline_config.py).
To change the unification logic, just edit that SQL file — no code changes needed here.

Transformation functions are registered in TRANSFORMATION_REGISTRY at the bottom of this file.
To add/remove/change transformations, edit that registry.
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

# Import config
try:
    from pipeline_config import (
        DB_CONN,
        UNIFICATION_SQL_FILE,
        UNIFIED_TABLE_SCHEMA, UNIFIED_TABLE_NAME,
        SCHEMA_RAW, SCHEMA_STAGING, SCHEMA_TRANSFORMED, SCHEMA_AUDIT,
    )
except ImportError:
    sys.path.insert(0, '/opt/airflow')
    from pipeline_config import (
        DB_CONN,
        UNIFICATION_SQL_FILE,
        UNIFIED_TABLE_SCHEMA, UNIFIED_TABLE_NAME,
        SCHEMA_RAW, SCHEMA_STAGING, SCHEMA_TRANSFORMED, SCHEMA_AUDIT,
    )

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================
# SCHEMA SETUP
# ============================================================

def create_schemas(engine):
    """Create all required schemas."""
    schemas = [SCHEMA_STAGING, SCHEMA_TRANSFORMED, SCHEMA_AUDIT]
    with engine.begin() as conn:
        for schema in schemas:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    logger.info(f"✓ Schemas ready: {schemas}")


# ============================================================
# PHASE 1: UNIFICATION
# ============================================================

def run_unification(engine, sql_file: str = None):
    """
    Execute the unification SQL to create staging.unified_shipments.

    This reads an external SQL file and wraps it in:
        DROP TABLE IF EXISTS staging.unified_shipments CASCADE;
        CREATE TABLE staging.unified_shipments AS
        <your SQL here>;

    To change the unification logic:
      1. Edit the SQL file (default: jne-audit-trail/unify_jne_tables_v3.sql)
      2. Re-run this pipeline step
      That's it — no Python code changes needed.

    Args:
        engine: SQLAlchemy engine
        sql_file: Path to SQL file. Defaults to UNIFICATION_SQL_FILE from config.

    Returns:
        int: Number of rows in the unified table
    """
    sql_file = sql_file or UNIFICATION_SQL_FILE

    logger.info("=" * 60)
    logger.info("PHASE 1: UNIFICATION")
    logger.info(f"SQL file: {sql_file}")
    logger.info("=" * 60)

    if not os.path.exists(sql_file):
        logger.error(f"Unification SQL file not found: {sql_file}")
        logger.error("Cannot create staging.unified_shipments without this file.")
        raise FileNotFoundError(f"SQL file not found: {sql_file}")

    # Read the SQL file
    with open(sql_file, 'r') as f:
        unification_sql = f.read()

    # Wrap in CREATE TABLE AS
    full_sql = f"""
    DROP TABLE IF EXISTS {UNIFIED_TABLE_SCHEMA}.{UNIFIED_TABLE_NAME} CASCADE;

    CREATE TABLE {UNIFIED_TABLE_SCHEMA}.{UNIFIED_TABLE_NAME} AS
    {unification_sql};

    ALTER TABLE {UNIFIED_TABLE_SCHEMA}.{UNIFIED_TABLE_NAME}
        ADD COLUMN IF NOT EXISTS etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    """

    # Execute with raw connection for multi-statement SQL
    with engine.begin() as conn:
        # Set search_path to find raw tables without schema prefix
        conn.execute(text(f"SET search_path TO {SCHEMA_RAW}, public, {SCHEMA_STAGING}"))
        conn.execute(text(full_sql))

        # Get row count
        result = conn.execute(text(
            f"SELECT COUNT(*) FROM {UNIFIED_TABLE_SCHEMA}.{UNIFIED_TABLE_NAME}"
        ))
        row_count = result.fetchone()[0]

    logger.info(f"✓ Created {UNIFIED_TABLE_SCHEMA}.{UNIFIED_TABLE_NAME} with {row_count:,} rows")
    return row_count


# ============================================================
# PHASE 2: TRANSFORMATIONS
# ============================================================
# Each transformation function takes (engine) and returns row_count.
# Register them in TRANSFORMATION_REGISTRY below.

def standardize_dates(df, date_columns):
    """Standardize date columns to datetime format."""
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


def transform_cms_cnote(engine):
    """
    Transform CMS_CNOTE — Main shipment table.
    Standardizes dates and adds data quality flags.
    """
    logger.info("Transforming CMS_CNOTE...")

    query = f"SELECT * FROM {SCHEMA_RAW}.cms_cnote"
    df = pd.read_sql(query, engine)

    logger.info(f"  Loaded {len(df)} rows from {SCHEMA_RAW}.cms_cnote")

    # Standardize dates
    date_cols = [col for col in df.columns
                 if 'date' in col.lower() or col in ['created_at', 'updated_at']]
    df = standardize_dates(df, date_cols)

    # Add data quality flags
    df['dq_has_nulls'] = df.isnull().any(axis=1)
    df['dq_check_date'] = datetime.now()
    df['transformed_at'] = datetime.now()

    # Write to transformed schema
    df.to_sql(
        name='cms_cnote',
        con=engine,
        schema=SCHEMA_TRANSFORMED,
        if_exists='replace',
        index=False,
        chunksize=1000
    )

    logger.info(f"  ✓ {len(df)} rows → {SCHEMA_TRANSFORMED}.cms_cnote")
    return len(df)


# ------------------------------------------------------------------
# ADD YOUR TRANSFORMATION FUNCTIONS HERE
# ------------------------------------------------------------------
# Example:
#
# def transform_cms_manifest(engine):
#     """Transform CMS_MANIFEST table."""
#     df = pd.read_sql(f"SELECT * FROM {SCHEMA_RAW}.cms_manifest", engine)
#     # ... your transformations ...
#     df.to_sql('cms_manifest', con=engine, schema=SCHEMA_TRANSFORMED,
#               if_exists='replace', index=False, chunksize=1000)
#     return len(df)


# ============================================================
# TRANSFORMATION REGISTRY
# ============================================================
# Add or remove entries here to control which transformations run.
# Each entry: ("display_name", function_reference)
# The pipeline runs them in order.

TRANSFORMATION_REGISTRY = [
    ("cms_cnote", transform_cms_cnote),
    # ("cms_manifest", transform_cms_manifest),   # ← uncomment when ready
    # ("cms_cnote_pod", transform_cms_cnote_pod),  # ← add more as needed
]


# ============================================================
# LOGGING
# ============================================================

def log_transformation(engine, table_name, row_count, status):
    """Log a transformation result to the audit schema."""
    try:
        audit_data = {
            'table_name': [table_name],
            'row_count': [row_count],
            'status': [status],
            'timestamp': [datetime.now()]
        }
        df_audit = pd.DataFrame(audit_data)
        df_audit.to_sql(
            name='transformation_log',
            con=engine,
            schema=SCHEMA_AUDIT,
            if_exists='append',
            index=False
        )
    except Exception as e:
        logger.warning(f"  Could not write to audit.transformation_log: {e}")


# ============================================================
# MAIN
# ============================================================

def main():
    """Main transformation pipeline."""
    import argparse

    parser = argparse.ArgumentParser(description='JNE Data Transformation Pipeline')
    parser.add_argument('--skip-unification', action='store_true',
                        help='Skip Phase 1 (unification) and only run transformations')
    parser.add_argument('--unification-only', action='store_true',
                        help='Only run Phase 1 (unification), skip transformations')
    parser.add_argument('--sql-file', type=str, default=UNIFICATION_SQL_FILE,
                        help='Override unification SQL file path')

    args = parser.parse_args()

    engine = create_engine(DB_CONN)
    create_schemas(engine)

    # --- Phase 1: Unification ---
    if not args.skip_unification:
        try:
            row_count = run_unification(engine, args.sql_file)
            log_transformation(engine, f'{UNIFIED_TABLE_SCHEMA}.{UNIFIED_TABLE_NAME}',
                               row_count, 'SUCCESS')
        except Exception as e:
            logger.error(f"✗ Unification failed: {e}")
            log_transformation(engine, f'{UNIFIED_TABLE_SCHEMA}.{UNIFIED_TABLE_NAME}',
                               0, 'FAILED')
            raise

    if args.unification_only:
        logger.info("--unification-only flag set. Skipping transformations.")
        return

    # --- Phase 2: Transformations ---
    logger.info("=" * 60)
    logger.info("PHASE 2: TRANSFORMATIONS")
    logger.info(f"Registered transformations: {len(TRANSFORMATION_REGISTRY)}")
    logger.info("=" * 60)

    for table_name, transform_fn in TRANSFORMATION_REGISTRY:
        try:
            rows = transform_fn(engine)
            log_transformation(engine, table_name, rows, 'SUCCESS')
            logger.info(f"  ✓ {table_name}: {rows} rows transformed")
        except Exception as e:
            logger.error(f"  ✗ {table_name} transformation failed: {e}")
            log_transformation(engine, table_name, 0, 'FAILED')
            # Continue with next transformation instead of crashing the whole pipeline
            continue

    logger.info("=" * 60)
    logger.info("Transformation Pipeline Complete!")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
