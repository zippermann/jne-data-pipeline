"""
Step 2: Data Transformation using Pandas
Standardizes raw data from PostgreSQL
"""

import pandas as pd
import logging
from sqlalchemy import create_engine, text
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection - using Docker network names
DB_CONN = "postgresql://jne_user:jne_secure_password_2024@jne-postgres:5432/jne_dashboard"

def get_engine():
    return create_engine(DB_CONN)

def create_schemas(engine):
    """Create schemas if they don't exist"""
    # SQLAlchemy 2.0 compatible - use with statement
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS transformed"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS audit"))
    logger.info("✓ Schemas created/verified")

def standardize_dates(df, date_columns):
    """Standardize date formats"""
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def transform_cms_cnote(engine):
    """Transform CMS_CNOTE - Main shipment table"""
    logger.info("Transforming CMS_CNOTE...")
    
    query = "SELECT * FROM raw.cms_cnote LIMIT 10000"
    df = pd.read_sql(query, engine)
    
    logger.info(f"Loaded {len(df)} rows from raw.cms_cnote")
    
    # Standardize dates
    date_cols = [col for col in df.columns if 'date' in col.lower() or col in ['created_at', 'updated_at']]
    df = standardize_dates(df, date_cols)
    
    # Add data quality flags
    df['dq_has_nulls'] = df.isnull().any(axis=1)
    df['dq_check_date'] = datetime.now()
    df['transformed_at'] = datetime.now()
    
    # Write to transformed schema
    df.to_sql(
        name='cms_cnote',
        con=engine,
        schema='transformed',
        if_exists='replace',
        index=False,
        chunksize=1000
    )
    
    logger.info(f"✓ Transformed {len(df)} rows to transformed.cms_cnote")
    return len(df)

def log_transformation(engine, table_name, row_count, status):
    """Log transformation to audit trail"""
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
        schema='audit',
        if_exists='append',
        index=False
    )

def main():
    """Main transformation pipeline"""
    logger.info("=" * 50)
    logger.info("Starting Data Transformation (Step 2)")
    logger.info("=" * 50)
    
    engine = get_engine()
    
    # Create schemas
    create_schemas(engine)
    
    # Transform CMS_CNOTE
    try:
        rows = transform_cms_cnote(engine)
        log_transformation(engine, 'cms_cnote', rows, 'SUCCESS')
        logger.info(f"✓ Transformation successful: {rows} rows")
    except Exception as e:
        logger.error(f"✗ Transformation failed: {e}")
        log_transformation(engine, 'cms_cnote', 0, 'FAILED')
        raise
    
    logger.info("=" * 50)
    logger.info("Transformation Complete!")
    logger.info("=" * 50)

if __name__ == '__main__':
    main()