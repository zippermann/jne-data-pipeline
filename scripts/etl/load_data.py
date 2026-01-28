"""
JNE Data Pipeline - ETL Script
Loads data from Excel files into PostgreSQL database

Usage:
    python load_data.py [--file PATH] [--schema SCHEMA]
"""

import os
import sys
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
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

def get_db_engine():
    """Create SQLAlchemy database engine."""
    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(connection_string)

def clean_column_name(col_name):
    """Clean column name for PostgreSQL compatibility."""
    col = col_name.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
    return ''.join(c for c in col if c.isalnum() or c == '_')

def load_excel_to_postgres(excel_path, schema='raw', if_exists='replace'):
    """
    Load all sheets from Excel file into PostgreSQL tables.
    
    Args:
        excel_path: Path to Excel file
        schema: Database schema to load into
        if_exists: How to handle existing tables ('replace', 'append', 'fail')
    """
    logger.info(f"Loading data from: {excel_path}")
    
    engine = get_db_engine()
    xlsx = pd.ExcelFile(excel_path)
    
    total_sheets = len(xlsx.sheet_names)
    total_rows = 0
    
    logger.info(f"Found {total_sheets} sheets to process")
    
    for i, sheet_name in enumerate(xlsx.sheet_names, 1):
        try:
            logger.info(f"[{i}/{total_sheets}] Processing: {sheet_name}")
            
            # Read sheet
            df = pd.read_excel(xlsx, sheet_name=sheet_name)
            
            # Clean column names
            df.columns = [clean_column_name(col) for col in df.columns]
            
            # Add audit columns
            df['created_at'] = datetime.now()
            df['updated_at'] = datetime.now()
            
            # Table name in lowercase
            table_name = sheet_name.lower()
            
            # Load to PostgreSQL
            df.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )
            
            rows_loaded = len(df)
            total_rows += rows_loaded
            logger.info(f"    ✓ Loaded {rows_loaded} rows into {schema}.{table_name}")
            
        except Exception as e:
            logger.error(f"    ✗ Failed to load {sheet_name}: {str(e)}")
            continue
    
    logger.info("=" * 50)
    logger.info(f"ETL Complete!")
    logger.info(f"Total sheets processed: {total_sheets}")
    logger.info(f"Total rows loaded: {total_rows:,}")
    
    return total_sheets, total_rows

def verify_data_load(schema='raw'):
    """Verify data was loaded correctly by checking row counts."""
    engine = get_db_engine()
    
    query = text(f"""
        SELECT 
            table_name,
            (xpath('/row/cnt/text()', xml_count))[1]::text::int AS row_count
        FROM (
            SELECT 
                table_name, 
                query_to_xml(
                    format('SELECT COUNT(*) AS cnt FROM %I.%I', '{schema}', table_name),
                    false, true, ''
                ) AS xml_count
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
        ) t
        ORDER BY row_count DESC;
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
    
    logger.info("\n" + "=" * 50)
    logger.info("Data Verification - Row Counts:")
    logger.info("=" * 50)
    
    for table_name, row_count in rows:
        logger.info(f"  {table_name}: {row_count:,} rows")

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='JNE Data Pipeline ETL')
    parser.add_argument(
        '--file', '-f',
        default='data/raw/JNE_RAW_COMBINED.xlsx',
        help='Path to Excel file'
    )
    parser.add_argument(
        '--schema', '-s',
        default='raw',
        help='Database schema (default: raw)'
    )
    parser.add_argument(
        '--mode', '-m',
        choices=['replace', 'append'],
        default='replace',
        help='How to handle existing data'
    )
    parser.add_argument(
        '--verify', '-v',
        action='store_true',
        help='Verify data after loading'
    )
    
    args = parser.parse_args()
    
    # Check if file exists
    if not os.path.exists(args.file):
        logger.error(f"File not found: {args.file}")
        logger.info("Please ensure the Excel file is in the correct location.")
        sys.exit(1)
    
    # Run ETL
    sheets, rows = load_excel_to_postgres(
        args.file,
        schema=args.schema,
        if_exists=args.mode
    )
    
    # Verify if requested
    if args.verify:
        verify_data_load(args.schema)
    
    logger.info("\nDone! You can now query the data in DBeaver or pgAdmin.")

if __name__ == '__main__':
    main()
