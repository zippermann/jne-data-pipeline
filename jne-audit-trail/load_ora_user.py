"""
Load ORA_USER table from JNE_RAW_COMBINED.xlsx into PostgreSQL
==============================================================

Usage:
    python load_ora_user.py
    python load_ora_user.py --excel-file "C:\path\to\JNE_RAW_COMBINED.xlsx"

Requirements:
    pip install pandas openpyxl psycopg2-binary sqlalchemy python-dotenv
"""

import os
import sys
import argparse
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

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


def load_ora_user(excel_file: str, schema: str = 'raw'):
    """Load ORA_USER sheet from Excel into PostgreSQL."""
    
    print("=" * 60)
    print("Loading ORA_USER into PostgreSQL")
    print("=" * 60)
    
    # Check file exists
    if not os.path.exists(excel_file):
        print(f"ERROR: File not found: {excel_file}")
        sys.exit(1)
    
    print(f"Reading from: {excel_file}")
    
    # Read Excel file
    try:
        xlsx = pd.ExcelFile(excel_file)
        print(f"Sheets found: {len(xlsx.sheet_names)}")
        
        # Check if ORA_USER exists
        if 'ORA_USER' not in xlsx.sheet_names:
            print("\nERROR: ORA_USER sheet not found!")
            print("Available sheets:")
            for s in xlsx.sheet_names:
                print(f"  - {s}")
            sys.exit(1)
        
        # Read ORA_USER sheet
        df = pd.read_excel(xlsx, sheet_name='ORA_USER')
        print(f"\nORA_USER loaded: {len(df)} rows, {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
    except Exception as e:
        print(f"ERROR reading Excel: {e}")
        sys.exit(1)
    
    # Clean column names (lowercase for PostgreSQL)
    df.columns = [col.lower().strip() for col in df.columns]
    print(f"\nCleaned columns: {list(df.columns)}")
    
    # Connect to PostgreSQL
    conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    
    try:
        engine = create_engine(conn_string)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print(f"\nConnected to PostgreSQL: {DB_CONFIG['database']}")
        
        # Create schema if not exists
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.commit()
            print(f"Schema '{schema}' ready")
        
        # Load data
        table_name = 'ora_user'
        print(f"\nLoading into {schema}.{table_name}...")
        
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='replace',  # Replace if exists
            index=False,
            method='multi',
            chunksize=1000
        )
        
        # Verify
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
            count = result.scalar()
            print(f"\n✓ Successfully loaded {count} rows into {schema}.{table_name}")
        
        # Show sample
        print("\nSample data (first 3 rows):")
        print(df.head(3).to_string())
        
        print("\n" + "=" * 60)
        print("ORA_USER loaded successfully!")
        print("=" * 60)
        print("\nNext steps:")
        print("  1. Run: python run_etl_with_audit_v3.py --dry-run")
        print("  2. Run: python run_etl_with_audit_v3.py --sql-file unify_jne_tables_v3.sql")
        
    except Exception as e:
        print(f"ERROR connecting to database: {e}")
        print("\nCheck your .env file or DB_CONFIG settings:")
        print(f"  Host: {DB_CONFIG['host']}")
        print(f"  Port: {DB_CONFIG['port']}")
        print(f"  Database: {DB_CONFIG['database']}")
        print(f"  User: {DB_CONFIG['user']}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Load ORA_USER into PostgreSQL')
    parser.add_argument('--excel-file', type=str, 
                        default='JNE_RAW_COMBINED.xlsx',
                        help='Path to JNE_RAW_COMBINED.xlsx')
    parser.add_argument('--schema', type=str, default='raw',
                        help='Target schema (default: raw)')
    
    args = parser.parse_args()
    load_ora_user(args.excel_file, args.schema)


if __name__ == '__main__':
    main()
