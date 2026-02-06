"""
JNE CSV Data Loader with Audit Trail
Loads multiple CSV files to PostgreSQL, preserving reference tables

Usage:
    python load_csv_data.py --csv-dir /path/to/csv/files
    python load_csv_data.py --csv-dir /path/to/csv/files --preserve-refs
    python load_csv_data.py --csv-dir /path/to/csv/files --host localhost
"""

import sys
import os
import argparse
import glob
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Tables to preserve (reference tables not in new data)
PRESERVE_TABLES = ['ora_zone', 'ora_user', 'mdt_city_origin', 'crossdock']


def get_connection_string(host='localhost', port='5432', database='jne_dashboard', 
                          user='jne_user', password='jne_secure_password_2024'):
    """Build database connection string"""
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def backup_reference_tables(engine):
    """Backup reference tables before loading new data"""
    logger.info("Backing up reference tables...")
    backed_up = []
    
    with engine.begin() as conn:
        for table in PRESERVE_TABLES:
            try:
                # Check if table exists
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


def load_csv_files(csv_dir, engine, preserve_refs=True):
    """Load all CSV files from directory to PostgreSQL"""
    
    # Create raw schema
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
    logger.info("✓ Raw schema ready")
    
    # Backup reference tables if needed
    backed_up = []
    if preserve_refs:
        backed_up = backup_reference_tables(engine)
    
    # Find all CSV files
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    
    if not csv_files:
        logger.error(f"No CSV files found in {csv_dir}")
        return None
    
    logger.info(f"Found {len(csv_files)} CSV files to load")
    
    # Load statistics
    total_files = len(csv_files)
    files_loaded = 0
    files_failed = 0
    total_records = 0
    results = []
    
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        # Table name: remove .csv, lowercase
        table_name = os.path.splitext(filename)[0].lower()
        
        try:
            logger.info(f"Loading: {filename}")
            
            # Read CSV with various encoding attempts
            df = None
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    df = pd.read_csv(csv_file, encoding=encoding, low_memory=False)
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                raise Exception("Could not decode file with any encoding")
            
            record_count = len(df)
            
            # Clean column names (lowercase, remove special chars)
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
            files_loaded += 1
            total_records += record_count
            
            results.append({
                'file': filename,
                'table': table_name,
                'records': record_count,
                'status': 'SUCCESS'
            })
            
        except Exception as e:
            logger.error(f"  ✗ Failed to load {filename}: {e}")
            files_failed += 1
            results.append({
                'file': filename,
                'table': table_name,
                'records': 0,
                'status': f'FAILED: {str(e)}'
            })
    
    # Restore reference tables
    if preserve_refs and backed_up:
        restore_reference_tables(engine, backed_up)
    
    # Print summary
    logger.info("=" * 60)
    logger.info("LOAD SUMMARY")
    logger.info(f"Total CSV files: {total_files}")
    logger.info(f"Successfully loaded: {files_loaded}")
    logger.info(f"Failed: {files_failed}")
    logger.info(f"Total records loaded: {total_records:,}")
    if preserve_refs:
        logger.info(f"Reference tables preserved: {', '.join(backed_up) if backed_up else 'None'}")
    logger.info("=" * 60)
    
    return {
        'total_files': total_files,
        'files_loaded': files_loaded,
        'files_failed': files_failed,
        'total_records': total_records,
        'details': results
    }


def verify_load(engine):
    """Verify all tables in raw schema"""
    logger.info("Verifying loaded tables...")
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'raw'
            ORDER BY table_name
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
    
    logger.info(f"Total records across all tables: {total_records:,}")


def main():
    parser = argparse.ArgumentParser(description='Load CSV files to PostgreSQL')
    parser.add_argument('--csv-dir', type=str, required=True,
                        help='Directory containing CSV files')
    parser.add_argument('--host', type=str, default='localhost',
                        help='PostgreSQL host (default: localhost, use jne-postgres for Docker)')
    parser.add_argument('--port', type=str, default='5432',
                        help='PostgreSQL port (default: 5432)')
    parser.add_argument('--database', type=str, default='jne_dashboard',
                        help='Database name (default: jne_dashboard)')
    parser.add_argument('--user', type=str, default='jne_user',
                        help='Database user (default: jne_user)')
    parser.add_argument('--password', type=str, default='jne_secure_password_2024',
                        help='Database password')
    parser.add_argument('--preserve-refs', action='store_true', default=True,
                        help='Preserve reference tables (ORA_ZONE, ORA_USER, etc.)')
    parser.add_argument('--no-preserve-refs', action='store_false', dest='preserve_refs',
                        help='Do not preserve reference tables')
    parser.add_argument('--verify', action='store_true',
                        help='Only verify existing data, do not load')
    
    args = parser.parse_args()
    
    # Build connection string
    conn_string = get_connection_string(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password
    )
    
    logger.info(f"Connecting to PostgreSQL at {args.host}:{args.port}/{args.database}")
    
    try:
        engine = create_engine(conn_string)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✓ Database connection successful")
        
        if args.verify:
            verify_load(engine)
        else:
            load_csv_files(args.csv_dir, engine, preserve_refs=args.preserve_refs)
            verify_load(engine)
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
