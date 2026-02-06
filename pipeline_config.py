"""
JNE Pipeline Configuration
===========================
Central config for all pipeline scripts.
Edit THIS file when you need to change table lists, paths, or data sources.

Last updated: 2026-02-06
"""

# ============================================================
# DATABASE CONNECTION
# ============================================================
# When running inside Docker (Airflow, ETL containers):
DB_CONN = "postgresql://jne_user:jne_secure_password_2024@jne-postgres:5432/jne_dashboard"

# When running locally outside Docker (e.g. debugging from your machine):
# DB_CONN = "postgresql://jne_user:jne_secure_password_2024@localhost:5432/jne_dashboard"


# ============================================================
# FILE PATHS (inside Docker container)
# ============================================================
# These map to the volume mounts in docker-compose.yml:
#   ./data/raw/csv  → /opt/airflow/data/raw/csv
#   ./data/raw      → /opt/airflow/data/raw

CSV_DIR = "/opt/airflow/data/raw/csv"
EXCEL_FILE = "/opt/airflow/data/raw/JNE_RAW_COMBINED.xlsx"


# ============================================================
# TABLE CLASSIFICATION
# ============================================================
# This is the single source of truth for which tables exist
# and how they are loaded. Edit these lists when tables change.

# Tables loaded from individual CSV files in CSV_DIR.
# The loader expects filenames like: cms_cnote.csv, cms_manifest.csv, etc.
# Filenames are matched case-insensitively.
CSV_TABLES = [
    "CMS_APICUST",
    "CMS_CNOTE",
    "CMS_CNOTE_AMO",
    "CMS_CNOTE_POD",
    "CMS_COST_DTRANSIT_AGEN",
    "CMS_COST_MTRANSIT_AGEN",
    "CMS_DBAG_HO",
    "CMS_DHI_HOC",
    "CMS_DHICNOTE",
    "CMS_DHOCNOTE",
    "CMS_DHOUNDEL_POD",
    "CMS_DHOV_RSHEET",
    "CMS_DMBAG",
    "CMS_DRCNOTE",
    "CMS_DROURATE",
    "CMS_DRSHEET",
    "CMS_DRSHEET_PRA",
    "CMS_DSJ",
    "CMS_DSMU",
    "CMS_DSTATUS",
    "CMS_MANIFEST",
    "CMS_MFCNOTE",
    "CMS_MHI_HOC",
    "CMS_MHICNOTE",
    "CMS_MHOCNOTE",
    "CMS_MHOUNDEL_POD",
    "CMS_MRCNOTE",
    "CMS_MRSHEET",
    "CMS_MSJ",
    "CMS_MSMU",
    "CMS_RDSJ",
    "LASTMILE_COURIER",
    "T_GOTO",
]

# Static lookup/reference tables loaded from the original Excel file.
# These sheets exist in JNE_RAW_COMBINED.xlsx and rarely change.
EXCEL_LOOKUP_TABLES = [
    "ORA_ZONE",
    "ORA_USER",
    "T_MDT_CITY_ORIGIN",
]

# Tables explicitly excluded from the pipeline.
# Listed here for documentation — the loader will skip these.
EXCLUDED_TABLES = [
    "T_CROSSDOCK_AWD",  # Removed from scope per project decision
]


# ============================================================
# UNIFICATION CONFIG
# ============================================================
# Path to the SQL file that joins all raw tables into staging.unified_shipments.
# When you revise the unification logic, just update the SQL file — no code change needed.

UNIFICATION_SQL_FILE = "/opt/airflow/jne-audit-trail/unify_jne_tables_v3.sql"

# Target table for the unification output
UNIFIED_TABLE_SCHEMA = "staging"
UNIFIED_TABLE_NAME = "unified_shipments"


# ============================================================
# SCHEMAS
# ============================================================
SCHEMA_RAW = "raw"
SCHEMA_STAGING = "staging"
SCHEMA_TRANSFORMED = "transformed"
SCHEMA_AUDIT = "audit"
