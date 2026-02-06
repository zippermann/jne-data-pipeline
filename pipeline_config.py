"""
JNE Pipeline Configuration
===========================
Central config for all pipeline scripts.
Edit THIS file when you need to change table lists, paths, or data sources.

Last updated: 2026-02-06
Metadata source: (JNE) Column Business Metadata.xlsx
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

# Tables explicitly excluded from the pipeline (not loaded at all).
# Listed here for documentation — the loader will skip these.
EXCLUDED_TABLES = [
    "T_CROSSDOCK_AWD",  # No metadata provided; removed from scope
]

# Tables loaded into raw schema but excluded from unification.
# These are kept in raw for reference/debugging but do not appear in
# staging.unified_shipments.
EXCLUDED_FROM_UNIFICATION = [
    "CMS_MHI_HOC",   # All columns excluded per metadata review
    "CMS_DHI_HOC",   # Parent CMS_MHI_HOC excluded; table removed from join chain
]


# ============================================================
# UNIFICATION CONFIG
# ============================================================
# Path to the SQL file that joins all raw tables into staging.unified_shipments.
# When you revise the unification logic, just update the SQL file — no code change needed.

UNIFICATION_SQL_FILE = "/opt/airflow/jne-audit-trail/unify_jne_tables_v4.sql"

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


# ============================================================
# COLUMN METADATA — derived from (JNE) Column Business Metadata.xlsx
# ============================================================
# Red-coloured columns in the metadata spreadsheet are excluded.
# Only columns listed here are selected during unification.

METADATA_FILE = "/opt/airflow/data/raw/(JNE) Column Business Metadata.xlsx"

# Columns to EXCLUDE per table (red in metadata).
# Tables not listed here keep all their columns.
EXCLUDED_COLUMNS = {
    "CMS_APICUST": [
        "APICUST_REQID", "APICUST_FLAG",
    ],
    "CMS_CNOTE": [
        "CNOTE_AE_ID", "CNOTE_PICKUP_NO", "CNOTE_POD_DATE",
        "CNOTE_POD_RECEIVER", "CNOTE_POD_CODE", "CNOTE_DIM",
        "CNOTE_DELIVERY_NAME", "CNOTE_DELIVERY_ADDR1",
        "CNOTE_DELIVERY_ADDR2", "CNOTE_DELIVERY_ADDR3",
        "CNOTE_DELIVERY_CITY", "CNOTE_DELIVERY_ZIP",
        "CNOTE_DELIVERY_REGION", "CNOTE_DELIVERY_COUNTRY",
        "CNOTE_DELIVERY_CONTACT", "CNOTE_DELIVERY_PHONE",
        "CNOTE_DELIVERY_TYPE", "CNOTE_COMMISION", "CNOTE_MANIFESTED",
        "CNOTE_INVOICED", "CNOTE_DELIVERED", "CNOTE_INBOUND",
        "CNOTE_HOLDIT", "CNOTE_HANDLING", "CNOTE_MGTFEE", "CNOTE_QRC",
        "CNOTE_QUICK", "CNOTE_VERIFIED", "CNOTE_VDATE", "CNOTE_VUSER",
        "CNOTE_RDATE", "CNOTE_RUSER", "CNOTE_RECEIVED", "CNOTE_LUID",
        "CNOTE_LDATE", "CNOTE_EDIT", "CNOTE_BILL_STATUS",
        "CNOTE_MANIFEST_NO", "CNOTE_RUNSHEET_NO", "CNOTE_DO",
        "CNOTE_BANK", "CNOTE_TRANS", "CNOTE_EUSER", "CNOTE_ACT_WEIGHT",
        "CNOTE_SHIPPER_ADDR4", "CNOTE_RECEIVER_ADDR4", "CNOTE_SMS",
        "CNOTE_CARD_AMOUNT", "CNOTE_CARD_DISC", "STATUS_1", "STATUS",
        "CNOTE_CTC", "CNOTE_YES_CANCEL", "CNOTE_VAT",
        "CNOTE_VAT_AMOUNT", "CNOTE_TYPE_CUST", "CNOTE_PROTECT_ID",
        "CNOTE_ZIP_SEQ",
    ],
    "CMS_DROURATE": [
        "DROURATE_ZONE", "DROURATE_TRANSIT", "DROURATE_DELIVERY",
        "DROURATE_LINEHAUL", "DROURATE_ADDCOST",
        "DROURATE_DELIVERY_NEXT", "DROURATE_WAREHOUSE",
        "DROURATE_UID", "DROURATE_LINEHAUL_NEXT",
    ],
    "CMS_MRCNOTE": [
        "MRCNOTE_AE_ID", "MRCNOTE_REFNO", "MRCNOTE_TYPE",
    ],
    "CMS_DRCNOTE": [
        "DRCNOTE_FLAG", "DRCNOTE_DO",
    ],
    "CMS_MANIFEST": [
        "MANIFEST_RECALL_NO", "MANIFEST_USER_AUDIT",
        "MANIFEST_TIME_AUDIT", "MANIFEST_FORM_AUDIT", "MANIFEST_MODA",
    ],
    "CMS_MFCNOTE": [
        "MFCNOTE_COST", "MFCNOTE_MGTFEE", "MFCNOTE_TRANSIT",
        "MFCNOTE_DELIVERY", "MFCNOTE_LINEHAUL", "MFCNOTE_ADDCOST",
        "MFCNOTE_FLAG", "MFCNOTE_HANDLING", "MFCNOTE_DESCRIPTION",
        "MFCNOTE_USER_AUDIT", "MFCNOTE_TIME_AUDIT",
        "MFCNOTE_FORM_AUDIT", "MFCNOTE_MAN_CODE", "MFCNOTE_MAN_DATE",
        "MFCNOTE_ORIGIN",
    ],
    "CMS_DSTATUS": [
        "DSTATUS_BAG_NO_NEW",
    ],
    "CMS_DMBAG": [
        "DMBAG_SPS", "DMBAG_INBOUND", "DMBAG_TYPE", "DMBAG_STATUS",
    ],
    "CMS_MSMU": [
        "MSMU_TYPE",
    ],
    "CMS_DSMU": [
        "DSMU_BAG_CANCEL", "DSMU_SPS", "DSMU_INBOUND",
        "DSMU_MANIFEST_NO",
    ],
    "CMS_DRSHEET_PRA": [
        "DRSHEET_DATE", "DRSHEET_STATUS", "DRSHEET_RECEIVER",
        "DRSHEET_FLAG", "DRSHEET_UID", "DRSHEET_UDATE", "DRSHEET_ZONE",
    ],
    "CMS_MRSHEET": [
        "MRSHEET_UDATE_DR", "MRSHEET_INB_APP_DR", "MRSHEET_INB_UID_DR",
        "MRSHEET_INB_UDATE_DR", "MRSHEET_ORIGIN",
    ],
    "LASTMILE_COURIER": [
        "COURIER_PHONE", "COURIER_EMAIL", "COURIER_PASSWORD",
        "COURIER_NIK", "PARENT_COURIER_ID", "COURIER_CUST_ID",
        "COURIER_VACANT1", "COURIER_VACANT2", "COURIER_VACANT3",
        "COURIER_VACANT4", "COURIER_VACANT5", "COURIER_VACANT6",
    ],
    "CMS_DHOV_RSHEET": [
        "DHOV_RSHEET_REDEL", "DHOV_RSHEET_OUTS",
        "DCSUNDEL_RSHEET_RSHEETNO", "DHOV_DRSHEET_DATE",
        "DHOV_DRSHEET_RECEIVER", "DHOV_DRSHEET_FLAG",
        "DHOV_DRSHEET_UID", "DHOV_DRSHEET_UDATE",
    ],
    "CMS_MHOUNDEL_POD": [
        "MHOUNDEL_FLOW",
    ],
    "CMS_DHOUNDEL_POD": [
        "DHOUNDEL_SEQ_NO", "DHOUNDEL_WEIGHT", "DHOUNDEL_SERVICE",
        "DHOUNDEL_DEST", "DHOUNDEL_GOODS",
    ],
    "CMS_MHOCNOTE": [
        "MHOCNOTE_ORIGIN", "MHOCNOTE_SERVICES", "MHOCNOTE_PRODUCT",
        "MHOCNOTE_CUST", "MHOCNOTE_USER_SCO", "MHOCNOTE_HVS",
        "MHOCNOTE_TYPE",
    ],
    "CMS_DHOCNOTE": [
        "DHOCNOTE_SEQ_NO", "DHOCNOTE_INBOUND", "DHOCNOTE_WEIGHT",
        "DHOCNOTE_SERVICE", "DHOCNOTE_DEST", "DHOCNOTE_GOODS",
        "DHOCNOTE_HANDLING",
    ],
    "CMS_COST_MTRANSIT_AGEN": [
        "MANIFEST_COST", "REAL_COST", "MANIFEST_TYPE",
    ],
    "CMS_COST_DTRANSIT_AGEN": [
        "CNOTE_COST",
    ],
    "CMS_MSJ": [
        "MSJ_ARMADA", "MSJ_CHAR1", "MSJ_CHAR2", "MSJ_CHAR3",
    ],
    "CMS_DHICNOTE": [
        "DHICNOTE_SEQ_NO",
    ],
    "ORA_USER": [
        "USER_CUST_ID", "USER_CUST_NAME",
    ],
}
