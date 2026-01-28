# JNE Audit Trail System

A complete audit trail implementation for the JNE data pipeline, designed to track ETL executions, data lineage, errors, and data quality metrics.

---

## 📁 Files Overview

| File | Description |
|------|-------------|
| `01-create-audit-schema.sql` | Creates audit schema, tables, functions, and views |
| `02-run-unification-with-audit.sql` | SQL wrapper template for running unification with audit |
| `03-populate-data-lineage.sql` | Populates data lineage table with column mappings |
| `run_etl_with_audit.py` | Python script for automated ETL with full audit trail |

---

## 🚀 Quick Start

### Step 1: Create Audit Schema

Run this in your PostgreSQL database (DBeaver, psql, or pgAdmin):

```sql
-- Connect to your database
\c jne_dashboard

-- Run the schema creation script
\i 01-create-audit-schema.sql
```

### Step 2: Populate Data Lineage

```sql
\i 03-populate-data-lineage.sql
```

### Step 3: Run ETL with Audit

**Option A: Using Python (Recommended)**
```bash
# Install dependencies
pip install psycopg2-binary python-dotenv

# Run ETL
python run_etl_with_audit.py --sql-file unify_jne_tables_v2.sql --triggered-by MANUAL

# Dry run (stats only)
python run_etl_with_audit.py --dry-run
```

**Option B: Using SQL wrapper**
```sql
-- Edit 02-run-unification-with-audit.sql
-- Insert your unification query where indicated
\i 02-run-unification-with-audit.sql
```

---

## 📊 Audit Trail Components

### 1. ETL Job Log (`audit.etl_job_log`)
Tracks each ETL execution:
- Job ID, name, status (RUNNING/SUCCESS/FAILED)
- Start/end timestamps, duration
- Row counts: source, target, inserted, updated, deleted
- Error messages if failed
- Triggered by (MANUAL/AIRFLOW/SCHEDULER)

### 2. Source Statistics (`audit.etl_source_stats`)
Per-table statistics for each job:
- Total rows before and after deduplication
- Duplicates removed count
- Join key column
- Match rate percentage

### 3. Data Lineage (`audit.data_lineage`)
Column-level lineage documentation:
- Target column → Source table/column
- Join path from CMS_CNOTE
- Transformations applied
- Business descriptions

### 4. Error Log (`audit.error_log`)
Captures errors and warnings:
- Severity level (DEBUG/INFO/WARNING/ERROR/CRITICAL)
- Source table and affected records
- Resolution status and notes

### 5. Data Quality Metrics (`audit.data_quality_metrics`)
Per-run quality scores:
- Completeness (% non-null required fields)
- Accuracy (valid values)
- Uniqueness (no duplicates)
- Overall DQ score

---

## 📋 Useful Queries

### View Latest Jobs
```sql
SELECT * FROM audit.v_latest_jobs;
```

### View Source Table Stats (Latest Run)
```sql
SELECT * FROM audit.v_latest_source_stats;
```

### View Open Errors
```sql
SELECT * FROM audit.v_open_errors;
```

### View Data Quality Trend
```sql
SELECT * FROM audit.v_dq_trend;
```

### Daily Job Summary
```sql
SELECT * FROM audit.v_daily_summary;
```

### Find Column Lineage
```sql
SELECT * FROM audit.data_lineage 
WHERE target_column ILIKE '%manifest%';
```

### Job Performance Over Time
```sql
SELECT 
    DATE(started_at) as run_date,
    COUNT(*) as runs,
    ROUND(AVG(duration_seconds), 2) as avg_duration,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count
FROM audit.etl_job_log
GROUP BY DATE(started_at)
ORDER BY run_date DESC;
```

---

## 🔧 Integration with Airflow

Example DAG snippet:

```python
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'jne_data_team',
    'retries': 1,
}

with DAG(
    'jne_daily_unification',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # 6 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    run_etl = PythonOperator(
        task_id='run_unification_etl',
        python_callable=run_etl_with_audit,
        op_kwargs={
            'sql_file': '/opt/airflow/dags/sql/unify_jne_tables_v2.sql',
            'triggered_by': 'AIRFLOW'
        }
    )
```

---

## 📈 Dashboard Metrics

The audit trail provides data for these dashboard metrics:

| Metric | Source |
|--------|--------|
| ETL Success Rate | `audit.v_daily_summary` |
| Avg Processing Time | `audit.etl_job_log.duration_seconds` |
| Data Quality Score | `audit.data_quality_metrics.overall_dq_score` |
| Duplicates Removed | `audit.etl_source_stats.duplicates_removed` |
| Records Processed | `audit.etl_job_log.target_rows_count` |
| Error Count | `audit.error_log` (count where OPEN) |

---

## 🔄 Maintenance

### Archive Old Audit Data
```sql
-- Archive jobs older than 90 days
INSERT INTO audit.etl_job_log_archive
SELECT * FROM audit.etl_job_log
WHERE started_at < CURRENT_DATE - INTERVAL '90 days';

DELETE FROM audit.etl_job_log
WHERE started_at < CURRENT_DATE - INTERVAL '90 days';
```

### Resolve Errors
```sql
UPDATE audit.error_log
SET resolution_status = 'RESOLVED',
    resolved_by = 'your_username',
    resolved_at = CURRENT_TIMESTAMP,
    resolution_notes = 'Fixed by updating source data'
WHERE error_id = 123;
```

---

## 📝 Notes

- All 34 source tables are tracked
- Deduplication stats show how many duplicates are removed from each table
- Data lineage documents the join path from CMS_CNOTE to each source table
- DQ metrics focus on completeness and uniqueness (extend as needed)
- Error resolution workflow: OPEN → IN_PROGRESS → RESOLVED/IGNORED

---

## 🏗️ Schema Diagram

```
audit.etl_job_log (1)
    │
    ├──< audit.etl_source_stats (many per job)
    │
    ├──< audit.error_log (many per job)
    │
    ├──< audit.data_quality_metrics (1 per job)
    │
    └──< audit.data_change_log (many per job)

audit.data_lineage (standalone reference table)
```

---

## 📞 Support

For questions about this audit trail system, contact the JNE Data Engineering team.
