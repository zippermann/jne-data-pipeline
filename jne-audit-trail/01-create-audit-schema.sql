-- ============================================================
-- JNE Data Pipeline - Audit Trail System
-- ============================================================
-- Based on unify_jne_tables_v2.sql (34 source tables)
-- 
-- This script creates:
--   1. audit schema and tables
--   2. Functions for logging ETL jobs
--   3. Triggers for change tracking
--   4. Views for reporting/dashboards
-- ============================================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS staging;

-- ============================================================
-- TABLE 1: ETL Job Log
-- Tracks each execution of the unification process
-- ============================================================
DROP TABLE IF EXISTS audit.etl_job_log CASCADE;
CREATE TABLE audit.etl_job_log (
    job_id              SERIAL PRIMARY KEY,
    job_name            VARCHAR(100) NOT NULL,
    job_type            VARCHAR(50) DEFAULT 'UNIFICATION',
    status              VARCHAR(20) NOT NULL,               -- RUNNING, SUCCESS, FAILED, WARNING
    started_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at        TIMESTAMP,
    duration_seconds    NUMERIC(10,2),
    
    -- Row counts
    source_tables_count INTEGER DEFAULT 34,
    source_rows_total   BIGINT,
    target_rows_count   BIGINT,
    rows_inserted       BIGINT DEFAULT 0,
    rows_updated        BIGINT DEFAULT 0,
    rows_deleted        BIGINT DEFAULT 0,
    rows_rejected       BIGINT DEFAULT 0,
    duplicates_removed  BIGINT DEFAULT 0,
    
    -- Execution context
    triggered_by        VARCHAR(100) DEFAULT 'MANUAL',      -- MANUAL, AIRFLOW, SCHEDULER
    server_host         VARCHAR(255),
    database_name       VARCHAR(100),
    
    -- Error handling
    error_message       TEXT,
    error_code          VARCHAR(20),
    
    notes               TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_etl_job_started ON audit.etl_job_log(started_at DESC);
CREATE INDEX idx_etl_job_status ON audit.etl_job_log(status);

-- ============================================================
-- TABLE 2: Source Table Statistics
-- Row counts and stats from each of the 34 source tables
-- ============================================================
DROP TABLE IF EXISTS audit.etl_source_stats CASCADE;
CREATE TABLE audit.etl_source_stats (
    stat_id             SERIAL PRIMARY KEY,
    job_id              INTEGER REFERENCES audit.etl_job_log(job_id) ON DELETE CASCADE,
    source_table        VARCHAR(100) NOT NULL,
    
    -- Counts
    rows_total          BIGINT DEFAULT 0,
    rows_after_dedup    BIGINT DEFAULT 0,
    rows_joined         BIGINT DEFAULT 0,          -- Successfully joined to CNOTE
    rows_unmatched      BIGINT DEFAULT 0,          -- No match (NULL after LEFT JOIN)
    duplicates_removed  BIGINT DEFAULT 0,
    null_key_count      BIGINT DEFAULT 0,
    
    -- Join information
    join_key_column     VARCHAR(100),
    join_type           VARCHAR(20) DEFAULT 'LEFT JOIN',
    dedup_column        VARCHAR(100),              -- Column used for ROW_NUMBER()
    
    -- Date range in source
    min_record_date     TIMESTAMP,
    max_record_date     TIMESTAMP,
    
    captured_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_source_stats_job ON audit.etl_source_stats(job_id);
CREATE INDEX idx_source_stats_table ON audit.etl_source_stats(source_table);

-- ============================================================
-- TABLE 3: Data Lineage
-- Maps source columns to unified table columns
-- ============================================================
DROP TABLE IF EXISTS audit.data_lineage CASCADE;
CREATE TABLE audit.data_lineage (
    lineage_id          SERIAL PRIMARY KEY,
    target_table        VARCHAR(100) DEFAULT 'unified_shipments',
    target_column       VARCHAR(100) NOT NULL,
    source_table        VARCHAR(100) NOT NULL,
    source_column       VARCHAR(100) NOT NULL,
    source_alias        VARCHAR(20),               -- Alias used in SQL (cn, pod, drc, etc.)
    
    -- Join path from CMS_CNOTE to this source
    join_path           TEXT,
    join_sequence       INTEGER,                   -- Order in join chain (1-34)
    
    -- Transformation applied
    transformation      VARCHAR(500),
    data_type           VARCHAR(50),
    is_key_column       BOOLEAN DEFAULT FALSE,
    is_nullable         BOOLEAN DEFAULT TRUE,
    
    description         TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_lineage_target ON audit.data_lineage(target_column);
CREATE INDEX idx_lineage_source ON audit.data_lineage(source_table);

-- ============================================================
-- TABLE 4: Error Log
-- Captures errors, warnings, and exceptions during ETL
-- ============================================================
DROP TABLE IF EXISTS audit.error_log CASCADE;
CREATE TABLE audit.error_log (
    error_id            SERIAL PRIMARY KEY,
    job_id              INTEGER REFERENCES audit.etl_job_log(job_id) ON DELETE SET NULL,
    error_timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_severity      VARCHAR(20) NOT NULL,      -- DEBUG, INFO, WARNING, ERROR, CRITICAL
    error_code          VARCHAR(50),
    error_message       TEXT NOT NULL,
    error_detail        TEXT,
    
    -- Context
    source_table        VARCHAR(100),
    source_column       VARCHAR(100),
    affected_record_id  VARCHAR(100),              -- CNOTE_NO if applicable
    affected_row_count  BIGINT,
    
    -- PostgreSQL error info
    sql_state           VARCHAR(10),
    pg_exception_detail TEXT,
    pg_exception_hint   TEXT,
    
    -- Resolution tracking
    resolution_status   VARCHAR(20) DEFAULT 'OPEN',  -- OPEN, IN_PROGRESS, RESOLVED, IGNORED
    resolved_by         VARCHAR(100),
    resolved_at         TIMESTAMP,
    resolution_notes    TEXT
);

CREATE INDEX idx_error_job ON audit.error_log(job_id);
CREATE INDEX idx_error_severity ON audit.error_log(error_severity);
CREATE INDEX idx_error_status ON audit.error_log(resolution_status);
CREATE INDEX idx_error_timestamp ON audit.error_log(error_timestamp DESC);

-- ============================================================
-- TABLE 5: Data Change Log
-- Tracks INSERT/UPDATE/DELETE on unified table
-- ============================================================
DROP TABLE IF EXISTS audit.data_change_log CASCADE;
CREATE TABLE audit.data_change_log (
    change_id           BIGSERIAL PRIMARY KEY,
    job_id              INTEGER,
    change_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_type         VARCHAR(10) NOT NULL,      -- INSERT, UPDATE, DELETE
    table_name          VARCHAR(100) DEFAULT 'unified_shipments',
    
    -- Record identification
    record_key          VARCHAR(100) NOT NULL,     -- CNOTE_NO
    
    -- Change details (for UPDATE)
    column_name         VARCHAR(100),
    old_value           TEXT,
    new_value           TEXT,
    
    changed_by          VARCHAR(100) DEFAULT 'ETL_PROCESS'
);

CREATE INDEX idx_change_record ON audit.data_change_log(record_key);
CREATE INDEX idx_change_timestamp ON audit.data_change_log(change_timestamp DESC);
CREATE INDEX idx_change_type ON audit.data_change_log(change_type);

-- Partition by month for large datasets (optional)
-- CREATE INDEX idx_change_month ON audit.data_change_log(DATE_TRUNC('month', change_timestamp));

-- ============================================================
-- TABLE 6: Data Quality Metrics
-- Quality scores per ETL run
-- ============================================================
DROP TABLE IF EXISTS audit.data_quality_metrics CASCADE;
CREATE TABLE audit.data_quality_metrics (
    metric_id           SERIAL PRIMARY KEY,
    job_id              INTEGER REFERENCES audit.etl_job_log(job_id) ON DELETE CASCADE,
    metric_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name          VARCHAR(100) DEFAULT 'unified_shipments',
    total_rows          BIGINT,
    
    -- COMPLETENESS (non-null required fields)
    completeness_score  NUMERIC(5,2),
    null_cnote_no       BIGINT DEFAULT 0,
    null_cnote_date     BIGINT DEFAULT 0,
    null_origin         BIGINT DEFAULT 0,
    null_destination    BIGINT DEFAULT 0,
    null_weight         BIGINT DEFAULT 0,
    null_amount         BIGINT DEFAULT 0,
    
    -- ACCURACY (valid values)
    accuracy_score      NUMERIC(5,2),
    invalid_dates       BIGINT DEFAULT 0,
    future_dates        BIGINT DEFAULT 0,
    negative_amounts    BIGINT DEFAULT 0,
    negative_weights    BIGINT DEFAULT 0,
    
    -- UNIQUENESS (no duplicates)
    uniqueness_score    NUMERIC(5,2),
    duplicate_cnotes    BIGINT DEFAULT 0,
    
    -- CONSISTENCY (logical rules)
    consistency_score   NUMERIC(5,2),
    origin_equals_dest  BIGINT DEFAULT 0,          -- Suspicious
    pod_before_cnote    BIGINT DEFAULT 0,          -- POD date before shipment date
    
    -- TIMELINESS (data freshness)
    timeliness_score    NUMERIC(5,2),
    oldest_record_days  INTEGER,
    newest_record_days  INTEGER,
    avg_record_age_days NUMERIC(10,2),
    
    -- INTEGRITY (referential)
    integrity_score     NUMERIC(5,2),
    orphan_manifests    BIGINT DEFAULT 0,
    orphan_runsheets    BIGINT DEFAULT 0,
    
    -- Overall
    overall_dq_score    NUMERIC(5,2)
);

CREATE INDEX idx_dq_job ON audit.data_quality_metrics(job_id);
CREATE INDEX idx_dq_timestamp ON audit.data_quality_metrics(metric_timestamp DESC);

-- ============================================================
-- TABLE 7: Process Checkpoint
-- For resumable ETL (track which tables processed)
-- ============================================================
DROP TABLE IF EXISTS audit.process_checkpoint CASCADE;
CREATE TABLE audit.process_checkpoint (
    checkpoint_id       SERIAL PRIMARY KEY,
    job_id              INTEGER REFERENCES audit.etl_job_log(job_id) ON DELETE CASCADE,
    step_name           VARCHAR(100) NOT NULL,
    step_sequence       INTEGER,
    status              VARCHAR(20) DEFAULT 'PENDING',  -- PENDING, RUNNING, COMPLETED, FAILED
    started_at          TIMESTAMP,
    completed_at        TIMESTAMP,
    rows_processed      BIGINT,
    error_message       TEXT
);

CREATE INDEX idx_checkpoint_job ON audit.process_checkpoint(job_id);


-- ============================================================
-- FUNCTIONS: ETL Job Management
-- ============================================================

-- Start a new ETL job
CREATE OR REPLACE FUNCTION audit.start_etl_job(
    p_job_name VARCHAR(100),
    p_job_type VARCHAR(50) DEFAULT 'UNIFICATION',
    p_triggered_by VARCHAR(100) DEFAULT 'MANUAL'
)
RETURNS INTEGER AS $$
DECLARE
    v_job_id INTEGER;
BEGIN
    INSERT INTO audit.etl_job_log (
        job_name, job_type, status, triggered_by, 
        server_host, database_name
    )
    VALUES (
        p_job_name, p_job_type, 'RUNNING', p_triggered_by,
        inet_server_addr()::VARCHAR, current_database()
    )
    RETURNING job_id INTO v_job_id;
    
    RAISE NOTICE 'Started ETL job % with ID %', p_job_name, v_job_id;
    RETURN v_job_id;
END;
$$ LANGUAGE plpgsql;

-- Complete ETL job successfully
CREATE OR REPLACE FUNCTION audit.complete_etl_job(
    p_job_id INTEGER,
    p_target_rows BIGINT,
    p_rows_inserted BIGINT DEFAULT 0,
    p_rows_updated BIGINT DEFAULT 0,
    p_duplicates_removed BIGINT DEFAULT 0,
    p_notes TEXT DEFAULT NULL
)
RETURNS VOID AS $$
DECLARE
    v_source_total BIGINT;
BEGIN
    -- Get total source rows
    SELECT COALESCE(SUM(rows_total), 0) INTO v_source_total
    FROM audit.etl_source_stats
    WHERE job_id = p_job_id;
    
    UPDATE audit.etl_job_log
    SET status = 'SUCCESS',
        completed_at = CURRENT_TIMESTAMP,
        duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)),
        source_rows_total = v_source_total,
        target_rows_count = p_target_rows,
        rows_inserted = p_rows_inserted,
        rows_updated = p_rows_updated,
        duplicates_removed = p_duplicates_removed,
        notes = p_notes
    WHERE job_id = p_job_id;
    
    RAISE NOTICE 'Completed ETL job % - % rows processed', p_job_id, p_target_rows;
END;
$$ LANGUAGE plpgsql;

-- Fail ETL job
CREATE OR REPLACE FUNCTION audit.fail_etl_job(
    p_job_id INTEGER,
    p_error_message TEXT,
    p_error_code VARCHAR(20) DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    UPDATE audit.etl_job_log
    SET status = 'FAILED',
        completed_at = CURRENT_TIMESTAMP,
        duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)),
        error_message = p_error_message,
        error_code = p_error_code
    WHERE job_id = p_job_id;
    
    -- Also log to error table
    INSERT INTO audit.error_log (job_id, error_severity, error_message, error_code)
    VALUES (p_job_id, 'CRITICAL', p_error_message, p_error_code);
    
    RAISE WARNING 'ETL job % FAILED: %', p_job_id, p_error_message;
END;
$$ LANGUAGE plpgsql;

-- Log source table statistics
CREATE OR REPLACE FUNCTION audit.log_source_stats(
    p_job_id INTEGER,
    p_source_table VARCHAR(100),
    p_rows_total BIGINT,
    p_rows_after_dedup BIGINT DEFAULT NULL,
    p_rows_joined BIGINT DEFAULT NULL,
    p_duplicates_removed BIGINT DEFAULT 0,
    p_join_key VARCHAR(100) DEFAULT NULL,
    p_dedup_column VARCHAR(100) DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO audit.etl_source_stats (
        job_id, source_table, rows_total, rows_after_dedup,
        rows_joined, duplicates_removed, join_key_column, dedup_column
    )
    VALUES (
        p_job_id, p_source_table, p_rows_total,
        COALESCE(p_rows_after_dedup, p_rows_total),
        p_rows_joined,
        p_duplicates_removed,
        p_join_key,
        p_dedup_column
    );
END;
$$ LANGUAGE plpgsql;

-- Log an error
CREATE OR REPLACE FUNCTION audit.log_error(
    p_job_id INTEGER,
    p_severity VARCHAR(20),
    p_message TEXT,
    p_source_table VARCHAR(100) DEFAULT NULL,
    p_affected_record VARCHAR(100) DEFAULT NULL,
    p_detail TEXT DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    v_error_id INTEGER;
BEGIN
    INSERT INTO audit.error_log (
        job_id, error_severity, error_message,
        source_table, affected_record_id, error_detail
    )
    VALUES (
        p_job_id, p_severity, p_message,
        p_source_table, p_affected_record, p_detail
    )
    RETURNING error_id INTO v_error_id;
    
    IF p_severity IN ('ERROR', 'CRITICAL') THEN
        RAISE WARNING '[%] %: %', p_severity, p_source_table, p_message;
    END IF;
    
    RETURN v_error_id;
END;
$$ LANGUAGE plpgsql;

-- Calculate data quality metrics
CREATE OR REPLACE FUNCTION audit.calculate_dq_metrics(
    p_job_id INTEGER,
    p_table_name VARCHAR(100) DEFAULT 'staging.unified_shipments'
)
RETURNS VOID AS $$
DECLARE
    v_total BIGINT;
    v_null_cnote BIGINT;
    v_null_origin BIGINT;
    v_null_dest BIGINT;
    v_null_weight BIGINT;
    v_duplicates BIGINT;
    v_completeness NUMERIC(5,2);
    v_uniqueness NUMERIC(5,2);
    v_overall NUMERIC(5,2);
BEGIN
    -- This is a template - adjust column names to match your unified table
    EXECUTE format('SELECT COUNT(*) FROM %s', p_table_name) INTO v_total;
    
    -- Calculate completeness (example - adjust columns)
    EXECUTE format('SELECT COUNT(*) FROM %s WHERE CNOTE_NO IS NULL', p_table_name) INTO v_null_cnote;
    EXECUTE format('SELECT COUNT(*) FROM %s WHERE CNOTE_ORIGIN IS NULL', p_table_name) INTO v_null_origin;
    EXECUTE format('SELECT COUNT(*) FROM %s WHERE CNOTE_DESTINATION IS NULL', p_table_name) INTO v_null_dest;
    
    -- Calculate uniqueness
    EXECUTE format('SELECT COUNT(*) - COUNT(DISTINCT CNOTE_NO) FROM %s', p_table_name) INTO v_duplicates;
    
    -- Scores
    v_completeness := CASE WHEN v_total > 0 
        THEN (1 - (v_null_cnote + v_null_origin + v_null_dest)::NUMERIC / (v_total * 3)) * 100 
        ELSE 0 END;
    v_uniqueness := CASE WHEN v_total > 0 
        THEN (1 - v_duplicates::NUMERIC / v_total) * 100 
        ELSE 0 END;
    v_overall := (v_completeness + v_uniqueness) / 2;
    
    INSERT INTO audit.data_quality_metrics (
        job_id, table_name, total_rows,
        null_cnote_no, null_origin, null_destination,
        duplicate_cnotes,
        completeness_score, uniqueness_score, overall_dq_score
    )
    VALUES (
        p_job_id, p_table_name, v_total,
        v_null_cnote, v_null_origin, v_null_dest,
        v_duplicates,
        v_completeness, v_uniqueness, v_overall
    );
    
    RAISE NOTICE 'DQ Score for job %: Completeness=%, Uniqueness=%, Overall=%', 
        p_job_id, v_completeness, v_uniqueness, v_overall;
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- VIEWS: Audit Reporting
-- ============================================================

-- Latest ETL jobs
CREATE OR REPLACE VIEW audit.v_latest_jobs AS
SELECT 
    job_id,
    job_name,
    status,
    started_at,
    completed_at,
    duration_seconds || 's' AS duration,
    source_rows_total,
    target_rows_count,
    rows_inserted,
    rows_updated,
    duplicates_removed,
    triggered_by,
    CASE WHEN status = 'FAILED' THEN error_message ELSE NULL END AS error
FROM audit.etl_job_log
ORDER BY started_at DESC
LIMIT 20;

-- Daily job summary
CREATE OR REPLACE VIEW audit.v_daily_summary AS
SELECT 
    DATE(started_at) AS run_date,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    ROUND(AVG(duration_seconds)::NUMERIC, 2) AS avg_duration_sec,
    SUM(target_rows_count) AS total_rows_processed,
    SUM(duplicates_removed) AS total_duplicates_removed
FROM audit.etl_job_log
GROUP BY DATE(started_at)
ORDER BY run_date DESC;

-- Source table stats for latest successful job
CREATE OR REPLACE VIEW audit.v_latest_source_stats AS
SELECT 
    s.source_table,
    s.rows_total,
    s.rows_after_dedup,
    s.duplicates_removed,
    s.rows_joined,
    CASE WHEN s.rows_total > 0 
        THEN ROUND((s.rows_joined::NUMERIC / s.rows_total) * 100, 2)
        ELSE 0 
    END AS join_rate_pct,
    s.join_key_column,
    s.dedup_column
FROM audit.etl_source_stats s
WHERE s.job_id = (
    SELECT MAX(job_id) FROM audit.etl_job_log WHERE status = 'SUCCESS'
)
ORDER BY s.source_table;

-- Open errors
CREATE OR REPLACE VIEW audit.v_open_errors AS
SELECT 
    e.error_id,
    e.job_id,
    e.error_timestamp,
    e.error_severity,
    e.source_table,
    e.error_message,
    e.affected_record_id,
    e.affected_row_count,
    j.job_name
FROM audit.error_log e
LEFT JOIN audit.etl_job_log j ON e.job_id = j.job_id
WHERE e.resolution_status = 'OPEN'
ORDER BY e.error_timestamp DESC;

-- Data quality trend
CREATE OR REPLACE VIEW audit.v_dq_trend AS
SELECT 
    DATE(metric_timestamp) AS metric_date,
    ROUND(AVG(completeness_score), 2) AS avg_completeness,
    ROUND(AVG(uniqueness_score), 2) AS avg_uniqueness,
    ROUND(AVG(overall_dq_score), 2) AS avg_overall,
    SUM(duplicate_cnotes) AS total_duplicates,
    SUM(total_rows) AS total_rows_checked
FROM audit.data_quality_metrics
GROUP BY DATE(metric_timestamp)
ORDER BY metric_date DESC;


-- ============================================================
-- SAMPLE USAGE
-- ============================================================
/*
-- 1. Start job
SELECT audit.start_etl_job('JNE_DAILY_UNIFICATION', 'UNIFICATION', 'AIRFLOW');
-- Returns: job_id = 1

-- 2. Log source stats for each table
SELECT audit.log_source_stats(1, 'CMS_CNOTE', 100, 100, 100, 0, 'CNOTE_NO', NULL);
SELECT audit.log_source_stats(1, 'CMS_CNOTE_POD', 100, 100, 95, 0, 'CNOTE_POD_NO', NULL);
SELECT audit.log_source_stats(1, 'CMS_MRCNOTE', 150, 120, 90, 30, 'MRCNOTE_NO', 'MRCNOTE_DATE');
-- ... repeat for all 34 tables

-- 3. Log any errors
SELECT audit.log_error(1, 'WARNING', '5 records with NULL join key', 'CMS_DRCNOTE', NULL, 'DRCNOTE_CNOTE_NO is NULL');

-- 4. Calculate DQ metrics
SELECT audit.calculate_dq_metrics(1, 'staging.unified_shipments');

-- 5. Complete job
SELECT audit.complete_etl_job(1, 100, 100, 0, 30, 'Daily run completed successfully');

-- 6. Query results
SELECT * FROM audit.v_latest_jobs;
SELECT * FROM audit.v_latest_source_stats;
SELECT * FROM audit.v_open_errors;
SELECT * FROM audit.v_dq_trend;
*/

