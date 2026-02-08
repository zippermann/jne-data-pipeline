-- ============================================================
-- JNE AUDIT TRAIL SCHEMA (Cleaned)
-- ============================================================
-- Three core tables matching deliverable requirements:
--   1. Traceability  — End-to-end data journey per AWB
--   2. Logs Monitoring — Batch process logs, success/failure counts
--   3. Integrity      — Immutable change records with LSN
--
-- Plus one supporting table for pipeline transform logging.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS audit;

-- ============================================================
-- CLEANUP: Drop legacy objects from old audit schema
-- ============================================================
DROP VIEW IF EXISTS audit.v_data_lineage_trace CASCADE;
DROP VIEW IF EXISTS audit.v_data_quality_scorecard CASCADE;
DROP VIEW IF EXISTS audit.v_pipeline_health_summary CASCADE;
DROP VIEW IF EXISTS audit.v_shipment_current_status CASCADE;
DROP VIEW IF EXISTS audit.v_shipment_journey CASCADE;
DROP VIEW IF EXISTS audit.v_shipment_status_summary CASCADE;
DROP VIEW IF EXISTS audit.v_tracking_daily_summary CASCADE;
DROP VIEW IF EXISTS audit.v_transformation_summary CASCADE;

DROP TABLE IF EXISTS audit.data_lineage CASCADE;
DROP TABLE IF EXISTS audit.data_quality_log CASCADE;
DROP TABLE IF EXISTS audit.etl_job_log CASCADE;
DROP TABLE IF EXISTS audit.shipment_tracking CASCADE;
DROP TABLE IF EXISTS audit.transformation_history CASCADE;
DROP TABLE IF EXISTS audit.user_activity CASCADE;

DROP FUNCTION IF EXISTS audit.start_etl_job(VARCHAR, VARCHAR, VARCHAR, JSONB) CASCADE;
DROP FUNCTION IF EXISTS audit.complete_etl_job(BIGINT, VARCHAR, BIGINT, BIGINT, BIGINT, TEXT) CASCADE;

-- ============================================================
-- 1. TRACEABILITY (Data Journey Tracking)
-- ============================================================
-- Tracks how each record moves through pipeline stages.
-- Maps to deliverable: Audit ID, AWB Number, Source Stage,
--   Target Stage, Timestamp, Transformation Logic
--
-- Example row:
--   AUD-001 | JNE998877 | PostgreSQL | Kafka Topic: raw_pkg
--           | 2026-01-22 09:00:01 | No change (CDC Capture)

CREATE TABLE IF NOT EXISTS audit.data_traceability (
    audit_id              BIGSERIAL PRIMARY KEY,
    awb_number            VARCHAR(50),          -- CNOTE_NO (NULL for batch-level entries)
    source_stage          VARCHAR(100) NOT NULL, -- e.g. "CSV File", "PostgreSQL (Raw)"
    target_stage          VARCHAR(100) NOT NULL, -- e.g. "PostgreSQL (Staging)"
    event_timestamp       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    transformation_logic  TEXT,                  -- e.g. "Pandas: Normalized status_date"
    batch_id              BIGINT,               -- FK to batch_log (optional)
    record_count          INT,                  -- rows affected in this step
    metadata              JSONB                 -- additional context
);

CREATE INDEX IF NOT EXISTS idx_trace_awb ON audit.data_traceability(awb_number);
CREATE INDEX IF NOT EXISTS idx_trace_timestamp ON audit.data_traceability(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_trace_batch ON audit.data_traceability(batch_id);

-- ============================================================
-- 2. LOGS MONITORING (Pipeline Health)
-- ============================================================
-- Captures per-batch process logs with record counts and errors.
-- Maps to deliverable: Batch ID, Application, Record Count (In),
--   Record Count (Out), Status, Error Details
--
-- Example row:
--   BATCH-101 | Airflow (ETL) | 50,000 | 49,998
--             | WARNING | 2 records failed DQ check

CREATE TABLE IF NOT EXISTS audit.batch_log (
    batch_id              BIGSERIAL PRIMARY KEY,
    application           VARCHAR(100) NOT NULL,  -- e.g. "Airflow (Load)", "Airflow (Unification)"
    record_count_in       BIGINT DEFAULT 0,
    record_count_out      BIGINT DEFAULT 0,
    status                VARCHAR(20) NOT NULL DEFAULT 'RUNNING',  -- RUNNING, SUCCESS, WARNING, FAILED
    error_details         TEXT,
    started_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at          TIMESTAMP,
    duration_seconds      NUMERIC(10,2),
    pipeline_stage        VARCHAR(50),            -- DATA_LOAD, DATA_UNIFICATION, DATA_TRANSFORMATION
    triggered_by          VARCHAR(100) DEFAULT 'AIRFLOW'
);

CREATE INDEX IF NOT EXISTS idx_batch_status ON audit.batch_log(status);
CREATE INDEX IF NOT EXISTS idx_batch_started ON audit.batch_log(started_at);
CREATE INDEX IF NOT EXISTS idx_batch_stage ON audit.batch_log(pipeline_stage);

-- ============================================================
-- 3. INTEGRITY (Immutable Change History)
-- ============================================================
-- Maintains immutable, append-only records of data changes.
-- Uses a monotonic LSN (Log Sequence Number) for ordering.
-- Maps to deliverable: Log ID, Sequence (LSN), AWB Number,
--   Status After, System Action, Captured At
--
-- Example row:
--   LOG-101 | 4501 | JNE554433 | Arrived at Hub
--           | Kafka CDC | 14:00:02

CREATE SEQUENCE IF NOT EXISTS audit.change_log_lsn_seq;

CREATE TABLE IF NOT EXISTS audit.change_log (
    log_id                BIGSERIAL PRIMARY KEY,
    sequence_lsn          BIGINT NOT NULL DEFAULT nextval('audit.change_log_lsn_seq'),
    awb_number            VARCHAR(50) NOT NULL,   -- CNOTE_NO
    status_after          VARCHAR(100),           -- e.g. "Manifested", "In Transit", "Delivered"
    system_action         VARCHAR(100) NOT NULL,  -- e.g. "Kafka CDC", "Airflow: Final Write to PostgreSQL"
    captured_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata              JSONB
);

CREATE INDEX IF NOT EXISTS idx_change_awb ON audit.change_log(awb_number);
CREATE INDEX IF NOT EXISTS idx_change_lsn ON audit.change_log(sequence_lsn);
CREATE INDEX IF NOT EXISTS idx_change_captured ON audit.change_log(captured_at);

-- ============================================================
-- SUPPORTING: Transformation Log
-- ============================================================
-- Simple per-table transform results (used by transform_tables.py).

CREATE TABLE IF NOT EXISTS audit.transformation_log (
    id                    BIGSERIAL PRIMARY KEY,
    table_name            VARCHAR(100) NOT NULL,
    row_count             BIGINT,
    status                VARCHAR(20) NOT NULL,
    timestamp             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- VIEWS (Deliverable-Formatted)
-- ============================================================

-- View: Traceability report
CREATE OR REPLACE VIEW audit.v_traceability AS
SELECT
    'AUD-' || LPAD(audit_id::TEXT, 3, '0') AS audit_id,
    awb_number,
    source_stage,
    target_stage,
    event_timestamp AS timestamp,
    transformation_logic
FROM audit.data_traceability
ORDER BY event_timestamp DESC;

-- View: Batch monitoring dashboard
CREATE OR REPLACE VIEW audit.v_batch_monitoring AS
SELECT
    'BATCH-' || LPAD(batch_id::TEXT, 3, '0') AS batch_id,
    application,
    record_count_in,
    record_count_out,
    status,
    error_details
FROM audit.batch_log
ORDER BY started_at DESC;

-- View: Integrity change log
CREATE OR REPLACE VIEW audit.v_integrity_log AS
SELECT
    'LOG-' || LPAD(log_id::TEXT, 3, '0') AS log_id,
    sequence_lsn,
    awb_number,
    status_after,
    system_action,
    captured_at
FROM audit.change_log
ORDER BY sequence_lsn DESC;

-- View: Pipeline health summary (for monitoring DAG)
CREATE OR REPLACE VIEW audit.v_pipeline_health AS
SELECT
    pipeline_stage,
    COUNT(*) AS total_batches,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
    SUM(record_count_in) AS total_records_in,
    SUM(record_count_out) AS total_records_out,
    ROUND(AVG(duration_seconds)::NUMERIC, 2) AS avg_duration_sec,
    MAX(started_at) AS last_run
FROM audit.batch_log
GROUP BY pipeline_stage
ORDER BY pipeline_stage;

-- View: Recent failures
CREATE OR REPLACE VIEW audit.v_recent_failures AS
SELECT
    'BATCH-' || LPAD(batch_id::TEXT, 3, '0') AS batch_id,
    application,
    pipeline_stage,
    started_at,
    record_count_in,
    record_count_out,
    error_details
FROM audit.batch_log
WHERE status IN ('FAILED', 'WARNING')
ORDER BY started_at DESC
LIMIT 50;

-- ============================================================
-- HELPER FUNCTIONS
-- ============================================================

-- Start a batch
CREATE OR REPLACE FUNCTION audit.start_batch(
    p_application VARCHAR,
    p_pipeline_stage VARCHAR,
    p_record_count_in BIGINT DEFAULT 0,
    p_triggered_by VARCHAR DEFAULT 'AIRFLOW'
) RETURNS BIGINT AS $$
DECLARE
    v_batch_id BIGINT;
BEGIN
    INSERT INTO audit.batch_log (
        application, pipeline_stage, record_count_in, status, triggered_by
    ) VALUES (
        p_application, p_pipeline_stage, p_record_count_in, 'RUNNING', p_triggered_by
    ) RETURNING batch_id INTO v_batch_id;
    RETURN v_batch_id;
END;
$$ LANGUAGE plpgsql;

-- Complete a batch
CREATE OR REPLACE FUNCTION audit.complete_batch(
    p_batch_id BIGINT,
    p_record_count_out BIGINT,
    p_status VARCHAR DEFAULT 'SUCCESS',
    p_error_details TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE audit.batch_log
    SET completed_at = CURRENT_TIMESTAMP,
        duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)),
        record_count_out = p_record_count_out,
        status = p_status,
        error_details = p_error_details
    WHERE batch_id = p_batch_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- GRANTS
-- ============================================================
GRANT USAGE ON SCHEMA audit TO jne_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA audit TO jne_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA audit TO jne_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA audit TO jne_user;

-- ============================================================
-- COMMENTS
-- ============================================================
COMMENT ON TABLE audit.data_traceability IS '1. Traceability: End-to-end data journey per AWB';
COMMENT ON TABLE audit.batch_log IS '2. Logs Monitoring: Per-batch process logs with counts and errors';
COMMENT ON TABLE audit.change_log IS '3. Integrity: Immutable change records with monotonic LSN';
COMMENT ON TABLE audit.transformation_log IS 'Supporting: Per-table transformation results';
