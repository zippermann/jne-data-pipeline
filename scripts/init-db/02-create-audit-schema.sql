-- ============================================================
-- JNE AUDIT TRAIL SCHEMA
-- ============================================================
-- Based on 3 Audit Frameworks from PDF pages 37-40:
--   1. Traceability: End-to-end data journey tracking
--   2. Logs Monitoring: Process logs, success/failure counts, error details
--   3. Integrity: Immutable records of data formatting & transformation
-- ============================================================

-- Create audit schema
CREATE SCHEMA IF NOT EXISTS audit;

-- ============================================================
-- COMPONENT 1: TRACEABILITY (Data Journey Tracking)
-- ============================================================
-- Purpose: Track which tables touched each record and when

CREATE TABLE IF NOT EXISTS audit.data_lineage (
    lineage_id BIGSERIAL PRIMARY KEY,
    source_table VARCHAR(100) NOT NULL,
    source_record_id VARCHAR(100),
    target_table VARCHAR(100),
    target_record_id VARCHAR(100),
    operation_type VARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE, TRANSFORM
    transformation_name VARCHAR(100),
    record_count INT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_by VARCHAR(100) DEFAULT CURRENT_USER,
    metadata JSONB -- Additional context
);

CREATE INDEX idx_lineage_source ON audit.data_lineage(source_table, source_record_id);
CREATE INDEX idx_lineage_target ON audit.data_lineage(target_table, target_record_id);
CREATE INDEX idx_lineage_processed_at ON audit.data_lineage(processed_at);

-- ============================================================
-- COMPONENT 2: LOGS MONITORING (Pipeline Health)
-- ============================================================
-- Purpose: Capture process logs, success/failure counts automatically

CREATE TABLE IF NOT EXISTS audit.etl_job_log (
    job_log_id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(200) NOT NULL,
    job_type VARCHAR(50) NOT NULL, -- LOAD, TRANSFORM, STREAM, QUALITY_CHECK
    pipeline_stage VARCHAR(50), -- DATA_UNIFICATION, DATA_REALTIME, DATA_STANDARDIZATION, DATA_TRANSFORMATION
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds INT,
    status VARCHAR(20) NOT NULL, -- RUNNING, SUCCESS, FAILED, PARTIAL
    records_processed INT DEFAULT 0,
    records_success INT DEFAULT 0,
    records_failed INT DEFAULT 0,
    error_message TEXT,
    error_details JSONB,
    executed_by VARCHAR(100) DEFAULT CURRENT_USER,
    execution_host VARCHAR(100),
    parameters JSONB, -- Job parameters
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_job_log_name ON audit.etl_job_log(job_name);
CREATE INDEX idx_job_log_status ON audit.etl_job_log(status);
CREATE INDEX idx_job_log_start_time ON audit.etl_job_log(start_time);
CREATE INDEX idx_job_log_pipeline_stage ON audit.etl_job_log(pipeline_stage);

-- ============================================================
-- COMPONENT 3: INTEGRITY (Immutable Change History)
-- ============================================================
-- Purpose: Maintain immutable records of data formatting & transformation

CREATE TABLE IF NOT EXISTS audit.data_quality_log (
    quality_log_id BIGSERIAL PRIMARY KEY,
    job_log_id BIGINT REFERENCES audit.etl_job_log(job_log_id),
    check_name VARCHAR(200) NOT NULL,
    check_type VARCHAR(50) NOT NULL, -- COMPLETENESS, ACCURACY, CONSISTENCY, VALIDITY, UNIQUENESS
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    check_query TEXT,
    expected_value VARCHAR(500),
    actual_value VARCHAR(500),
    records_checked INT,
    records_passed INT,
    records_failed INT,
    pass_percentage DECIMAL(5,2),
    status VARCHAR(20) NOT NULL, -- PASS, FAIL, WARNING
    error_details JSONB,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    checked_by VARCHAR(100) DEFAULT CURRENT_USER
);

CREATE INDEX idx_quality_log_table ON audit.data_quality_log(table_name);
CREATE INDEX idx_quality_log_status ON audit.data_quality_log(status);
CREATE INDEX idx_quality_log_checked_at ON audit.data_quality_log(checked_at);

-- ============================================================
-- Transformation History (Version Control)
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.transformation_history (
    transform_id BIGSERIAL PRIMARY KEY,
    job_log_id BIGINT REFERENCES audit.etl_job_log(job_log_id),
    transformation_name VARCHAR(200) NOT NULL,
    transformation_type VARCHAR(50), -- CLEANSING, STANDARDIZATION, ENRICHMENT, AGGREGATION
    source_table VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    transformation_logic TEXT, -- SQL or description
    records_before INT,
    records_after INT,
    records_inserted INT DEFAULT 0,
    records_updated INT DEFAULT 0,
    records_deleted INT DEFAULT 0,
    columns_affected TEXT[], -- Array of column names
    transformation_rules JSONB,
    version VARCHAR(20),
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    executed_by VARCHAR(100) DEFAULT CURRENT_USER
);

CREATE INDEX idx_transform_source ON audit.transformation_history(source_table);
CREATE INDEX idx_transform_target ON audit.transformation_history(target_table);
CREATE INDEX idx_transform_executed_at ON audit.transformation_history(executed_at);

-- ============================================================
-- User Activity Tracking
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.user_activity (
    activity_id BIGSERIAL PRIMARY KEY,
    user_name VARCHAR(100) NOT NULL,
    activity_type VARCHAR(50) NOT NULL, -- LOGIN, QUERY, EXPORT, MODIFY
    table_name VARCHAR(100),
    action_description TEXT,
    ip_address VARCHAR(45),
    user_agent TEXT,
    query_executed TEXT,
    rows_affected INT,
    activity_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    session_id VARCHAR(100)
);

CREATE INDEX idx_user_activity_user ON audit.user_activity(user_name);
CREATE INDEX idx_user_activity_timestamp ON audit.user_activity(activity_timestamp);

-- ============================================================
-- AUDIT VIEWS (For Reporting & Dashboards)
-- ============================================================

-- View 1: Pipeline Health Summary
CREATE OR REPLACE VIEW audit.v_pipeline_health_summary AS
SELECT 
    pipeline_stage,
    job_type,
    COUNT(*) as total_jobs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_jobs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_jobs,
    SUM(CASE WHEN status = 'PARTIAL' THEN 1 ELSE 0 END) as partial_jobs,
    SUM(records_processed) as total_records_processed,
    SUM(records_failed) as total_records_failed,
    AVG(duration_seconds) as avg_duration_seconds,
    MAX(start_time) as last_run_time
FROM audit.etl_job_log
GROUP BY pipeline_stage, job_type
ORDER BY pipeline_stage, job_type;

-- View 2: Recent Job Failures
CREATE OR REPLACE VIEW audit.v_recent_failures AS
SELECT 
    job_log_id,
    job_name,
    job_type,
    pipeline_stage,
    start_time,
    duration_seconds,
    records_processed,
    records_failed,
    error_message,
    executed_by
FROM audit.etl_job_log
WHERE status = 'FAILED'
ORDER BY start_time DESC
LIMIT 50;

-- View 3: Data Quality Scorecard
CREATE OR REPLACE VIEW audit.v_data_quality_scorecard AS
SELECT 
    table_name,
    check_type,
    COUNT(*) as total_checks,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed_checks,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
    SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) as warning_checks,
    ROUND(AVG(pass_percentage), 2) as avg_pass_percentage,
    MAX(checked_at) as last_check_time
FROM audit.data_quality_log
GROUP BY table_name, check_type
ORDER BY table_name, check_type;

-- View 4: Data Lineage Trace
CREATE OR REPLACE VIEW audit.v_data_lineage_trace AS
SELECT 
    lineage_id,
    source_table,
    source_record_id,
    operation_type,
    transformation_name,
    target_table,
    target_record_id,
    record_count,
    processed_at,
    processed_by
FROM audit.data_lineage
ORDER BY processed_at DESC;

-- View 5: Transformation Summary
CREATE OR REPLACE VIEW audit.v_transformation_summary AS
SELECT 
    transformation_name,
    transformation_type,
    source_table,
    target_table,
    COUNT(*) as execution_count,
    SUM(records_before) as total_records_before,
    SUM(records_after) as total_records_after,
    SUM(records_inserted) as total_inserted,
    SUM(records_updated) as total_updated,
    SUM(records_deleted) as total_deleted,
    MAX(executed_at) as last_executed
FROM audit.transformation_history
GROUP BY transformation_name, transformation_type, source_table, target_table
ORDER BY last_executed DESC;

-- ============================================================
-- HELPER FUNCTIONS
-- ============================================================

-- Function to start ETL job logging
CREATE OR REPLACE FUNCTION audit.start_etl_job(
    p_job_name VARCHAR,
    p_job_type VARCHAR,
    p_pipeline_stage VARCHAR,
    p_parameters JSONB DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    v_job_log_id BIGINT;
BEGIN
    INSERT INTO audit.etl_job_log (
        job_name,
        job_type,
        pipeline_stage,
        start_time,
        status,
        parameters
    ) VALUES (
        p_job_name,
        p_job_type,
        p_pipeline_stage,
        CURRENT_TIMESTAMP,
        'RUNNING',
        p_parameters
    ) RETURNING job_log_id INTO v_job_log_id;
    
    RETURN v_job_log_id;
END;
$$ LANGUAGE plpgsql;

-- Function to complete ETL job logging
CREATE OR REPLACE FUNCTION audit.complete_etl_job(
    p_job_log_id BIGINT,
    p_status VARCHAR,
    p_records_processed INT DEFAULT 0,
    p_records_success INT DEFAULT 0,
    p_records_failed INT DEFAULT 0,
    p_error_message TEXT DEFAULT NULL,
    p_error_details JSONB DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE audit.etl_job_log
    SET 
        end_time = CURRENT_TIMESTAMP,
        duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time))::INT,
        status = p_status,
        records_processed = p_records_processed,
        records_success = p_records_success,
        records_failed = p_records_failed,
        error_message = p_error_message,
        error_details = p_error_details
    WHERE job_log_id = p_job_log_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- GRANTS (Adjust based on your user roles)
-- ============================================================

GRANT USAGE ON SCHEMA audit TO jne_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA audit TO jne_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA audit TO jne_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA audit TO jne_user;

-- ============================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================

COMMENT ON SCHEMA audit IS 'JNE Audit Trail System - Tracks data lineage, pipeline health, and data quality';
COMMENT ON TABLE audit.data_lineage IS 'Traceability: Tracks end-to-end data journey across tables';
COMMENT ON TABLE audit.etl_job_log IS 'Logs Monitoring: Captures process execution, success/failure counts';
COMMENT ON TABLE audit.data_quality_log IS 'Integrity: Records data quality validation results';
COMMENT ON TABLE audit.transformation_history IS 'Integrity: Immutable transformation version history';

-- ============================================================
-- END OF AUDIT SCHEMA CREATION
-- ============================================================
