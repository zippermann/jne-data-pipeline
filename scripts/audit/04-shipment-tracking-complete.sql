-- ============================================================
-- JNE AUDIT TRAIL: Shipment Tracking Schema & Triggers
-- ============================================================
-- Based on JNE Package Journey (Correct Flow):
--   Shipper → Pickup → Receiving → Outbound → Manifest/Transit → 
--   Inbound → Destination → Runsheet → Delivered
--
-- Tables mapped from JNE_Table_Business_Metadata.xlsx:
--   CMS_APICUST     - Order Created (Marketplace)
--   CMS_CNOTE       - Cnote Masterdata (Pickup/Drop-off)
--   CMS_MRCNOTE     - Receival Masterdata
--   CMS_DRCNOTE     - Receival at Cnote level
--   CMS_DHOCNOTE    - Handover Outbound (leaving origin)
--   CMS_MANIFEST    - Manifest Masterdata
--   CMS_MFCNOTE     - Manifest at Cnote level (in transit)
--   CMS_DHICNOTE    - Handover Inbound (arriving at destination)
--   CMS_MRSHEET     - Runsheet Masterdata
--   CMS_DRSHEET     - Runsheet at Cnote level
--   CMS_CNOTE_POD   - Proof of Delivery
--   CMS_DHOV_RSHEET - Runsheet delivery details
-- ============================================================

-- Create audit schema if not exists
CREATE SCHEMA IF NOT EXISTS audit;

-- ============================================================
-- SHIPMENT TRACKING TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS audit.shipment_tracking (
    log_id SERIAL PRIMARY KEY,
    sequence_lsn BIGINT GENERATED ALWAYS AS IDENTITY,
    awb_number VARCHAR(50) NOT NULL,
    status_before VARCHAR(100),
    status_after VARCHAR(100) NOT NULL,
    system_action VARCHAR(100) NOT NULL,
    source_table VARCHAR(100),
    source_record_id VARCHAR(100),
    location_code VARCHAR(50),
    location_name VARCHAR(200),
    captured_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    captured_by VARCHAR(100) DEFAULT CURRENT_USER,
    metadata JSONB
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_ship_track_awb ON audit.shipment_tracking(awb_number);
CREATE INDEX IF NOT EXISTS idx_ship_track_status ON audit.shipment_tracking(status_after);
CREATE INDEX IF NOT EXISTS idx_ship_track_captured ON audit.shipment_tracking(captured_at);
CREATE INDEX IF NOT EXISTS idx_ship_track_source ON audit.shipment_tracking(source_table);

COMMENT ON TABLE audit.shipment_tracking IS 'Immutable log of shipment position/status changes';

-- ============================================================
-- VIEWS FOR QUERYING SHIPMENT DATA
-- ============================================================

-- Drop existing views first (required when column structure changes)
DROP VIEW IF EXISTS audit.v_shipment_journey CASCADE;
DROP VIEW IF EXISTS audit.v_shipment_current_status CASCADE;
DROP VIEW IF EXISTS audit.v_tracking_daily_summary CASCADE;

-- View: Complete journey timeline for any AWB
CREATE OR REPLACE VIEW audit.v_shipment_journey AS
SELECT 
    log_id,
    sequence_lsn,
    awb_number,
    status_before,
    status_after,
    system_action,
    location_code,
    location_name,
    TO_CHAR(captured_at, 'YYYY-MM-DD HH24:MI:SS') as captured_at_str,
    captured_at,
    ROW_NUMBER() OVER (PARTITION BY awb_number ORDER BY sequence_lsn) as stage_number,
    captured_at - LAG(captured_at) OVER (PARTITION BY awb_number ORDER BY sequence_lsn) as time_since_prev
FROM audit.shipment_tracking
ORDER BY awb_number, sequence_lsn;

-- View: Current status of each shipment
CREATE OR REPLACE VIEW audit.v_shipment_current_status AS
SELECT DISTINCT ON (awb_number)
    awb_number,
    status_after as current_status,
    location_code,
    location_name,
    captured_at as last_update,
    system_action,
    (SELECT COUNT(*) FROM audit.shipment_tracking t2 WHERE t2.awb_number = t.awb_number) as total_stages
FROM audit.shipment_tracking t
ORDER BY awb_number, captured_at DESC;

-- View: Daily tracking summary
CREATE OR REPLACE VIEW audit.v_tracking_daily_summary AS
SELECT 
    DATE(captured_at) as track_date,
    status_after,
    COUNT(*) as event_count,
    COUNT(DISTINCT awb_number) as unique_shipments
FROM audit.shipment_tracking
GROUP BY DATE(captured_at), status_after
ORDER BY track_date DESC, event_count DESC;

-- ============================================================
-- FUNCTION: Log shipment status (for manual/programmatic use)
-- ============================================================
CREATE OR REPLACE FUNCTION audit.log_shipment_status(
    p_awb_number VARCHAR,
    p_status_after VARCHAR,
    p_system_action VARCHAR,
    p_source_table VARCHAR DEFAULT NULL,
    p_source_record_id VARCHAR DEFAULT NULL,
    p_location_code VARCHAR DEFAULT NULL,
    p_location_name VARCHAR DEFAULT NULL,
    p_metadata JSONB DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_status_before VARCHAR;
    v_log_id INTEGER;
BEGIN
    -- Get previous status
    SELECT status_after INTO v_status_before
    FROM audit.shipment_tracking
    WHERE awb_number = p_awb_number
    ORDER BY sequence_lsn DESC
    LIMIT 1;
    
    -- Insert new record
    INSERT INTO audit.shipment_tracking (
        awb_number, status_before, status_after, system_action,
        source_table, source_record_id, location_code, location_name, metadata
    ) VALUES (
        p_awb_number, v_status_before, p_status_after, p_system_action,
        p_source_table, p_source_record_id, p_location_code, p_location_name, p_metadata
    ) RETURNING log_id INTO v_log_id;
    
    RETURN v_log_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- FUNCTION: Get shipment journey (for querying)
-- ============================================================
CREATE OR REPLACE FUNCTION audit.get_shipment_journey(p_awb VARCHAR)
RETURNS TABLE (
    stage INT,
    status VARCHAR,
    location VARCHAR,
    timestamp TIMESTAMP WITH TIME ZONE,
    source VARCHAR,
    duration INTERVAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ROW_NUMBER() OVER (ORDER BY sequence_lsn)::INT,
        status_after::VARCHAR,
        COALESCE(location_name, location_code, 'Unknown')::VARCHAR,
        captured_at,
        source_table::VARCHAR,
        LEAD(captured_at) OVER (ORDER BY sequence_lsn) - captured_at
    FROM audit.shipment_tracking
    WHERE awb_number = p_awb
    ORDER BY sequence_lsn;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- FUNCTION: Backfill tracking from existing data
-- ============================================================
CREATE OR REPLACE FUNCTION audit.backfill_shipment_tracking(
    p_limit INTEGER DEFAULT 1000
) RETURNS TABLE (
    source_table TEXT,
    records_logged INTEGER
) AS $$
DECLARE
    v_count INTEGER;
BEGIN
    -- Backfill from CMS_CNOTE (Cnote Created)
    BEGIN
        INSERT INTO audit.shipment_tracking (awb_number, status_after, system_action, source_table, location_code)
        SELECT DISTINCT ON (cnote_no)
            cnote_no::VARCHAR,
            'Cnote Created',
            'Backfill: Historical Data',
            'raw.cms_cnote',
            cnote_branch_id::VARCHAR
        FROM raw.cms_cnote
        WHERE cnote_no IS NOT NULL
        AND cnote_no::VARCHAR NOT IN (
            SELECT awb_number FROM audit.shipment_tracking WHERE status_after = 'Cnote Created'
        )
        LIMIT p_limit;
        GET DIAGNOSTICS v_count = ROW_COUNT;
        source_table := 'cms_cnote';
        records_logged := v_count;
        RETURN NEXT;
    EXCEPTION WHEN undefined_table THEN
        source_table := 'cms_cnote';
        records_logged := 0;
        RETURN NEXT;
    END;
    
    -- Backfill from CMS_CNOTE_POD (Delivered)
    BEGIN
        INSERT INTO audit.shipment_tracking (awb_number, status_after, system_action, source_table, location_code)
        SELECT DISTINCT ON (pod_cnote_no)
            pod_cnote_no::VARCHAR,
            'Delivered (POD)',
            'Backfill: Historical Data',
            'raw.cms_cnote_pod',
            pod_branch::VARCHAR
        FROM raw.cms_cnote_pod
        WHERE pod_cnote_no IS NOT NULL
        AND pod_cnote_no::VARCHAR NOT IN (
            SELECT awb_number FROM audit.shipment_tracking WHERE status_after = 'Delivered (POD)'
        )
        LIMIT p_limit;
        GET DIAGNOSTICS v_count = ROW_COUNT;
        source_table := 'cms_cnote_pod';
        records_logged := v_count;
        RETURN NEXT;
    EXCEPTION WHEN undefined_table THEN
        source_table := 'cms_cnote_pod';
        records_logged := 0;
        RETURN NEXT;
    END;
    
    -- Backfill from CMS_DRSHEET (On Runsheet)
    BEGIN
        INSERT INTO audit.shipment_tracking (awb_number, status_after, system_action, source_table, location_code)
        SELECT DISTINCT ON (drsheet_cnote_no)
            drsheet_cnote_no::VARCHAR,
            'On Delivery Runsheet',
            'Backfill: Historical Data',
            'raw.cms_drsheet',
            drsheet_branch_id::VARCHAR
        FROM raw.cms_drsheet
        WHERE drsheet_cnote_no IS NOT NULL
        AND drsheet_cnote_no::VARCHAR NOT IN (
            SELECT awb_number FROM audit.shipment_tracking WHERE status_after = 'On Delivery Runsheet'
        )
        LIMIT p_limit;
        GET DIAGNOSTICS v_count = ROW_COUNT;
        source_table := 'cms_drsheet';
        records_logged := v_count;
        RETURN NEXT;
    EXCEPTION WHEN undefined_table THEN
        source_table := 'cms_drsheet';
        records_logged := 0;
        RETURN NEXT;
    END;
    
    -- Backfill from CMS_DRCNOTE (Received)
    BEGIN
        INSERT INTO audit.shipment_tracking (awb_number, status_after, system_action, source_table, location_code)
        SELECT DISTINCT ON (drcnote_cnote_no)
            drcnote_cnote_no::VARCHAR,
            'Received at Facility',
            'Backfill: Historical Data',
            'raw.cms_drcnote',
            drcnote_branch_id::VARCHAR
        FROM raw.cms_drcnote
        WHERE drcnote_cnote_no IS NOT NULL
        AND drcnote_cnote_no::VARCHAR NOT IN (
            SELECT awb_number FROM audit.shipment_tracking WHERE status_after = 'Received at Facility'
        )
        LIMIT p_limit;
        GET DIAGNOSTICS v_count = ROW_COUNT;
        source_table := 'cms_drcnote';
        records_logged := v_count;
        RETURN NEXT;
    EXCEPTION WHEN undefined_table THEN
        source_table := 'cms_drcnote';
        records_logged := 0;
        RETURN NEXT;
    END;
    
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- RUN INITIAL BACKFILL (Optional - uncomment to run)
-- ============================================================
-- SELECT * FROM audit.backfill_shipment_tracking(5000);

-- ============================================================
-- VERIFICATION QUERIES
-- ============================================================
-- Check tracking data:
-- SELECT COUNT(*), status_after FROM audit.shipment_tracking GROUP BY status_after;
--
-- Get journey for specific AWB:
-- SELECT * FROM audit.get_shipment_journey('YOUR_AWB_HERE');
--
-- Today's summary:
-- SELECT * FROM audit.v_tracking_daily_summary WHERE track_date = CURRENT_DATE;
