-- ============================================================
-- JNE DATA PIPELINE: Date Conversion & Fixed Audit Views
-- ============================================================
-- This script:
-- 1. Creates a function to convert Excel serial dates to PostgreSQL timestamps
-- 2. Creates a function to format dates as dd/mm/yy
-- 3. Updates audit views with correct POD column and all stages displayed
-- ============================================================

-- ============================================================
-- PART 1: DATE CONVERSION FUNCTIONS
-- ============================================================

-- Function to convert Excel serial number to PostgreSQL timestamp
CREATE OR REPLACE FUNCTION staging.excel_to_timestamp(excel_date NUMERIC)
RETURNS TIMESTAMP AS $$
BEGIN
    IF excel_date IS NULL OR excel_date <= 0 THEN
        RETURN NULL;
    END IF;
    -- Excel epoch is 1899-12-30 (accounting for Excel's leap year bug)
    -- The integer part is days, decimal part is time of day
    RETURN TIMESTAMP '1899-12-30 00:00:00' + (excel_date * INTERVAL '1 day');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to format timestamp as dd/mm/yy
CREATE OR REPLACE FUNCTION staging.format_date_ddmmyy(ts TIMESTAMP)
RETURNS TEXT AS $$
BEGIN
    IF ts IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN TO_CHAR(ts, 'DD/MM/YY');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Combined: Excel serial to dd/mm/yy string
CREATE OR REPLACE FUNCTION staging.excel_to_ddmmyy(excel_date NUMERIC)
RETURNS TEXT AS $$
BEGIN
    IF excel_date IS NULL OR excel_date <= 0 THEN
        RETURN NULL;
    END IF;
    RETURN TO_CHAR(
        TIMESTAMP '1899-12-30 00:00:00' + (excel_date * INTERVAL '1 day'),
        'DD/MM/YY'
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Combined: Excel serial to dd/mm/yy HH24:MI string (with time)
CREATE OR REPLACE FUNCTION staging.excel_to_ddmmyy_time(excel_date NUMERIC)
RETURNS TEXT AS $$
BEGIN
    IF excel_date IS NULL OR excel_date <= 0 THEN
        RETURN NULL;
    END IF;
    RETURN TO_CHAR(
        TIMESTAMP '1899-12-30 00:00:00' + (excel_date * INTERVAL '1 day'),
        'DD/MM/YY HH24:MI'
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Test the conversion (45977.22... should be around late 2025)
-- SELECT staging.excel_to_timestamp(45977.2295023148);
-- SELECT staging.excel_to_ddmmyy(45977.2295023148);

-- ============================================================
-- PART 2: DELIVERY STAGE MONITOR (ALL STAGES WITH TIMESTAMPS)
-- ============================================================
-- Shows every stage for each shipment with formatted timestamps
-- FIXED: Uses cnote_pod_creation_date instead of cnote_pod_date

DROP VIEW IF EXISTS audit.v_delivery_stage_monitor CASCADE;

CREATE OR REPLACE VIEW audit.v_delivery_stage_monitor AS
SELECT 
    cnote_no,
    
    -- Basic shipment info
    cnote_origin,
    cnote_destination,
    cnote_services_code AS service,
    
    -- ============================================================
    -- STAGE 1: Order Created
    -- ============================================================
    CASE WHEN cnote_date IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage1_order_status,
    staging.excel_to_ddmmyy_time(cnote_date) AS stage1_order_date,
    cnote_user AS stage1_user,
    
    -- ============================================================
    -- STAGE 2: Manifest Created (Package grouped for dispatch)
    -- ============================================================
    CASE WHEN mfcnote_man_no IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage2_manifest_status,
    staging.excel_to_ddmmyy_time(mfcnote_man_date) AS stage2_manifest_date,
    mfcnote_origin AS stage2_origin,
    
    -- ============================================================
    -- STAGE 3: In Transit (Dispatched from origin)
    -- ============================================================
    CASE WHEN manifest_date IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage3_transit_status,
    staging.excel_to_ddmmyy_time(manifest_date) AS stage3_transit_date,
    manifest_origin AS stage3_from,
    manifest_thru AS stage3_to,
    
    -- ============================================================
    -- STAGE 4: Arrived at Hub (Inbound scan)
    -- ============================================================
    CASE WHEN dhicnote_no IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage4_hub_inbound_status,
    staging.excel_to_ddmmyy_time(dhicnote_tdate) AS stage4_hub_inbound_date,
    mhicnote_zone AS stage4_hub_location,
    mhicnote_user_id AS stage4_user,
    
    -- ============================================================
    -- STAGE 5: Departed Hub (Outbound scan)
    -- ============================================================
    CASE WHEN dhocnote_no IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage5_hub_outbound_status,
    staging.excel_to_ddmmyy_time(dhocnote_tdate) AS stage5_hub_outbound_date,
    mhocnote_zone_dest AS stage5_destination,
    mhocnote_user_id AS stage5_user,
    
    -- ============================================================
    -- STAGE 6: Out for Delivery (Runsheet assigned)
    -- ============================================================
    CASE WHEN drsheet_no IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage6_out_for_delivery_status,
    staging.excel_to_ddmmyy_time(drsheet_date) AS stage6_runsheet_date,
    drsheet_no AS stage6_runsheet_no,
    mrsheet_courier_id AS stage6_courier,
    
    -- ============================================================
    -- STAGE 7: Delivered / Undelivered (POD)
    -- FIXED: Using cnote_pod_creation_date instead of cnote_pod_date
    -- ============================================================
    CASE 
        WHEN cnote_pod_creation_date IS NOT NULL THEN 'DELIVERED'
        WHEN dhoundel_no IS NOT NULL THEN 'UNDELIVERED'
        ELSE 'PENDING'
    END AS stage7_delivery_status,
    staging.excel_to_ddmmyy_time(cnote_pod_creation_date) AS stage7_pod_date,
    cnote_pod_receiver AS stage7_receiver,
    cnote_pod_code AS stage7_pod_code,
    
    -- ============================================================
    -- SUMMARY FIELDS
    -- ============================================================
    
    -- Current Stage (highest completed stage)
    CASE 
        WHEN cnote_pod_creation_date IS NOT NULL THEN '7-DELIVERED'
        WHEN dhoundel_no IS NOT NULL THEN '7-UNDELIVERED'
        WHEN drsheet_no IS NOT NULL THEN '6-OUT_FOR_DELIVERY'
        WHEN dhocnote_no IS NOT NULL THEN '5-AT_DESTINATION_HUB'
        WHEN dhicnote_no IS NOT NULL THEN '4-AT_TRANSIT_HUB'
        WHEN manifest_date IS NOT NULL THEN '3-IN_TRANSIT'
        WHEN mfcnote_man_no IS NOT NULL THEN '2-MANIFESTED'
        WHEN cnote_no IS NOT NULL THEN '1-ORDER_CREATED'
        ELSE '0-UNKNOWN'
    END AS current_stage,
    
    -- Stages completed count
    (
        CASE WHEN cnote_date IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN mfcnote_man_no IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN manifest_date IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN dhicnote_no IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN dhocnote_no IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN drsheet_no IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN cnote_pod_creation_date IS NOT NULL OR dhoundel_no IS NOT NULL THEN 1 ELSE 0 END
    ) AS stages_completed,
    
    -- Is delivered flag
    CASE WHEN cnote_pod_creation_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_delivered,
    
    -- Is undelivered flag  
    CASE WHEN dhoundel_no IS NOT NULL AND cnote_pod_creation_date IS NULL THEN TRUE ELSE FALSE END AS is_undelivered

FROM staging.unified_shipments;

-- ============================================================
-- PART 3: PACKAGE JOURNEY VIEW (FIXED WITH FORMATTED DATES)
-- ============================================================

DROP VIEW IF EXISTS audit.v_package_journey CASCADE;

CREATE OR REPLACE VIEW audit.v_package_journey AS
SELECT 
    cnote_no,
    cnote_origin,
    cnote_destination,
    cnote_services_code AS service,
    
    -- Stage 1: Order
    staging.excel_to_ddmmyy_time(cnote_date) AS order_date,
    cnote_user AS order_user,
    
    -- Stage 2: Manifest
    staging.excel_to_ddmmyy_time(mfcnote_man_date) AS manifest_date,
    mfcnote_origin AS manifest_origin,
    
    -- Stage 3: Transit
    staging.excel_to_ddmmyy_time(manifest_date) AS dispatch_date,
    manifest_origin || ' → ' || COALESCE(manifest_thru, cnote_destination) AS route,
    
    -- Stage 4: Hub Inbound
    staging.excel_to_ddmmyy_time(dhicnote_tdate) AS hub_inbound_date,
    mhicnote_zone AS hub_inbound_location,
    mhicnote_user_id AS hub_inbound_user,
    
    -- Stage 5: Hub Outbound
    staging.excel_to_ddmmyy_time(dhocnote_tdate) AS hub_outbound_date,
    mhocnote_zone_dest AS hub_outbound_destination,
    mhocnote_user_id AS hub_outbound_user,
    
    -- Stage 6: Out for Delivery
    staging.excel_to_ddmmyy_time(drsheet_date) AS runsheet_date,
    drsheet_no AS runsheet_no,
    mrsheet_courier_id AS courier_id,
    
    -- Stage 7: Delivered (FIXED)
    staging.excel_to_ddmmyy_time(cnote_pod_creation_date) AS delivered_date,
    cnote_pod_receiver AS receiver,
    cnote_pod_code AS pod_code,
    
    -- Final Status
    CASE 
        WHEN cnote_pod_creation_date IS NOT NULL THEN 'DELIVERED'
        WHEN dhoundel_no IS NOT NULL THEN 'UNDELIVERED'
        WHEN drsheet_no IS NOT NULL THEN 'OUT_FOR_DELIVERY'
        WHEN dhocnote_no IS NOT NULL THEN 'AT_DESTINATION_HUB'
        WHEN dhicnote_no IS NOT NULL THEN 'AT_TRANSIT_HUB'
        WHEN manifest_date IS NOT NULL THEN 'IN_TRANSIT'
        WHEN mfcnote_man_no IS NOT NULL THEN 'MANIFESTED'
        ELSE 'ORDER_CREATED'
    END AS current_status

FROM staging.unified_shipments;

-- ============================================================
-- PART 4: PIPELINE HEALTH (FIXED)
-- ============================================================

DROP VIEW IF EXISTS audit.v_pipeline_health CASCADE;

CREATE OR REPLACE VIEW audit.v_pipeline_health AS
SELECT 
    'BATCH-' || LPAD(ROW_NUMBER() OVER (ORDER BY batch_date DESC)::TEXT, 3, '0') AS batch_id,
    batch_date,
    record_count_total,
    delivered_count,
    undelivered_count,
    in_delivery_count,
    missing_cnote_count,
    missing_origin_count,
    missing_destination_count,
    invalid_weight_count,
    batch_status,
    error_details
FROM (
    SELECT 
        DATE(etl_loaded_at) AS batch_date,
        COUNT(*) AS record_count_total,
        
        -- FIXED: Use cnote_pod_creation_date
        COUNT(CASE WHEN cnote_pod_creation_date IS NOT NULL THEN 1 END) AS delivered_count,
        COUNT(CASE WHEN dhoundel_no IS NOT NULL AND cnote_pod_creation_date IS NULL THEN 1 END) AS undelivered_count,
        COUNT(CASE WHEN drsheet_no IS NOT NULL AND cnote_pod_creation_date IS NULL THEN 1 END) AS in_delivery_count,
        
        -- Data Quality Checks
        COUNT(CASE WHEN cnote_no IS NULL THEN 1 END) AS missing_cnote_count,
        COUNT(CASE WHEN cnote_origin IS NULL THEN 1 END) AS missing_origin_count,
        COUNT(CASE WHEN cnote_destination IS NULL THEN 1 END) AS missing_destination_count,
        COUNT(CASE WHEN cnote_weight IS NULL OR cnote_weight <= 0 THEN 1 END) AS invalid_weight_count,
        
        -- Status
        CASE 
            WHEN COUNT(CASE WHEN cnote_no IS NULL THEN 1 END) = 0 
                 AND COUNT(CASE WHEN cnote_origin IS NULL THEN 1 END) = 0 
            THEN 'SUCCESS'
            WHEN COUNT(CASE WHEN cnote_no IS NULL THEN 1 END) < COUNT(*) * 0.01 
            THEN 'WARNING'
            ELSE 'ERROR'
        END AS batch_status,
        
        CASE 
            WHEN COUNT(CASE WHEN cnote_no IS NULL THEN 1 END) > 0 
            THEN COUNT(CASE WHEN cnote_no IS NULL THEN 1 END) || ' records missing CNOTE_NO'
            WHEN COUNT(CASE WHEN cnote_weight IS NULL OR cnote_weight <= 0 THEN 1 END) > 0
            THEN COUNT(CASE WHEN cnote_weight IS NULL OR cnote_weight <= 0 THEN 1 END) || ' records with invalid weight'
            ELSE 'No errors detected'
        END AS error_details
        
    FROM staging.unified_shipments
    GROUP BY DATE(etl_loaded_at)
) sub
ORDER BY batch_date DESC;

-- ============================================================
-- PART 5: DATA QUALITY SCORECARD (FIXED)
-- ============================================================

DROP VIEW IF EXISTS audit.v_data_quality_scorecard CASCADE;

CREATE OR REPLACE VIEW audit.v_data_quality_scorecard AS
SELECT
    -- Completeness (% non-null)
    ROUND(100.0 * COUNT(cnote_no) / NULLIF(COUNT(*), 0), 2) AS completeness_cnote_no,
    ROUND(100.0 * COUNT(cnote_date) / NULLIF(COUNT(*), 0), 2) AS completeness_cnote_date,
    ROUND(100.0 * COUNT(cnote_origin) / NULLIF(COUNT(*), 0), 2) AS completeness_origin,
    ROUND(100.0 * COUNT(cnote_destination) / NULLIF(COUNT(*), 0), 2) AS completeness_destination,
    ROUND(100.0 * COUNT(cnote_weight) / NULLIF(COUNT(*), 0), 2) AS completeness_weight,
    ROUND(100.0 * COUNT(cnote_shipper_name) / NULLIF(COUNT(*), 0), 2) AS completeness_shipper,
    ROUND(100.0 * COUNT(cnote_receiver_name) / NULLIF(COUNT(*), 0), 2) AS completeness_receiver,
    
    -- Validity
    ROUND(100.0 * COUNT(CASE WHEN cnote_weight > 0 THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS validity_weight_positive,
    ROUND(100.0 * COUNT(CASE WHEN LENGTH(cnote_origin) >= 3 THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS validity_origin_format,
    ROUND(100.0 * COUNT(CASE WHEN LENGTH(cnote_destination) >= 3 THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS validity_destination_format,
    
    -- Overall score
    ROUND((
        100.0 * COUNT(cnote_no) / NULLIF(COUNT(*), 0) +
        100.0 * COUNT(cnote_origin) / NULLIF(COUNT(*), 0) +
        100.0 * COUNT(cnote_destination) / NULLIF(COUNT(*), 0) +
        100.0 * COUNT(cnote_weight) / NULLIF(COUNT(*), 0)
    ) / 4, 2) AS overall_completeness_score,
    
    COUNT(*) AS total_records,
    NOW() AS scored_at
    
FROM staging.unified_shipments;

-- ============================================================
-- PART 6: STAGE DISTRIBUTION (FIXED)
-- ============================================================

DROP VIEW IF EXISTS audit.v_stage_distribution CASCADE;

CREATE OR REPLACE VIEW audit.v_stage_distribution AS
SELECT 
    current_stage,
    COUNT(*) AS shipment_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM (
    SELECT 
        CASE 
            WHEN cnote_pod_creation_date IS NOT NULL THEN '7-DELIVERED'
            WHEN dhoundel_no IS NOT NULL THEN '7-UNDELIVERED'
            WHEN drsheet_no IS NOT NULL THEN '6-OUT_FOR_DELIVERY'
            WHEN dhocnote_no IS NOT NULL THEN '5-AT_DESTINATION_HUB'
            WHEN dhicnote_no IS NOT NULL THEN '4-AT_TRANSIT_HUB'
            WHEN manifest_date IS NOT NULL THEN '3-IN_TRANSIT'
            WHEN mfcnote_man_no IS NOT NULL THEN '2-MANIFESTED'
            WHEN cnote_no IS NOT NULL THEN '1-ORDER_CREATED'
            ELSE '0-UNKNOWN'
        END AS current_stage
    FROM staging.unified_shipments
) sub
GROUP BY current_stage
ORDER BY current_stage DESC;

-- ============================================================
-- PART 7: USER TRACEABILITY (WITH FORMATTED DATES)
-- ============================================================

DROP VIEW IF EXISTS audit.v_user_traceability CASCADE;

CREATE OR REPLACE VIEW audit.v_user_traceability AS
SELECT 
    cnote_no,
    
    -- Stage 1: Order Creation
    cnote_user AS stage1_order_user,
    staging.excel_to_ddmmyy_time(cnote_date) AS stage1_timestamp,
    
    -- Stage 2: Runsheet Pickup
    mrcnote_user_id AS stage2_pickup_user,
    staging.excel_to_ddmmyy_time(mrcnote_date) AS stage2_timestamp,
    
    -- Stage 3: Hub Inbound
    mhicnote_user_id AS stage3_hub_inbound_user,
    staging.excel_to_ddmmyy_time(mhicnote_date) AS stage3_timestamp,
    
    -- Stage 4: Hub Outbound
    mhocnote_user_id AS stage4_hub_outbound_user,
    staging.excel_to_ddmmyy_time(mhocnote_date) AS stage4_timestamp,
    
    -- Stage 5: Delivery Courier
    mrsheet_courier_id AS stage5_courier_id,
    staging.excel_to_ddmmyy_time(mrsheet_date) AS stage5_timestamp,
    
    -- Stage 6: Hub Operations
    mhi_uid AS stage6_hub_ops_user,
    staging.excel_to_ddmmyy_time(mhi_date) AS stage6_timestamp,
    
    -- ORA_USER lookup
    ora_user_name,
    ora_user_origin AS user_branch,
    ora_user_zone_code AS user_zone,
    
    -- Count of stages with user tracking
    (
        CASE WHEN cnote_user IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN mrcnote_user_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN mhicnote_user_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN mhocnote_user_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN mrsheet_courier_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN mhi_uid IS NOT NULL THEN 1 ELSE 0 END
    ) AS stages_with_user_tracking

FROM staging.unified_shipments;

-- ============================================================
-- PART 8: USER ACTIVITY SUMMARY
-- ============================================================

DROP VIEW IF EXISTS audit.v_user_activity_summary CASCADE;

CREATE OR REPLACE VIEW audit.v_user_activity_summary AS
SELECT 
    cnote_user AS user_id,
    ora_user_name AS user_name,
    ora_user_origin AS user_branch,
    COUNT(*) AS orders_created,
    COUNT(CASE WHEN cnote_pod_creation_date IS NOT NULL THEN 1 END) AS orders_delivered,
    staging.excel_to_ddmmyy(MIN(cnote_date)) AS first_activity,
    staging.excel_to_ddmmyy(MAX(cnote_date)) AS last_activity
FROM staging.unified_shipments
WHERE cnote_user IS NOT NULL
GROUP BY cnote_user, ora_user_name, ora_user_origin
ORDER BY orders_created DESC;

-- ============================================================
-- VERIFICATION QUERIES
-- ============================================================

-- Test date conversion
SELECT 
    'Date Conversion Test' AS test,
    staging.excel_to_timestamp(45977.2295023148) AS as_timestamp,
    staging.excel_to_ddmmyy(45977.2295023148) AS as_ddmmyy,
    staging.excel_to_ddmmyy_time(45977.2295023148) AS as_ddmmyy_time;

-- Check stage distribution (should now show DELIVERED instead of OUT_FOR_DELIVERY)
SELECT * FROM audit.v_stage_distribution;

-- Sample delivery stage monitor
SELECT 
    cnote_no,
    stage1_order_status, stage1_order_date,
    stage6_out_for_delivery_status, stage6_runsheet_date,
    stage7_delivery_status, stage7_pod_date,
    current_stage,
    stages_completed
FROM audit.v_delivery_stage_monitor
LIMIT 10;

-- Check data quality
SELECT * FROM audit.v_data_quality_scorecard;
