-- ============================================================
-- JNE AUDIT TRAIL VIEWS
-- ============================================================
-- Based on 3 Audit Frameworks:
--   1. Traceability (Package Journey Log)
--   2. Automated Monitoring (Pipeline Health Log)
--   3. Integrity (Validation & Version Log)
-- Plus:
--   4. Real-Time Delivery Stage Monitor
--   5. User Traceability by Stage
-- ============================================================

-- Create audit schema if not exists
CREATE SCHEMA IF NOT EXISTS audit;

-- ============================================================
-- 1. TRACEABILITY: Package Journey Log
-- ============================================================
-- Objective: Provide unbroken "Chain of Custody" for a single record

CREATE OR REPLACE VIEW audit.v_package_journey AS
SELECT 
    cnote_no,
    cnote_date,
    cnote_origin,
    cnote_destination,
    cnote_services_code AS service_type,
    cnote_shipper_name,
    cnote_receiver_name,
    
    -- Stage 1: Order Created
    CASE WHEN cnote_no IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage_1_order_created,
    cnote_date AS stage_1_timestamp,
    cnote_user AS stage_1_user,
    
    -- Stage 2: Manifest Created (Origin)
    CASE WHEN mfcnote_man_no IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage_2_manifest_created,
    mfcnote_man_date AS stage_2_timestamp,
    mfcnote_origin AS stage_2_location,
    
    -- Stage 3: Outbound from Origin
    CASE WHEN manifest_date IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage_3_outbound,
    manifest_date AS stage_3_timestamp,
    manifest_origin AS stage_3_origin,
    manifest_thru AS stage_3_destination,
    
    -- Stage 4: Inbound at HO (Hub)
    CASE WHEN dhicnote_no IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage_4_inbound_ho,
    dhicnote_tdate AS stage_4_timestamp,
    mhicnote_zone AS stage_4_zone,
    mhicnote_user_id AS stage_4_user,
    
    -- Stage 5: Outbound from HO
    CASE WHEN dhocnote_no IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage_5_outbound_ho,
    dhocnote_tdate AS stage_5_timestamp,
    mhocnote_zone_dest AS stage_5_destination,
    mhocnote_user_id AS stage_5_user,
    
    -- Stage 6: Runsheet Assigned (Last Mile)
    CASE WHEN drsheet_no IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage_6_runsheet,
    drsheet_date AS stage_6_timestamp,
    mrsheet_courier_id AS stage_6_courier,
    
    -- Stage 7: Delivered
    CASE WHEN cnote_pod_date IS NOT NULL THEN 'COMPLETED' 
         WHEN dhoundel_no IS NOT NULL THEN 'UNDELIVERED'
         ELSE 'PENDING' END AS stage_7_delivery,
    cnote_pod_date AS stage_7_timestamp,
    cnote_pod_receiver AS stage_7_receiver,
    cnote_pod_code AS stage_7_pod_code,
    
    -- Current Status Summary
    CASE 
        WHEN cnote_pod_date IS NOT NULL THEN 'DELIVERED'
        WHEN dhoundel_no IS NOT NULL THEN 'UNDELIVERED'
        WHEN drsheet_no IS NOT NULL THEN 'OUT_FOR_DELIVERY'
        WHEN dhocnote_no IS NOT NULL THEN 'AT_DESTINATION_HUB'
        WHEN dhicnote_no IS NOT NULL THEN 'AT_TRANSIT_HUB'
        WHEN manifest_date IS NOT NULL THEN 'IN_TRANSIT'
        WHEN mfcnote_man_no IS NOT NULL THEN 'MANIFESTED'
        WHEN cnote_no IS NOT NULL THEN 'ORDER_CREATED'
        ELSE 'UNKNOWN'
    END AS current_status,
    
    -- Count completed stages
    (CASE WHEN cnote_no IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN mfcnote_man_no IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN manifest_date IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN dhicnote_no IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN dhocnote_no IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN drsheet_no IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN cnote_pod_date IS NOT NULL THEN 1 ELSE 0 END
    ) AS stages_completed,
    
    etl_loaded_at
    
FROM staging.unified_shipments;

-- ============================================================
-- 2. AUTOMATED MONITORING: Pipeline Health Log
-- ============================================================
-- Objective: Track Success/Failure Counts and ensure zero-loss ingestion

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
    error_details,
    batch_start,
    batch_end
FROM (
    SELECT 
        DATE(etl_loaded_at) AS batch_date,
        COUNT(*) AS record_count_total,
        
        -- Record counts by status
        COUNT(CASE WHEN cnote_pod_date IS NOT NULL THEN 1 END) AS delivered_count,
        COUNT(CASE WHEN dhoundel_no IS NOT NULL AND cnote_pod_date IS NULL THEN 1 END) AS undelivered_count,
        COUNT(CASE WHEN drsheet_no IS NOT NULL AND cnote_pod_date IS NULL THEN 1 END) AS in_delivery_count,
        
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
        
        -- Error summary
        CASE 
            WHEN COUNT(CASE WHEN cnote_no IS NULL THEN 1 END) > 0 
            THEN COUNT(CASE WHEN cnote_no IS NULL THEN 1 END) || ' records missing CNOTE_NO'
            WHEN COUNT(CASE WHEN cnote_weight IS NULL OR cnote_weight <= 0 THEN 1 END) > 0
            THEN COUNT(CASE WHEN cnote_weight IS NULL OR cnote_weight <= 0 THEN 1 END) || ' records with invalid weight'
            ELSE 'No errors detected'
        END AS error_details,
        
        MIN(etl_loaded_at) AS batch_start,
        MAX(etl_loaded_at) AS batch_end
        
    FROM staging.unified_shipments
    GROUP BY DATE(etl_loaded_at)
) sub
ORDER BY batch_date DESC;

-- ============================================================
-- 3. INTEGRITY: Data Quality Scorecard
-- ============================================================
-- Objective: Guarantee data formatting is immutable and verifiable

CREATE OR REPLACE VIEW audit.v_data_quality_scorecard AS
SELECT
    -- Completeness Metrics (% of non-null values)
    ROUND(100.0 * COUNT(cnote_no) / NULLIF(COUNT(*), 0), 2) AS completeness_cnote_no,
    ROUND(100.0 * COUNT(cnote_date) / NULLIF(COUNT(*), 0), 2) AS completeness_cnote_date,
    ROUND(100.0 * COUNT(cnote_origin) / NULLIF(COUNT(*), 0), 2) AS completeness_origin,
    ROUND(100.0 * COUNT(cnote_destination) / NULLIF(COUNT(*), 0), 2) AS completeness_destination,
    ROUND(100.0 * COUNT(cnote_weight) / NULLIF(COUNT(*), 0), 2) AS completeness_weight,
    ROUND(100.0 * COUNT(cnote_shipper_name) / NULLIF(COUNT(*), 0), 2) AS completeness_shipper,
    ROUND(100.0 * COUNT(cnote_receiver_name) / NULLIF(COUNT(*), 0), 2) AS completeness_receiver,
    
    -- Validity Metrics
    ROUND(100.0 * COUNT(CASE WHEN cnote_weight > 0 THEN 1 END) / NULLIF(COUNT(cnote_weight), 0), 2) AS validity_weight_positive,
    ROUND(100.0 * COUNT(CASE WHEN LENGTH(cnote_origin) >= 3 THEN 1 END) / NULLIF(COUNT(cnote_origin), 0), 2) AS validity_origin_format,
    ROUND(100.0 * COUNT(CASE WHEN LENGTH(cnote_destination) >= 3 THEN 1 END) / NULLIF(COUNT(cnote_destination), 0), 2) AS validity_destination_format,
    
    -- Overall Scores
    ROUND((
        (100.0 * COUNT(cnote_no) / NULLIF(COUNT(*), 0)) +
        (100.0 * COUNT(cnote_origin) / NULLIF(COUNT(*), 0)) +
        (100.0 * COUNT(cnote_destination) / NULLIF(COUNT(*), 0)) +
        (100.0 * COUNT(cnote_weight) / NULLIF(COUNT(*), 0))
    ) / 4, 2) AS overall_completeness_score,
    
    COUNT(*) AS total_records,
    NOW() AS scored_at
    
FROM staging.unified_shipments;

-- ============================================================
-- 4. REAL-TIME DELIVERY STAGE MONITOR
-- ============================================================
-- Shows current stage of each shipment based on missing entries

CREATE OR REPLACE VIEW audit.v_delivery_stage_monitor AS
SELECT 
    cnote_no,
    cnote_date,
    cnote_origin,
    cnote_destination,
    cnote_services_code AS service,
    
    -- Current Stage (based on what's filled vs missing)
    CASE 
        WHEN cnote_pod_date IS NOT NULL THEN '7-DELIVERED'
        WHEN dhoundel_no IS NOT NULL THEN '7-UNDELIVERED'
        WHEN drsheet_no IS NOT NULL THEN '6-OUT_FOR_DELIVERY'
        WHEN dhocnote_no IS NOT NULL THEN '5-OUTBOUND_DESTINATION'
        WHEN dhicnote_no IS NOT NULL THEN '4-INBOUND_HUB'
        WHEN manifest_date IS NOT NULL THEN '3-IN_TRANSIT'
        WHEN mfcnote_man_no IS NOT NULL THEN '2-MANIFESTED'
        WHEN cnote_no IS NOT NULL THEN '1-ORDER_CREATED'
        ELSE '0-UNKNOWN'
    END AS current_stage,
    
    -- Next Expected Action
    CASE 
        WHEN cnote_pod_date IS NOT NULL THEN 'COMPLETE'
        WHEN dhoundel_no IS NOT NULL THEN 'RE-ATTEMPT DELIVERY'
        WHEN drsheet_no IS NOT NULL THEN 'AWAITING DELIVERY CONFIRMATION'
        WHEN dhocnote_no IS NOT NULL THEN 'AWAITING RUNSHEET ASSIGNMENT'
        WHEN dhicnote_no IS NOT NULL THEN 'AWAITING OUTBOUND FROM HUB'
        WHEN manifest_date IS NOT NULL THEN 'AWAITING HUB INBOUND'
        WHEN mfcnote_man_no IS NOT NULL THEN 'AWAITING DISPATCH'
        WHEN cnote_no IS NOT NULL THEN 'AWAITING MANIFEST'
        ELSE 'CHECK DATA'
    END AS next_action,
    
    -- Last Activity Timestamp (use COALESCE to find most recent)
    COALESCE(
        cnote_pod_date,
        dhoundel_create_date,
        drsheet_date,
        dhocnote_tdate,
        dhicnote_tdate,
        manifest_date,
        mfcnote_man_date,
        cnote_date
    ) AS last_activity,
    
    -- Flag stale shipments (not delivered)
    CASE 
        WHEN cnote_pod_date IS NOT NULL THEN FALSE
        ELSE TRUE
    END AS is_pending
    
FROM staging.unified_shipments;

-- ============================================================
-- 5. USER TRACEABILITY BY STAGE
-- ============================================================
-- Track which user inputted an entry at each delivery stage

CREATE OR REPLACE VIEW audit.v_user_traceability AS
SELECT 
    cnote_no,
    cnote_date,
    cnote_origin,
    cnote_destination,
    
    -- Stage 1: Order Creation
    cnote_user AS stage1_order_user,
    cnote_date AS stage1_timestamp,
    
    -- Stage 2: Runsheet Creation (Pickup)
    mrcnote_user_id AS stage2_runsheet_user,
    mrcnote_date AS stage2_timestamp,
    
    -- Stage 3: Inbound at Hub
    mhicnote_user_id AS stage3_inbound_user,
    mhicnote_date AS stage3_timestamp,
    
    -- Stage 4: Outbound from Hub  
    mhocnote_user_id AS stage4_outbound_user,
    mhocnote_date AS stage4_timestamp,
    
    -- Stage 5: Runsheet Assignment (Delivery)
    mrsheet_courier_id AS stage5_courier_id,
    mrsheet_date AS stage5_timestamp,
    
    -- Stage 6: Hub Operations
    mhi_uid AS stage6_hub_user,
    mhi_date AS stage6_timestamp,
    
    -- ORA_USER lookup (for user details)
    ora_user_name,
    ora_user_origin AS user_branch,
    ora_user_zone_code AS user_zone,
    
    -- Count of stages with user tracking
    (CASE WHEN cnote_user IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN mrcnote_user_id IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN mhicnote_user_id IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN mhocnote_user_id IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN mrsheet_courier_id IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN mhi_uid IS NOT NULL THEN 1 ELSE 0 END
    ) AS stages_with_user_tracking
    
FROM staging.unified_shipments;

-- ============================================================
-- SUMMARY STATISTICS VIEWS
-- ============================================================

-- Stage Distribution Summary
CREATE OR REPLACE VIEW audit.v_stage_distribution AS
SELECT 
    CASE 
        WHEN cnote_pod_date IS NOT NULL THEN 'DELIVERED'
        WHEN dhoundel_no IS NOT NULL THEN 'UNDELIVERED'
        WHEN drsheet_no IS NOT NULL THEN 'OUT_FOR_DELIVERY'
        WHEN dhocnote_no IS NOT NULL THEN 'AT_DESTINATION_HUB'
        WHEN dhicnote_no IS NOT NULL THEN 'AT_TRANSIT_HUB'
        WHEN manifest_date IS NOT NULL THEN 'IN_TRANSIT'
        WHEN mfcnote_man_no IS NOT NULL THEN 'MANIFESTED'
        WHEN cnote_no IS NOT NULL THEN 'ORDER_CREATED'
        ELSE 'UNKNOWN'
    END AS current_stage,
    COUNT(*) AS shipment_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM staging.unified_shipments
GROUP BY 1
ORDER BY shipment_count DESC;

-- User Activity Summary
CREATE OR REPLACE VIEW audit.v_user_activity_summary AS
SELECT 
    cnote_user AS user_id,
    ora_user_name AS user_name,
    ora_user_origin AS user_branch,
    COUNT(*) AS orders_created,
    COUNT(CASE WHEN cnote_pod_date IS NOT NULL THEN 1 END) AS orders_delivered,
    MIN(cnote_date) AS first_activity,
    MAX(cnote_date) AS last_activity
FROM staging.unified_shipments
WHERE cnote_user IS NOT NULL
GROUP BY cnote_user, ora_user_name, ora_user_origin
ORDER BY orders_created DESC;

-- ============================================================
-- GRANT PERMISSIONS (adjust as needed)
-- ============================================================
-- GRANT SELECT ON ALL TABLES IN SCHEMA audit TO jne_readonly;

SELECT 'Audit views created successfully!' AS status;
