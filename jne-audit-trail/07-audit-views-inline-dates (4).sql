-- ============================================================
-- JNE DATA PIPELINE: Fixed Audit Views with Inline Date Conversion
-- ============================================================
-- This version uses inline date conversion (no separate functions)
-- Excel date = days since 1899-12-30
-- ============================================================

-- ============================================================
-- PART 1: DELIVERY STAGE MONITOR (ALL STAGES WITH TIMESTAMPS)
-- ============================================================

DROP VIEW IF EXISTS audit.v_delivery_stage_monitor CASCADE;

CREATE OR REPLACE VIEW audit.v_delivery_stage_monitor AS
SELECT 
    cnote_no,
    
    -- Basic shipment info
    cnote_origin,
    cnote_destination,
    cnote_services_code AS service,
    
    -- ============================================================
    -- DELIVERY CATEGORY (Package Journey Type)
    -- ============================================================
    CASE 
        -- Direct Intracity: Same city/zone (origin equals destination or same area code)
        WHEN cnote_origin = cnote_destination THEN 'Direct Intracity'
        WHEN LEFT(cnote_origin, 8) = LEFT(cnote_destination, 8) THEN 'Direct Intracity'
        
        -- Transit Domestic: Goes through gateway (manifest_thru is populated and different from final destination)
        WHEN manifest_thru IS NOT NULL 
             AND manifest_thru != '' 
             AND manifest_thru != cnote_destination 
             AND LEFT(cnote_origin, 3) != LEFT(cnote_destination, 3)
        THEN 'Transit Domestic'
        
        -- Direct Intercity: Same main branch (first 3 chars) but different city
        WHEN LEFT(cnote_origin, 3) = LEFT(cnote_destination, 3) 
             AND cnote_origin != cnote_destination 
        THEN 'Direct Intercity'
        
        -- Direct Domestic: Different main branch, direct manifest (no transit)
        WHEN LEFT(cnote_origin, 3) != LEFT(cnote_destination, 3)
             AND (manifest_thru IS NULL OR manifest_thru = '' OR manifest_thru = cnote_destination)
        THEN 'Direct Domestic'
        
        ELSE 'Unknown'
    END AS delivery_category,
    
    -- ============================================================
    -- STAGE 1: Order Created
    -- ============================================================
    CASE WHEN cnote_date IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage1_order_status,
    CASE WHEN cnote_date IS NOT NULL AND cnote_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (cnote_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS stage1_order_date,
    cnote_user AS stage1_user,
    
    -- ============================================================
    -- STAGE 2: Manifest Created (Package grouped for dispatch)
    -- ============================================================
    CASE WHEN mfcnote_man_no IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage2_manifest_status,
    CASE WHEN manifest_date IS NOT NULL AND manifest_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (manifest_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS stage2_manifest_date,
    mfcnote_man_no AS stage2_manifest_no,
    mfcnote_origin AS stage2_origin,
    
    -- ============================================================
    -- STAGE 3: In Transit (Dispatched from origin)
    -- ============================================================
    -- Note: Using manifest_date as dispatch timestamp since separate dispatch date not available
    CASE WHEN manifest_origin IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage3_transit_status,
    CASE WHEN manifest_date IS NOT NULL AND manifest_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (manifest_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS stage3_dispatch_date,
    manifest_origin AS stage3_from,
    manifest_thru AS stage3_gateway,
    cnote_destination AS stage3_to,
    
    -- ============================================================
    -- STAGE 4: Arrived at Hub (Inbound scan)
    -- ============================================================
    CASE WHEN dhicnote_no IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage4_hub_inbound_status,
    CASE WHEN dhicnote_tdate IS NOT NULL AND dhicnote_tdate > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (dhicnote_tdate * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS stage4_hub_inbound_date,
    mhicnote_zone AS stage4_hub_location,
    mhicnote_user_id AS stage4_user,
    
    -- ============================================================
    -- STAGE 5: Departed Hub (Outbound scan)
    -- ============================================================
    CASE WHEN dhocnote_no IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage5_hub_outbound_status,
    CASE WHEN dhocnote_tdate IS NOT NULL AND dhocnote_tdate > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (dhocnote_tdate * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS stage5_hub_outbound_date,
    mhocnote_zone_dest AS stage5_destination,
    mhocnote_user_id AS stage5_user,
    
    -- ============================================================
    -- STAGE 6: Out for Delivery (Runsheet assigned)
    -- ============================================================
    CASE WHEN drsheet_no IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END AS stage6_out_for_delivery_status,
    CASE WHEN drsheet_date IS NOT NULL AND drsheet_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (drsheet_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS stage6_runsheet_date,
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
    CASE WHEN cnote_pod_creation_date IS NOT NULL AND cnote_pod_creation_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (cnote_pod_creation_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS stage7_pod_date,
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
-- PART 2: PACKAGE JOURNEY VIEW
-- ============================================================

DROP VIEW IF EXISTS audit.v_package_journey CASCADE;

CREATE OR REPLACE VIEW audit.v_package_journey AS
SELECT 
    cnote_no,
    cnote_origin,
    cnote_destination,
    cnote_services_code AS service,
    
    -- ============================================================
    -- DELIVERY CATEGORY (Package Journey Type)
    -- ============================================================
    CASE 
        -- Direct Intracity: Same city/zone
        WHEN cnote_origin = cnote_destination THEN 'Direct Intracity'
        WHEN LEFT(cnote_origin, 8) = LEFT(cnote_destination, 8) THEN 'Direct Intracity'
        
        -- Transit Domestic: Goes through gateway
        WHEN manifest_thru IS NOT NULL 
             AND manifest_thru != '' 
             AND manifest_thru != cnote_destination 
             AND LEFT(cnote_origin, 3) != LEFT(cnote_destination, 3)
        THEN 'Transit Domestic'
        
        -- Direct Intercity: Same main branch but different city
        WHEN LEFT(cnote_origin, 3) = LEFT(cnote_destination, 3) 
             AND cnote_origin != cnote_destination 
        THEN 'Direct Intercity'
        
        -- Direct Domestic: Different main branch, direct manifest
        WHEN LEFT(cnote_origin, 3) != LEFT(cnote_destination, 3)
             AND (manifest_thru IS NULL OR manifest_thru = '' OR manifest_thru = cnote_destination)
        THEN 'Direct Domestic'
        
        ELSE 'Unknown'
    END AS delivery_category,
    
    -- Stage 1: Order
    CASE WHEN cnote_date IS NOT NULL AND cnote_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (cnote_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS order_date,
    cnote_user AS order_user,
    
    -- Stage 2: Manifest
    CASE WHEN manifest_date IS NOT NULL AND manifest_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (manifest_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS manifest_created_date,
    mfcnote_man_no AS manifest_no,
    mfcnote_origin AS manifest_origin,
    
    -- Stage 3: Transit/Dispatch
    manifest_origin AS dispatch_origin,
    manifest_thru AS transit_gateway,
    manifest_origin || ' → ' || COALESCE(manifest_thru, cnote_destination) AS route,
    
    -- Stage 4: Hub Inbound
    CASE WHEN dhicnote_tdate IS NOT NULL AND dhicnote_tdate > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (dhicnote_tdate * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS hub_inbound_date,
    mhicnote_zone AS hub_inbound_location,
    mhicnote_user_id AS hub_inbound_user,
    
    -- Stage 5: Hub Outbound
    CASE WHEN dhocnote_tdate IS NOT NULL AND dhocnote_tdate > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (dhocnote_tdate * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS hub_outbound_date,
    mhocnote_zone_dest AS hub_outbound_destination,
    mhocnote_user_id AS hub_outbound_user,
    
    -- Stage 6: Out for Delivery
    CASE WHEN drsheet_date IS NOT NULL AND drsheet_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (drsheet_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS runsheet_date,
    drsheet_no AS runsheet_no,
    mrsheet_courier_id AS courier_id,
    
    -- Stage 7: Delivered (FIXED)
    CASE WHEN cnote_pod_creation_date IS NOT NULL AND cnote_pod_creation_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (cnote_pod_creation_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS delivered_date,
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
-- PART 3: PIPELINE HEALTH
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
-- PART 4: DATA QUALITY SCORECARD
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
-- PART 5: STAGE DISTRIBUTION
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
-- PART 6: USER TRACEABILITY
-- ============================================================

DROP VIEW IF EXISTS audit.v_user_traceability CASCADE;

CREATE OR REPLACE VIEW audit.v_user_traceability AS
SELECT 
    cnote_no,
    
    -- Stage 1: Order Creation
    cnote_user AS stage1_order_user,
    CASE WHEN cnote_date IS NOT NULL AND cnote_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (cnote_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS stage1_timestamp,
    
    -- Stage 2: Runsheet Pickup
    mrcnote_user_id AS stage2_pickup_user,
    CASE WHEN mrcnote_date IS NOT NULL AND mrcnote_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (mrcnote_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS stage2_timestamp,
    
    -- Stage 3: Hub Inbound
    mhicnote_user_id AS stage3_hub_inbound_user,
    CASE WHEN mhicnote_date IS NOT NULL AND mhicnote_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (mhicnote_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS stage3_timestamp,
    
    -- Stage 4: Hub Outbound
    mhocnote_user_id AS stage4_hub_outbound_user,
    CASE WHEN mhocnote_date IS NOT NULL AND mhocnote_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (mhocnote_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS stage4_timestamp,
    
    -- Stage 5: Delivery Courier
    mrsheet_courier_id AS stage5_courier_id,
    CASE WHEN mrsheet_date IS NOT NULL AND mrsheet_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (mrsheet_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS stage5_timestamp,
    
    -- Stage 6: Hub Operations
    mhi_uid AS stage6_hub_ops_user,
    CASE WHEN mhi_date IS NOT NULL AND mhi_date > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (mhi_date * INTERVAL '1 day'), 'DD/MM/YY HH24:MI')
         ELSE NULL END AS stage6_timestamp,
    
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
-- PART 7: USER ACTIVITY SUMMARY
-- ============================================================

DROP VIEW IF EXISTS audit.v_user_activity_summary CASCADE;

CREATE OR REPLACE VIEW audit.v_user_activity_summary AS
SELECT 
    cnote_user AS user_id,
    ora_user_name AS user_name,
    ora_user_origin AS user_branch,
    COUNT(*) AS orders_created,
    COUNT(CASE WHEN cnote_pod_creation_date IS NOT NULL THEN 1 END) AS orders_delivered,
    CASE WHEN MIN(cnote_date) IS NOT NULL AND MIN(cnote_date) > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (MIN(cnote_date) * INTERVAL '1 day'), 'DD/MM/YY')
         ELSE NULL END AS first_activity,
    CASE WHEN MAX(cnote_date) IS NOT NULL AND MAX(cnote_date) > 0 
         THEN TO_CHAR(TIMESTAMP '1899-12-30' + (MAX(cnote_date) * INTERVAL '1 day'), 'DD/MM/YY')
         ELSE NULL END AS last_activity
FROM staging.unified_shipments
WHERE cnote_user IS NOT NULL
GROUP BY cnote_user, ora_user_name, ora_user_origin
ORDER BY orders_created DESC;

-- ============================================================
-- PART 8: DELIVERY CATEGORY DISTRIBUTION
-- ============================================================

DROP VIEW IF EXISTS audit.v_delivery_category_distribution CASCADE;

CREATE OR REPLACE VIEW audit.v_delivery_category_distribution AS
SELECT 
    delivery_category,
    COUNT(*) AS shipment_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM (
    SELECT 
        CASE 
            WHEN cnote_origin = cnote_destination THEN 'Direct Intracity'
            WHEN LEFT(cnote_origin, 8) = LEFT(cnote_destination, 8) THEN 'Direct Intracity'
            WHEN manifest_thru IS NOT NULL 
                 AND manifest_thru != '' 
                 AND manifest_thru != cnote_destination 
                 AND LEFT(cnote_origin, 3) != LEFT(cnote_destination, 3)
            THEN 'Transit Domestic'
            WHEN LEFT(cnote_origin, 3) = LEFT(cnote_destination, 3) 
                 AND cnote_origin != cnote_destination 
            THEN 'Direct Intercity'
            WHEN LEFT(cnote_origin, 3) != LEFT(cnote_destination, 3)
                 AND (manifest_thru IS NULL OR manifest_thru = '' OR manifest_thru = cnote_destination)
            THEN 'Direct Domestic'
            ELSE 'Unknown'
        END AS delivery_category
    FROM staging.unified_shipments
) sub
GROUP BY delivery_category
ORDER BY shipment_count DESC;

-- ============================================================
-- PART 9: SOURCE TABLE LINEAGE (Join Relationships)
-- ============================================================
-- Shows how each source table joins into unified_shipments

DROP VIEW IF EXISTS audit.v_source_table_lineage CASCADE;

CREATE OR REPLACE VIEW audit.v_source_table_lineage AS
SELECT * FROM (VALUES
    -- Base Table
    ('CMS_CNOTE', 'BASE TABLE', '-', 'Base table - all shipments start here', 1),
    
    -- Direct joins to CMS_CNOTE via CNOTE_NO
    ('CMS_CNOTE_AMO', 'CMS_CNOTE', 'CNOTE_NO = CNOTE_AMO_NO', 'Additional amount/fee information', 2),
    ('CMS_CNOTE_POD', 'CMS_CNOTE', 'CNOTE_NO = CNOTE_POD_NO', 'Proof of Delivery details', 2),
    ('CMS_DRCNOTE', 'CMS_CNOTE', 'CNOTE_NO = DRCNOTE_CNOTE_NO', 'Detail receiving note', 2),
    ('CMS_MRCNOTE', 'CMS_CNOTE', 'CNOTE_NO = MRCNOTE_CNOTE_NO', 'Master receiving note', 2),
    ('CMS_DSTATUS', 'CMS_CNOTE', 'CNOTE_NO = DSTATUS_CNOTE_NO', 'Delivery status tracking', 2),
    ('CMS_DHOUNDEL_POD', 'CMS_CNOTE', 'CNOTE_NO = DHOUNDEL_CNOTE_NO', 'Undelivered POD details', 2),
    ('CMS_MHOUNDEL_POD', 'CMS_CNOTE', 'CNOTE_NO = MHOUNDEL_CNOTE_NO', 'Master undelivered POD', 2),
    
    -- Manifest chain (CNOTE → MFCNOTE → MANIFEST)
    ('CMS_MFCNOTE', 'CMS_CNOTE', 'CNOTE_NO = MFCNOTE_CNOTE_NO', 'Manifest consignment note link', 2),
    ('CMS_MANIFEST', 'CMS_MFCNOTE', 'MFCNOTE_MAN_NO = MANIFEST_NO', 'Manifest header details', 3),
    
    -- Bag chain (MANIFEST → DMBAG → DBAG_HO)
    ('CMS_DMBAG', 'CMS_MANIFEST', 'MANIFEST_NO = DMBAG_MAN_NO', 'Detail manifest bag', 4),
    ('CMS_DBAG_HO', 'CMS_DMBAG', 'DMBAG_BAG_NO = DBAG_BAG_NO', 'Bag handover details', 5),
    
    -- SMU chain (MANIFEST → DSMU → MSMU)
    ('CMS_DSMU', 'CMS_MANIFEST', 'MANIFEST_NO = DSMU_MAN_NO', 'Detail SMU (shipping unit)', 4),
    ('CMS_MSMU', 'CMS_DSMU', 'DSMU_SMU_NO = MSMU_SMU_NO', 'Master SMU details', 5),
    
    -- Hub Inbound chain (CNOTE → DHICNOTE → MHICNOTE → DHI_HOC → MHI_HOC)
    ('CMS_DHICNOTE', 'CMS_CNOTE', 'CNOTE_NO = DHICNOTE_CNOTE_NO', 'Detail hub inbound consignment', 2),
    ('CMS_MHICNOTE', 'CMS_DHICNOTE', 'DHICNOTE_NO = MHICNOTE_DHICNOTE_NO', 'Master hub inbound consignment', 3),
    ('CMS_DHI_HOC', 'CMS_MHICNOTE', 'MHICNOTE_NO = DHI_NO', 'Detail hub inbound handover', 4),
    ('CMS_MHI_HOC', 'CMS_DHI_HOC', 'DHI_NO = MHI_DHI_NO', 'Master hub inbound handover', 5),
    
    -- Hub Outbound chain (CNOTE → DHOCNOTE → MHOCNOTE)
    ('CMS_DHOCNOTE', 'CMS_CNOTE', 'CNOTE_NO = DHOCNOTE_CNOTE_NO', 'Detail hub outbound consignment', 2),
    ('CMS_MHOCNOTE', 'CMS_DHOCNOTE', 'DHOCNOTE_NO = MHOCNOTE_DHOCNOTE_NO', 'Master hub outbound consignment', 3),
    
    -- Runsheet chain (CNOTE → DRSHEET → MRSHEET)
    ('CMS_DRSHEET', 'CMS_CNOTE', 'CNOTE_NO = DRSHEET_CNOTE_NO', 'Detail runsheet', 2),
    ('CMS_MRSHEET', 'CMS_DRSHEET', 'DRSHEET_NO = MRSHEET_DRSHEET_NO', 'Master runsheet', 3),
    ('CMS_DRSHEET_PRA', 'CMS_DRSHEET', 'DRSHEET_NO = DRSHEET_PRA_DRSHEET_NO', 'Runsheet pre-alert', 3),
    ('CMS_DHOV_RSHEET', 'CMS_DRSHEET', 'DRSHEET_NO = DHOV_DRSHEET_NO', 'Runsheet handover detail', 3),
    
    -- SJ (Surat Jalan) chain
    ('CMS_DSJ', 'CMS_MANIFEST', 'MANIFEST_NO = DSJ_MAN_NO', 'Detail surat jalan', 4),
    ('CMS_MSJ', 'CMS_DSJ', 'DSJ_SJ_NO = MSJ_SJ_NO', 'Master surat jalan', 5),
    ('CMS_RDSJ', 'CMS_DSJ', 'DSJ_SJ_NO = RDSJ_SJ_NO', 'Return detail surat jalan', 5),
    
    -- Transit cost tables
    ('CMS_COST_DTRANSIT_AGEN', 'CMS_CNOTE', 'CNOTE_NO = COST_DTRANSIT_CNOTE_NO', 'Detail transit agent cost', 2),
    ('CMS_COST_MTRANSIT_AGEN', 'CMS_COST_DTRANSIT_AGEN', 'COST_DTRANSIT_NO = COST_MTRANSIT_DTRANSIT_NO', 'Master transit agent cost', 3),
    
    -- Reference/Lookup tables
    ('CMS_DROURATE', 'CMS_CNOTE', 'CNOTE_ORIGIN + CNOTE_DESTINATION = DROURATE route lookup', 'Route rate reference', 2),
    ('ORA_ZONE', 'CMS_CNOTE', 'CNOTE_ORIGIN/DESTINATION zone lookup', 'Zone reference data', 2),
    ('ORA_USER', 'CMS_CNOTE', 'CNOTE_USER = USER_ID', 'User reference data', 2),
    ('T_MDT_CITY_ORIGIN', 'CMS_CNOTE', 'CNOTE_ORIGIN city lookup', 'City origin reference', 2),
    ('T_GOTO', 'CMS_CNOTE', 'CNOTE route optimization lookup', 'Route optimization data', 2),
    ('T_CROSSDOCK_AWB', 'CMS_CNOTE', 'CNOTE_NO = AWB crossdock lookup', 'Crossdock AWB reference', 2),
    ('LASTMILE_COURIER', 'CMS_MRSHEET', 'MRSHEET_COURIER_ID = COURIER_ID', 'Last mile courier details', 4),
    ('CMS_APICUST', 'CMS_CNOTE', 'CNOTE customer API lookup', 'API customer reference', 2)
    
) AS t(source_table, joins_to, join_condition, description, join_level)
ORDER BY join_level, source_table;

-- ============================================================
-- PART 10: SOURCE TABLE STATISTICS
-- ============================================================
-- Shows record counts and data presence for each source table contribution

DROP VIEW IF EXISTS audit.v_source_table_stats CASCADE;

CREATE OR REPLACE VIEW audit.v_source_table_stats AS
SELECT 
    'CMS_CNOTE' AS source_table,
    'BASE TABLE' AS joins_to,
    'CNOTE_NO' AS join_column,
    COUNT(*) AS total_records,
    COUNT(cnote_no) AS records_with_data,
    100.00 AS data_coverage_pct
FROM staging.unified_shipments

UNION ALL

SELECT 
    'CMS_CNOTE_POD' AS source_table,
    'CMS_CNOTE' AS joins_to,
    'CNOTE_NO' AS join_column,
    COUNT(*) AS total_records,
    COUNT(cnote_pod_creation_date) AS records_with_data,
    ROUND(100.0 * COUNT(cnote_pod_creation_date) / NULLIF(COUNT(*), 0), 2) AS data_coverage_pct
FROM staging.unified_shipments

UNION ALL

SELECT 
    'CMS_MFCNOTE' AS source_table,
    'CMS_CNOTE' AS joins_to,
    'CNOTE_NO = MFCNOTE_CNOTE_NO' AS join_column,
    COUNT(*) AS total_records,
    COUNT(mfcnote_man_no) AS records_with_data,
    ROUND(100.0 * COUNT(mfcnote_man_no) / NULLIF(COUNT(*), 0), 2) AS data_coverage_pct
FROM staging.unified_shipments

UNION ALL

SELECT 
    'CMS_MANIFEST' AS source_table,
    'CMS_MFCNOTE' AS joins_to,
    'MFCNOTE_MAN_NO = MANIFEST_NO' AS join_column,
    COUNT(*) AS total_records,
    COUNT(manifest_origin) AS records_with_data,
    ROUND(100.0 * COUNT(manifest_origin) / NULLIF(COUNT(*), 0), 2) AS data_coverage_pct
FROM staging.unified_shipments

UNION ALL

SELECT 
    'CMS_DHICNOTE' AS source_table,
    'CMS_CNOTE' AS joins_to,
    'CNOTE_NO = DHICNOTE_CNOTE_NO' AS join_column,
    COUNT(*) AS total_records,
    COUNT(dhicnote_no) AS records_with_data,
    ROUND(100.0 * COUNT(dhicnote_no) / NULLIF(COUNT(*), 0), 2) AS data_coverage_pct
FROM staging.unified_shipments

UNION ALL

SELECT 
    'CMS_MHICNOTE' AS source_table,
    'CMS_DHICNOTE' AS joins_to,
    'DHICNOTE_NO = MHICNOTE_DHICNOTE_NO' AS join_column,
    COUNT(*) AS total_records,
    COUNT(mhicnote_zone) AS records_with_data,
    ROUND(100.0 * COUNT(mhicnote_zone) / NULLIF(COUNT(*), 0), 2) AS data_coverage_pct
FROM staging.unified_shipments

UNION ALL

SELECT 
    'CMS_DHOCNOTE' AS source_table,
    'CMS_CNOTE' AS joins_to,
    'CNOTE_NO = DHOCNOTE_CNOTE_NO' AS join_column,
    COUNT(*) AS total_records,
    COUNT(dhocnote_no) AS records_with_data,
    ROUND(100.0 * COUNT(dhocnote_no) / NULLIF(COUNT(*), 0), 2) AS data_coverage_pct
FROM staging.unified_shipments

UNION ALL

SELECT 
    'CMS_MHOCNOTE' AS source_table,
    'CMS_DHOCNOTE' AS joins_to,
    'DHOCNOTE_NO = MHOCNOTE_DHOCNOTE_NO' AS join_column,
    COUNT(*) AS total_records,
    COUNT(mhocnote_zone_dest) AS records_with_data,
    ROUND(100.0 * COUNT(mhocnote_zone_dest) / NULLIF(COUNT(*), 0), 2) AS data_coverage_pct
FROM staging.unified_shipments

UNION ALL

SELECT 
    'CMS_DRSHEET' AS source_table,
    'CMS_CNOTE' AS joins_to,
    'CNOTE_NO = DRSHEET_CNOTE_NO' AS join_column,
    COUNT(*) AS total_records,
    COUNT(drsheet_no) AS records_with_data,
    ROUND(100.0 * COUNT(drsheet_no) / NULLIF(COUNT(*), 0), 2) AS data_coverage_pct
FROM staging.unified_shipments

UNION ALL

SELECT 
    'CMS_MRSHEET' AS source_table,
    'CMS_DRSHEET' AS joins_to,
    'DRSHEET_NO = MRSHEET_DRSHEET_NO' AS join_column,
    COUNT(*) AS total_records,
    COUNT(mrsheet_courier_id) AS records_with_data,
    ROUND(100.0 * COUNT(mrsheet_courier_id) / NULLIF(COUNT(*), 0), 2) AS data_coverage_pct
FROM staging.unified_shipments

UNION ALL

SELECT 
    'CMS_DHOUNDEL_POD' AS source_table,
    'CMS_CNOTE' AS joins_to,
    'CNOTE_NO = DHOUNDEL_CNOTE_NO' AS join_column,
    COUNT(*) AS total_records,
    COUNT(dhoundel_no) AS records_with_data,
    ROUND(100.0 * COUNT(dhoundel_no) / NULLIF(COUNT(*), 0), 2) AS data_coverage_pct
FROM staging.unified_shipments

UNION ALL

SELECT 
    'ORA_USER' AS source_table,
    'CMS_CNOTE' AS joins_to,
    'CNOTE_USER = USER_ID' AS join_column,
    COUNT(*) AS total_records,
    COUNT(ora_user_name) AS records_with_data,
    ROUND(100.0 * COUNT(ora_user_name) / NULLIF(COUNT(*), 0), 2) AS data_coverage_pct
FROM staging.unified_shipments

ORDER BY data_coverage_pct DESC;

-- ============================================================
-- VERIFICATION QUERIES
-- ============================================================

-- Check stage distribution (should now show DELIVERED instead of OUT_FOR_DELIVERY)
SELECT * FROM audit.v_stage_distribution;

-- Check delivery category distribution
SELECT * FROM audit.v_delivery_category_distribution;

-- View source table join relationships (lineage)
SELECT * FROM audit.v_source_table_lineage;

-- View source table statistics (data coverage)
SELECT * FROM audit.v_source_table_stats;

-- Sample delivery stage monitor with category
SELECT 
    cnote_no,
    cnote_origin,
    cnote_destination,
    delivery_category,
    stage2_manifest_status,
    stage2_manifest_date,
    stage2_manifest_no,
    stage7_delivery_status, 
    stage7_pod_date,
    current_stage,
    stages_completed
FROM audit.v_delivery_stage_monitor
LIMIT 10;

-- Sample package journey with category
SELECT 
    cnote_no,
    cnote_origin,
    cnote_destination,
    delivery_category,
    order_date,
    manifest_created_date,
    manifest_no,
    delivered_date,
    current_status
FROM audit.v_package_journey
LIMIT 10;

-- Check data quality
SELECT * FROM audit.v_data_quality_scorecard;
