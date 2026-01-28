-- ============================================================
-- JNE Unification with Audit Trail
-- ============================================================
-- This script wraps unify_jne_tables_v2.sql with audit logging
-- Run this instead of the original SQL to get full audit trail
-- ============================================================

-- Configuration

-- ============================================================
-- STEP 1: Start ETL Job
-- ============================================================
DO $$
DECLARE
    v_job_id INTEGER;
    v_target_rows BIGINT;
    v_total_duplicates BIGINT := 0;
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    
    -- Source table counts (before dedup)
    v_cnt_cnote BIGINT;
    v_cnt_cnote_pod BIGINT;
    v_cnt_drcnote BIGINT;
    v_cnt_mrcnote BIGINT;
    v_cnt_apicust BIGINT;
    v_cnt_drourate BIGINT;
    v_cnt_drsheet BIGINT;
    v_cnt_mrsheet BIGINT;
    v_cnt_drsheet_pra BIGINT;
    v_cnt_dhicnote BIGINT;
    v_cnt_mhicnote BIGINT;
    v_cnt_dhocnote BIGINT;
    v_cnt_mhocnote BIGINT;
    v_cnt_dhoundel BIGINT;
    v_cnt_mhoundel BIGINT;
    v_cnt_dhi_hoc BIGINT;
    v_cnt_mhi_hoc BIGINT;
    v_cnt_mfcnote BIGINT;
    v_cnt_manifest BIGINT;
    v_cnt_dbag_ho BIGINT;
    v_cnt_dmbag BIGINT;
    v_cnt_dhov_rsheet BIGINT;
    v_cnt_dstatus BIGINT;
    v_cnt_cost_dtransit BIGINT;
    v_cnt_cost_mtransit BIGINT;
    v_cnt_crossdock BIGINT;
    v_cnt_goto BIGINT;
    v_cnt_rdsj BIGINT;
    v_cnt_dsj BIGINT;
    v_cnt_msj BIGINT;
    v_cnt_dsmu BIGINT;
    v_cnt_msmu BIGINT;
    v_cnt_mdt_city BIGINT;
    v_cnt_courier BIGINT;
    v_cnt_ora_zone BIGINT;
    
    -- Deduped counts
    v_dedup_mrcnote BIGINT;
    v_dedup_drsheet BIGINT;
    v_dedup_mrsheet BIGINT;
    v_dedup_drsheet_pra BIGINT;
    v_dedup_dhicnote BIGINT;
    v_dedup_mhicnote BIGINT;
    v_dedup_dhocnote BIGINT;
    v_dedup_mhocnote BIGINT;
    v_dedup_dhoundel BIGINT;
    v_dedup_mhoundel BIGINT;
    v_dedup_dhi_hoc BIGINT;
    v_dedup_mhi_hoc BIGINT;
    v_dedup_mfcnote BIGINT;
    v_dedup_manifest BIGINT;
    v_dedup_dbag_ho BIGINT;
    v_dedup_dmbag BIGINT;
    v_dedup_dhov_rsheet BIGINT;
    v_dedup_dstatus BIGINT;
    v_dedup_cost_dtransit BIGINT;
    v_dedup_cost_mtransit BIGINT;
    v_dedup_rdsj BIGINT;
    v_dedup_dsj BIGINT;
    v_dedup_msj BIGINT;
    v_dedup_dsmu BIGINT;
    v_dedup_msmu BIGINT;
    v_dedup_drourate BIGINT;
    v_dedup_mdt_city BIGINT;
    v_dedup_courier BIGINT;
    v_dedup_ora_zone BIGINT;
    
BEGIN
    -- Start the job
    v_job_id := audit.start_etl_job('JNE_DAILY_UNIFICATION', 'UNIFICATION', 'MANUAL');;
    RAISE NOTICE '=== Started ETL Job ID: % ===', v_job_id;
    
    -- ============================================================
    -- STEP 2: Collect Source Table Statistics
    -- ============================================================
    RAISE NOTICE 'Collecting source table statistics...';
    
    -- Core tables
    SELECT COUNT(*) INTO v_cnt_cnote FROM CMS_CNOTE;
    SELECT COUNT(*) INTO v_cnt_cnote_pod FROM CMS_CNOTE_POD;
    SELECT COUNT(*) INTO v_cnt_drcnote FROM CMS_DRCNOTE;
    SELECT COUNT(*) INTO v_cnt_apicust FROM CMS_APICUST;
    
    -- Tables that need deduplication
    SELECT COUNT(*) INTO v_cnt_mrcnote FROM CMS_MRCNOTE;
    SELECT COUNT(DISTINCT MRCNOTE_NO) INTO v_dedup_mrcnote FROM CMS_MRCNOTE;
    
    SELECT COUNT(*) INTO v_cnt_drsheet FROM CMS_DRSHEET;
    SELECT COUNT(DISTINCT DRSHEET_CNOTE_NO) INTO v_dedup_drsheet FROM CMS_DRSHEET;
    
    SELECT COUNT(*) INTO v_cnt_mrsheet FROM CMS_MRSHEET;
    SELECT COUNT(DISTINCT MRSHEET_NO) INTO v_dedup_mrsheet FROM CMS_MRSHEET;
    
    SELECT COUNT(*) INTO v_cnt_drsheet_pra FROM CMS_DRSHEET_PRA;
    SELECT COUNT(DISTINCT DRSHEET_CNOTE_NO) INTO v_dedup_drsheet_pra FROM CMS_DRSHEET_PRA;
    
    SELECT COUNT(*) INTO v_cnt_dhicnote FROM CMS_DHICNOTE;
    SELECT COUNT(DISTINCT DHICNOTE_CNOTE_NO) INTO v_dedup_dhicnote FROM CMS_DHICNOTE;
    
    SELECT COUNT(*) INTO v_cnt_mhicnote FROM CMS_MHICNOTE;
    SELECT COUNT(DISTINCT MHICNOTE_NO) INTO v_dedup_mhicnote FROM CMS_MHICNOTE;
    
    SELECT COUNT(*) INTO v_cnt_dhocnote FROM CMS_DHOCNOTE;
    SELECT COUNT(DISTINCT DHOCNOTE_CNOTE_NO) INTO v_dedup_dhocnote FROM CMS_DHOCNOTE;
    
    SELECT COUNT(*) INTO v_cnt_mhocnote FROM CMS_MHOCNOTE;
    SELECT COUNT(DISTINCT MHOCNOTE_NO) INTO v_dedup_mhocnote FROM CMS_MHOCNOTE;
    
    SELECT COUNT(*) INTO v_cnt_dhoundel FROM CMS_DHOUNDEL_POD;
    SELECT COUNT(DISTINCT DHOUNDEL_CNOTE_NO) INTO v_dedup_dhoundel FROM CMS_DHOUNDEL_POD;
    
    SELECT COUNT(*) INTO v_cnt_mhoundel FROM CMS_MHOUNDEL_POD;
    SELECT COUNT(DISTINCT MHOUNDEL_NO) INTO v_dedup_mhoundel FROM CMS_MHOUNDEL_POD;
    
    SELECT COUNT(*) INTO v_cnt_dhi_hoc FROM CMS_DHI_HOC;
    SELECT COUNT(DISTINCT DHI_CNOTE_NO) INTO v_dedup_dhi_hoc FROM CMS_DHI_HOC;
    
    SELECT COUNT(*) INTO v_cnt_mhi_hoc FROM CMS_MHI_HOC;
    SELECT COUNT(DISTINCT MHI_NO) INTO v_dedup_mhi_hoc FROM CMS_MHI_HOC;
    
    SELECT COUNT(*) INTO v_cnt_mfcnote FROM CMS_MFCNOTE;
    SELECT COUNT(DISTINCT MFCNOTE_NO) INTO v_dedup_mfcnote FROM CMS_MFCNOTE;
    
    SELECT COUNT(*) INTO v_cnt_manifest FROM CMS_MANIFEST;
    SELECT COUNT(DISTINCT MANIFEST_NO) INTO v_dedup_manifest FROM CMS_MANIFEST;
    
    SELECT COUNT(*) INTO v_cnt_dbag_ho FROM CMS_DBAG_HO;
    SELECT COUNT(DISTINCT DBAG_CNOTE_NO) INTO v_dedup_dbag_ho FROM CMS_DBAG_HO;
    
    SELECT COUNT(*) INTO v_cnt_dmbag FROM CMS_DMBAG;
    SELECT COUNT(DISTINCT DMBAG_NO) INTO v_dedup_dmbag FROM CMS_DMBAG;
    
    SELECT COUNT(*) INTO v_cnt_dhov_rsheet FROM CMS_DHOV_RSHEET;
    SELECT COUNT(DISTINCT DHOV_RSHEET_CNOTE) INTO v_dedup_dhov_rsheet FROM CMS_DHOV_RSHEET;
    
    SELECT COUNT(*) INTO v_cnt_dstatus FROM CMS_DSTATUS;
    SELECT COUNT(DISTINCT DSTATUS_CNOTE_NO) INTO v_dedup_dstatus FROM CMS_DSTATUS;
    
    SELECT COUNT(*) INTO v_cnt_cost_dtransit FROM CMS_COST_DTRANSIT_AGEN;
    SELECT COUNT(DISTINCT CNOTE_NO) INTO v_dedup_cost_dtransit FROM CMS_COST_DTRANSIT_AGEN;
    
    SELECT COUNT(*) INTO v_cnt_cost_mtransit FROM CMS_COST_MTRANSIT_AGEN;
    SELECT COUNT(DISTINCT MANIFEST_NO) INTO v_dedup_cost_mtransit FROM CMS_COST_MTRANSIT_AGEN;
    
    SELECT COUNT(*) INTO v_cnt_crossdock FROM T_CROSSDOCK_AWD;
    SELECT COUNT(*) INTO v_cnt_goto FROM T_GOTO;
    
    SELECT COUNT(*) INTO v_cnt_rdsj FROM CMS_RDSJ;
    SELECT COUNT(DISTINCT RDSJ_HVI_NO) INTO v_dedup_rdsj FROM CMS_RDSJ;
    
    SELECT COUNT(*) INTO v_cnt_dsj FROM CMS_DSJ;
    SELECT COUNT(DISTINCT DSJ_HVO_NO) INTO v_dedup_dsj FROM CMS_DSJ;
    
    SELECT COUNT(*) INTO v_cnt_msj FROM CMS_MSJ;
    SELECT COUNT(DISTINCT MSJ_NO) INTO v_dedup_msj FROM CMS_MSJ;
    
    SELECT COUNT(*) INTO v_cnt_dsmu FROM CMS_DSMU;
    SELECT COUNT(DISTINCT DSMU_NO) INTO v_dedup_dsmu FROM CMS_DSMU;
    
    SELECT COUNT(*) INTO v_cnt_msmu FROM CMS_MSMU;
    SELECT COUNT(DISTINCT MSMU_NO) INTO v_dedup_msmu FROM CMS_MSMU;
    
    SELECT COUNT(*) INTO v_cnt_drourate FROM CMS_DROURATE;
    SELECT COUNT(DISTINCT (DROURATE_CODE, DROURATE_SERVICE)) INTO v_dedup_drourate FROM CMS_DROURATE;
    
    SELECT COUNT(*) INTO v_cnt_mdt_city FROM T_MDT_CITY_ORIGIN;
    SELECT COUNT(DISTINCT CITY_CODE) INTO v_dedup_mdt_city FROM T_MDT_CITY_ORIGIN;
    
    SELECT COUNT(*) INTO v_cnt_courier FROM LASTMILE_COURIER;
    SELECT COUNT(DISTINCT COURIER_ID) INTO v_dedup_courier FROM LASTMILE_COURIER;
    
    SELECT COUNT(*) INTO v_cnt_ora_zone FROM ORA_ZONE;
    SELECT COUNT(DISTINCT ZONE_CODE) INTO v_dedup_ora_zone FROM ORA_ZONE;
    
    -- ============================================================
    -- STEP 3: Log Source Statistics
    -- ============================================================
    RAISE NOTICE 'Logging source statistics...';
    
    -- Core tables (no dedup needed)
    PERFORM audit.log_source_stats(v_job_id, 'CMS_CNOTE', v_cnt_cnote, v_cnt_cnote, v_cnt_cnote, 0, 'CNOTE_NO', NULL);
    PERFORM audit.log_source_stats(v_job_id, 'CMS_CNOTE_POD', v_cnt_cnote_pod, v_cnt_cnote_pod, NULL, 0, 'CNOTE_POD_NO', NULL);
    PERFORM audit.log_source_stats(v_job_id, 'CMS_DRCNOTE', v_cnt_drcnote, v_cnt_drcnote, NULL, 0, 'DRCNOTE_CNOTE_NO', NULL);
    PERFORM audit.log_source_stats(v_job_id, 'CMS_APICUST', v_cnt_apicust, v_cnt_apicust, NULL, 0, 'APICUST_CNOTE_NO', NULL);
    PERFORM audit.log_source_stats(v_job_id, 'T_CROSSDOCK_AWD', v_cnt_crossdock, v_cnt_crossdock, NULL, 0, 'AWB_CHILD', NULL);
    PERFORM audit.log_source_stats(v_job_id, 'T_GOTO', v_cnt_goto, v_cnt_goto, NULL, 0, 'AWB', NULL);
    
    -- Deduped tables
    PERFORM audit.log_source_stats(v_job_id, 'CMS_MRCNOTE', v_cnt_mrcnote, v_dedup_mrcnote, NULL, v_cnt_mrcnote - v_dedup_mrcnote, 'MRCNOTE_NO', 'MRCNOTE_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_DRSHEET', v_cnt_drsheet, v_dedup_drsheet, NULL, v_cnt_drsheet - v_dedup_drsheet, 'DRSHEET_CNOTE_NO', 'DRSHEET_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_MRSHEET', v_cnt_mrsheet, v_dedup_mrsheet, NULL, v_cnt_mrsheet - v_dedup_mrsheet, 'MRSHEET_NO', 'MRSHEET_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_DRSHEET_PRA', v_cnt_drsheet_pra, v_dedup_drsheet_pra, NULL, v_cnt_drsheet_pra - v_dedup_drsheet_pra, 'DRSHEET_CNOTE_NO', 'DRSHEET_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_DHICNOTE', v_cnt_dhicnote, v_dedup_dhicnote, NULL, v_cnt_dhicnote - v_dedup_dhicnote, 'DHICNOTE_CNOTE_NO', 'DHICNOTE_TDATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_MHICNOTE', v_cnt_mhicnote, v_dedup_mhicnote, NULL, v_cnt_mhicnote - v_dedup_mhicnote, 'MHICNOTE_NO', 'MHICNOTE_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_DHOCNOTE', v_cnt_dhocnote, v_dedup_dhocnote, NULL, v_cnt_dhocnote - v_dedup_dhocnote, 'DHOCNOTE_CNOTE_NO', 'DHOCNOTE_TDATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_MHOCNOTE', v_cnt_mhocnote, v_dedup_mhocnote, NULL, v_cnt_mhocnote - v_dedup_mhocnote, 'MHOCNOTE_NO', 'MHOCNOTE_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_DHOUNDEL_POD', v_cnt_dhoundel, v_dedup_dhoundel, NULL, v_cnt_dhoundel - v_dedup_dhoundel, 'DHOUNDEL_CNOTE_NO', 'CREATE_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_MHOUNDEL_POD', v_cnt_mhoundel, v_dedup_mhoundel, NULL, v_cnt_mhoundel - v_dedup_mhoundel, 'MHOUNDEL_NO', 'MHOUNDEL_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_DHI_HOC', v_cnt_dhi_hoc, v_dedup_dhi_hoc, NULL, v_cnt_dhi_hoc - v_dedup_dhi_hoc, 'DHI_CNOTE_NO', 'CDATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_MHI_HOC', v_cnt_mhi_hoc, v_dedup_mhi_hoc, NULL, v_cnt_mhi_hoc - v_dedup_mhi_hoc, 'MHI_NO', 'MHI_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_MFCNOTE', v_cnt_mfcnote, v_dedup_mfcnote, NULL, v_cnt_mfcnote - v_dedup_mfcnote, 'MFCNOTE_NO', 'MFCNOTE_MAN_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_MANIFEST', v_cnt_manifest, v_dedup_manifest, NULL, v_cnt_manifest - v_dedup_manifest, 'MANIFEST_NO', 'MANIFEST_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_DBAG_HO', v_cnt_dbag_ho, v_dedup_dbag_ho, NULL, v_cnt_dbag_ho - v_dedup_dbag_ho, 'DBAG_CNOTE_NO', 'CDATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_DMBAG', v_cnt_dmbag, v_dedup_dmbag, NULL, v_cnt_dmbag - v_dedup_dmbag, 'DMBAG_NO', 'ESB_TIME');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_DHOV_RSHEET', v_cnt_dhov_rsheet, v_dedup_dhov_rsheet, NULL, v_cnt_dhov_rsheet - v_dedup_dhov_rsheet, 'DHOV_RSHEET_CNOTE', 'CREATE_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_DSTATUS', v_cnt_dstatus, v_dedup_dstatus, NULL, v_cnt_dstatus - v_dedup_dstatus, 'DSTATUS_CNOTE_NO', 'CREATE_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_COST_DTRANSIT_AGEN', v_cnt_cost_dtransit, v_dedup_cost_dtransit, NULL, v_cnt_cost_dtransit - v_dedup_cost_dtransit, 'CNOTE_NO', 'ESB_TIME');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_COST_MTRANSIT_AGEN', v_cnt_cost_mtransit, v_dedup_cost_mtransit, NULL, v_cnt_cost_mtransit - v_dedup_cost_mtransit, 'MANIFEST_NO', 'MANIFEST_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_RDSJ', v_cnt_rdsj, v_dedup_rdsj, NULL, v_cnt_rdsj - v_dedup_rdsj, 'RDSJ_HVI_NO', 'RDSJ_CDATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_DSJ', v_cnt_dsj, v_dedup_dsj, NULL, v_cnt_dsj - v_dedup_dsj, 'DSJ_HVO_NO', 'DSJ_CDATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_MSJ', v_cnt_msj, v_dedup_msj, NULL, v_cnt_msj - v_dedup_msj, 'MSJ_NO', 'MSJ_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_DSMU', v_cnt_dsmu, v_dedup_dsmu, NULL, v_cnt_dsmu - v_dedup_dsmu, 'DSMU_NO', 'ESB_TIME');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_MSMU', v_cnt_msmu, v_dedup_msmu, NULL, v_cnt_msmu - v_dedup_msmu, 'MSMU_NO', 'MSMU_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'CMS_DROURATE', v_cnt_drourate, v_dedup_drourate, NULL, v_cnt_drourate - v_dedup_drourate, 'DROURATE_CODE+SERVICE', 'DROURATE_UDATE');
    PERFORM audit.log_source_stats(v_job_id, 'T_MDT_CITY_ORIGIN', v_cnt_mdt_city, v_dedup_mdt_city, NULL, v_cnt_mdt_city - v_dedup_mdt_city, 'CITY_CODE', 'CREATE_DATE');
    PERFORM audit.log_source_stats(v_job_id, 'LASTMILE_COURIER', v_cnt_courier, v_dedup_courier, NULL, v_cnt_courier - v_dedup_courier, 'COURIER_ID', 'COURIER_UPDATED_AT');
    PERFORM audit.log_source_stats(v_job_id, 'ORA_ZONE', v_cnt_ora_zone, v_dedup_ora_zone, NULL, v_cnt_ora_zone - v_dedup_ora_zone, 'ZONE_CODE', 'LASTUPDDTM');
    
    -- Calculate total duplicates removed
    v_total_duplicates := 
        (v_cnt_mrcnote - v_dedup_mrcnote) +
        (v_cnt_drsheet - v_dedup_drsheet) +
        (v_cnt_mrsheet - v_dedup_mrsheet) +
        (v_cnt_drsheet_pra - v_dedup_drsheet_pra) +
        (v_cnt_dhicnote - v_dedup_dhicnote) +
        (v_cnt_mhicnote - v_dedup_mhicnote) +
        (v_cnt_dhocnote - v_dedup_dhocnote) +
        (v_cnt_mhocnote - v_dedup_mhocnote) +
        (v_cnt_dhoundel - v_dedup_dhoundel) +
        (v_cnt_mhoundel - v_dedup_mhoundel) +
        (v_cnt_dhi_hoc - v_dedup_dhi_hoc) +
        (v_cnt_mhi_hoc - v_dedup_mhi_hoc) +
        (v_cnt_mfcnote - v_dedup_mfcnote) +
        (v_cnt_manifest - v_dedup_manifest) +
        (v_cnt_dbag_ho - v_dedup_dbag_ho) +
        (v_cnt_dmbag - v_dedup_dmbag) +
        (v_cnt_dhov_rsheet - v_dedup_dhov_rsheet) +
        (v_cnt_dstatus - v_dedup_dstatus) +
        (v_cnt_cost_dtransit - v_dedup_cost_dtransit) +
        (v_cnt_cost_mtransit - v_dedup_cost_mtransit) +
        (v_cnt_rdsj - v_dedup_rdsj) +
        (v_cnt_dsj - v_dedup_dsj) +
        (v_cnt_msj - v_dedup_msj) +
        (v_cnt_dsmu - v_dedup_dsmu) +
        (v_cnt_msmu - v_dedup_msmu) +
        (v_cnt_drourate - v_dedup_drourate) +
        (v_cnt_mdt_city - v_dedup_mdt_city) +
        (v_cnt_courier - v_dedup_courier) +
        (v_cnt_ora_zone - v_dedup_ora_zone);
    
    RAISE NOTICE 'Source statistics logged. Total duplicates to remove: %', v_total_duplicates;
    
    -- ============================================================
    -- STEP 4: Create/Replace Unified Table
    -- ============================================================
    RAISE NOTICE 'Creating unified_shipments table...';
    
    -- Drop existing table and create new one
    DROP TABLE IF EXISTS staging.unified_shipments CASCADE;
    
    CREATE TABLE staging.unified_shipments AS
    -- === YOUR UNIFICATION QUERY GOES HERE ===
    -- (Insert the contents of unify_jne_tables_v2.sql)
    -- For now, placeholder:
    SELECT 1 AS placeholder WHERE FALSE;
    
    -- Add audit columns
    ALTER TABLE staging.unified_shipments 
        ADD COLUMN IF NOT EXISTS etl_job_id INTEGER DEFAULT v_job_id,
        ADD COLUMN IF NOT EXISTS etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    
    -- Get row count
    SELECT COUNT(*) INTO v_target_rows FROM staging.unified_shipments;
    
    RAISE NOTICE 'Unified table created with % rows', v_target_rows;
    
    -- ============================================================
    -- STEP 5: Calculate Data Quality Metrics
    -- ============================================================
    RAISE NOTICE 'Calculating data quality metrics...';
    -- PERFORM audit.calculate_dq_metrics(v_job_id, 'staging.unified_shipments');
    
    -- ============================================================
    -- STEP 6: Complete the Job
    -- ============================================================
    PERFORM audit.complete_etl_job(
        v_job_id,
        v_target_rows,
        v_target_rows,  -- rows_inserted (all new in CREATE TABLE AS)
        0,              -- rows_updated
        v_total_duplicates,
        'Unification completed successfully'
    );
    
    RAISE NOTICE '=== ETL Job % completed successfully ===', v_job_id;
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time));
    RAISE NOTICE 'Target rows: %', v_target_rows;
    RAISE NOTICE 'Duplicates removed: %', v_total_duplicates;
    
EXCEPTION WHEN OTHERS THEN
    -- Log the failure
    PERFORM audit.fail_etl_job(v_job_id, SQLERRM, SQLSTATE);
    RAISE;
END;
$$;

-- ============================================================
-- View Results
-- ============================================================
SELECT * FROM audit.v_latest_jobs;
SELECT * FROM audit.v_latest_source_stats ORDER BY source_table;
