-- ============================================================
-- JNE Tables Unification SQL Script (v3 - All 37 Tables)
-- ============================================================
-- Main table: CMS_CNOTE (maintains 1 row = 1 shipment)
-- Uses CTEs with ROW_NUMBER() to dedupe and prevent row explosion
--
-- ALL 37 TABLES INCLUDED:
-- --------------------------------------------------------
-- Core (5): CMS_CNOTE, CMS_CNOTE_POD, CMS_CNOTE_AMO, CMS_APICUST, CMS_DROURATE
-- Runsheet (6): CMS_DRCNOTE, CMS_MRCNOTE, CMS_DRSHEET, CMS_MRSHEET, CMS_DRSHEET_PRA, CMS_DHOV_RSHEET
-- Inbound HO (2): CMS_DHICNOTE, CMS_MHICNOTE
-- Outbound HO (2): CMS_DHOCNOTE, CMS_MHOCNOTE
-- Undelivered (2): CMS_DHOUNDEL_POD, CMS_MHOUNDEL_POD
-- Hub Ops (2): CMS_DHI_HOC, CMS_MHI_HOC
-- Manifest (2): CMS_MFCNOTE, CMS_MANIFEST
-- Bags (2): CMS_DBAG_HO, CMS_DMBAG
-- SJ Chain (3): CMS_DSJ, CMS_MSJ, CMS_RDSJ
-- SMU (2): CMS_DSMU, CMS_MSMU
-- Status/Cost (3): CMS_DSTATUS, CMS_COST_DTRANSIT_AGEN, CMS_COST_MTRANSIT_AGEN
-- Reference (5): ORA_ZONE, ORA_USER, T_MDT_CITY_ORIGIN, LASTMILE_COURIER, CMS_DROURATE
-- Crossdock (2): T_CROSSDOCK_AWD, T_GOTO
-- ============================================================

WITH 
-- ============================================================
-- DEDUPLICATION CTEs (28 total)
-- ============================================================

-- CMS_CNOTE_AMO: Dedupe by CNOTE_NO
cnote_amo_deduped AS (
    SELECT * FROM (
        SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a.CNOTE_NO ORDER BY a.CDATE DESC NULLS LAST) AS rn
        FROM CMS_CNOTE_AMO a
    ) sub WHERE rn = 1
),

-- MRCNOTE: Multiple rows per NO, keep latest by date
mrcnote_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MRCNOTE_NO ORDER BY m.MRCNOTE_DATE DESC NULLS LAST) AS rn
        FROM CMS_MRCNOTE m
    ) sub WHERE rn = 1
),

-- DRSHEET: Multiple rows per CNOTE, keep latest by date
drsheet_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DRSHEET_CNOTE_NO ORDER BY d.DRSHEET_DATE DESC NULLS LAST) AS rn
        FROM CMS_DRSHEET d
    ) sub WHERE rn = 1
),

-- MRSHEET: Multiple rows per NO, keep latest by date
mrsheet_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MRSHEET_NO ORDER BY m.MRSHEET_DATE DESC NULLS LAST) AS rn
        FROM CMS_MRSHEET m
    ) sub WHERE rn = 1
),

-- DRSHEET_PRA: Multiple rows per CNOTE, keep latest by date
drsheet_pra_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DRSHEET_CNOTE_NO ORDER BY d.DRSHEET_DATE DESC NULLS LAST) AS rn
        FROM CMS_DRSHEET_PRA d
    ) sub WHERE rn = 1
),

-- DHICNOTE: Multiple rows per CNOTE, keep latest by date
dhicnote_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DHICNOTE_CNOTE_NO ORDER BY d.DHICNOTE_TDATE DESC NULLS LAST) AS rn
        FROM CMS_DHICNOTE d
    ) sub WHERE rn = 1
),

-- MHICNOTE: Multiple rows per NO, keep latest by date
mhicnote_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MHICNOTE_NO ORDER BY m.MHICNOTE_DATE DESC NULLS LAST) AS rn
        FROM CMS_MHICNOTE m
    ) sub WHERE rn = 1
),

-- DHOCNOTE: Multiple rows per CNOTE, keep latest by date
dhocnote_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DHOCNOTE_CNOTE_NO ORDER BY d.DHOCNOTE_TDATE DESC NULLS LAST) AS rn
        FROM CMS_DHOCNOTE d
    ) sub WHERE rn = 1
),

-- MHOCNOTE: Multiple rows per NO, keep latest by date
mhocnote_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MHOCNOTE_NO ORDER BY m.MHOCNOTE_DATE DESC NULLS LAST) AS rn
        FROM CMS_MHOCNOTE m
    ) sub WHERE rn = 1
),

-- DHOUNDEL_POD: Dedupe by CNOTE
dhoundel_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DHOUNDEL_CNOTE_NO ORDER BY d.CREATE_DATE DESC NULLS LAST) AS rn
        FROM CMS_DHOUNDEL_POD d
    ) sub WHERE rn = 1
),

-- MHOUNDEL_POD: Dedupe by NO
mhoundel_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MHOUNDEL_NO ORDER BY m.MHOUNDEL_DATE DESC NULLS LAST) AS rn
        FROM CMS_MHOUNDEL_POD m
    ) sub WHERE rn = 1
),

-- DHI_HOC: Dedupe by CNOTE
dhi_hoc_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DHI_CNOTE_NO ORDER BY d.CDATE DESC NULLS LAST) AS rn
        FROM CMS_DHI_HOC d
    ) sub WHERE rn = 1
),

-- MHI_HOC: Dedupe by NO
mhi_hoc_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MHI_NO ORDER BY m.MHI_DATE DESC NULLS LAST) AS rn
        FROM CMS_MHI_HOC m
    ) sub WHERE rn = 1
),

-- MFCNOTE: Multiple rows per CNOTE, keep latest
mfcnote_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MFCNOTE_NO ORDER BY m.MFCNOTE_MAN_DATE DESC NULLS LAST) AS rn
        FROM CMS_MFCNOTE m
    ) sub WHERE rn = 1
),

-- MANIFEST: Multiple rows per NO, keep latest by date
manifest_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MANIFEST_NO ORDER BY m.MANIFEST_DATE DESC NULLS LAST) AS rn
        FROM CMS_MANIFEST m
    ) sub WHERE rn = 1
),

-- DBAG_HO: Dedupe by CNOTE
dbag_ho_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DBAG_CNOTE_NO ORDER BY d.CDATE DESC NULLS LAST) AS rn
        FROM CMS_DBAG_HO d
    ) sub WHERE rn = 1
),

-- DMBAG: Dedupe by NO
dmbag_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DMBAG_NO ORDER BY d.ESB_TIME DESC NULLS LAST) AS rn
        FROM CMS_DMBAG d
    ) sub WHERE rn = 1
),

-- DHOV_RSHEET: Dedupe by CNOTE
dhov_rsheet_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DHOV_RSHEET_CNOTE ORDER BY d.CREATE_DATE DESC NULLS LAST) AS rn
        FROM CMS_DHOV_RSHEET d
    ) sub WHERE rn = 1
),

-- DSTATUS: Dedupe by CNOTE
dstatus_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DSTATUS_CNOTE_NO ORDER BY d.CREATE_DATE DESC NULLS LAST) AS rn
        FROM CMS_DSTATUS d
    ) sub WHERE rn = 1
),

-- COST_DTRANSIT_AGEN: Dedupe by CNOTE
cost_dtransit_deduped AS (
    SELECT * FROM (
        SELECT c.*, ROW_NUMBER() OVER (PARTITION BY c.CNOTE_NO ORDER BY c.ESB_TIME DESC NULLS LAST) AS rn
        FROM CMS_COST_DTRANSIT_AGEN c
    ) sub WHERE rn = 1
),

-- COST_MTRANSIT_AGEN: Dedupe by MANIFEST_NO
cost_mtransit_deduped AS (
    SELECT * FROM (
        SELECT c.*, ROW_NUMBER() OVER (PARTITION BY c.MANIFEST_NO ORDER BY c.MANIFEST_DATE DESC NULLS LAST) AS rn
        FROM CMS_COST_MTRANSIT_AGEN c
    ) sub WHERE rn = 1
),

-- RDSJ: Dedupe by HVI
rdsj_deduped AS (
    SELECT * FROM (
        SELECT r.*, ROW_NUMBER() OVER (PARTITION BY r.RDSJ_HVI_NO ORDER BY r.RDSJ_CDATE DESC NULLS LAST) AS rn
        FROM CMS_RDSJ r
    ) sub WHERE rn = 1
),

-- DSJ: Dedupe by HVO
dsj_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DSJ_HVO_NO ORDER BY d.DSJ_CDATE DESC NULLS LAST) AS rn
        FROM CMS_DSJ d
    ) sub WHERE rn = 1
),

-- MSJ: Dedupe by NO
msj_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MSJ_NO ORDER BY m.MSJ_DATE DESC NULLS LAST) AS rn
        FROM CMS_MSJ m
    ) sub WHERE rn = 1
),

-- DSMU: Dedupe by NO
dsmu_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DSMU_NO ORDER BY d.ESB_TIME DESC NULLS LAST) AS rn
        FROM CMS_DSMU d
    ) sub WHERE rn = 1
),

-- MSMU: Dedupe by NO
msmu_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MSMU_NO ORDER BY m.MSMU_DATE DESC NULLS LAST) AS rn
        FROM CMS_MSMU m
    ) sub WHERE rn = 1
),

-- DROURATE: Dedupe by CODE + SERVICE
drourate_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DROURATE_CODE, d.DROURATE_SERVICE ORDER BY d.DROURATE_UDATE DESC NULLS LAST) AS rn
        FROM CMS_DROURATE d
    ) sub WHERE rn = 1
),

-- T_MDT_CITY_ORIGIN: Dedupe by CITY_CODE
mdt_city_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.CITY_CODE ORDER BY m.CREATE_DATE DESC NULLS LAST) AS rn
        FROM T_MDT_CITY_ORIGIN m
    ) sub WHERE rn = 1
),

-- LASTMILE_COURIER: Dedupe by COURIER_ID
courier_deduped AS (
    SELECT * FROM (
        SELECT c.*, ROW_NUMBER() OVER (PARTITION BY c.COURIER_ID ORDER BY c.COURIER_UPDATED_AT DESC NULLS LAST) AS rn
        FROM LASTMILE_COURIER c
    ) sub WHERE rn = 1
),

-- ORA_ZONE: Dedupe by ZONE_CODE
ora_zone_deduped AS (
    SELECT * FROM (
        SELECT o.*, ROW_NUMBER() OVER (PARTITION BY o.ZONE_CODE ORDER BY o.LASTUPDDTM DESC NULLS LAST) AS rn
        FROM ORA_ZONE o
    ) sub WHERE rn = 1
),

-- ORA_USER: Dedupe by USER_ID
ora_user_deduped AS (
    SELECT * FROM (
        SELECT u.*, ROW_NUMBER() OVER (PARTITION BY u.USER_ID ORDER BY u.USER_DATE DESC NULLS LAST) AS rn
        FROM ORA_USER u
    ) sub WHERE rn = 1
)

-- ============================================================
-- MAIN SELECT WITH ALL LEFT JOINS (37 TABLES)
-- ============================================================
SELECT 
    -- ========================================
    -- 1. CMS_CNOTE (all 117 columns)
    -- ========================================
    cn.*,
    
    -- ========================================
    -- 2. CMS_CNOTE_POD (4 columns)
    -- ========================================
    pod.CNOTE_POD_STATUS,
    pod.CNOTE_POD_DELIVERED,
    pod.CNOTE_POD_DOC_NO,
    pod.CNOTE_POD_CREATION_DATE,
    
    -- ========================================
    -- 3. CMS_CNOTE_AMO (11 columns) - NEW
    -- ========================================
    amo.CNOTE_BRANCH_ID AS AMO_BRANCH_ID,
    amo.FIELDCHAR1 AS AMO_FIELDCHAR1,
    amo.FIELDCHAR2 AS AMO_FIELDCHAR2,
    amo.FIELDCHAR3 AS AMO_FIELDCHAR3,
    amo.FIELDCHAR4 AS AMO_FIELDCHAR4,
    amo.FIELDCHAR5 AS AMO_FIELDCHAR5,
    amo.FIELDNUM1 AS AMO_FIELDNUM1,
    amo.FIELDNUM2 AS AMO_FIELDNUM2,
    amo.FIELDNUM3 AS AMO_FIELDNUM3,
    amo.FIELDNUM4 AS AMO_FIELDNUM4,
    amo.FIELDNUM5 AS AMO_FIELDNUM5,
    amo.CDATE AS AMO_CDATE,
    
    -- ========================================
    -- 4. CMS_APICUST (15 columns)
    -- ========================================
    api.APICUST_ORDER_ID,
    api.APICUST_BRANCH,
    api.APICUST_CUST_NO,
    api.APICUST_INS_FLAG,
    api.APICUST_COD_FLAG,
    api.APICUST_COD_AMOUNT,
    api.CREATE_DATE AS APICUST_CREATE_DATE,
    api.APICUST_REQID,
    api.APICUST_FLAG,
    api.APICUST_LATITUDE,
    api.APICUST_LONGITUDE,
    api.APICUST_SHIPMENT_TYPE,
    api.APICUST_MERCHAN_ID,
    api.APICUST_NAME,
    api.SHIPPER_PROVIDER,
    
    -- ========================================
    -- 5. CMS_DROURATE (8 columns)
    -- ========================================
    drou.DROURATE_ZONE,
    drou.DROURATE_TRANSIT,
    drou.DROURATE_DELIVERY,
    drou.DROURATE_LINEHAUL,
    drou.DROURATE_ETD_FROM,
    drou.DROURATE_ETD_THRU,
    drou.DROURATE_ACTIVE,
    drou.DROURATE_YES_LATE,
    
    -- ========================================
    -- 6. CMS_DRCNOTE (8 columns)
    -- ========================================
    drc.DRCNOTE_NO,
    drc.DRCNOTE_QTY,
    drc.DRCNOTE_REMARKS,
    drc.DRCNOTE_TDATE,
    drc.DRCNOTE_TUSER,
    drc.DRCNOTE_FLAG,
    drc.DRCNOTE_PAYMENT,
    drc.DRCNOTE_DO,
    
    -- ========================================
    -- 7. CMS_MRCNOTE (11 columns)
    -- ========================================
    mrc.MRCNOTE_DATE,
    mrc.MRCNOTE_BRANCH_ID,
    mrc.MRCNOTE_USER_ID,
    mrc.MRCNOTE_COURIER_ID,
    mrc.MRCNOTE_AE_ID,
    mrc.MRCNOTE_USER1,
    mrc.MRCNOTE_USER2,
    mrc.MRCNOTE_SIGNDATE,
    mrc.MRCNOTE_PAYMENT,
    mrc.MRCNOTE_REFNO,
    mrc.MRCNOTE_TYPE,
    
    -- ========================================
    -- 8. CMS_DRSHEET (8 columns)
    -- ========================================
    drs.DRSHEET_NO,
    drs.DRSHEET_DATE,
    drs.DRSHEET_STATUS,
    drs.DRSHEET_RECEIVER,
    drs.DRSHEET_FLAG,
    drs.DRSHEET_UID,
    drs.DRSHEET_UDATE,
    drs.CREATION_DATE AS DRSHEET_CREATION_DATE,
    
    -- ========================================
    -- 9. CMS_MRSHEET (12 columns)
    -- ========================================
    mrs.MRSHEET_BRANCH,
    mrs.MRSHEET_DATE,
    mrs.MRSHEET_COURIER_ID,
    mrs.MRSHEET_UID,
    mrs.MRSHEET_UDATE,
    mrs.MRSHEET_APPROVED_DR,
    mrs.MRSHEET_UID_DR,
    mrs.MRSHEET_UDATE_DR,
    mrs.MRSHEET_INB_APP_DR,
    mrs.MRSHEET_INB_UID_DR,
    mrs.MRSHEET_INB_UDATE_DR,
    mrs.MRSHEET_ORIGIN,
    
    -- ========================================
    -- 10. CMS_DRSHEET_PRA (9 columns)
    -- ========================================
    pra.DRSHEET_NO AS DRSHEET_PRA_NO,
    pra.DRSHEET_DATE AS DRSHEET_PRA_DATE,
    pra.DRSHEET_STATUS AS DRSHEET_PRA_STATUS,
    pra.DRSHEET_RECEIVER AS DRSHEET_PRA_RECEIVER,
    pra.DRSHEET_FLAG AS DRSHEET_PRA_FLAG,
    pra.DRSHEET_UID AS DRSHEET_PRA_UID,
    pra.DRSHEET_UDATE AS DRSHEET_PRA_UDATE,
    pra.CREATION_DATE AS DRSHEET_PRA_CREATION_DATE,
    pra.DRSHEET_ZONE AS DRSHEET_PRA_ZONE,
    
    -- ========================================
    -- 11. CMS_DHICNOTE (5 columns)
    -- ========================================
    dhi.DHICNOTE_NO,
    dhi.DHICNOTE_SEQ_NO,
    dhi.DHICNOTE_QTY,
    dhi.DHICNOTE_REMARKS AS DHICNOTE_REMARKS,
    dhi.DHICNOTE_TDATE,
    
    -- ========================================
    -- 12. CMS_MHICNOTE (10 columns)
    -- ========================================
    mhi.MHICNOTE_BRANCH_ID,
    mhi.MHICNOTE_ZONE,
    mhi.MHICNOTE_REF_NO,
    mhi.MHICNOTE_DATE,
    mhi.MHICNOTE_ZONE_ORIG,
    mhi.MHICNOTE_USER_ID,
    mhi.MHICNOTE_USER1,
    mhi.MHICNOTE_USER2,
    mhi.MHICNOTE_SIGNDATE,
    mhi.MHICNOTE_APPROVE,
    
    -- ========================================
    -- 13. CMS_DHOCNOTE (11 columns)
    -- ========================================
    dho.DHOCNOTE_NO,
    dho.DHOCNOTE_SEQ_NO,
    dho.DHOCNOTE_QTY,
    dho.DHOCNOTE_REMARKS,
    dho.DHOCNOTE_TDATE,
    dho.DHOCNOTE_INBOUND,
    dho.DHOCNOTE_WEIGHT,
    dho.DHOCNOTE_SERVICE,
    dho.DHOCNOTE_DEST,
    dho.DHOCNOTE_GOODS,
    dho.DHOCNOTE_HANDLING,
    
    -- ========================================
    -- 14. CMS_MHOCNOTE (19 columns)
    -- ========================================
    mho.MHOCNOTE_BRANCH_ID,
    mho.MHOCNOTE_ZONE AS MHOCNOTE_ZONE,
    mho.MHOCNOTE_DATE,
    mho.MHOCNOTE_ZONE_DEST,
    mho.MHOCNOTE_USER_ID,
    mho.MHOCNOTE_USER1,
    mho.MHOCNOTE_USER2,
    mho.MHOCNOTE_SIGNDATE,
    mho.MHOCNOTE_APPROVE,
    mho.MHOCNOTE_REMARKS,
    mho.MHOCNOTE_ORIGIN,
    mho.MHOCNOTE_COURIER_ID,
    mho.MHOCNOTE_SERVICES,
    mho.MHOCNOTE_PRODUCT,
    mho.MHOCNOTE_CUST,
    mho.MHOCNOTE_USER_SCO,
    mho.MHOCNOTE_HVS,
    mho.MHOCNOTE_APP_DATE,
    mho.MHOCNOTE_TYPE,
    
    -- ========================================
    -- 15. CMS_DHOUNDEL_POD (10 columns)
    -- ========================================
    dhund.DHOUNDEL_NO,
    dhund.DHOUNDEL_SEQ_NO,
    dhund.DHOUNDEL_QTY,
    dhund.DHOUNDEL_REMARKS,
    dhund.DHOUNDEL_WEIGHT,
    dhund.DHOUNDEL_SERVICE,
    dhund.DHOUNDEL_DEST,
    dhund.DHOUNDEL_GOODS,
    dhund.DHOUNDEL_HRS,
    dhund.CREATE_DATE AS DHOUNDEL_CREATE_DATE,
    
    -- ========================================
    -- 16. CMS_MHOUNDEL_POD (10 columns)
    -- ========================================
    mhund.MHOUNDEL_BRANCH_ID,
    mhund.MHOUNDEL_REMARKS AS MHOUNDEL_REMARKS,
    mhund.MHOUNDEL_DATE,
    mhund.MHOUNDEL_USER_ID,
    mhund.MHOUNDEL_ZONE,
    mhund.MHOUNDEL_APPROVE,
    mhund.MHOUNDEL_USER1,
    mhund.MHOUNDEL_USER2,
    mhund.MHOUNDEL_SIGNDATE,
    mhund.MHOUNDEL_FLOW,
    
    -- ========================================
    -- 17. CMS_DHI_HOC (8 columns)
    -- ========================================
    dhihoc.DHI_NO,
    dhihoc.DHI_SEQ_NO,
    dhihoc.DHI_ONO,
    dhihoc.DHI_SEQ_ONO,
    dhihoc.DHI_CNOTE_QTY,
    dhihoc.CDATE AS DHI_HOC_CDATE,
    dhihoc.DHI_REMARKS,
    dhihoc.DHI_DO,
    
    -- ========================================
    -- 18. CMS_MHI_HOC (11 columns)
    -- ========================================
    mhihoc.MHI_REF_NO,
    mhihoc.MHI_DATE,
    mhihoc.MHI_UID,
    mhihoc.MHI_APPROVE,
    mhihoc.MHI_BRANCH,
    mhihoc.MHI_APPROVE_DATE,
    mhihoc.MHI_REMARKS,
    mhihoc.MHI_USER1,
    mhihoc.MHI_USER2,
    mhihoc.MHI_ZONE,
    mhihoc.MHI_COURIER,
    
    -- ========================================
    -- 19. CMS_MFCNOTE (8 columns)
    -- ========================================
    mfc.MFCNOTE_MAN_NO,
    mfc.MFCNOTE_BAG_NO,
    mfc.MFCNOTE_COST,
    mfc.MFCNOTE_TRANSIT,
    mfc.MFCNOTE_DELIVERY,
    mfc.MFCNOTE_LINEHAUL,
    mfc.MFCNOTE_ORIGIN,
    mfc.MFCNOTE_MAN_DATE,
    
    -- ========================================
    -- 20. CMS_MANIFEST (7 columns)
    -- ========================================
    man.MANIFEST_DATE,
    man.MANIFEST_ROUTE,
    man.MANIFEST_FROM,
    man.MANIFEST_THRU,
    man.MANIFEST_ORIGIN,
    man.MANIFEST_MODA,
    man.MANIFEST_APPROVED,
    
    -- ========================================
    -- 21. CMS_DBAG_HO (9 columns)
    -- ========================================
    dbag.DBAG_HO_NO,
    dbag.DBAG_NO,
    dbag.DBAG_CNOTE_QTY,
    dbag.DBAG_CNOTE_WEIGHT,
    dbag.DBAG_CNOTE_DESTINATION,
    dbag.CDATE AS DBAG_HO_CDATE,
    dbag.DBAG_CNOTE_SERVICE,
    dbag.DBAG_CNOTE_DATE,
    dbag.DBAG_ZONE_DEST,
    
    -- ========================================
    -- 22. CMS_DMBAG (8 columns)
    -- ========================================
    dmbag.DMBAG_BAG_NO,
    dmbag.DMBAG_ORIGIN,
    dmbag.DMBAG_DESTINATION,
    dmbag.DMBAG_WEIGHT,
    dmbag.DMBAG_SPS,
    dmbag.DMBAG_INBOUND,
    dmbag.DMBAG_TYPE,
    dmbag.DMBAG_STATUS,
    
    -- ========================================
    -- 23. CMS_DHOV_RSHEET (22 columns)
    -- ========================================
    dhov.DHOV_RSHEET_NO,
    dhov.DHOV_RSHEET_DO,
    dhov.DHOV_RSHEET_COD,
    dhov.DHOV_RSHEET_UNDEL,
    dhov.DHOV_RSHEET_REDEL,
    dhov.DHOV_RSHEET_OUTS,
    dhov.DHOV_RSHEET_QTY,
    dhov.CREATE_DATE AS DHOV_CREATE_DATE,
    dhov.DHOV_RSHEET_UZONE,
    dhov.DHOV_RSHEET_CYCLE,
    dhov.DCSUNDEL_RSHEET_RSHEETNO,
    dhov.DHOV_RSHEET_RSHEETNO,
    dhov.DHOV_DRSHEET_EPAY_VEND,
    dhov.DHOV_DRSHEET_EPAY_TRXID,
    dhov.DHOV_DRSHEET_EPAY_AMOUNT,
    dhov.DHOV_DRSHEET_EPAY_DEVICE,
    dhov.DHOV_DRSHEET_DATE,
    dhov.DHOV_DRSHEET_STATUS,
    dhov.DHOV_DRSHEET_RECEIVER,
    dhov.DHOV_DRSHEET_FLAG,
    dhov.DHOV_DRSHEET_UID,
    dhov.DHOV_DRSHEET_UDATE,
    
    -- ========================================
    -- 24. CMS_DSJ (3 columns)
    -- ========================================
    dsj.DSJ_NO,
    dsj.DSJ_BAG_NO,
    dsj.DSJ_CDATE,
    
    -- ========================================
    -- 25. CMS_MSJ (5 columns)
    -- ========================================
    msj.MSJ_DATE,
    msj.MSJ_DEST,
    msj.MSJ_ORIG,
    msj.MSJ_COURIER_ID,
    msj.MSJ_ARMADA,
    
    -- ========================================
    -- 26. CMS_RDSJ (4 columns)
    -- ========================================
    rdsj.RDSJ_NO,
    rdsj.RDSJ_BAG_NO,
    rdsj.RDSJ_HVO_NO,
    rdsj.RDSJ_CDATE,
    
    -- ========================================
    -- 27. CMS_DSMU (6 columns)
    -- ========================================
    dsmu.DSMU_FLIGHT_NO,
    dsmu.DSMU_FLIGHT_DATE,
    dsmu.DSMU_BAG_NO,
    dsmu.DSMU_WEIGHT AS DSMU_WEIGHT,
    dsmu.DSMU_BAG_ORIGIN,
    dsmu.DSMU_BAG_DESTINATION,
    
    -- ========================================
    -- 28. CMS_MSMU (11 columns)
    -- ========================================
    msmu.MSMU_DATE,
    msmu.MSMU_ORIGIN,
    msmu.MSMU_DESTINATION,
    msmu.MSMU_FLIGHT_NO,
    msmu.MSMU_FLIGHT_DATE,
    msmu.MSMU_ETD,
    msmu.MSMU_ETA,
    msmu.MSMU_QTY,
    msmu.MSMU_WEIGHT,
    msmu.MSMU_STATUS,
    msmu.MSMU_MODA,
    
    -- ========================================
    -- 29. CMS_DSTATUS (12 columns)
    -- ========================================
    dst.DSTATUS_NO,
    dst.DSTATUS_STATUS,
    dst.DSTATUS_REMARKS,
    dst.CREATE_DATE AS DSTATUS_CREATE_DATE,
    dst.DSTATUS_STATUS_DATE,
    dst.DSTATUS_MANIFEST_NO_OLD,
    dst.DSTATUS_BAG_NO_OLD,
    dst.DSTATUS_MANIFEST_NO_NEW,
    dst.DSTATUS_MANIFEST_DEST,
    dst.DSTATUS_MANIFEST_THRU,
    dst.DSTATUS_BAG_NO_NEW,
    dst.DSTATUS_ZONE_CODE,
    
    -- ========================================
    -- 30. CMS_COST_DTRANSIT_AGEN (8 columns)
    -- ========================================
    costd.DMANIFEST_NO AS COST_D_MANIFEST_NO,
    costd.CNOTE_ORIGIN AS COST_D_ORIGIN,
    costd.CNOTE_DESTINATION AS COST_D_DESTINATION,
    costd.CNOTE_QTY AS COST_D_QTY,
    costd.CNOTE_WEIGHT AS COST_D_WEIGHT,
    costd.CNOTE_COST AS COST_D_COST,
    costd.CNOTE_SERVICES_CODE AS COST_D_SERVICES_CODE,
    costd.DMANIFEST_DOC_REF AS COST_D_DOC_REF,
    
    -- ========================================
    -- 31. CMS_COST_MTRANSIT_AGEN (11 columns)
    -- ========================================
    costm.MANIFEST_DATE AS COST_M_MANIFEST_DATE,
    costm.BRANCH_ID AS COST_M_BRANCH_ID,
    costm.DESTINATION AS COST_M_DESTINATION,
    costm.CTC_WEIGHT AS COST_M_CTC_WEIGHT,
    costm.ACT_WEIGHT AS COST_M_ACT_WEIGHT,
    costm.MANIFEST_APPROVED AS COST_M_APPROVED,
    costm.REMARK AS COST_M_REMARK,
    costm.MANIFEST_COST AS COST_M_COST,
    costm.REAL_COST AS COST_M_REAL_COST,
    costm.MANIFEST_TYPE AS COST_M_TYPE,
    costm.MANIFEST_DOC_REF AS COST_M_DOC_REF,
    
    -- ========================================
    -- 32. T_CROSSDOCK_AWD (2 columns)
    -- ========================================
    xd.AWB_MASTER,
    xd.CREATE_DATE AS CROSSDOCK_CREATE_DATE,
    
    -- ========================================
    -- 33. T_GOTO (5 columns)
    -- ========================================
    gt.CROSSDOCK_ROLE,
    gt.FIELD_CHAR1,
    gt.FIELD_NUMBER1,
    gt.FIELD_DATE1,
    gt.CREATE_DATE AS GOTO_CREATE_DATE,
    
    -- ========================================
    -- 34. T_MDT_CITY_ORIGIN (4 columns)
    -- ========================================
    mdt.CITY_BRANCH AS MDT_CITY_BRANCH,
    mdt.CITY_ORIGIN AS MDT_CITY_ORIGIN,
    mdt.CITY_MTS AS MDT_CITY_MTS,
    mdt.CITY_ACTIVE AS MDT_CITY_ACTIVE,
    
    -- ========================================
    -- 35. LASTMILE_COURIER (9 columns)
    -- ========================================
    cour.COURIER_NAME,
    cour.COURIER_PHONE,
    cour.COURIER_BRANCH AS COURIER_BRANCH,
    cour.COURIER_ZONE AS COURIER_ZONE,
    cour.COURIER_ORIGIN AS COURIER_ORIGIN,
    cour.COURIER_ACTIVE,
    cour.COURIER_ARMADA,
    cour.COURIER_TYPE,
    cour.COURIER_LEVEL,
    
    -- ========================================
    -- 36. ORA_ZONE (8 columns)
    -- ========================================
    ora.ZONE_BRANCH AS ORA_ZONE_BRANCH,
    ora.ZONE_DESC AS ORA_ZONE_DESC,
    ora.ZONE_ACTIVE AS ORA_ZONE_ACTIVE,
    ora.ZONE_TYPE AS ORA_ZONE_TYPE,
    ora.ZONE_KOTA AS ORA_ZONE_KOTA,
    ora.ZONE_KECAMATAN AS ORA_ZONE_KECAMATAN,
    ora.ZONE_PROVINSI AS ORA_ZONE_PROVINSI,
    ora.ZONE_CATEGORY AS ORA_ZONE_CATEGORY,
    
    -- ========================================
    -- 37. ORA_USER (9 columns) - NEW
    -- ========================================
    usr.USER_NAME AS ORA_USER_NAME,
    usr.USER_CUST_ID AS ORA_USER_CUST_ID,
    usr.USER_CUST_NAME AS ORA_USER_CUST_NAME,
    usr.USER_ZONE_CODE AS ORA_USER_ZONE_CODE,
    usr.USER_ACTIVE AS ORA_USER_ACTIVE,
    usr.USER_DATE AS ORA_USER_DATE,
    usr.USER_ORIGIN AS ORA_USER_ORIGIN,
    usr.USER_NIK AS ORA_USER_NIK,
    usr.USER_VACANT10 AS ORA_USER_VACANT10

FROM CMS_CNOTE cn

-- ============================================================
-- JOIN CHAIN (37 TABLES)
-- ============================================================

-- 2. CMS_CNOTE_POD (1:1)
LEFT JOIN CMS_CNOTE_POD pod 
    ON cn.CNOTE_NO = pod.CNOTE_POD_NO

-- 3. CMS_CNOTE_AMO (1:1) - NEW
LEFT JOIN cnote_amo_deduped amo 
    ON cn.CNOTE_NO = amo.CNOTE_NO

-- 4. CMS_APICUST (1:1)
LEFT JOIN CMS_APICUST api 
    ON cn.CNOTE_NO = api.APICUST_CNOTE_NO

-- 5. CMS_DROURATE (reference)
LEFT JOIN drourate_deduped drou 
    ON cn.CNOTE_ROUTE_CODE = drou.DROURATE_CODE 
    AND cn.CNOTE_SERVICES_CODE = drou.DROURATE_SERVICE

-- 6. CMS_DRCNOTE (1:1)
LEFT JOIN CMS_DRCNOTE drc 
    ON cn.CNOTE_NO = drc.DRCNOTE_CNOTE_NO

-- 7. CMS_MRCNOTE (via DRCNOTE_NO)
LEFT JOIN mrcnote_deduped mrc 
    ON drc.DRCNOTE_NO = mrc.MRCNOTE_NO

-- 8. CMS_DRSHEET
LEFT JOIN drsheet_deduped drs 
    ON cn.CNOTE_NO = drs.DRSHEET_CNOTE_NO

-- 9. CMS_MRSHEET (via DRSHEET_NO)
LEFT JOIN mrsheet_deduped mrs 
    ON drs.DRSHEET_NO = mrs.MRSHEET_NO

-- 10. CMS_DRSHEET_PRA
LEFT JOIN drsheet_pra_deduped pra 
    ON cn.CNOTE_NO = pra.DRSHEET_CNOTE_NO

-- 11. CMS_DHICNOTE
LEFT JOIN dhicnote_deduped dhi 
    ON cn.CNOTE_NO = dhi.DHICNOTE_CNOTE_NO

-- 12. CMS_MHICNOTE (via DHICNOTE_NO)
LEFT JOIN mhicnote_deduped mhi 
    ON dhi.DHICNOTE_NO = mhi.MHICNOTE_NO

-- 13. CMS_DHOCNOTE
LEFT JOIN dhocnote_deduped dho 
    ON cn.CNOTE_NO = dho.DHOCNOTE_CNOTE_NO

-- 14. CMS_MHOCNOTE (via DHOCNOTE_NO)
LEFT JOIN mhocnote_deduped mho 
    ON dho.DHOCNOTE_NO = mho.MHOCNOTE_NO

-- 15. CMS_DHOUNDEL_POD
LEFT JOIN dhoundel_deduped dhund 
    ON cn.CNOTE_NO = dhund.DHOUNDEL_CNOTE_NO

-- 16. CMS_MHOUNDEL_POD (via DHOUNDEL_NO)
LEFT JOIN mhoundel_deduped mhund 
    ON dhund.DHOUNDEL_NO = mhund.MHOUNDEL_NO

-- 17. CMS_DHI_HOC
LEFT JOIN dhi_hoc_deduped dhihoc 
    ON cn.CNOTE_NO = dhihoc.DHI_CNOTE_NO

-- 18. CMS_MHI_HOC (via DHI_NO)
LEFT JOIN mhi_hoc_deduped mhihoc 
    ON dhihoc.DHI_NO = mhihoc.MHI_NO

-- 19. CMS_MFCNOTE
LEFT JOIN mfcnote_deduped mfc 
    ON cn.CNOTE_NO = mfc.MFCNOTE_NO

-- 20. CMS_MANIFEST (via MFCNOTE_MAN_NO)
LEFT JOIN manifest_deduped man 
    ON mfc.MFCNOTE_MAN_NO = man.MANIFEST_NO

-- 21. CMS_DBAG_HO
LEFT JOIN dbag_ho_deduped dbag 
    ON cn.CNOTE_NO = dbag.DBAG_CNOTE_NO

-- 22. CMS_DMBAG (via DBAG_NO)
LEFT JOIN dmbag_deduped dmbag 
    ON dbag.DBAG_NO = dmbag.DMBAG_NO

-- 23. CMS_DHOV_RSHEET
LEFT JOIN dhov_rsheet_deduped dhov 
    ON cn.CNOTE_NO = dhov.DHOV_RSHEET_CNOTE

-- 24. CMS_RDSJ (via DHICNOTE_NO) - Must come before DSJ
LEFT JOIN rdsj_deduped rdsj 
    ON dhi.DHICNOTE_NO = rdsj.RDSJ_HVI_NO

-- 25. CMS_DSJ (via RDSJ_HVO_NO from RDSJ)
LEFT JOIN dsj_deduped dsj 
    ON rdsj.RDSJ_HVO_NO = dsj.DSJ_HVO_NO

-- 26. CMS_MSJ (via DSJ_NO)
LEFT JOIN msj_deduped msj 
    ON dsj.DSJ_NO = msj.MSJ_NO

-- 27. CMS_DSMU (via DSJ_NO)
LEFT JOIN dsmu_deduped dsmu 
    ON dsj.DSJ_NO = dsmu.DSMU_NO

-- 28. CMS_MSMU (via DSJ_NO)
LEFT JOIN msmu_deduped msmu 
    ON dsj.DSJ_NO = msmu.MSMU_NO

-- 29. CMS_DSTATUS
LEFT JOIN dstatus_deduped dst 
    ON cn.CNOTE_NO = CAST(dst.DSTATUS_CNOTE_NO AS VARCHAR(50))

-- 30. CMS_COST_DTRANSIT_AGEN
LEFT JOIN cost_dtransit_deduped costd 
    ON cn.CNOTE_NO = costd.CNOTE_NO

-- 31. CMS_COST_MTRANSIT_AGEN (via MFCNOTE_MAN_NO)
LEFT JOIN cost_mtransit_deduped costm 
    ON mfc.MFCNOTE_MAN_NO = costm.MANIFEST_NO

-- 32. T_CROSSDOCK_AWD (1:1)
LEFT JOIN T_CROSSDOCK_AWD xd 
    ON cn.CNOTE_NO = xd.AWB_CHILD

-- 33. T_GOTO (1:1)
LEFT JOIN T_GOTO gt 
    ON cn.CNOTE_NO = gt.AWB

-- 34. T_MDT_CITY_ORIGIN (via CNOTE_ORIGIN)
LEFT JOIN mdt_city_deduped mdt 
    ON cn.CNOTE_ORIGIN = mdt.CITY_CODE

-- 35. LASTMILE_COURIER (via MRSHEET_COURIER_ID)
LEFT JOIN courier_deduped cour 
    ON mrs.MRSHEET_COURIER_ID = cour.COURIER_ID

-- 36. ORA_ZONE (via DRSHEET_PRA_ZONE)
LEFT JOIN ora_zone_deduped ora 
    ON CAST(pra.DRSHEET_ZONE AS VARCHAR(50)) = ora.ZONE_CODE

-- 37. ORA_USER (via CNOTE_USER) - NEW
LEFT JOIN ora_user_deduped usr 
    ON cn.CNOTE_USER = usr.USER_ID

;
