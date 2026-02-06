-- ============================================================
-- JNE Tables Unification SQL Script (v4 - Metadata-Filtered)
-- ============================================================
-- Source: (JNE) Column Business Metadata.xlsx
-- Date:   2026-02-06
--
-- Changes from v3:
--   • Removed CMS_MHI_HOC  (FULLY EXCLUDED — all columns red)
--   • Removed CMS_DHI_HOC  (FULLY EXCLUDED — parent excluded)
--   • Removed T_CROSSDOCK_AWD (no metadata / no CSV)
--   • CMS_CNOTE: explicit 59 kept columns (was cn.*)
--   • Every other table: only kept columns (red columns dropped)
--   • Column counts validated against metadata spreadsheet
--
-- TABLES IN UNIFICATION (34 of 37):
-- --------------------------------------------------------
-- Core (5):      CMS_CNOTE, CMS_CNOTE_POD, CMS_CNOTE_AMO,
--                CMS_APICUST, CMS_DROURATE
-- Runsheet (6):  CMS_DRCNOTE, CMS_MRCNOTE, CMS_DRSHEET,
--                CMS_MRSHEET, CMS_DRSHEET_PRA, CMS_DHOV_RSHEET
-- Inbound (2):   CMS_DHICNOTE, CMS_MHICNOTE
-- Outbound (2):  CMS_DHOCNOTE, CMS_MHOCNOTE
-- Undelivered (2): CMS_DHOUNDEL_POD, CMS_MHOUNDEL_POD
-- Manifest (2):  CMS_MFCNOTE, CMS_MANIFEST
-- Bags (2):      CMS_DBAG_HO, CMS_DMBAG
-- SJ Chain (3):  CMS_DSJ, CMS_MSJ, CMS_RDSJ
-- SMU (2):       CMS_DSMU, CMS_MSMU
-- Status/Cost (3): CMS_DSTATUS, CMS_COST_DTRANSIT_AGEN,
--                   CMS_COST_MTRANSIT_AGEN
-- Reference (4): ORA_ZONE, ORA_USER, T_MDT_CITY_ORIGIN,
--                LASTMILE_COURIER
-- Crossdock (1): T_GOTO
--
-- EXCLUDED FROM UNIFICATION:
--   CMS_MHI_HOC, CMS_DHI_HOC, T_CROSSDOCK_AWD
-- ============================================================

WITH
-- ============================================================
-- DEDUPLICATION CTEs
-- ============================================================

-- CMS_CNOTE_AMO: Dedupe by CNOTE_NO (keep latest by CDATE)
cnote_amo_deduped AS (
    SELECT * FROM (
        SELECT a.*, ROW_NUMBER() OVER (
            PARTITION BY a.CNOTE_NO ORDER BY a.CDATE DESC NULLS LAST
        ) AS rn
        FROM CMS_CNOTE_AMO a
    ) sub WHERE rn = 1
),

-- CMS_MRCNOTE: Multiple rows per NO, keep latest by date
mrcnote_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (
            PARTITION BY m.MRCNOTE_NO ORDER BY m.MRCNOTE_DATE DESC NULLS LAST
        ) AS rn
        FROM CMS_MRCNOTE m
    ) sub WHERE rn = 1
),

-- CMS_DRSHEET: Multiple rows per CNOTE, keep latest by date
drsheet_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (
            PARTITION BY d.DRSHEET_CNOTE_NO ORDER BY d.DRSHEET_DATE DESC NULLS LAST
        ) AS rn
        FROM CMS_DRSHEET d
    ) sub WHERE rn = 1
),

-- CMS_MRSHEET: Multiple rows per NO, keep latest by date
mrsheet_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (
            PARTITION BY m.MRSHEET_NO ORDER BY m.MRSHEET_DATE DESC NULLS LAST
        ) AS rn
        FROM CMS_MRSHEET m
    ) sub WHERE rn = 1
),

-- CMS_DRSHEET_PRA: Multiple rows per CNOTE, keep latest by CREATION_DATE
drsheet_pra_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (
            PARTITION BY d.DRSHEET_CNOTE_NO ORDER BY d.CREATION_DATE DESC NULLS LAST
        ) AS rn
        FROM CMS_DRSHEET_PRA d
    ) sub WHERE rn = 1
),

-- CMS_DHICNOTE: Multiple rows per CNOTE, keep latest by TDATE
dhicnote_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (
            PARTITION BY d.DHICNOTE_CNOTE_NO ORDER BY d.DHICNOTE_TDATE DESC NULLS LAST
        ) AS rn
        FROM CMS_DHICNOTE d
    ) sub WHERE rn = 1
),

-- CMS_MHICNOTE: Multiple rows per NO, keep latest by date
mhicnote_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (
            PARTITION BY m.MHICNOTE_NO ORDER BY m.MHICNOTE_DATE DESC NULLS LAST
        ) AS rn
        FROM CMS_MHICNOTE m
    ) sub WHERE rn = 1
),

-- CMS_DHOCNOTE: Multiple rows per CNOTE, keep latest by TDATE
dhocnote_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (
            PARTITION BY d.DHOCNOTE_CNOTE_NO ORDER BY d.DHOCNOTE_TDATE DESC NULLS LAST
        ) AS rn
        FROM CMS_DHOCNOTE d
    ) sub WHERE rn = 1
),

-- CMS_MHOCNOTE: Multiple rows per NO, keep latest by date
mhocnote_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (
            PARTITION BY m.MHOCNOTE_NO ORDER BY m.MHOCNOTE_DATE DESC NULLS LAST
        ) AS rn
        FROM CMS_MHOCNOTE m
    ) sub WHERE rn = 1
),

-- CMS_DHOUNDEL_POD: Dedupe by CNOTE
dhoundel_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (
            PARTITION BY d.DHOUNDEL_CNOTE_NO ORDER BY d.CREATE_DATE DESC NULLS LAST
        ) AS rn
        FROM CMS_DHOUNDEL_POD d
    ) sub WHERE rn = 1
),

-- CMS_MHOUNDEL_POD: Dedupe by NO
mhoundel_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (
            PARTITION BY m.MHOUNDEL_NO ORDER BY m.MHOUNDEL_DATE DESC NULLS LAST
        ) AS rn
        FROM CMS_MHOUNDEL_POD m
    ) sub WHERE rn = 1
),

-- CMS_MFCNOTE: Multiple rows per CNOTE, keep latest
mfcnote_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (
            PARTITION BY m.MFCNOTE_NO ORDER BY m.MFCNOTE_CRDATE DESC NULLS LAST
        ) AS rn
        FROM CMS_MFCNOTE m
    ) sub WHERE rn = 1
),

-- CMS_MANIFEST: Multiple rows per NO, keep latest by date
manifest_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (
            PARTITION BY m.MANIFEST_NO ORDER BY m.MANIFEST_DATE DESC NULLS LAST
        ) AS rn
        FROM CMS_MANIFEST m
    ) sub WHERE rn = 1
),

-- CMS_DBAG_HO: Dedupe by CNOTE
dbag_ho_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (
            PARTITION BY d.DBAG_CNOTE_NO ORDER BY d.CDATE DESC NULLS LAST
        ) AS rn
        FROM CMS_DBAG_HO d
    ) sub WHERE rn = 1
),

-- CMS_DMBAG: Dedupe by NO
dmbag_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (
            PARTITION BY d.DMBAG_NO ORDER BY d.ESB_TIME DESC NULLS LAST
        ) AS rn
        FROM CMS_DMBAG d
    ) sub WHERE rn = 1
),

-- CMS_DHOV_RSHEET: Dedupe by CNOTE
dhov_rsheet_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (
            PARTITION BY d.DHOV_RSHEET_CNOTE ORDER BY d.CREATE_DATE DESC NULLS LAST
        ) AS rn
        FROM CMS_DHOV_RSHEET d
    ) sub WHERE rn = 1
),

-- CMS_DSTATUS: Dedupe by CNOTE
dstatus_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (
            PARTITION BY d.DSTATUS_CNOTE_NO ORDER BY d.CREATE_DATE DESC NULLS LAST
        ) AS rn
        FROM CMS_DSTATUS d
    ) sub WHERE rn = 1
),

-- CMS_COST_DTRANSIT_AGEN: Dedupe by CNOTE
cost_dtransit_deduped AS (
    SELECT * FROM (
        SELECT c.*, ROW_NUMBER() OVER (
            PARTITION BY c.CNOTE_NO ORDER BY c.ESB_TIME DESC NULLS LAST
        ) AS rn
        FROM CMS_COST_DTRANSIT_AGEN c
    ) sub WHERE rn = 1
),

-- CMS_COST_MTRANSIT_AGEN: Dedupe by MANIFEST_NO
cost_mtransit_deduped AS (
    SELECT * FROM (
        SELECT c.*, ROW_NUMBER() OVER (
            PARTITION BY c.MANIFEST_NO ORDER BY c.MANIFEST_DATE DESC NULLS LAST
        ) AS rn
        FROM CMS_COST_MTRANSIT_AGEN c
    ) sub WHERE rn = 1
),

-- CMS_RDSJ: Dedupe by HVI
rdsj_deduped AS (
    SELECT * FROM (
        SELECT r.*, ROW_NUMBER() OVER (
            PARTITION BY r.RDSJ_HVI_NO ORDER BY r.RDSJ_CDATE DESC NULLS LAST
        ) AS rn
        FROM CMS_RDSJ r
    ) sub WHERE rn = 1
),

-- CMS_DSJ: Dedupe by HVO
dsj_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (
            PARTITION BY d.DSJ_HVO_NO ORDER BY d.DSJ_CDATE DESC NULLS LAST
        ) AS rn
        FROM CMS_DSJ d
    ) sub WHERE rn = 1
),

-- CMS_MSJ: Dedupe by NO
msj_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (
            PARTITION BY m.MSJ_NO ORDER BY m.MSJ_DATE DESC NULLS LAST
        ) AS rn
        FROM CMS_MSJ m
    ) sub WHERE rn = 1
),

-- CMS_DSMU: Dedupe by NO
dsmu_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (
            PARTITION BY d.DSMU_NO ORDER BY d.ESB_TIME DESC NULLS LAST
        ) AS rn
        FROM CMS_DSMU d
    ) sub WHERE rn = 1
),

-- CMS_MSMU: Dedupe by NO
msmu_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (
            PARTITION BY m.MSMU_NO ORDER BY m.MSMU_DATE DESC NULLS LAST
        ) AS rn
        FROM CMS_MSMU m
    ) sub WHERE rn = 1
),

-- CMS_DROURATE: Dedupe by CODE + SERVICE
drourate_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (
            PARTITION BY d.DROURATE_CODE, d.DROURATE_SERVICE ORDER BY d.DROURATE_UDATE DESC NULLS LAST
        ) AS rn
        FROM CMS_DROURATE d
    ) sub WHERE rn = 1
),

-- T_MDT_CITY_ORIGIN: Dedupe by CITY_CODE
mdt_city_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (
            PARTITION BY m.CITY_CODE ORDER BY m.CREATE_DATE DESC NULLS LAST
        ) AS rn
        FROM T_MDT_CITY_ORIGIN m
    ) sub WHERE rn = 1
),

-- LASTMILE_COURIER: Dedupe by COURIER_ID
courier_deduped AS (
    SELECT * FROM (
        SELECT c.*, ROW_NUMBER() OVER (
            PARTITION BY c.COURIER_ID ORDER BY c.COURIER_UPDATED_AT DESC NULLS LAST
        ) AS rn
        FROM LASTMILE_COURIER c
    ) sub WHERE rn = 1
),

-- ORA_ZONE: Dedupe by ZONE_CODE
ora_zone_deduped AS (
    SELECT * FROM (
        SELECT o.*, ROW_NUMBER() OVER (
            PARTITION BY o.ZONE_CODE ORDER BY o.LASTUPDDTM DESC NULLS LAST
        ) AS rn
        FROM ORA_ZONE o
    ) sub WHERE rn = 1
),

-- ORA_USER: Dedupe by USER_ID
ora_user_deduped AS (
    SELECT * FROM (
        SELECT u.*, ROW_NUMBER() OVER (
            PARTITION BY u.USER_ID ORDER BY u.USER_DATE DESC NULLS LAST
        ) AS rn
        FROM ORA_USER u
    ) sub WHERE rn = 1
)

-- ============================================================
-- MAIN SELECT — 34 tables, kept columns only
-- ============================================================
SELECT
    -- ========================================
    -- 1. CMS_CNOTE (59 of 117 kept)
    -- ========================================
    cn.CNOTE_NO,
    cn.CNOTE_DATE,
    cn.CNOTE_BRANCH_ID,
    cn.CNOTE_SERVICES_CODE,
    cn.CNOTE_CUST_NO,
    cn.CNOTE_ROUTE_CODE,
    cn.CNOTE_ORIGIN,
    cn.CNOTE_DESTINATION,
    cn.CNOTE_QTY,
    cn.CNOTE_WEIGHT,
    cn.CNOTE_SHIPPER_NAME,
    cn.CNOTE_SHIPPER_ADDR1,
    cn.CNOTE_SHIPPER_ADDR2,
    cn.CNOTE_SHIPPER_ADDR3,
    cn.CNOTE_SHIPPER_CITY,
    cn.CNOTE_SHIPPER_ZIP,
    cn.CNOTE_SHIPPER_REGION,
    cn.CNOTE_SHIPPER_COUNTRY,
    cn.CNOTE_SHIPPER_CONTACT,
    cn.CNOTE_SHIPPER_PHONE,
    cn.CNOTE_RECEIVER_NAME,
    cn.CNOTE_RECEIVER_ADDR1,
    cn.CNOTE_RECEIVER_ADDR2,
    cn.CNOTE_RECEIVER_ADDR3,
    cn.CNOTE_RECEIVER_CITY,
    cn.CNOTE_RECEIVER_ZIP,
    cn.CNOTE_RECEIVER_REGION,
    cn.CNOTE_RECEIVER_COUNTRY,
    cn.CNOTE_RECEIVER_CONTACT,
    cn.CNOTE_RECEIVER_PHONE,
    cn.CNOTE_GOODS_TYPE,
    cn.CNOTE_GOODS_DESCR,
    cn.CNOTE_GOODS_VALUE,
    cn.CNOTE_SPECIAL_INS,
    cn.CNOTE_INSURANCE_ID,
    cn.CNOTE_INSURANCE_VALUE,
    cn.CNOTE_PAYMENT_TYPE,
    cn.CNOTE_CURRENCY,
    cn.CNOTE_AMOUNT,
    cn.CNOTE_ADDITIONAL_FEE,
    cn.CNOTE_NOTICE,
    cn.CNOTE_PRINTED,
    cn.CNOTE_CANCEL,
    cn.CNOTE_HOLD,
    cn.CNOTE_HOLD_REASON,
    cn.CNOTE_USER,
    cn.CNOTE_REFNO,
    cn.CNOTE_INSURANCE_NO,
    cn.CNOTE_OTHER_FEE,
    cn.CNOTE_CURC_PAYMENT,
    cn.CNOTE_CURC_RATE,
    cn.CNOTE_PAYMENT_BY,
    cn.CNOTE_AMOUNT_PAYMENT,
    cn.CNOTE_BILNOTE,
    cn.CNOTE_CRDATE,
    cn.CNOTE_PACKING,
    cn.CNOTE_CARD_NO,
    cn.CNOTE_ECNOTE,
    cn.CNOTE_SES_FRM,

    -- ========================================
    -- 2. CMS_CNOTE_POD (7 of 7 — ALL KEPT)
    -- ========================================
    pod.CNOTE_POD_NO,
    pod.CNOTE_POD_DATE,
    pod.CNOTE_POD_RECEIVER,
    pod.CNOTE_POD_STATUS,
    pod.CNOTE_POD_DELIVERED,
    pod.CNOTE_POD_DOC_NO,
    pod.CNOTE_POD_CREATION_DATE,

    -- ========================================
    -- 3. CMS_CNOTE_AMO (13 of 13 — ALL KEPT)
    -- ========================================
    amo.CNOTE_BRANCH_ID  AS AMO_BRANCH_ID,
    amo.FIELDCHAR1       AS AMO_FIELDCHAR1,
    amo.FIELDCHAR2       AS AMO_FIELDCHAR2,
    amo.FIELDCHAR3       AS AMO_FIELDCHAR3,
    amo.FIELDCHAR4       AS AMO_FIELDCHAR4,
    amo.FIELDCHAR5       AS AMO_FIELDCHAR5,
    amo.FIELDNUM1        AS AMO_FIELDNUM1,
    amo.FIELDNUM2        AS AMO_FIELDNUM2,
    amo.FIELDNUM3        AS AMO_FIELDNUM3,
    amo.FIELDNUM4        AS AMO_FIELDNUM4,
    amo.FIELDNUM5        AS AMO_FIELDNUM5,
    amo.CDATE            AS AMO_CDATE,

    -- ========================================
    -- 4. CMS_APICUST (42 of 44 — 2 excluded)
    --    Excluded: APICUST_REQID, APICUST_FLAG
    -- ========================================
    api.APICUST_ORDER_ID,
    api.APICUST_CNOTE_NO  AS APICUST_CNOTE_NO,
    api.APICUST_ORIGIN,
    api.APICUST_BRANCH,
    api.APICUST_CUST_NO,
    api.APICUST_SERVICES_CODE AS APICUST_SERVICES_CODE,
    api.APICUST_DESTINATION,
    api.APICUST_SHIPPER_NAME,
    api.APICUST_SHIPPER_ADDR1,
    api.APICUST_SHIPPER_ADDR2,
    api.APICUST_SHIPPER_ADDR3,
    api.APICUST_SHIPPER_CITY,
    api.APICUST_SHIPPER_ZIP,
    api.APICUST_SHIPPER_REGION,
    api.APICUST_SHIPPER_COUNTRY,
    api.APICUST_SHIPPER_CONTACT,
    api.APICUST_SHIPPER_PHONE,
    api.APICUST_RECEIVER_NAME,
    api.APICUST_RECEIVER_ADDR1,
    api.APICUST_RECEIVER_ADDR2,
    api.APICUST_RECEIVER_ADDR3,
    api.APICUST_RECEIVER_CITY,
    api.APICUST_RECEIVER_ZIP,
    api.APICUST_RECEIVER_REGION,
    api.APICUST_RECEIVER_COUNTRY,
    api.APICUST_RECEIVER_CONTACT,
    api.APICUST_RECEIVER_PHONE,
    api.APICUST_QTY,
    api.APICUST_WEIGHT,
    api.APICUST_GOODS_DESCR,
    api.APICUST_GOODS_VALUE,
    api.APICUST_SPECIAL_INS,
    api.APICUST_INS_FLAG,
    api.APICUST_COD_FLAG,
    api.APICUST_COD_AMOUNT,
    api.CREATE_DATE       AS APICUST_CREATE_DATE,
    api.APICUST_LATITUDE,
    api.APICUST_LONGITUDE,
    api.APICUST_SHIPMENT_TYPE,
    api.APICUST_MERCHAN_ID,
    api.APICUST_NAME,
    api.SHIPPER_PROVIDER,

    -- ========================================
    -- 5. CMS_DROURATE (8 of 17 — 9 excluded)
    --    Kept: CODE, SERVICE, ACTIVE, ETD_FROM,
    --          ETD_THRU, UDATE, TIME, YES_LATE
    -- ========================================
    drou.DROURATE_CODE,
    drou.DROURATE_SERVICE AS DROURATE_SERVICE,
    drou.DROURATE_ACTIVE,
    drou.DROURATE_UDATE,
    drou.DROURATE_ETD_FROM,
    drou.DROURATE_ETD_THRU,
    drou.DROURATE_TIME,
    drou.DROURATE_YES_LATE,

    -- ========================================
    -- 6. CMS_DRCNOTE (7 of 9 — 2 excluded)
    --    Excluded: DRCNOTE_FLAG, DRCNOTE_DO
    -- ========================================
    drc.DRCNOTE_NO,
    drc.DRCNOTE_CNOTE_NO  AS DRCNOTE_CNOTE_NO,
    drc.DRCNOTE_QTY,
    drc.DRCNOTE_REMARKS,
    drc.DRCNOTE_TDATE,
    drc.DRCNOTE_TUSER,
    drc.DRCNOTE_PAYMENT,

    -- ========================================
    -- 7. CMS_MRCNOTE (9 of 12 — 3 excluded)
    --    Excluded: MRCNOTE_AE_ID, MRCNOTE_REFNO,
    --             MRCNOTE_TYPE
    -- ========================================
    mrc.MRCNOTE_NO        AS MRCNOTE_NO,
    mrc.MRCNOTE_DATE,
    mrc.MRCNOTE_BRANCH_ID,
    mrc.MRCNOTE_USER_ID,
    mrc.MRCNOTE_COURIER_ID,
    mrc.MRCNOTE_USER1,
    mrc.MRCNOTE_USER2,
    mrc.MRCNOTE_SIGNDATE,
    mrc.MRCNOTE_PAYMENT,

    -- ========================================
    -- 8. CMS_DRSHEET (9 of 9 — ALL KEPT)
    -- ========================================
    drs.DRSHEET_NO,
    drs.DRSHEET_CNOTE_NO  AS DRSHEET_CNOTE_NO,
    drs.DRSHEET_DATE,
    drs.DRSHEET_STATUS,
    drs.DRSHEET_RECEIVER,
    drs.DRSHEET_FLAG,
    drs.DRSHEET_UID,
    drs.DRSHEET_UDATE,
    drs.CREATION_DATE     AS DRSHEET_CREATION_DATE,

    -- ========================================
    -- 9. CMS_MRSHEET (8 of 13 — 5 excluded)
    --    Excluded: MRSHEET_UDATE_DR, MRSHEET_INB_APP_DR,
    --             MRSHEET_INB_UID_DR, MRSHEET_INB_UDATE_DR,
    --             MRSHEET_ORIGIN
    -- ========================================
    mrsht.MRSHEET_BRANCH,
    mrsht.MRSHEET_NO      AS MRSHEET_NO,
    mrsht.MRSHEET_DATE,
    mrsht.MRSHEET_COURIER_ID,
    mrsht.MRSHEET_UID,
    mrsht.MRSHEET_UDATE,
    mrsht.MRSHEET_APPROVED_DR,
    mrsht.MRSHEET_UID_DR,

    -- ========================================
    -- 10. CMS_DRSHEET_PRA (3 of 10 — 7 excluded)
    --     Kept: DRSHEET_NO, DRSHEET_CNOTE_NO, CREATION_DATE
    -- ========================================
    pra.DRSHEET_NO        AS DRSHEET_PRA_NO,
    pra.DRSHEET_CNOTE_NO  AS DRSHEET_PRA_CNOTE_NO,
    pra.CREATION_DATE     AS DRSHEET_PRA_CREATION_DATE,

    -- ========================================
    -- 11. CMS_DHICNOTE (5 of 6 — 1 excluded)
    --     Excluded: DHICNOTE_SEQ_NO
    -- ========================================
    dhicn.DHICNOTE_NO,
    dhicn.DHICNOTE_CNOTE_NO AS DHICNOTE_CNOTE_NO,
    dhicn.DHICNOTE_QTY,
    dhicn.DHICNOTE_REMARKS,
    dhicn.DHICNOTE_TDATE,

    -- ========================================
    -- 12. CMS_MHICNOTE (11 of 11 — ALL KEPT)
    -- ========================================
    mhicn.MHICNOTE_BRANCH_ID,
    mhicn.MHICNOTE_ZONE,
    mhicn.MHICNOTE_NO     AS MHICNOTE_NO,
    mhicn.MHICNOTE_REF_NO,
    mhicn.MHICNOTE_DATE,
    mhicn.MHICNOTE_ZONE_ORIG,
    mhicn.MHICNOTE_USER_ID,
    mhicn.MHICNOTE_USER1,
    mhicn.MHICNOTE_USER2,
    mhicn.MHICNOTE_SIGNDATE,
    mhicn.MHICNOTE_APPROVE,

    -- ========================================
    -- 13. CMS_DHOCNOTE (5 of 12 — 7 excluded)
    --     Excluded: DHOCNOTE_SEQ_NO, DHOCNOTE_INBOUND,
    --              DHOCNOTE_WEIGHT, DHOCNOTE_SERVICE,
    --              DHOCNOTE_DEST, DHOCNOTE_GOODS,
    --              DHOCNOTE_HANDLING
    -- ========================================
    dhoc.DHOCNOTE_NO,
    dhoc.DHOCNOTE_CNOTE_NO AS DHOCNOTE_CNOTE_NO,
    dhoc.DHOCNOTE_QTY,
    dhoc.DHOCNOTE_REMARKS,
    dhoc.DHOCNOTE_TDATE,

    -- ========================================
    -- 14. CMS_MHOCNOTE (13 of 20 — 7 excluded)
    --     Excluded: MHOCNOTE_ORIGIN, MHOCNOTE_SERVICES,
    --              MHOCNOTE_PRODUCT, MHOCNOTE_CUST,
    --              MHOCNOTE_USER_SCO, MHOCNOTE_HVS,
    --              MHOCNOTE_TYPE
    -- ========================================
    mhoc.MHOCNOTE_BRANCH_ID,
    mhoc.MHOCNOTE_ZONE,
    mhoc.MHOCNOTE_NO      AS MHOCNOTE_NO,
    mhoc.MHOCNOTE_DATE,
    mhoc.MHOCNOTE_ZONE_DEST,
    mhoc.MHOCNOTE_USER_ID AS MHOCNOTE_USER_ID,
    mhoc.MHOCNOTE_USER1,
    mhoc.MHOCNOTE_USER2,
    mhoc.MHOCNOTE_SIGNDATE AS MHOCNOTE_SIGNDATE,
    mhoc.MHOCNOTE_APPROVE,
    mhoc.MHOCNOTE_REMARKS,
    mhoc.MHOCNOTE_COURIER_ID,
    mhoc.MHOCNOTE_APP_DATE,

    -- ========================================
    -- 15. CMS_DHOUNDEL_POD (6 of 11 — 5 excluded)
    --     Excluded: DHOUNDEL_SEQ_NO, DHOUNDEL_WEIGHT,
    --              DHOUNDEL_SERVICE, DHOUNDEL_DEST,
    --              DHOUNDEL_GOODS
    -- ========================================
    dhund.DHOUNDEL_NO,
    dhund.DHOUNDEL_CNOTE_NO AS DHOUNDEL_CNOTE_NO,
    dhund.DHOUNDEL_QTY,
    dhund.DHOUNDEL_REMARKS,
    dhund.DHOUNDEL_HRS,
    dhund.CREATE_DATE      AS DHOUNDEL_CREATE_DATE,

    -- ========================================
    -- 16. CMS_MHOUNDEL_POD (10 of 11 — 1 excluded)
    --     Excluded: MHOUNDEL_FLOW
    -- ========================================
    mhund.MHOUNDEL_BRANCH_ID,
    mhund.MHOUNDEL_NO     AS MHOUNDEL_NO,
    mhund.MHOUNDEL_REMARKS AS MHOUNDEL_REMARKS,
    mhund.MHOUNDEL_DATE,
    mhund.MHOUNDEL_USER_ID,
    mhund.MHOUNDEL_ZONE,
    mhund.MHOUNDEL_APPROVE,
    mhund.MHOUNDEL_USER1,
    mhund.MHOUNDEL_USER2,
    mhund.MHOUNDEL_SIGNDATE,

    -- ========================================
    -- 17. CMS_MFCNOTE (5 of 20 — 15 excluded)
    --     Kept: MFCNOTE_MAN_NO, MFCNOTE_BAG_NO,
    --           MFCNOTE_NO, MFCNOTE_WEIGHT, MFCNOTE_CRDATE
    -- ========================================
    mfc.MFCNOTE_MAN_NO,
    mfc.MFCNOTE_BAG_NO,
    mfc.MFCNOTE_NO         AS MFCNOTE_CNOTE_NO,
    mfc.MFCNOTE_WEIGHT,
    mfc.MFCNOTE_CRDATE,

    -- ========================================
    -- 18. CMS_MANIFEST (13 of 18 — 5 excluded)
    --     Excluded: MANIFEST_RECALL_NO, MANIFEST_USER_AUDIT,
    --              MANIFEST_TIME_AUDIT, MANIFEST_FORM_AUDIT,
    --              MANIFEST_MODA
    -- ========================================
    man.MANIFEST_NO        AS MANIFEST_NO,
    man.MANIFEST_DATE,
    man.MANIFEST_ROUTE,
    man.MANIFEST_FROM,
    man.MANIFEST_THRU,
    man.MANIFEST_NOTICE,
    man.MANIFEST_APPROVED,
    man.MANIFEST_ORIGIN,
    man.MANIFEST_CODE,
    man.MANIFEST_UID,
    man.MANIFEST_CRDATE,
    man.MANIFEST_CANCELED,
    man.MANIFEST_CANCELED_UID,

    -- ========================================
    -- 19. CMS_DBAG_HO (10 of 10 — ALL KEPT)
    -- ========================================
    dbag.DBAG_HO_NO,
    dbag.DBAG_NO,
    dbag.DBAG_CNOTE_NO     AS DBAG_CNOTE_NO,
    dbag.DBAG_CNOTE_QTY,
    dbag.DBAG_CNOTE_WEIGHT,
    dbag.DBAG_CNOTE_DESTINATION,
    dbag.CDATE             AS DBAG_HO_CDATE,
    dbag.DBAG_CNOTE_SERVICE,
    dbag.DBAG_CNOTE_DATE,
    dbag.DBAG_ZONE_DEST,

    -- ========================================
    -- 20. CMS_DMBAG (7 of 11 — 4 excluded)
    --     Excluded: DMBAG_SPS, DMBAG_INBOUND,
    --              DMBAG_TYPE, DMBAG_STATUS
    -- ========================================
    dmbag.DMBAG_NO         AS DMBAG_NO,
    dmbag.DMBAG_BAG_NO,
    dmbag.DMBAG_ORIGIN,
    dmbag.DMBAG_DESTINATION,
    dmbag.DMBAG_WEIGHT,
    dmbag.ESB_TIME         AS DMBAG_ESB_TIME,
    dmbag.ESB_ID           AS DMBAG_ESB_ID,

    -- ========================================
    -- 21. CMS_DHOV_RSHEET (15 of 23 — 8 excluded)
    --     Excluded: DHOV_RSHEET_REDEL, DHOV_RSHEET_OUTS,
    --              DCSUNDEL_RSHEET_RSHEETNO,
    --              DHOV_DRSHEET_DATE, DHOV_DRSHEET_RECEIVER,
    --              DHOV_DRSHEET_FLAG, DHOV_DRSHEET_UID,
    --              DHOV_DRSHEET_UDATE
    -- ========================================
    dhov.DHOV_RSHEET_NO,
    dhov.DHOV_RSHEET_CNOTE AS DHOV_RSHEET_CNOTE,
    dhov.DHOV_RSHEET_DO,
    dhov.DHOV_RSHEET_COD,
    dhov.DHOV_RSHEET_UNDEL,
    dhov.DHOV_RSHEET_QTY,
    dhov.CREATE_DATE       AS DHOV_CREATE_DATE,
    dhov.DHOV_RSHEET_UZONE,
    dhov.DHOV_RSHEET_CYCLE,
    dhov.DHOV_RSHEET_RSHEETNO,
    dhov.DHOV_DRSHEET_EPAY_VEND,
    dhov.DHOV_DRSHEET_EPAY_TRXID,
    dhov.DHOV_DRSHEET_EPAY_AMOUNT,
    dhov.DHOV_DRSHEET_EPAY_DEVICE,
    dhov.DHOV_DRSHEET_STATUS,

    -- ========================================
    -- 22. CMS_DSJ (5 of 5 — ALL KEPT)
    -- ========================================
    dsj.DSJ_NO,
    dsj.DSJ_BAG_NO,
    dsj.DSJ_HVO_NO,
    dsj.DSJ_UID,
    dsj.DSJ_CDATE,

    -- ========================================
    -- 23. CMS_MSJ (13 of 17 — 4 excluded)
    --     Excluded: MSJ_ARMADA, MSJ_CHAR1,
    --              MSJ_CHAR2, MSJ_CHAR3
    -- ========================================
    msj.MSJ_NO             AS MSJ_NO,
    msj.MSJ_BRANCH_ID,
    msj.MSJ_DATE,
    msj.MSJ_USER1,
    msj.MSJ_USER2,
    msj.MSJ_APPROVE,
    msj.MSJ_CDATE,
    msj.MSJ_UID,
    msj.MSJ_REMARKS,
    msj.MSJ_SIGNDATE,
    msj.MSJ_DEST,
    msj.MSJ_ORIG,
    msj.MSJ_COURIER_ID,

    -- ========================================
    -- 24. CMS_RDSJ (6 of 6 — ALL KEPT)
    -- ========================================
    rdsj.RDSJ_NO,
    rdsj.RDSJ_BAG_NO,
    rdsj.RDSJ_HVO_NO,
    rdsj.RDSJ_UID,
    rdsj.RDSJ_CDATE,
    rdsj.RDSJ_HVI_NO,

    -- ========================================
    -- 25. CMS_DSMU (10 of 14 — 4 excluded)
    --     Excluded: DSMU_BAG_CANCEL, DSMU_SPS,
    --              DSMU_INBOUND, DSMU_MANIFEST_NO
    -- ========================================
    dsmu.DSMU_NO,
    dsmu.DSMU_FLIGHT_NO,
    dsmu.DSMU_FLIGHT_DATE,
    dsmu.DSMU_BAG_NO       AS DSMU_BAG_NO,
    dsmu.DSMU_WEIGHT,
    dsmu.DSMU_BAG_ORIGIN,
    dsmu.DSMU_BAG_DESTINATION,
    dsmu.ESB_TIME          AS DSMU_ESB_TIME,
    dsmu.ESB_ID            AS DSMU_ESB_ID,
    dsmu.DSMU_POLICE_LICENSE_PLATE,

    -- ========================================
    -- 26. CMS_MSMU (25 of 26 — 1 excluded)
    --     Excluded: MSMU_TYPE
    -- ========================================
    msmu.MSMU_NO           AS MSMU_NO,
    msmu.MSMU_DATE,
    msmu.MSMU_ORIGIN,
    msmu.MSMU_DESTINATION,
    msmu.MSMU_FLIGHT_NO    AS MSMU_FLIGHT_NO,
    msmu.MSMU_FLIGHT_DATE  AS MSMU_FLIGHT_DATE,
    msmu.MSMU_ETD,
    msmu.MSMU_ETA,
    msmu.MSMU_QTY,
    msmu.MSMU_WEIGHT,
    msmu.MSMU_USER,
    msmu.MSMU_FLAG,
    msmu.MSMU_REMARKS,
    msmu.MSMU_STATUS,
    msmu.MSMU_WRH_DATE,
    msmu.MSMU_WRH_TIME,
    msmu.MSMU_OFF_DATE,
    msmu.MSMU_OFF_TIME,
    msmu.MSMU_CONFIRM,
    msmu.MSMU_CANCEL,
    msmu.MSMU_USER_CANCEL,
    msmu.MSMU_REPLACE,
    msmu.MSMU_MODA,
    msmu.MSMU_POLICE_LICENSE_PLATE,
    msmu.MSMU_HOURS,

    -- ========================================
    -- 27. CMS_DSTATUS (12 of 13 — 1 excluded)
    --     Excluded: DSTATUS_BAG_NO_NEW
    -- ========================================
    dst.DSTATUS_NO,
    dst.DSTATUS_CNOTE_NO   AS DSTATUS_CNOTE_NO,
    dst.DSTATUS_STATUS,
    dst.DSTATUS_REMARKS,
    dst.CREATE_DATE        AS DSTATUS_CREATE_DATE,
    dst.DSTATUS_STATUS_DATE,
    dst.DSTATUS_MANIFEST_NO_OLD,
    dst.DSTATUS_BAG_NO_OLD,
    dst.DSTATUS_MANIFEST_NO_NEW,
    dst.DSTATUS_MANIFEST_DEST,
    dst.DSTATUS_MANIFEST_THRU,
    dst.DSTATUS_ZONE_CODE,

    -- ========================================
    -- 28. CMS_COST_DTRANSIT_AGEN (10 of 11 — 1 excluded)
    --     Excluded: CNOTE_COST
    -- ========================================
    costd.DMANIFEST_NO     AS COST_D_MANIFEST_NO,
    costd.CNOTE_NO         AS COST_D_CNOTE_NO,
    costd.CNOTE_ORIGIN     AS COST_D_ORIGIN,
    costd.CNOTE_DESTINATION AS COST_D_DESTINATION,
    costd.CNOTE_QTY        AS COST_D_QTY,
    costd.CNOTE_WEIGHT     AS COST_D_WEIGHT,
    costd.CNOTE_SERVICES_CODE AS COST_D_SERVICES_CODE,
    costd.ESB_TIME         AS COST_D_ESB_TIME,
    costd.ESB_ID           AS COST_D_ESB_ID,
    costd.DMANIFEST_DOC_REF AS COST_D_DOC_REF,

    -- ========================================
    -- 29. CMS_COST_MTRANSIT_AGEN (12 of 15 — 3 excluded)
    --     Excluded: MANIFEST_COST, REAL_COST,
    --              MANIFEST_TYPE
    -- ========================================
    costm.MANIFEST_NO      AS COST_M_MANIFEST_NO,
    costm.MANIFEST_DATE    AS COST_M_MANIFEST_DATE,
    costm.BRANCH_ID        AS COST_M_BRANCH_ID,
    costm.DESTINATION      AS COST_M_DESTINATION,
    costm.CTC_WEIGHT       AS COST_M_CTC_WEIGHT,
    costm.ACT_WEIGHT       AS COST_M_ACT_WEIGHT,
    costm.MANIFEST_APPROVED AS COST_M_APPROVED,
    costm.REMARK           AS COST_M_REMARK,
    costm.MANIFEST_UID     AS COST_M_UID,
    costm.ESB_TIME         AS COST_M_ESB_TIME,
    costm.ESB_ID           AS COST_M_ESB_ID,
    costm.MANIFEST_DOC_REF AS COST_M_DOC_REF,

    -- ========================================
    -- 30. T_GOTO (6 of 6 — ALL KEPT)
    -- ========================================
    gt.AWB                 AS GOTO_AWB,
    gt.CROSSDOCK_ROLE,
    gt.FIELD_CHAR1,
    gt.FIELD_NUMBER1,
    gt.FIELD_DATE1,
    gt.CREATE_DATE         AS GOTO_CREATE_DATE,

    -- ========================================
    -- 31. T_MDT_CITY_ORIGIN (6 of 6 — ALL KEPT)
    -- ========================================
    mdt.CITY_BRANCH        AS MDT_CITY_BRANCH,
    mdt.CITY_CODE          AS MDT_CITY_CODE,
    mdt.CITY_ORIGIN        AS MDT_CITY_ORIGIN,
    mdt.CITY_MTS           AS MDT_CITY_MTS,
    mdt.CITY_ACTIVE        AS MDT_CITY_ACTIVE,
    mdt.CREATE_DATE        AS MDT_CREATE_DATE,

    -- ========================================
    -- 32. LASTMILE_COURIER (17 of 29 — 12 excluded)
    --     Excluded: PII (PHONE, EMAIL, PASSWORD, NIK),
    --              vacant columns, PARENT/CUST IDs
    -- ========================================
    cour.COURIER_ID,
    cour.COURIER_NAME,
    cour.COURIER_REGIONAL,
    cour.COURIER_BRANCH    AS COURIER_BRANCH,
    cour.COURIER_ZONE      AS COURIER_ZONE,
    cour.COURIER_ORIGIN    AS COURIER_ORIGIN,
    cour.COURIER_ACTIVE,
    cour.COURIER_SP_VALUE,
    cour.COURIER_INCENTIVE_GROUP,
    cour.COURIER_ARMADA,
    cour.COURIER_EMPLOYEE_STATUS,
    cour.COURIER_CREATED_AT,
    cour.COURIER_UPDATED_AT,
    cour.COURIER_ROLE_ID,
    cour.COURIER_LEVEL,
    cour.COURIER_COMPANY_ID,
    cour.COURIER_TYPE,

    -- ========================================
    -- 33. ORA_ZONE (25 of 25 — ALL KEPT)
    -- ========================================
    ora.ZONE_BRANCH        AS ORA_ZONE_BRANCH,
    ora.ZONE_CODE          AS ORA_ZONE_CODE,
    ora.ZONE_DESC          AS ORA_ZONE_DESC,
    ora.ZONE_UID           AS ORA_ZONE_UID,
    ora.ZONE_ACTIVE        AS ORA_ZONE_ACTIVE,
    ora.ZONE_SEQ           AS ORA_ZONE_SEQ,
    ora.ZONE_DATE          AS ORA_ZONE_DATE,
    ora.ZONE_TYPE          AS ORA_ZONE_TYPE,
    ora.ZONE_ECNOTE_PARM   AS ORA_ZONE_ECNOTE_PARM,
    ora.ZONE_ORIGIN        AS ORA_ZONE_ORIGIN,
    ora.ZONE_NAME          AS ORA_ZONE_NAME,
    ora.ZONE_ADDR1         AS ORA_ZONE_ADDR1,
    ora.ZONE_ADDR2         AS ORA_ZONE_ADDR2,
    ora.ZONE_ADDR3         AS ORA_ZONE_ADDR3,
    ora.ZONE_DP_FLAG       AS ORA_ZONE_DP_FLAG,
    ora.ZONE_LATITUDE      AS ORA_ZONE_LATITUDE,
    ora.ZONE_LONGTITIDE    AS ORA_ZONE_LONGTITIDE,
    ora.ZONE_KOTA          AS ORA_ZONE_KOTA,
    ora.ZONE_KECAMATAN     AS ORA_ZONE_KECAMATAN,
    ora.ZONE_PROVINSI      AS ORA_ZONE_PROVINSI,
    ora.ZONE_CATEGORY      AS ORA_ZONE_CATEGORY,
    ora.LASTUPDDTM         AS ORA_ZONE_LASTUPDDTM,
    ora.LASTUPDBY          AS ORA_ZONE_LASTUPDBY,
    ora.LASTUPDPROCESS     AS ORA_ZONE_LASTUPDPROCESS,
    ora.ZONE_KELURAHAN     AS ORA_ZONE_KELURAHAN,

    -- ========================================
    -- 34. ORA_USER (8 of 10 — 2 excluded)
    --     Excluded: USER_CUST_ID, USER_CUST_NAME
    -- ========================================
    usr.USER_ID            AS ORA_USER_ID,
    usr.USER_NAME          AS ORA_USER_NAME,
    usr.USER_ZONE_CODE     AS ORA_USER_ZONE_CODE,
    usr.USER_ACTIVE        AS ORA_USER_ACTIVE,
    usr.USER_DATE          AS ORA_USER_DATE,
    usr.USER_ORIGIN        AS ORA_USER_ORIGIN,
    usr.USER_NIK           AS ORA_USER_NIK,
    usr.USER_VACANT10      AS ORA_USER_VACANT10

FROM CMS_CNOTE cn

-- ============================================================
-- JOIN CHAIN (34 tables)
-- ============================================================

-- 2. CMS_CNOTE_POD (1:1 on CNOTE_NO)
LEFT JOIN CMS_CNOTE_POD pod
    ON cn.CNOTE_NO = pod.CNOTE_POD_NO

-- 3. CMS_CNOTE_AMO (1:1 on CNOTE_NO)
LEFT JOIN cnote_amo_deduped amo
    ON cn.CNOTE_NO = amo.CNOTE_NO

-- 4. CMS_APICUST (1:1 on CNOTE_NO)
LEFT JOIN CMS_APICUST api
    ON cn.CNOTE_NO = api.APICUST_CNOTE_NO

-- 5. CMS_DROURATE (reference on route+service)
LEFT JOIN drourate_deduped drou
    ON cn.CNOTE_ROUTE_CODE = drou.DROURATE_CODE
    AND cn.CNOTE_SERVICES_CODE = drou.DROURATE_SERVICE

-- 6. CMS_DRCNOTE (1:1 on CNOTE_NO)
LEFT JOIN CMS_DRCNOTE drc
    ON cn.CNOTE_NO = drc.DRCNOTE_CNOTE_NO

-- 7. CMS_MRCNOTE (via DRCNOTE_NO)
LEFT JOIN mrcnote_deduped mrc
    ON drc.DRCNOTE_NO = mrc.MRCNOTE_NO

-- 8. CMS_DRSHEET (on CNOTE_NO)
LEFT JOIN drsheet_deduped drs
    ON cn.CNOTE_NO = drs.DRSHEET_CNOTE_NO

-- 9. CMS_MRSHEET (via DRSHEET_NO)
LEFT JOIN mrsheet_deduped mrsht
    ON drs.DRSHEET_NO = mrsht.MRSHEET_NO

-- 10. CMS_DRSHEET_PRA (on CNOTE_NO)
LEFT JOIN drsheet_pra_deduped pra
    ON cn.CNOTE_NO = pra.DRSHEET_CNOTE_NO

-- 11. CMS_DHICNOTE (on CNOTE_NO)
LEFT JOIN dhicnote_deduped dhicn
    ON cn.CNOTE_NO = dhicn.DHICNOTE_CNOTE_NO

-- 12. CMS_MHICNOTE (via DHICNOTE_NO)
LEFT JOIN mhicnote_deduped mhicn
    ON dhicn.DHICNOTE_NO = mhicn.MHICNOTE_NO

-- 13. CMS_DHOCNOTE (on CNOTE_NO)
LEFT JOIN dhocnote_deduped dhoc
    ON cn.CNOTE_NO = dhoc.DHOCNOTE_CNOTE_NO

-- 14. CMS_MHOCNOTE (via DHOCNOTE_NO)
LEFT JOIN mhocnote_deduped mhoc
    ON dhoc.DHOCNOTE_NO = mhoc.MHOCNOTE_NO

-- 15. CMS_DHOUNDEL_POD (on CNOTE_NO)
LEFT JOIN dhoundel_deduped dhund
    ON cn.CNOTE_NO = dhund.DHOUNDEL_CNOTE_NO

-- 16. CMS_MHOUNDEL_POD (via DHOUNDEL_NO)
LEFT JOIN mhoundel_deduped mhund
    ON dhund.DHOUNDEL_NO = mhund.MHOUNDEL_NO

-- 17. CMS_MFCNOTE (on CNOTE_NO → MFCNOTE_NO)
LEFT JOIN mfcnote_deduped mfc
    ON cn.CNOTE_NO = mfc.MFCNOTE_NO

-- 18. CMS_MANIFEST (via MFCNOTE_MAN_NO)
LEFT JOIN manifest_deduped man
    ON mfc.MFCNOTE_MAN_NO = man.MANIFEST_NO

-- 19. CMS_DBAG_HO (on CNOTE_NO)
LEFT JOIN dbag_ho_deduped dbag
    ON cn.CNOTE_NO = dbag.DBAG_CNOTE_NO

-- 20. CMS_DMBAG (via DBAG_NO)
LEFT JOIN dmbag_deduped dmbag
    ON dbag.DBAG_NO = dmbag.DMBAG_NO

-- 21. CMS_DHOV_RSHEET (on CNOTE_NO)
LEFT JOIN dhov_rsheet_deduped dhov
    ON cn.CNOTE_NO = dhov.DHOV_RSHEET_CNOTE

-- 22. CMS_RDSJ (via DHICNOTE_NO → RDSJ_HVI_NO)
LEFT JOIN rdsj_deduped rdsj
    ON dhicn.DHICNOTE_NO = rdsj.RDSJ_HVI_NO

-- 23. CMS_DSJ (via RDSJ_HVO_NO)
LEFT JOIN dsj_deduped dsj
    ON rdsj.RDSJ_HVO_NO = dsj.DSJ_HVO_NO

-- 24. CMS_MSJ (via DSJ_NO)
LEFT JOIN msj_deduped msj
    ON dsj.DSJ_NO = msj.MSJ_NO

-- 25. CMS_DSMU (via DSJ_NO)
LEFT JOIN dsmu_deduped dsmu
    ON dsj.DSJ_NO = dsmu.DSMU_NO

-- 26. CMS_MSMU (via DSJ_NO)
LEFT JOIN msmu_deduped msmu
    ON dsj.DSJ_NO = msmu.MSMU_NO

-- 27. CMS_DSTATUS (on CNOTE_NO)
LEFT JOIN dstatus_deduped dst
    ON cn.CNOTE_NO = CAST(dst.DSTATUS_CNOTE_NO AS VARCHAR(50))

-- 28. CMS_COST_DTRANSIT_AGEN (on CNOTE_NO)
LEFT JOIN cost_dtransit_deduped costd
    ON cn.CNOTE_NO = costd.CNOTE_NO

-- 29. CMS_COST_MTRANSIT_AGEN (via MFCNOTE_MAN_NO)
LEFT JOIN cost_mtransit_deduped costm
    ON mfc.MFCNOTE_MAN_NO = costm.MANIFEST_NO

-- 30. T_GOTO (1:1 on CNOTE_NO → AWB)
LEFT JOIN T_GOTO gt
    ON cn.CNOTE_NO = gt.AWB

-- 31. T_MDT_CITY_ORIGIN (via CNOTE_ORIGIN → CITY_CODE)
LEFT JOIN mdt_city_deduped mdt
    ON cn.CNOTE_ORIGIN = mdt.CITY_CODE

-- 32. LASTMILE_COURIER (via MRSHEET_COURIER_ID)
LEFT JOIN courier_deduped cour
    ON mrsht.MRSHEET_COURIER_ID = cour.COURIER_ID

-- 33. ORA_ZONE (via DRSHEET_PRA zone → ZONE_CODE)
LEFT JOIN ora_zone_deduped ora
    ON CAST(pra.DRSHEET_ZONE AS VARCHAR(50)) = ora.ZONE_CODE

-- 34. ORA_USER (via CNOTE_USER → USER_ID)
LEFT JOIN ora_user_deduped usr
    ON cn.CNOTE_USER = usr.USER_ID

;
