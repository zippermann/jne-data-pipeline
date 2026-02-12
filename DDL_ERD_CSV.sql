-- ============================================
-- JNE ERD Schema - CSV Data (10,000-row dataset)
-- Source: /data/raw/csv/ (33 CSV files)
-- Schema: erd
-- ============================================
-- This DDL defines the Entity-Relationship Diagram
-- for the 10,000-row CSV dataset. Unlike the previous
-- ERD (DDL_ERD_JNE.txt) which was built for the 500-row
-- Excel data in staging schema, this ERD:
--   - Uses the dedicated "erd" schema
--   - Is sourced from CSV files (not JNE_RAW_COMBINED.xlsx)
--   - Excludes Excel-only lookup tables (ORA_ZONE, ORA_USER, T_MDT_CITY_ORIGIN)
--   - Excludes T_CROSSDOCK_AWD (not present in CSV data)
-- ============================================

-- Create the ERD schema
CREATE SCHEMA IF NOT EXISTS erd AUTHORIZATION jne_user;

-- ============================================
-- PARENT TABLES (no foreign key dependencies)
-- These tables must be created first as they
-- are referenced by child tables via FKs.
-- ============================================

-- erd.cms_cost_mtransit_agen (2,146 rows, 15 columns)
-- Master transit cost/agent records per manifest
DROP TABLE IF EXISTS erd.cms_cost_mtransit_agen CASCADE;
CREATE TABLE erd.cms_cost_mtransit_agen (
	"MANIFEST_NO" text NOT NULL,
	"MANIFEST_DATE" float8 NULL,
	"BRANCH_ID" text NULL,
	"DESTINATION" text NULL,
	"CTC_WEIGHT" float8 NULL,
	"ACT_WEIGHT" float8 NULL,
	"MANIFEST_APPROVED" text NULL,
	"REMARK" text NULL,
	"MANIFEST_COST" float8 NULL,
	"REAL_COST" float8 NULL,
	"MANIFEST_UID" text NULL,
	"ESB_TIME" timestamp NULL,
	"ESB_ID" text NULL,
	"MANIFEST_TYPE" float8 NULL,
	"MANIFEST_DOC_REF" text NULL,
	CONSTRAINT cms_cost_mtransit_agen_pkey PRIMARY KEY ("MANIFEST_NO")
);


-- erd.cms_dmbag (25,789 rows, 11 columns)
-- Domestic bag records
DROP TABLE IF EXISTS erd.cms_dmbag CASCADE;
CREATE TABLE erd.cms_dmbag (
	"DMBAG_NO" text NULL,
	"DMBAG_BAG_NO" text NULL,
	"DMBAG_ORIGIN" text NULL,
	"DMBAG_DESTINATION" text NULL,
	"DMBAG_WEIGHT" float8 NULL,
	"DMBAG_SPS" float8 NULL,
	"DMBAG_INBOUND" text NULL,
	"DMBAG_TYPE" float8 NULL,
	"DMBAG_STATUS" float8 NULL,
	"ESB_TIME" timestamp NULL,
	"ESB_ID" text NULL
);


-- erd.cms_drourate (264,677 rows, 17 columns)
-- Route rate reference: transit times, costs, linehaul rates
DROP TABLE IF EXISTS erd.cms_drourate CASCADE;
CREATE TABLE erd.cms_drourate (
	"DROURATE_CODE" text NOT NULL,
	"DROURATE_SERVICE" text NOT NULL,
	"DROURATE_ZONE" text NULL,
	"DROURATE_TRANSIT" float8 NULL,
	"DROURATE_DELIVERY" float8 NULL,
	"DROURATE_LINEHAUL" float8 NULL,
	"DROURATE_ADDCOST" float8 NULL,
	"DROURATE_DELIVERY_NEXT" float8 NULL,
	"DROURATE_ACTIVE" text NULL,
	"DROURATE_WAREHOUSE" float8 NULL,
	"DROURATE_UID" text NULL,
	"DROURATE_UDATE" timestamp NULL,
	"DROURATE_ETD_FROM" float8 NULL,
	"DROURATE_ETD_THRU" float8 NULL,
	"DROURATE_TIME" text NULL,
	"DROURATE_LINEHAUL_NEXT" float8 NULL,
	"DROURATE_YES_LATE" time NULL,
	CONSTRAINT cms_drourate_pkey PRIMARY KEY ("DROURATE_CODE", "DROURATE_SERVICE")
);


-- erd.cms_dstatus (82 rows, 13 columns)
-- Shipment status change tracking
DROP TABLE IF EXISTS erd.cms_dstatus CASCADE;
CREATE TABLE erd.cms_dstatus (
	"DSTATUS_NO" text NULL,
	"DSTATUS_CNOTE_NO" int8 NOT NULL,
	"DSTATUS_STATUS" text NULL,
	"DSTATUS_REMARKS" text NULL,
	"CREATE_DATE" float8 NULL,
	"DSTATUS_STATUS_DATE" float8 NULL,
	"DSTATUS_MANIFEST_NO_OLD" float8 NULL,
	"DSTATUS_BAG_NO_OLD" float8 NULL,
	"DSTATUS_MANIFEST_NO_NEW" float8 NULL,
	"DSTATUS_MANIFEST_DEST" float8 NULL,
	"DSTATUS_MANIFEST_THRU" float8 NULL,
	"DSTATUS_BAG_NO_NEW" float8 NULL,
	"DSTATUS_ZONE_CODE" text NULL,
	CONSTRAINT cms_dstatus_pkey PRIMARY KEY ("DSTATUS_CNOTE_NO")
);


-- erd.cms_manifest (23,425 rows, 18 columns)
-- Master manifest records
DROP TABLE IF EXISTS erd.cms_manifest CASCADE;
CREATE TABLE erd.cms_manifest (
	"MANIFEST_NO" text NOT NULL,
	"MANIFEST_RECALL_NO" float8 NULL,
	"MANIFEST_DATE" float8 NULL,
	"MANIFEST_ROUTE" text NULL,
	"MANIFEST_FROM" text NULL,
	"MANIFEST_THRU" text NULL,
	"MANIFEST_NOTICE" text NULL,
	"MANIFEST_APPROVED" text NULL,
	"MANIFEST_ORIGIN" text NULL,
	"MANIFEST_CODE" int8 NULL,
	"MANIFEST_UID" text NULL,
	"MANIFEST_USER_AUDIT" float8 NULL,
	"MANIFEST_TIME_AUDIT" float8 NULL,
	"MANIFEST_FORM_AUDIT" float8 NULL,
	"MANIFEST_CRDATE" float8 NULL,
	"MANIFEST_CANCELED" text NULL,
	"MANIFEST_CANCELED_UID" float8 NULL,
	"MANIFEST_MODA" text NULL,
	CONSTRAINT cms_manifest_pkey PRIMARY KEY ("MANIFEST_NO")
);


-- erd.cms_mhi_hoc (4,581 rows, 12 columns)
-- Master handover-in (inbound handover header)
DROP TABLE IF EXISTS erd.cms_mhi_hoc CASCADE;
CREATE TABLE erd.cms_mhi_hoc (
	"MHI_NO" text NOT NULL,
	"MHI_REF_NO" int8 NULL,
	"MHI_DATE" float8 NULL,
	"MHI_UID" text NULL,
	"MHI_APPROVE" text NULL,
	"MHI_BRANCH" text NULL,
	"MHI_APPROVE_DATE" float8 NULL,
	"MHI_REMARKS" float8 NULL,
	"MHI_USER1" text NULL,
	"MHI_USER2" float8 NULL,
	"MHI_ZONE" float8 NULL,
	"MHI_COURIER" float8 NULL,
	CONSTRAINT cms_mhi_hoc_pkey PRIMARY KEY ("MHI_NO")
);


-- erd.cms_mhicnote (6,206 rows, 11 columns)
-- Master handover-in consignment note header
DROP TABLE IF EXISTS erd.cms_mhicnote CASCADE;
CREATE TABLE erd.cms_mhicnote (
	"MHICNOTE_BRANCH_ID" text NULL,
	"MHICNOTE_ZONE" text NULL,
	"MHICNOTE_NO" text NOT NULL,
	"MHICNOTE_REF_NO" text NULL,
	"MHICNOTE_DATE" float8 NULL,
	"MHICNOTE_ZONE_ORIG" text NULL,
	"MHICNOTE_USER_ID" text NULL,
	"MHICNOTE_USER1" float8 NULL,
	"MHICNOTE_USER2" float8 NULL,
	"MHICNOTE_SIGNDATE" int8 NULL,
	"MHICNOTE_APPROVE" text NULL,
	CONSTRAINT cms_mhicnote_pkey PRIMARY KEY ("MHICNOTE_NO")
);


-- erd.cms_mhocnote (6,221 rows, 20 columns)
-- Master handover-out consignment note header
DROP TABLE IF EXISTS erd.cms_mhocnote CASCADE;
CREATE TABLE erd.cms_mhocnote (
	"MHOCNOTE_BRANCH_ID" text NULL,
	"MHOCNOTE_ZONE" text NULL,
	"MHOCNOTE_NO" text NOT NULL,
	"MHOCNOTE_DATE" float8 NULL,
	"MHOCNOTE_ZONE_DEST" text NULL,
	"MHOCNOTE_USER_ID" text NULL,
	"MHOCNOTE_USER1" text NULL,
	"MHOCNOTE_USER2" text NULL,
	"MHOCNOTE_SIGNDATE" float8 NULL,
	"MHOCNOTE_APPROVE" text NULL,
	"MHOCNOTE_REMARKS" text NULL,
	"MHOCNOTE_ORIGIN" float8 NULL,
	"MHOCNOTE_COURIER_ID" float8 NULL,
	"MHOCNOTE_SERVICES" float8 NULL,
	"MHOCNOTE_PRODUCT" float8 NULL,
	"MHOCNOTE_CUST" float8 NULL,
	"MHOCNOTE_USER_SCO" float8 NULL,
	"MHOCNOTE_HVS" text NULL,
	"MHOCNOTE_APP_DATE" float8 NULL,
	"MHOCNOTE_TYPE" float8 NULL,
	CONSTRAINT cms_mhocnote_pkey PRIMARY KEY ("MHOCNOTE_NO")
);


-- erd.cms_mhoundel_pod (248 rows, 11 columns)
-- Master undelivered POD header
DROP TABLE IF EXISTS erd.cms_mhoundel_pod CASCADE;
CREATE TABLE erd.cms_mhoundel_pod (
	"MHOUNDEL_BRANCH_ID" text NULL,
	"MHOUNDEL_NO" text NOT NULL,
	"MHOUNDEL_REMARKS" text NULL,
	"MHOUNDEL_DATE" float8 NULL,
	"MHOUNDEL_USER_ID" text NULL,
	"MHOUNDEL_ZONE" text NULL,
	"MHOUNDEL_APPROVE" text NULL,
	"MHOUNDEL_USER1" text NULL,
	"MHOUNDEL_USER2" text NULL,
	"MHOUNDEL_SIGNDATE" int8 NULL,
	"MHOUNDEL_FLOW" text NULL,
	CONSTRAINT cms_mhoundel_pod_pk PRIMARY KEY ("MHOUNDEL_NO")
);


-- erd.cms_mrcnote (5,337 rows, 12 columns)
-- Master runsheet consignment note header
DROP TABLE IF EXISTS erd.cms_mrcnote CASCADE;
CREATE TABLE erd.cms_mrcnote (
	"MRCNOTE_NO" text NOT NULL,
	"MRCNOTE_DATE" float8 NULL,
	"MRCNOTE_BRANCH_ID" text NULL,
	"MRCNOTE_USER_ID" text NULL,
	"MRCNOTE_COURIER_ID" text NULL,
	"MRCNOTE_AE_ID" float8 NULL,
	"MRCNOTE_USER1" text NULL,
	"MRCNOTE_USER2" text NULL,
	"MRCNOTE_SIGNDATE" float8 NULL,
	"MRCNOTE_PAYMENT" int8 NULL,
	"MRCNOTE_REFNO" float8 NULL,
	"MRCNOTE_TYPE" float8 NULL,
	CONSTRAINT cms_mrcnote_pkey PRIMARY KEY ("MRCNOTE_NO")
);


-- erd.cms_msj (8,159 rows, 17 columns)
-- Master shipment journey header
DROP TABLE IF EXISTS erd.cms_msj CASCADE;
CREATE TABLE erd.cms_msj (
	"MSJ_NO" text NOT NULL,
	"MSJ_BRANCH_ID" text NULL,
	"MSJ_DATE" float8 NULL,
	"MSJ_USER1" text NULL,
	"MSJ_USER2" text NULL,
	"MSJ_APPROVE" text NULL,
	"MSJ_CDATE" float8 NULL,
	"MSJ_UID" text NULL,
	"MSJ_REMARKS" text NULL,
	"MSJ_SIGNDATE" float8 NULL,
	"MSJ_DEST" text NULL,
	"MSJ_ORIG" text NULL,
	"MSJ_COURIER_ID" float8 NULL,
	"MSJ_ARMADA" float8 NULL,
	"MSJ_CHAR1" float8 NULL,
	"MSJ_CHAR2" float8 NULL,
	"MSJ_CHAR3" float8 NULL,
	CONSTRAINT cms_msj_pkey PRIMARY KEY ("MSJ_NO")
);


-- erd.cms_msmu (39,546 rows, 26 columns)
-- Master shipment movement unit
DROP TABLE IF EXISTS erd.cms_msmu CASCADE;
CREATE TABLE erd.cms_msmu (
	"MSMU_NO" text NOT NULL,
	"MSMU_DATE" float8 NULL,
	"MSMU_ORIGIN" text NULL,
	"MSMU_DESTINATION" text NULL,
	"MSMU_FLIGHT_NO" text NULL,
	"MSMU_FLIGHT_DATE" float8 NULL,
	"MSMU_ETD" float8 NULL,
	"MSMU_ETA" float8 NULL,
	"MSMU_QTY" float8 NULL,
	"MSMU_WEIGHT" float8 NULL,
	"MSMU_USER" text NULL,
	"MSMU_FLAG" text NULL,
	"MSMU_REMARKS" text NULL,
	"MSMU_STATUS" text NULL,
	"MSMU_WRH_DATE" float8 NULL,
	"MSMU_WRH_TIME" text NULL,
	"MSMU_OFF_DATE" float8 NULL,
	"MSMU_OFF_TIME" text NULL,
	"MSMU_CONFIRM" text NULL,
	"MSMU_CANCEL" float8 NULL,
	"MSMU_USER_CANCEL" float8 NULL,
	"MSMU_REPLACE" float8 NULL,
	"MSMU_TYPE" float8 NULL,
	"MSMU_MODA" text NULL,
	"MSMU_POLICE_LICENSE_PLATE" text NULL,
	"MSMU_HOURS" float8 NULL,
	CONSTRAINT cms_msmu_pkey PRIMARY KEY ("MSMU_NO")
);


-- erd.cms_rdsj (8,160 rows, 6 columns)
-- Return domestic shipment journey detail
DROP TABLE IF EXISTS erd.cms_rdsj CASCADE;
CREATE TABLE erd.cms_rdsj (
	"RDSJ_NO" text NOT NULL,
	"RDSJ_BAG_NO" text NOT NULL,
	"RDSJ_HVO_NO" text NOT NULL,
	"RDSJ_UID" float8 NULL,
	"RDSJ_CDATE" float8 NULL,
	"RDSJ_HVI_NO" text NULL,
	CONSTRAINT cms_rdsj_pkey PRIMARY KEY ("RDSJ_NO", "RDSJ_BAG_NO", "RDSJ_HVO_NO")
);


-- erd.lastmile_courier (10,978 rows, 29 columns)
-- Last-mile courier reference data
DROP TABLE IF EXISTS erd.lastmile_courier CASCADE;
CREATE TABLE erd.lastmile_courier (
	"COURIER_ID" text NOT NULL,
	"COURIER_NAME" text NULL,
	"COURIER_PHONE" float8 NULL,
	"COURIER_EMAIL" text NULL,
	"COURIER_PASSWORD" text NULL,
	"COURIER_NIK" text NULL,
	"COURIER_REGIONAL" text NULL,
	"COURIER_BRANCH" text NULL,
	"COURIER_ZONE" text NULL,
	"COURIER_ORIGIN" text NULL,
	"COURIER_ACTIVE" int8 NULL,
	"COURIER_SP_VALUE" int8 NULL,
	"COURIER_INCENTIVE_GROUP" text NULL,
	"COURIER_ARMADA" text NULL,
	"COURIER_EMPLOYEE_STATUS" text NULL,
	"COURIER_CREATED_AT" float8 NULL,
	"COURIER_UPDATED_AT" float8 NULL,
	"COURIER_ROLE_ID" float8 NULL,
	"COURIER_LEVEL" float8 NULL,
	"COURIER_COMPANY_ID" float8 NULL,
	"COURIER_TYPE" int8 NULL,
	"PARENT_COURIER_ID" float8 NULL,
	"COURIER_CUST_ID" float8 NULL,
	"COURIER_VACANT1" float8 NULL,
	"COURIER_VACANT2" float8 NULL,
	"COURIER_VACANT3" float8 NULL,
	"COURIER_VACANT4" float8 NULL,
	"COURIER_VACANT5" float8 NULL,
	"COURIER_VACANT6" float8 NULL,
	CONSTRAINT lastmile_courier_pkey PRIMARY KEY ("COURIER_ID")
);


-- erd.t_goto (0 rows, 6 columns)
-- Crossdock goto/routing records
DROP TABLE IF EXISTS erd.t_goto CASCADE;
CREATE TABLE erd.t_goto (
	"AWB" text NOT NULL,
	"CROSSDOCK_ROLE" text NULL,
	"FIELD_CHAR1" float8 NULL,
	"FIELD_NUMBER1" float8 NULL,
	"FIELD_DATE1" float8 NULL,
	"CREATE_DATE" float8 NULL,
	CONSTRAINT t_goto_pkey PRIMARY KEY ("AWB")
);


-- ============================================
-- CHILD TABLES (with foreign key dependencies)
-- These tables reference the parent tables above.
-- ============================================

-- erd.cms_cnote (10,108 rows, 117 columns)
-- Core consignment note: the central shipment record
DROP TABLE IF EXISTS erd.cms_cnote CASCADE;
CREATE TABLE erd.cms_cnote (
	"CNOTE_NO" text NOT NULL,
	"CNOTE_DATE" float8 NULL,
	"CNOTE_BRANCH_ID" text NULL,
	"CNOTE_AE_ID" float8 NULL,
	"CNOTE_PICKUP_NO" float8 NULL,
	"CNOTE_SERVICES_CODE" text NULL,
	"CNOTE_POD_DATE" float8 NULL,
	"CNOTE_POD_RECEIVER" float8 NULL,
	"CNOTE_POD_CODE" float8 NULL,
	"CNOTE_CUST_NO" int8 NULL,
	"CNOTE_ROUTE_CODE" text NULL,
	"CNOTE_ORIGIN" text NULL,
	"CNOTE_DESTINATION" text NULL,
	"CNOTE_QTY" int8 NULL,
	"CNOTE_WEIGHT" float8 NULL,
	"CNOTE_DIM" float8 NULL,
	"CNOTE_SHIPPER_NAME" text NULL,
	"CNOTE_SHIPPER_ADDR1" text NULL,
	"CNOTE_SHIPPER_ADDR2" text NULL,
	"CNOTE_SHIPPER_ADDR3" text NULL,
	"CNOTE_SHIPPER_CITY" text NULL,
	"CNOTE_SHIPPER_ZIP" text NULL,
	"CNOTE_SHIPPER_REGION" text NULL,
	"CNOTE_SHIPPER_COUNTRY" text NULL,
	"CNOTE_SHIPPER_CONTACT" text NULL,
	"CNOTE_SHIPPER_PHONE" text NULL,
	"CNOTE_RECEIVER_NAME" text NULL,
	"CNOTE_RECEIVER_ADDR1" text NULL,
	"CNOTE_RECEIVER_ADDR2" text NULL,
	"CNOTE_RECEIVER_ADDR3" text NULL,
	"CNOTE_RECEIVER_CITY" text NULL,
	"CNOTE_RECEIVER_ZIP" float8 NULL,
	"CNOTE_RECEIVER_REGION" text NULL,
	"CNOTE_RECEIVER_COUNTRY" text NULL,
	"CNOTE_RECEIVER_CONTACT" text NULL,
	"CNOTE_RECEIVER_PHONE" text NULL,
	"CNOTE_DELIVERY_NAME" float8 NULL,
	"CNOTE_DELIVERY_ADDR1" float8 NULL,
	"CNOTE_DELIVERY_ADDR2" float8 NULL,
	"CNOTE_DELIVERY_ADDR3" float8 NULL,
	"CNOTE_DELIVERY_CITY" float8 NULL,
	"CNOTE_DELIVERY_ZIP" float8 NULL,
	"CNOTE_DELIVERY_REGION" float8 NULL,
	"CNOTE_DELIVERY_COUNTRY" float8 NULL,
	"CNOTE_DELIVERY_CONTACT" float8 NULL,
	"CNOTE_DELIVERY_PHONE" float8 NULL,
	"CNOTE_DELIVERY_TYPE" float8 NULL,
	"CNOTE_GOODS_TYPE" int8 NULL,
	"CNOTE_GOODS_DESCR" text NULL,
	"CNOTE_GOODS_VALUE" float8 NULL,
	"CNOTE_SPECIAL_INS" text NULL,
	"CNOTE_INSURANCE_ID" text NULL,
	"CNOTE_INSURANCE_VALUE" int8 NULL,
	"CNOTE_PAYMENT_TYPE" int8 NULL,
	"CNOTE_CURRENCY" text NULL,
	"CNOTE_AMOUNT" int8 NULL,
	"CNOTE_ADDITIONAL_FEE" float8 NULL,
	"CNOTE_NOTICE" float8 NULL,
	"CNOTE_COMMISION" float8 NULL,
	"CNOTE_PRINTED" text NULL,
	"CNOTE_MANIFESTED" text NULL,
	"CNOTE_INVOICED" float8 NULL,
	"CNOTE_CANCEL" float8 NULL,
	"CNOTE_HOLD" float8 NULL,
	"CNOTE_HOLD_REASON" float8 NULL,
	"CNOTE_USER" text NULL,
	"CNOTE_DELIVERED" float8 NULL,
	"CNOTE_INBOUND" float8 NULL,
	"CNOTE_HOLDIT" float8 NULL,
	"CNOTE_HANDLING" int8 NULL,
	"CNOTE_MGTFEE" float8 NULL,
	"CNOTE_QRC" float8 NULL,
	"CNOTE_QUICK" text NULL,
	"CNOTE_REFNO" text NULL,
	"CNOTE_VERIFIED" float8 NULL,
	"CNOTE_VDATE" float8 NULL,
	"CNOTE_VUSER" float8 NULL,
	"CNOTE_RDATE" float8 NULL,
	"CNOTE_RUSER" float8 NULL,
	"CNOTE_RECEIVED" float8 NULL,
	"CNOTE_LUID" float8 NULL,
	"CNOTE_LDATE" float8 NULL,
	"CNOTE_EDIT" float8 NULL,
	"CNOTE_BILL_STATUS" text NULL,
	"CNOTE_MANIFEST_NO" float8 NULL,
	"CNOTE_RUNSHEET_NO" float8 NULL,
	"CNOTE_DO" float8 NULL,
	"CNOTE_INSURANCE_NO" text NULL,
	"CNOTE_OTHER_FEE" float8 NULL,
	"CNOTE_CURC_PAYMENT" int8 NULL,
	"CNOTE_BANK" float8 NULL,
	"CNOTE_CURC_RATE" int8 NULL,
	"CNOTE_PAYMENT_BY" text NULL,
	"CNOTE_AMOUNT_PAYMENT" int8 NULL,
	"CNOTE_TRANS" float8 NULL,
	"CNOTE_EUSER" float8 NULL,
	"CNOTE_ACT_WEIGHT" float8 NULL,
	"CNOTE_BILNOTE" text NULL,
	"CNOTE_CRDATE" float8 NULL,
	"CNOTE_SHIPPER_ADDR4" float8 NULL,
	"CNOTE_RECEIVER_ADDR4" float8 NULL,
	"CNOTE_PACKING" float8 NULL,
	"CNOTE_SMS" float8 NULL,
	"CNOTE_CARD_NO" float8 NULL,
	"CNOTE_CARD_AMOUNT" float8 NULL,
	"CNOTE_CARD_DISC" float8 NULL,
	"CNOTE_ECNOTE" text NULL,
	"STATUS_1" float8 NULL,
	"STATUS" text NULL,
	"CNOTE_CTC" float8 NULL,
	"CNOTE_YES_CANCEL" float8 NULL,
	"CNOTE_VAT" text NULL,
	"CNOTE_VAT_AMOUNT" float8 NULL,
	"CNOTE_SES_FRM" text NULL,
	"CNOTE_TYPE_CUST" float8 NULL,
	"CNOTE_PROTECT_ID" float8 NULL,
	"CNOTE_ZIP_SEQ" float8 NULL,
	CONSTRAINT cms_cnote_pkey PRIMARY KEY ("CNOTE_NO"),
	CONSTRAINT fk_cnote_drourate FOREIGN KEY ("CNOTE_ROUTE_CODE","CNOTE_SERVICES_CODE") REFERENCES erd.cms_drourate("DROURATE_CODE","DROURATE_SERVICE")
);


-- erd.cms_cnote_amo (1,357 rows, 13 columns)
-- Consignment note additional/AMO fields
DROP TABLE IF EXISTS erd.cms_cnote_amo CASCADE;
CREATE TABLE erd.cms_cnote_amo (
	"CNOTE_NO" text NOT NULL,
	"CNOTE_BRANCH_ID" text NULL,
	"FIELDCHAR1" text NULL,
	"FIELDCHAR2" float8 NULL,
	"FIELDCHAR3" float8 NULL,
	"FIELDCHAR4" float8 NULL,
	"FIELDCHAR5" float8 NULL,
	"FIELDNUM1" float8 NULL,
	"FIELDNUM2" float8 NULL,
	"FIELDNUM3" float8 NULL,
	"FIELDNUM4" float8 NULL,
	"FIELDNUM5" float8 NULL,
	"CDATE" float8 NULL,
	CONSTRAINT cms_cnote_amo_pkey PRIMARY KEY ("CNOTE_NO"),
	CONSTRAINT fk_cnote_amo_cnote FOREIGN KEY ("CNOTE_NO") REFERENCES erd.cms_cnote("CNOTE_NO")
);


-- erd.cms_cnote_pod (9,887 rows, 7 columns)
-- Proof of delivery records
DROP TABLE IF EXISTS erd.cms_cnote_pod CASCADE;
CREATE TABLE erd.cms_cnote_pod (
	"CNOTE_POD_NO" text NOT NULL,
	"CNOTE_POD_DATE" float8 NULL,
	"CNOTE_POD_RECEIVER" text NULL,
	"CNOTE_POD_STATUS" text NULL,
	"CNOTE_POD_DELIVERED" text NULL,
	"CNOTE_POD_DOC_NO" text NULL,
	"CNOTE_POD_CREATION_DATE" float8 NULL,
	CONSTRAINT cms_cnote_pod_pkey PRIMARY KEY ("CNOTE_POD_NO"),
	CONSTRAINT fk_cnote_pod_cnote FOREIGN KEY ("CNOTE_POD_NO") REFERENCES erd.cms_cnote("CNOTE_NO")
);


-- erd.cms_cost_dtransit_agen (2,146 rows, 11 columns)
-- Detail transit cost per consignment note per manifest
DROP TABLE IF EXISTS erd.cms_cost_dtransit_agen CASCADE;
CREATE TABLE erd.cms_cost_dtransit_agen (
	"DMANIFEST_NO" text NOT NULL,
	"CNOTE_NO" text NOT NULL,
	"CNOTE_ORIGIN" text NULL,
	"CNOTE_DESTINATION" text NULL,
	"CNOTE_QTY" int8 NULL,
	"CNOTE_WEIGHT" float8 NULL,
	"CNOTE_COST" float8 NULL,
	"CNOTE_SERVICES_CODE" text NULL,
	"ESB_TIME" timestamp NULL,
	"ESB_ID" text NULL,
	"DMANIFEST_DOC_REF" text NULL,
	CONSTRAINT cms_cost_dtransit_agen_pkey PRIMARY KEY ("DMANIFEST_NO", "CNOTE_NO"),
	CONSTRAINT fk_cost_d_cnote FOREIGN KEY ("CNOTE_NO") REFERENCES erd.cms_cnote("CNOTE_NO"),
	CONSTRAINT fk_cost_d_cost_m FOREIGN KEY ("DMANIFEST_NO") REFERENCES erd.cms_cost_mtransit_agen("MANIFEST_NO")
);


-- erd.cms_dbag_ho (6,223 rows, 10 columns)
-- Domestic bag handover detail per consignment note
DROP TABLE IF EXISTS erd.cms_dbag_ho CASCADE;
CREATE TABLE erd.cms_dbag_ho (
	"DBAG_HO_NO" text NULL,
	"DBAG_NO" text NOT NULL,
	"DBAG_CNOTE_NO" text NOT NULL,
	"DBAG_CNOTE_QTY" float8 NULL,
	"DBAG_CNOTE_WEIGHT" float8 NULL,
	"DBAG_CNOTE_DESTINATION" text NULL,
	"CDATE" float8 NULL,
	"DBAG_CNOTE_SERVICE" text NULL,
	"DBAG_CNOTE_DATE" float8 NULL,
	"DBAG_ZONE_DEST" text NULL,
	CONSTRAINT cms_dbag_ho_pkey PRIMARY KEY ("DBAG_NO", "DBAG_CNOTE_NO"),
	CONSTRAINT fk_dbag_ho_cnote FOREIGN KEY ("DBAG_CNOTE_NO") REFERENCES erd.cms_cnote("CNOTE_NO")
);


-- erd.cms_dhi_hoc (4,581 rows, 9 columns)
-- Detail handover-in consignment records
DROP TABLE IF EXISTS erd.cms_dhi_hoc CASCADE;
CREATE TABLE erd.cms_dhi_hoc (
	"DHI_NO" text NOT NULL,
	"DHI_SEQ_NO" int8 NOT NULL,
	"DHI_ONO" text NULL,
	"DHI_SEQ_ONO" int8 NULL,
	"DHI_CNOTE_NO" text NULL,
	"DHI_CNOTE_QTY" int8 NULL,
	"CDATE" float8 NULL,
	"DHI_REMARKS" float8 NULL,
	"DHI_DO" text NULL,
	CONSTRAINT cms_dhi_hoc_pkey PRIMARY KEY ("DHI_NO", "DHI_SEQ_NO"),
	CONSTRAINT fk_dhi_hoc_mhi FOREIGN KEY ("DHI_NO") REFERENCES erd.cms_mhi_hoc("MHI_NO")
);


-- erd.cms_dhicnote (6,206 rows, 6 columns)
-- Detail handover-in consignment note line items
DROP TABLE IF EXISTS erd.cms_dhicnote CASCADE;
CREATE TABLE erd.cms_dhicnote (
	"DHICNOTE_NO" text NOT NULL,
	"DHICNOTE_SEQ_NO" int8 NOT NULL,
	"DHICNOTE_CNOTE_NO" text NULL,
	"DHICNOTE_QTY" int8 NULL,
	"DHICNOTE_REMARKS" float8 NULL,
	"DHICNOTE_TDATE" float8 NULL,
	CONSTRAINT cms_dhicnote_pkey PRIMARY KEY ("DHICNOTE_NO", "DHICNOTE_SEQ_NO"),
	CONSTRAINT fk_dhicnote_cnote FOREIGN KEY ("DHICNOTE_CNOTE_NO") REFERENCES erd.cms_cnote("CNOTE_NO"),
	CONSTRAINT fk_dhicnote_mhicnote FOREIGN KEY ("DHICNOTE_NO") REFERENCES erd.cms_mhicnote("MHICNOTE_NO")
);


-- erd.cms_dhocnote (6,221 rows, 12 columns)
-- Detail handover-out consignment note line items
DROP TABLE IF EXISTS erd.cms_dhocnote CASCADE;
CREATE TABLE erd.cms_dhocnote (
	"DHOCNOTE_NO" text NOT NULL,
	"DHOCNOTE_SEQ_NO" int8 NOT NULL,
	"DHOCNOTE_CNOTE_NO" text NULL,
	"DHOCNOTE_QTY" int8 NULL,
	"DHOCNOTE_REMARKS" float8 NULL,
	"DHOCNOTE_TDATE" float8 NULL,
	"DHOCNOTE_INBOUND" float8 NULL,
	"DHOCNOTE_WEIGHT" float8 NULL,
	"DHOCNOTE_SERVICE" float8 NULL,
	"DHOCNOTE_DEST" float8 NULL,
	"DHOCNOTE_GOODS" float8 NULL,
	"DHOCNOTE_HANDLING" float8 NULL,
	CONSTRAINT cms_dhocnote_pkey PRIMARY KEY ("DHOCNOTE_NO", "DHOCNOTE_SEQ_NO"),
	CONSTRAINT fk_dhocnote_cnote FOREIGN KEY ("DHOCNOTE_CNOTE_NO") REFERENCES erd.cms_cnote("CNOTE_NO"),
	CONSTRAINT fk_dhocnote_mhocnote FOREIGN KEY ("DHOCNOTE_NO") REFERENCES erd.cms_mhocnote("MHOCNOTE_NO")
);


-- erd.cms_dhoundel_pod (248 rows, 11 columns)
-- Detail undelivered POD line items
DROP TABLE IF EXISTS erd.cms_dhoundel_pod CASCADE;
CREATE TABLE erd.cms_dhoundel_pod (
	"DHOUNDEL_NO" text NULL,
	"DHOUNDEL_SEQ_NO" int8 NULL,
	"DHOUNDEL_CNOTE_NO" text NULL,
	"DHOUNDEL_QTY" int8 NULL,
	"DHOUNDEL_REMARKS" text NULL,
	"DHOUNDEL_WEIGHT" float8 NULL,
	"DHOUNDEL_SERVICE" float8 NULL,
	"DHOUNDEL_DEST" float8 NULL,
	"DHOUNDEL_GOODS" float8 NULL,
	"DHOUNDEL_HRS" text NULL,
	"CREATE_DATE" float8 NULL,
	CONSTRAINT fk_dhoundel_cnote FOREIGN KEY ("DHOUNDEL_CNOTE_NO") REFERENCES erd.cms_cnote("CNOTE_NO"),
	CONSTRAINT fk_dhoundel_mhoundel FOREIGN KEY ("DHOUNDEL_NO") REFERENCES erd.cms_mhoundel_pod("MHOUNDEL_NO")
);


-- erd.cms_drcnote (5,337 rows, 9 columns)
-- Detail runsheet consignment note line items
DROP TABLE IF EXISTS erd.cms_drcnote CASCADE;
CREATE TABLE erd.cms_drcnote (
	"DRCNOTE_NO" text NULL,
	"DRCNOTE_CNOTE_NO" text NOT NULL,
	"DRCNOTE_QTY" int8 NULL,
	"DRCNOTE_REMARKS" text NULL,
	"DRCNOTE_TDATE" float8 NULL,
	"DRCNOTE_TUSER" text NULL,
	"DRCNOTE_FLAG" float8 NULL,
	"DRCNOTE_PAYMENT" int8 NULL,
	"DRCNOTE_DO" text NULL,
	CONSTRAINT cms_drcnote_pkey PRIMARY KEY ("DRCNOTE_CNOTE_NO"),
	CONSTRAINT fk_drcnote_cnote FOREIGN KEY ("DRCNOTE_CNOTE_NO") REFERENCES erd.cms_cnote("CNOTE_NO"),
	CONSTRAINT fk_drcnote_mrcnote FOREIGN KEY ("DRCNOTE_NO") REFERENCES erd.cms_mrcnote("MRCNOTE_NO")
);


-- erd.cms_drsheet_pra (4,847 rows, 10 columns)
-- Delivery runsheet PRA (pre-alert) records
DROP TABLE IF EXISTS erd.cms_drsheet_pra CASCADE;
CREATE TABLE erd.cms_drsheet_pra (
	"DRSHEET_NO" text NULL,
	"DRSHEET_CNOTE_NO" text NOT NULL,
	"DRSHEET_DATE" float8 NULL,
	"DRSHEET_STATUS" float8 NULL,
	"DRSHEET_RECEIVER" float8 NULL,
	"DRSHEET_FLAG" float8 NULL,
	"DRSHEET_UID" float8 NULL,
	"DRSHEET_UDATE" float8 NULL,
	"CREATION_DATE" float8 NULL,
	"DRSHEET_ZONE" float8 NULL,
	CONSTRAINT cms_drsheet_pra_pkey PRIMARY KEY ("DRSHEET_CNOTE_NO"),
	CONSTRAINT fk_drsheet_pra_cnote FOREIGN KEY ("DRSHEET_CNOTE_NO") REFERENCES erd.cms_cnote("CNOTE_NO")
);


-- erd.cms_dsj (8,159 rows, 5 columns)
-- Domestic shipment journey detail (bag-level)
DROP TABLE IF EXISTS erd.cms_dsj CASCADE;
CREATE TABLE erd.cms_dsj (
	"DSJ_NO" text NOT NULL,
	"DSJ_BAG_NO" text NOT NULL,
	"DSJ_HVO_NO" text NOT NULL,
	"DSJ_UID" text NULL,
	"DSJ_CDATE" float8 NULL,
	CONSTRAINT cms_dsj_pkey PRIMARY KEY ("DSJ_NO", "DSJ_BAG_NO", "DSJ_HVO_NO"),
	CONSTRAINT fk_dsj_msj FOREIGN KEY ("DSJ_NO") REFERENCES erd.cms_msj("MSJ_NO")
);


-- erd.cms_dsmu (39,369 rows, 14 columns)
-- Domestic shipment movement unit detail
DROP TABLE IF EXISTS erd.cms_dsmu CASCADE;
CREATE TABLE erd.cms_dsmu (
	"DSMU_NO" text NOT NULL,
	"DSMU_FLIGHT_NO" text NULL,
	"DSMU_FLIGHT_DATE" float8 NULL,
	"DSMU_BAG_NO" text NOT NULL,
	"DSMU_WEIGHT" float8 NULL,
	"DSMU_BAG_ORIGIN" text NULL,
	"DSMU_BAG_DESTINATION" text NULL,
	"DSMU_BAG_CANCEL" float8 NULL,
	"DSMU_SPS" float8 NULL,
	"DSMU_INBOUND" text NULL,
	"ESB_TIME" timestamp NULL,
	"ESB_ID" text NULL,
	"DSMU_MANIFEST_NO" float8 NULL,
	"DSMU_POLICE_LICENSE_PLATE" text NULL,
	CONSTRAINT cms_dsmu_pkey PRIMARY KEY ("DSMU_NO", "DSMU_BAG_NO"),
	CONSTRAINT fk_dsmu_msmu FOREIGN KEY ("DSMU_NO") REFERENCES erd.cms_msmu("MSMU_NO")
);


-- erd.cms_mfcnote (23,425 rows, 20 columns)
-- Manifest-to-consignment-note mapping
DROP TABLE IF EXISTS erd.cms_mfcnote CASCADE;
CREATE TABLE erd.cms_mfcnote (
	"MFCNOTE_MAN_NO" text NOT NULL,
	"MFCNOTE_BAG_NO" text NULL,
	"MFCNOTE_NO" text NOT NULL,
	"MFCNOTE_WEIGHT" float8 NULL,
	"MFCNOTE_COST" int8 NULL,
	"MFCNOTE_MGTFEE" float8 NULL,
	"MFCNOTE_TRANSIT" float8 NULL,
	"MFCNOTE_DELIVERY" float8 NULL,
	"MFCNOTE_LINEHAUL" float8 NULL,
	"MFCNOTE_ADDCOST" float8 NULL,
	"MFCNOTE_FLAG" text NULL,
	"MFCNOTE_HANDLING" float8 NULL,
	"MFCNOTE_DESCRIPTION" float8 NULL,
	"MFCNOTE_USER_AUDIT" float8 NULL,
	"MFCNOTE_TIME_AUDIT" float8 NULL,
	"MFCNOTE_FORM_AUDIT" float8 NULL,
	"MFCNOTE_MAN_CODE" float8 NULL,
	"MFCNOTE_MAN_DATE" float8 NULL,
	"MFCNOTE_CRDATE" float8 NULL,
	"MFCNOTE_ORIGIN" text NULL,
	CONSTRAINT cms_mfcnote_pkey PRIMARY KEY ("MFCNOTE_MAN_NO", "MFCNOTE_NO"),
	CONSTRAINT fk_mfcnote_cnote FOREIGN KEY ("MFCNOTE_NO") REFERENCES erd.cms_cnote("CNOTE_NO"),
	CONSTRAINT fk_mfcnote_manifest FOREIGN KEY ("MFCNOTE_MAN_NO") REFERENCES erd.cms_manifest("MANIFEST_NO")
);


-- erd.cms_mrsheet (10,978 rows, 13 columns)
-- Master runsheet records (linked to couriers)
DROP TABLE IF EXISTS erd.cms_mrsheet CASCADE;
CREATE TABLE erd.cms_mrsheet (
	"MRSHEET_BRANCH" text NULL,
	"MRSHEET_NO" text NOT NULL,
	"MRSHEET_DATE" float8 NULL,
	"MRSHEET_COURIER_ID" text NULL,
	"MRSHEET_UID" text NULL,
	"MRSHEET_UDATE" float8 NULL,
	"MRSHEET_APPROVED_DR" text NULL,
	"MRSHEET_UID_DR" text NULL,
	"MRSHEET_UDATE_DR" float8 NULL,
	"MRSHEET_INB_APP_DR" float8 NULL,
	"MRSHEET_INB_UID_DR" text NULL,
	"MRSHEET_INB_UDATE_DR" float8 NULL,
	"MRSHEET_ORIGIN" float8 NULL,
	CONSTRAINT cms_mrsheet_pkey PRIMARY KEY ("MRSHEET_NO"),
	CONSTRAINT fk_mrsheet_courier FOREIGN KEY ("MRSHEET_COURIER_ID") REFERENCES erd.lastmile_courier("COURIER_ID")
);


-- erd.cms_apicust (4,389 rows, 44 columns)
-- API customer orders
DROP TABLE IF EXISTS erd.cms_apicust CASCADE;
CREATE TABLE erd.cms_apicust (
	"APICUST_ORDER_ID" text NOT NULL,
	"APICUST_CNOTE_NO" text NULL,
	"APICUST_ORIGIN" text NULL,
	"APICUST_BRANCH" text NULL,
	"APICUST_CUST_NO" int8 NULL,
	"APICUST_SERVICES_CODE" text NULL,
	"APICUST_DESTINATION" text NULL,
	"APICUST_SHIPPER_NAME" text NULL,
	"APICUST_SHIPPER_ADDR1" text NULL,
	"APICUST_SHIPPER_ADDR2" text NULL,
	"APICUST_SHIPPER_ADDR3" text NULL,
	"APICUST_SHIPPER_CITY" text NULL,
	"APICUST_SHIPPER_ZIP" text NULL,
	"APICUST_SHIPPER_REGION" text NULL,
	"APICUST_SHIPPER_COUNTRY" text NULL,
	"APICUST_SHIPPER_CONTACT" text NULL,
	"APICUST_SHIPPER_PHONE" text NULL,
	"APICUST_RECEIVER_NAME" text NULL,
	"APICUST_RECEIVER_ADDR1" text NULL,
	"APICUST_RECEIVER_ADDR2" text NULL,
	"APICUST_RECEIVER_ADDR3" text NULL,
	"APICUST_RECEIVER_CITY" text NULL,
	"APICUST_RECEIVER_ZIP" float8 NULL,
	"APICUST_RECEIVER_REGION" text NULL,
	"APICUST_RECEIVER_COUNTRY" text NULL,
	"APICUST_RECEIVER_CONTACT" text NULL,
	"APICUST_RECEIVER_PHONE" text NULL,
	"APICUST_QTY" int8 NULL,
	"APICUST_WEIGHT" float8 NULL,
	"APICUST_GOODS_DESCR" text NULL,
	"APICUST_GOODS_VALUE" float8 NULL,
	"APICUST_SPECIAL_INS" text NULL,
	"APICUST_INS_FLAG" text NULL,
	"APICUST_COD_FLAG" text NULL,
	"APICUST_COD_AMOUNT" float8 NULL,
	"CREATE_DATE" float8 NULL,
	"APICUST_REQID" float8 NULL,
	"APICUST_FLAG" text NULL,
	"APICUST_LATITUDE" float8 NULL,
	"APICUST_LONGITUDE" float8 NULL,
	"APICUST_SHIPMENT_TYPE" text NULL,
	"APICUST_MERCHAN_ID" text NULL,
	"APICUST_NAME" text NULL,
	"SHIPPER_PROVIDER" text NULL,
	CONSTRAINT cms_apicust_pkey PRIMARY KEY ("APICUST_ORDER_ID"),
	CONSTRAINT fk_apicust_cnote FOREIGN KEY ("APICUST_CNOTE_NO") REFERENCES erd.cms_cnote("CNOTE_NO")
);


-- erd.cms_dhov_rsheet (9,661 rows, 23 columns)
-- Delivery handover runsheet detail
DROP TABLE IF EXISTS erd.cms_dhov_rsheet CASCADE;
CREATE TABLE erd.cms_dhov_rsheet (
	"DHOV_RSHEET_NO" text NOT NULL,
	"DHOV_RSHEET_CNOTE" text NOT NULL,
	"DHOV_RSHEET_DO" float8 NULL,
	"DHOV_RSHEET_COD" float8 NULL,
	"DHOV_RSHEET_UNDEL" float8 NULL,
	"DHOV_RSHEET_REDEL" float8 NULL,
	"DHOV_RSHEET_OUTS" float8 NULL,
	"DHOV_RSHEET_QTY" float8 NULL,
	"CREATE_DATE" float8 NULL,
	"DHOV_RSHEET_UZONE" text NULL,
	"DHOV_RSHEET_CYCLE" text NULL,
	"DCSUNDEL_RSHEET_RSHEETNO" float8 NULL,
	"DHOV_RSHEET_RSHEETNO" text NULL,
	"DHOV_DRSHEET_EPAY_VEND" float8 NULL,
	"DHOV_DRSHEET_EPAY_TRXID" text NULL,
	"DHOV_DRSHEET_EPAY_AMOUNT" float8 NULL,
	"DHOV_DRSHEET_EPAY_DEVICE" text NULL,
	"DHOV_DRSHEET_DATE" float8 NULL,
	"DHOV_DRSHEET_STATUS" text NULL,
	"DHOV_DRSHEET_RECEIVER" float8 NULL,
	"DHOV_DRSHEET_FLAG" float8 NULL,
	"DHOV_DRSHEET_UID" float8 NULL,
	"DHOV_DRSHEET_UDATE" float8 NULL,
	CONSTRAINT cms_dhov_rsheet_pkey PRIMARY KEY ("DHOV_RSHEET_NO", "DHOV_RSHEET_CNOTE"),
	CONSTRAINT fk_dhov_rsheet_cnote FOREIGN KEY ("DHOV_RSHEET_CNOTE") REFERENCES erd.cms_cnote("CNOTE_NO"),
	CONSTRAINT fk_dhov_rsheet_mrsheet FOREIGN KEY ("DHOV_RSHEET_RSHEETNO") REFERENCES erd.cms_mrsheet("MRSHEET_NO")
);


-- erd.cms_drsheet (10,978 rows, 9 columns)
-- Delivery runsheet detail per consignment note
DROP TABLE IF EXISTS erd.cms_drsheet CASCADE;
CREATE TABLE erd.cms_drsheet (
	"DRSHEET_NO" text NOT NULL,
	"DRSHEET_CNOTE_NO" text NOT NULL,
	"DRSHEET_DATE" float8 NULL,
	"DRSHEET_STATUS" text NULL,
	"DRSHEET_RECEIVER" text NULL,
	"DRSHEET_FLAG" text NULL,
	"DRSHEET_UID" text NULL,
	"DRSHEET_UDATE" float8 NULL,
	"CREATION_DATE" float8 NULL,
	CONSTRAINT cms_drsheet_pkey PRIMARY KEY ("DRSHEET_NO", "DRSHEET_CNOTE_NO"),
	CONSTRAINT fk_drsheet_cnote FOREIGN KEY ("DRSHEET_CNOTE_NO") REFERENCES erd.cms_cnote("CNOTE_NO"),
	CONSTRAINT fk_drsheet_mrsheet FOREIGN KEY ("DRSHEET_NO") REFERENCES erd.cms_mrsheet("MRSHEET_NO")
);


-- ============================================
-- Indexes for query performance
-- ============================================
CREATE INDEX IF NOT EXISTS idx_erd_cnote_no ON erd.cms_cnote("CNOTE_NO");
CREATE INDEX IF NOT EXISTS idx_erd_cnote_date ON erd.cms_cnote("CNOTE_DATE");
CREATE INDEX IF NOT EXISTS idx_erd_cnote_branch ON erd.cms_cnote("CNOTE_BRANCH_ID");
CREATE INDEX IF NOT EXISTS idx_erd_cnote_origin ON erd.cms_cnote("CNOTE_ORIGIN");
CREATE INDEX IF NOT EXISTS idx_erd_cnote_destination ON erd.cms_cnote("CNOTE_DESTINATION");
CREATE INDEX IF NOT EXISTS idx_erd_manifest_no ON erd.cms_manifest("MANIFEST_NO");
CREATE INDEX IF NOT EXISTS idx_erd_mfcnote_no ON erd.cms_mfcnote("MFCNOTE_NO");
CREATE INDEX IF NOT EXISTS idx_erd_apicust_cnote ON erd.cms_apicust("APICUST_CNOTE_NO");
CREATE INDEX IF NOT EXISTS idx_erd_drsheet_cnote ON erd.cms_drsheet("DRSHEET_CNOTE_NO");
CREATE INDEX IF NOT EXISTS idx_erd_dhov_cnote ON erd.cms_dhov_rsheet("DHOV_RSHEET_CNOTE");
CREATE INDEX IF NOT EXISTS idx_erd_courier_id ON erd.lastmile_courier("COURIER_ID");

-- ============================================
-- Grant permissions
-- ============================================
GRANT ALL PRIVILEGES ON SCHEMA erd TO jne_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA erd TO jne_user;
