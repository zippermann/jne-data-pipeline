-- ============================================
-- JNE Dashboard Database Schema
-- Auto-generated from JNE_RAW_COMBINED.xlsx
-- Tables: 36 | Total Columns: ~601
-- ============================================

-- Create schemas for data organization
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- Table: CMS_APICUST (93 rows, 44 columns)
DROP TABLE IF EXISTS raw.cms_apicust CASCADE;
CREATE TABLE raw.cms_apicust (
    id SERIAL PRIMARY KEY,
    apicust_order_id VARCHAR(255),
    apicust_cnote_no VARCHAR(255),
    apicust_origin VARCHAR(255),
    apicust_branch VARCHAR(255),
    apicust_cust_no BIGINT,
    apicust_services_code VARCHAR(255),
    apicust_destination VARCHAR(255),
    apicust_shipper_name VARCHAR(255),
    apicust_shipper_addr1 VARCHAR(255),
    apicust_shipper_addr2 VARCHAR(255),
    apicust_shipper_addr3 VARCHAR(255),
    apicust_shipper_city VARCHAR(255),
    apicust_shipper_zip VARCHAR(255),
    apicust_shipper_region VARCHAR(255),
    apicust_shipper_country VARCHAR(255),
    apicust_shipper_contact VARCHAR(255),
    apicust_shipper_phone VARCHAR(255),
    apicust_receiver_name VARCHAR(255),
    apicust_receiver_addr1 VARCHAR(255),
    apicust_receiver_addr2 VARCHAR(255),
    apicust_receiver_addr3 VARCHAR(255),
    apicust_receiver_city VARCHAR(255),
    apicust_receiver_zip DOUBLE PRECISION,
    apicust_receiver_region VARCHAR(255),
    apicust_receiver_country VARCHAR(255),
    apicust_receiver_contact VARCHAR(255),
    apicust_receiver_phone VARCHAR(255),
    apicust_qty BIGINT,
    apicust_weight DOUBLE PRECISION,
    apicust_goods_descr VARCHAR(255),
    apicust_goods_value DOUBLE PRECISION,
    apicust_special_ins VARCHAR(255),
    apicust_ins_flag VARCHAR(255),
    apicust_cod_flag VARCHAR(255),
    apicust_cod_amount DOUBLE PRECISION,
    create_date DOUBLE PRECISION,
    apicust_reqid DOUBLE PRECISION,
    apicust_flag VARCHAR(255),
    apicust_latitude DOUBLE PRECISION,
    apicust_longitude DOUBLE PRECISION,
    apicust_shipment_type VARCHAR(255),
    apicust_merchan_id VARCHAR(255),
    apicust_name VARCHAR(255),
    shipper_provider VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_CNOTE_AMO (9 rows, 13 columns)
DROP TABLE IF EXISTS raw.cms_cnote_amo CASCADE;
CREATE TABLE raw.cms_cnote_amo (
    id SERIAL PRIMARY KEY,
    cnote_no VARCHAR(255),
    cnote_branch_id VARCHAR(255),
    fieldchar1 VARCHAR(255),
    fieldchar2 DOUBLE PRECISION,
    fieldchar3 DOUBLE PRECISION,
    fieldchar4 DOUBLE PRECISION,
    fieldchar5 DOUBLE PRECISION,
    fieldnum1 DOUBLE PRECISION,
    fieldnum2 DOUBLE PRECISION,
    fieldnum3 DOUBLE PRECISION,
    fieldnum4 DOUBLE PRECISION,
    fieldnum5 DOUBLE PRECISION,
    cdate DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_CNOTE_POD (100 rows, 7 columns)
DROP TABLE IF EXISTS raw.cms_cnote_pod CASCADE;
CREATE TABLE raw.cms_cnote_pod (
    id SERIAL PRIMARY KEY,
    cnote_pod_no VARCHAR(255),
    cnote_pod_date DOUBLE PRECISION,
    cnote_pod_receiver VARCHAR(255),
    cnote_pod_status VARCHAR(255),
    cnote_pod_delivered VARCHAR(255),
    cnote_pod_doc_no VARCHAR(255),
    cnote_pod_creation_date DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_CNOTE (100 rows, 117 columns)
DROP TABLE IF EXISTS raw.cms_cnote CASCADE;
CREATE TABLE raw.cms_cnote (
    id SERIAL PRIMARY KEY,
    cnote_no VARCHAR(255),
    cnote_date DOUBLE PRECISION,
    cnote_branch_id VARCHAR(255),
    cnote_ae_id DOUBLE PRECISION,
    cnote_pickup_no DOUBLE PRECISION,
    cnote_services_code VARCHAR(255),
    cnote_pod_date DOUBLE PRECISION,
    cnote_pod_receiver DOUBLE PRECISION,
    cnote_pod_code DOUBLE PRECISION,
    cnote_cust_no BIGINT,
    cnote_route_code VARCHAR(255),
    cnote_origin VARCHAR(255),
    cnote_destination VARCHAR(255),
    cnote_qty BIGINT,
    cnote_weight DOUBLE PRECISION,
    cnote_dim DOUBLE PRECISION,
    cnote_shipper_name VARCHAR(255),
    cnote_shipper_addr1 VARCHAR(255),
    cnote_shipper_addr2 VARCHAR(255),
    cnote_shipper_addr3 VARCHAR(255),
    cnote_shipper_city VARCHAR(255),
    cnote_shipper_zip VARCHAR(255),
    cnote_shipper_region VARCHAR(255),
    cnote_shipper_country VARCHAR(255),
    cnote_shipper_contact VARCHAR(255),
    cnote_shipper_phone VARCHAR(255),
    cnote_receiver_name VARCHAR(255),
    cnote_receiver_addr1 VARCHAR(255),
    cnote_receiver_addr2 VARCHAR(255),
    cnote_receiver_addr3 VARCHAR(255),
    cnote_receiver_city VARCHAR(255),
    cnote_receiver_zip DOUBLE PRECISION,
    cnote_receiver_region VARCHAR(255),
    cnote_receiver_country VARCHAR(255),
    cnote_receiver_contact VARCHAR(255),
    cnote_receiver_phone VARCHAR(255),
    cnote_delivery_name DOUBLE PRECISION,
    cnote_delivery_addr1 DOUBLE PRECISION,
    cnote_delivery_addr2 DOUBLE PRECISION,
    cnote_delivery_addr3 DOUBLE PRECISION,
    cnote_delivery_city DOUBLE PRECISION,
    cnote_delivery_zip DOUBLE PRECISION,
    cnote_delivery_region DOUBLE PRECISION,
    cnote_delivery_country DOUBLE PRECISION,
    cnote_delivery_contact DOUBLE PRECISION,
    cnote_delivery_phone DOUBLE PRECISION,
    cnote_delivery_type DOUBLE PRECISION,
    cnote_goods_type BIGINT,
    cnote_goods_descr VARCHAR(255),
    cnote_goods_value DOUBLE PRECISION,
    cnote_special_ins VARCHAR(255),
    cnote_insurance_id VARCHAR(255),
    cnote_insurance_value BIGINT,
    cnote_payment_type BIGINT,
    cnote_currency VARCHAR(255),
    cnote_amount BIGINT,
    cnote_additional_fee DOUBLE PRECISION,
    cnote_notice DOUBLE PRECISION,
    cnote_commision DOUBLE PRECISION,
    cnote_printed VARCHAR(255),
    cnote_manifested VARCHAR(255),
    cnote_invoiced DOUBLE PRECISION,
    cnote_cancel DOUBLE PRECISION,
    cnote_hold DOUBLE PRECISION,
    cnote_hold_reason DOUBLE PRECISION,
    cnote_user VARCHAR(255),
    cnote_delivered DOUBLE PRECISION,
    cnote_inbound DOUBLE PRECISION,
    cnote_holdit DOUBLE PRECISION,
    cnote_handling BIGINT,
    cnote_mgtfee DOUBLE PRECISION,
    cnote_qrc DOUBLE PRECISION,
    cnote_quick VARCHAR(255),
    cnote_refno VARCHAR(255),
    cnote_verified DOUBLE PRECISION,
    cnote_vdate DOUBLE PRECISION,
    cnote_vuser DOUBLE PRECISION,
    cnote_rdate DOUBLE PRECISION,
    cnote_ruser DOUBLE PRECISION,
    cnote_received DOUBLE PRECISION,
    cnote_luid DOUBLE PRECISION,
    cnote_ldate DOUBLE PRECISION,
    cnote_edit DOUBLE PRECISION,
    cnote_bill_status VARCHAR(255),
    cnote_manifest_no DOUBLE PRECISION,
    cnote_runsheet_no DOUBLE PRECISION,
    cnote_do DOUBLE PRECISION,
    cnote_insurance_no VARCHAR(255),
    cnote_other_fee DOUBLE PRECISION,
    cnote_curc_payment BIGINT,
    cnote_bank DOUBLE PRECISION,
    cnote_curc_rate BIGINT,
    cnote_payment_by VARCHAR(255),
    cnote_amount_payment BIGINT,
    cnote_trans DOUBLE PRECISION,
    cnote_euser DOUBLE PRECISION,
    cnote_act_weight DOUBLE PRECISION,
    cnote_bilnote VARCHAR(255),
    cnote_crdate DOUBLE PRECISION,
    cnote_shipper_addr4 DOUBLE PRECISION,
    cnote_receiver_addr4 DOUBLE PRECISION,
    cnote_packing DOUBLE PRECISION,
    cnote_sms DOUBLE PRECISION,
    cnote_card_no DOUBLE PRECISION,
    cnote_card_amount DOUBLE PRECISION,
    cnote_card_disc DOUBLE PRECISION,
    cnote_ecnote VARCHAR(255),
    status_1 DOUBLE PRECISION,
    status VARCHAR(255),
    cnote_ctc DOUBLE PRECISION,
    cnote_yes_cancel DOUBLE PRECISION,
    cnote_vat VARCHAR(255),
    cnote_vat_amount DOUBLE PRECISION,
    cnote_ses_frm VARCHAR(255),
    cnote_type_cust DOUBLE PRECISION,
    cnote_protect_id DOUBLE PRECISION,
    cnote_zip_seq DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_COST_DTRANSIT_AGEN (4 rows, 11 columns)
DROP TABLE IF EXISTS raw.cms_cost_dtransit_agen CASCADE;
CREATE TABLE raw.cms_cost_dtransit_agen (
    id SERIAL PRIMARY KEY,
    dmanifest_no VARCHAR(255),
    cnote_no VARCHAR(255),
    cnote_origin VARCHAR(255),
    cnote_destination VARCHAR(255),
    cnote_qty BIGINT,
    cnote_weight DOUBLE PRECISION,
    cnote_cost DOUBLE PRECISION,
    cnote_services_code VARCHAR(255),
    esb_time TIMESTAMP,
    esb_id VARCHAR(255),
    dmanifest_doc_ref VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_COST_MTRANSIT_AGEN (4 rows, 15 columns)
DROP TABLE IF EXISTS raw.cms_cost_mtransit_agen CASCADE;
CREATE TABLE raw.cms_cost_mtransit_agen (
    id SERIAL PRIMARY KEY,
    manifest_no VARCHAR(255),
    manifest_date DOUBLE PRECISION,
    branch_id VARCHAR(255),
    destination VARCHAR(255),
    ctc_weight DOUBLE PRECISION,
    act_weight DOUBLE PRECISION,
    manifest_approved VARCHAR(255),
    remark VARCHAR(255),
    manifest_cost DOUBLE PRECISION,
    real_cost DOUBLE PRECISION,
    manifest_uid VARCHAR(255),
    esb_time TIMESTAMP,
    esb_id VARCHAR(255),
    manifest_type DOUBLE PRECISION,
    manifest_doc_ref VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_DBAG_HO (57 rows, 10 columns)
DROP TABLE IF EXISTS raw.cms_dbag_ho CASCADE;
CREATE TABLE raw.cms_dbag_ho (
    id SERIAL PRIMARY KEY,
    dbag_ho_no VARCHAR(255),
    dbag_no VARCHAR(255),
    dbag_cnote_no VARCHAR(255),
    dbag_cnote_qty DOUBLE PRECISION,
    dbag_cnote_weight DOUBLE PRECISION,
    dbag_cnote_destination VARCHAR(255),
    cdate DOUBLE PRECISION,
    dbag_cnote_service VARCHAR(255),
    dbag_cnote_date DOUBLE PRECISION,
    dbag_zone_dest VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_DHI_HOC (5 rows, 9 columns)
DROP TABLE IF EXISTS raw.cms_dhi_hoc CASCADE;
CREATE TABLE raw.cms_dhi_hoc (
    id SERIAL PRIMARY KEY,
    dhi_no VARCHAR(255),
    dhi_seq_no BIGINT,
    dhi_ono VARCHAR(255),
    dhi_seq_ono BIGINT,
    dhi_cnote_no VARCHAR(255),
    dhi_cnote_qty BIGINT,
    cdate DOUBLE PRECISION,
    dhi_remarks DOUBLE PRECISION,
    dhi_do VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_DHICNOTE (57 rows, 6 columns)
DROP TABLE IF EXISTS raw.cms_dhicnote CASCADE;
CREATE TABLE raw.cms_dhicnote (
    id SERIAL PRIMARY KEY,
    dhicnote_no VARCHAR(255),
    dhicnote_seq_no BIGINT,
    dhicnote_cnote_no VARCHAR(255),
    dhicnote_qty BIGINT,
    dhicnote_remarks DOUBLE PRECISION,
    dhicnote_tdate DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_DHOCNOTE (57 rows, 12 columns)
DROP TABLE IF EXISTS raw.cms_dhocnote CASCADE;
CREATE TABLE raw.cms_dhocnote (
    id SERIAL PRIMARY KEY,
    dhocnote_no VARCHAR(255),
    dhocnote_seq_no BIGINT,
    dhocnote_cnote_no VARCHAR(255),
    dhocnote_qty BIGINT,
    dhocnote_remarks DOUBLE PRECISION,
    dhocnote_tdate DOUBLE PRECISION,
    dhocnote_inbound DOUBLE PRECISION,
    dhocnote_weight DOUBLE PRECISION,
    dhocnote_service DOUBLE PRECISION,
    dhocnote_dest DOUBLE PRECISION,
    dhocnote_goods DOUBLE PRECISION,
    dhocnote_handling DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_DHOUNDEL_POD (1 rows, 11 columns)
DROP TABLE IF EXISTS raw.cms_dhoundel_pod CASCADE;
CREATE TABLE raw.cms_dhoundel_pod (
    id SERIAL PRIMARY KEY,
    dhoundel_no VARCHAR(255),
    dhoundel_seq_no BIGINT,
    dhoundel_cnote_no VARCHAR(255),
    dhoundel_qty BIGINT,
    dhoundel_remarks VARCHAR(255),
    dhoundel_weight DOUBLE PRECISION,
    dhoundel_service DOUBLE PRECISION,
    dhoundel_dest DOUBLE PRECISION,
    dhoundel_goods DOUBLE PRECISION,
    dhoundel_hrs VARCHAR(255),
    create_date DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_DHOV_RSHEET (100 rows, 23 columns)
DROP TABLE IF EXISTS raw.cms_dhov_rsheet CASCADE;
CREATE TABLE raw.cms_dhov_rsheet (
    id SERIAL PRIMARY KEY,
    dhov_rsheet_no VARCHAR(255),
    dhov_rsheet_cnote VARCHAR(255),
    dhov_rsheet_do DOUBLE PRECISION,
    dhov_rsheet_cod DOUBLE PRECISION,
    dhov_rsheet_undel DOUBLE PRECISION,
    dhov_rsheet_redel DOUBLE PRECISION,
    dhov_rsheet_outs DOUBLE PRECISION,
    dhov_rsheet_qty DOUBLE PRECISION,
    create_date DOUBLE PRECISION,
    dhov_rsheet_uzone VARCHAR(255),
    dhov_rsheet_cycle VARCHAR(255),
    dcsundel_rsheet_rsheetno DOUBLE PRECISION,
    dhov_rsheet_rsheetno VARCHAR(255),
    dhov_drsheet_epay_vend DOUBLE PRECISION,
    dhov_drsheet_epay_trxid VARCHAR(255),
    dhov_drsheet_epay_amount DOUBLE PRECISION,
    dhov_drsheet_epay_device VARCHAR(255),
    dhov_drsheet_date DOUBLE PRECISION,
    dhov_drsheet_status VARCHAR(255),
    dhov_drsheet_receiver DOUBLE PRECISION,
    dhov_drsheet_flag DOUBLE PRECISION,
    dhov_drsheet_uid DOUBLE PRECISION,
    dhov_drsheet_udate DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_DMBAG (43 rows, 11 columns)
DROP TABLE IF EXISTS raw.cms_dmbag CASCADE;
CREATE TABLE raw.cms_dmbag (
    id SERIAL PRIMARY KEY,
    dmbag_no VARCHAR(255),
    dmbag_bag_no VARCHAR(255),
    dmbag_origin VARCHAR(255),
    dmbag_destination VARCHAR(255),
    dmbag_weight DOUBLE PRECISION,
    dmbag_sps DOUBLE PRECISION,
    dmbag_inbound VARCHAR(255),
    dmbag_type DOUBLE PRECISION,
    dmbag_status DOUBLE PRECISION,
    esb_time TIMESTAMP,
    esb_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_DRCNOTE (100 rows, 9 columns)
DROP TABLE IF EXISTS raw.cms_drcnote CASCADE;
CREATE TABLE raw.cms_drcnote (
    id SERIAL PRIMARY KEY,
    drcnote_no VARCHAR(255),
    drcnote_cnote_no VARCHAR(255),
    drcnote_qty BIGINT,
    drcnote_remarks VARCHAR(255),
    drcnote_tdate DOUBLE PRECISION,
    drcnote_tuser VARCHAR(255),
    drcnote_flag DOUBLE PRECISION,
    drcnote_payment BIGINT,
    drcnote_do VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_DROURATE (2676 rows, 17 columns)
DROP TABLE IF EXISTS raw.cms_drourate CASCADE;
CREATE TABLE raw.cms_drourate (
    id SERIAL PRIMARY KEY,
    drourate_code VARCHAR(255),
    drourate_service VARCHAR(255),
    drourate_zone VARCHAR(255),
    drourate_transit DOUBLE PRECISION,
    drourate_delivery DOUBLE PRECISION,
    drourate_linehaul DOUBLE PRECISION,
    drourate_addcost DOUBLE PRECISION,
    drourate_delivery_next DOUBLE PRECISION,
    drourate_active VARCHAR(255),
    drourate_warehouse DOUBLE PRECISION,
    drourate_uid VARCHAR(255),
    drourate_udate TIMESTAMP,
    drourate_etd_from DOUBLE PRECISION,
    drourate_etd_thru DOUBLE PRECISION,
    drourate_time VARCHAR(255),
    drourate_linehaul_next DOUBLE PRECISION,
    drourate_yes_late VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_DRSHEET_PRA (64 rows, 10 columns)
DROP TABLE IF EXISTS raw.cms_drsheet_pra CASCADE;
CREATE TABLE raw.cms_drsheet_pra (
    id SERIAL PRIMARY KEY,
    drsheet_no VARCHAR(255),
    drsheet_cnote_no VARCHAR(255),
    drsheet_date DOUBLE PRECISION,
    drsheet_status DOUBLE PRECISION,
    drsheet_receiver DOUBLE PRECISION,
    drsheet_flag DOUBLE PRECISION,
    drsheet_uid DOUBLE PRECISION,
    drsheet_udate DOUBLE PRECISION,
    creation_date DOUBLE PRECISION,
    drsheet_zone DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_DRSHEET (107 rows, 9 columns)
DROP TABLE IF EXISTS raw.cms_drsheet CASCADE;
CREATE TABLE raw.cms_drsheet (
    id SERIAL PRIMARY KEY,
    drsheet_no VARCHAR(255),
    drsheet_cnote_no VARCHAR(255),
    drsheet_date DOUBLE PRECISION,
    drsheet_status VARCHAR(255),
    drsheet_receiver VARCHAR(255),
    drsheet_flag VARCHAR(255),
    drsheet_uid VARCHAR(255),
    drsheet_udate DOUBLE PRECISION,
    creation_date DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_DSJ (113 rows, 5 columns)
DROP TABLE IF EXISTS raw.cms_dsj CASCADE;
CREATE TABLE raw.cms_dsj (
    id SERIAL PRIMARY KEY,
    dsj_no VARCHAR(255),
    dsj_bag_no VARCHAR(255),
    dsj_hvo_no VARCHAR(255),
    dsj_uid VARCHAR(255),
    dsj_cdate DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_DSMU (168 rows, 14 columns)
DROP TABLE IF EXISTS raw.cms_dsmu CASCADE;
CREATE TABLE raw.cms_dsmu (
    id SERIAL PRIMARY KEY,
    dsmu_no VARCHAR(255),
    dsmu_flight_no VARCHAR(255),
    dsmu_flight_date DOUBLE PRECISION,
    dsmu_bag_no VARCHAR(255),
    dsmu_weight DOUBLE PRECISION,
    dsmu_bag_origin VARCHAR(255),
    dsmu_bag_destination VARCHAR(255),
    dsmu_bag_cancel DOUBLE PRECISION,
    dsmu_sps DOUBLE PRECISION,
    dsmu_inbound VARCHAR(255),
    esb_time TIMESTAMP,
    esb_id VARCHAR(255),
    dsmu_manifest_no DOUBLE PRECISION,
    dsmu_police_license_plate VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_DSTATUS (5 rows, 13 columns)
DROP TABLE IF EXISTS raw.cms_dstatus CASCADE;
CREATE TABLE raw.cms_dstatus (
    id SERIAL PRIMARY KEY,
    dstatus_no VARCHAR(255),
    dstatus_cnote_no BIGINT,
    dstatus_status VARCHAR(255),
    dstatus_remarks VARCHAR(255),
    create_date DOUBLE PRECISION,
    dstatus_status_date DOUBLE PRECISION,
    dstatus_manifest_no_old DOUBLE PRECISION,
    dstatus_bag_no_old DOUBLE PRECISION,
    dstatus_manifest_no_new DOUBLE PRECISION,
    dstatus_manifest_dest DOUBLE PRECISION,
    dstatus_manifest_thru DOUBLE PRECISION,
    dstatus_bag_no_new DOUBLE PRECISION,
    dstatus_zone_code VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_MANIFEST (274 rows, 18 columns)
DROP TABLE IF EXISTS raw.cms_manifest CASCADE;
CREATE TABLE raw.cms_manifest (
    id SERIAL PRIMARY KEY,
    manifest_no VARCHAR(255),
    manifest_recall_no DOUBLE PRECISION,
    manifest_date DOUBLE PRECISION,
    manifest_route VARCHAR(255),
    manifest_from VARCHAR(255),
    manifest_thru VARCHAR(255),
    manifest_notice VARCHAR(255),
    manifest_approved VARCHAR(255),
    manifest_origin VARCHAR(255),
    manifest_code BIGINT,
    manifest_uid VARCHAR(255),
    manifest_user_audit DOUBLE PRECISION,
    manifest_time_audit DOUBLE PRECISION,
    manifest_form_audit DOUBLE PRECISION,
    manifest_crdate DOUBLE PRECISION,
    manifest_canceled VARCHAR(255),
    manifest_canceled_uid DOUBLE PRECISION,
    manifest_moda VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_MFCNOTE (274 rows, 20 columns)
DROP TABLE IF EXISTS raw.cms_mfcnote CASCADE;
CREATE TABLE raw.cms_mfcnote (
    id SERIAL PRIMARY KEY,
    mfcnote_man_no VARCHAR(255),
    mfcnote_bag_no VARCHAR(255),
    mfcnote_no VARCHAR(255),
    mfcnote_weight DOUBLE PRECISION,
    mfcnote_cost BIGINT,
    mfcnote_mgtfee DOUBLE PRECISION,
    mfcnote_transit DOUBLE PRECISION,
    mfcnote_delivery DOUBLE PRECISION,
    mfcnote_linehaul DOUBLE PRECISION,
    mfcnote_addcost DOUBLE PRECISION,
    mfcnote_flag VARCHAR(255),
    mfcnote_handling DOUBLE PRECISION,
    mfcnote_description DOUBLE PRECISION,
    mfcnote_user_audit DOUBLE PRECISION,
    mfcnote_time_audit DOUBLE PRECISION,
    mfcnote_form_audit DOUBLE PRECISION,
    mfcnote_man_code DOUBLE PRECISION,
    mfcnote_man_date DOUBLE PRECISION,
    mfcnote_crdate DOUBLE PRECISION,
    mfcnote_origin VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_MHI_HOC (5 rows, 12 columns)
DROP TABLE IF EXISTS raw.cms_mhi_hoc CASCADE;
CREATE TABLE raw.cms_mhi_hoc (
    id SERIAL PRIMARY KEY,
    mhi_no VARCHAR(255),
    mhi_ref_no BIGINT,
    mhi_date DOUBLE PRECISION,
    mhi_uid VARCHAR(255),
    mhi_approve VARCHAR(255),
    mhi_branch VARCHAR(255),
    mhi_approve_date DOUBLE PRECISION,
    mhi_remarks DOUBLE PRECISION,
    mhi_user1 VARCHAR(255),
    mhi_user2 DOUBLE PRECISION,
    mhi_zone DOUBLE PRECISION,
    mhi_courier DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_MHICNOTE (57 rows, 11 columns)
DROP TABLE IF EXISTS raw.cms_mhicnote CASCADE;
CREATE TABLE raw.cms_mhicnote (
    id SERIAL PRIMARY KEY,
    mhicnote_branch_id VARCHAR(255),
    mhicnote_zone VARCHAR(255),
    mhicnote_no VARCHAR(255),
    mhicnote_ref_no VARCHAR(255),
    mhicnote_date DOUBLE PRECISION,
    mhicnote_zone_orig VARCHAR(255),
    mhicnote_user_id VARCHAR(255),
    mhicnote_user1 DOUBLE PRECISION,
    mhicnote_user2 DOUBLE PRECISION,
    mhicnote_signdate BIGINT,
    mhicnote_approve VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_MHOCNOTE (57 rows, 20 columns)
DROP TABLE IF EXISTS raw.cms_mhocnote CASCADE;
CREATE TABLE raw.cms_mhocnote (
    id SERIAL PRIMARY KEY,
    mhocnote_branch_id VARCHAR(255),
    mhocnote_zone VARCHAR(255),
    mhocnote_no VARCHAR(255),
    mhocnote_date DOUBLE PRECISION,
    mhocnote_zone_dest VARCHAR(255),
    mhocnote_user_id VARCHAR(255),
    mhocnote_user1 VARCHAR(255),
    mhocnote_user2 VARCHAR(255),
    mhocnote_signdate DOUBLE PRECISION,
    mhocnote_approve VARCHAR(255),
    mhocnote_remarks VARCHAR(255),
    mhocnote_origin DOUBLE PRECISION,
    mhocnote_courier_id DOUBLE PRECISION,
    mhocnote_services DOUBLE PRECISION,
    mhocnote_product DOUBLE PRECISION,
    mhocnote_cust DOUBLE PRECISION,
    mhocnote_user_sco DOUBLE PRECISION,
    mhocnote_hvs VARCHAR(255),
    mhocnote_app_date DOUBLE PRECISION,
    mhocnote_type DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_MHOUNDEL_POD (1 rows, 11 columns)
DROP TABLE IF EXISTS raw.cms_mhoundel_pod CASCADE;
CREATE TABLE raw.cms_mhoundel_pod (
    id SERIAL PRIMARY KEY,
    mhoundel_branch_id VARCHAR(255),
    mhoundel_no VARCHAR(255),
    mhoundel_remarks VARCHAR(255),
    mhoundel_date DOUBLE PRECISION,
    mhoundel_user_id VARCHAR(255),
    mhoundel_zone VARCHAR(255),
    mhoundel_approve VARCHAR(255),
    mhoundel_user1 VARCHAR(255),
    mhoundel_user2 VARCHAR(255),
    mhoundel_signdate BIGINT,
    mhoundel_flow VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_MRCNOTE (100 rows, 12 columns)
DROP TABLE IF EXISTS raw.cms_mrcnote CASCADE;
CREATE TABLE raw.cms_mrcnote (
    id SERIAL PRIMARY KEY,
    mrcnote_no VARCHAR(255),
    mrcnote_date DOUBLE PRECISION,
    mrcnote_branch_id VARCHAR(255),
    mrcnote_user_id VARCHAR(255),
    mrcnote_courier_id VARCHAR(255),
    mrcnote_ae_id DOUBLE PRECISION,
    mrcnote_user1 VARCHAR(255),
    mrcnote_user2 VARCHAR(255),
    mrcnote_signdate DOUBLE PRECISION,
    mrcnote_payment BIGINT,
    mrcnote_refno DOUBLE PRECISION,
    mrcnote_type DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_MRSHEET (107 rows, 13 columns)
DROP TABLE IF EXISTS raw.cms_mrsheet CASCADE;
CREATE TABLE raw.cms_mrsheet (
    id SERIAL PRIMARY KEY,
    mrsheet_branch VARCHAR(255),
    mrsheet_no VARCHAR(255),
    mrsheet_date DOUBLE PRECISION,
    mrsheet_courier_id VARCHAR(255),
    mrsheet_uid VARCHAR(255),
    mrsheet_udate DOUBLE PRECISION,
    mrsheet_approved_dr VARCHAR(255),
    mrsheet_uid_dr VARCHAR(255),
    mrsheet_udate_dr DOUBLE PRECISION,
    mrsheet_inb_app_dr DOUBLE PRECISION,
    mrsheet_inb_uid_dr VARCHAR(255),
    mrsheet_inb_udate_dr DOUBLE PRECISION,
    mrsheet_origin DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_MSJ (113 rows, 17 columns)
DROP TABLE IF EXISTS raw.cms_msj CASCADE;
CREATE TABLE raw.cms_msj (
    id SERIAL PRIMARY KEY,
    msj_no VARCHAR(255),
    msj_branch_id VARCHAR(255),
    msj_date DOUBLE PRECISION,
    msj_user1 VARCHAR(255),
    msj_user2 VARCHAR(255),
    msj_approve VARCHAR(255),
    msj_cdate DOUBLE PRECISION,
    msj_uid VARCHAR(255),
    msj_remarks VARCHAR(255),
    msj_signdate DOUBLE PRECISION,
    msj_dest VARCHAR(255),
    msj_orig VARCHAR(255),
    msj_courier_id DOUBLE PRECISION,
    msj_armada DOUBLE PRECISION,
    msj_char1 DOUBLE PRECISION,
    msj_char2 DOUBLE PRECISION,
    msj_char3 DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_MSMU (168 rows, 26 columns)
DROP TABLE IF EXISTS raw.cms_msmu CASCADE;
CREATE TABLE raw.cms_msmu (
    id SERIAL PRIMARY KEY,
    msmu_no VARCHAR(255),
    msmu_date DOUBLE PRECISION,
    msmu_origin VARCHAR(255),
    msmu_destination VARCHAR(255),
    msmu_flight_no VARCHAR(255),
    msmu_flight_date DOUBLE PRECISION,
    msmu_etd DOUBLE PRECISION,
    msmu_eta DOUBLE PRECISION,
    msmu_qty DOUBLE PRECISION,
    msmu_weight DOUBLE PRECISION,
    msmu_user VARCHAR(255),
    msmu_flag VARCHAR(255),
    msmu_remarks VARCHAR(255),
    msmu_status VARCHAR(255),
    msmu_wrh_date DOUBLE PRECISION,
    msmu_wrh_time VARCHAR(255),
    msmu_off_date DOUBLE PRECISION,
    msmu_off_time VARCHAR(255),
    msmu_confirm VARCHAR(255),
    msmu_cancel DOUBLE PRECISION,
    msmu_user_cancel DOUBLE PRECISION,
    msmu_replace DOUBLE PRECISION,
    msmu_type DOUBLE PRECISION,
    msmu_moda VARCHAR(255),
    msmu_police_license_plate VARCHAR(255),
    msmu_hours DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: CMS_RDSJ (113 rows, 6 columns)
DROP TABLE IF EXISTS raw.cms_rdsj CASCADE;
CREATE TABLE raw.cms_rdsj (
    id SERIAL PRIMARY KEY,
    rdsj_no VARCHAR(255),
    rdsj_bag_no VARCHAR(255),
    rdsj_hvo_no VARCHAR(255),
    rdsj_uid DOUBLE PRECISION,
    rdsj_cdate DOUBLE PRECISION,
    rdsj_hvi_no VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: LASTMILE_COURIER (107 rows, 29 columns)
DROP TABLE IF EXISTS raw.lastmile_courier CASCADE;
CREATE TABLE raw.lastmile_courier (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR(255),
    courier_name VARCHAR(255),
    courier_phone DOUBLE PRECISION,
    courier_email VARCHAR(255),
    courier_password VARCHAR(255),
    courier_nik VARCHAR(255),
    courier_regional VARCHAR(255),
    courier_branch VARCHAR(255),
    courier_zone VARCHAR(255),
    courier_origin VARCHAR(255),
    courier_active BIGINT,
    courier_sp_value BIGINT,
    courier_incentive_group VARCHAR(255),
    courier_armada VARCHAR(255),
    courier_employee_status VARCHAR(255),
    courier_created_at DOUBLE PRECISION,
    courier_updated_at DOUBLE PRECISION,
    courier_role_id DOUBLE PRECISION,
    courier_level DOUBLE PRECISION,
    courier_company_id DOUBLE PRECISION,
    courier_type BIGINT,
    parent_courier_id DOUBLE PRECISION,
    courier_cust_id DOUBLE PRECISION,
    courier_vacant1 DOUBLE PRECISION,
    courier_vacant2 DOUBLE PRECISION,
    courier_vacant3 DOUBLE PRECISION,
    courier_vacant4 DOUBLE PRECISION,
    courier_vacant5 DOUBLE PRECISION,
    courier_vacant6 DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: ORA_ZONE (15707 rows, 25 columns)
DROP TABLE IF EXISTS raw.ora_zone CASCADE;
CREATE TABLE raw.ora_zone (
    id SERIAL PRIMARY KEY,
    zone_branch VARCHAR(255),
    zone_code VARCHAR(255),
    zone_desc VARCHAR(255),
    zone_uid VARCHAR(255),
    zone_active VARCHAR(255),
    zone_seq VARCHAR(255),
    zone_date DOUBLE PRECISION,
    zone_type DOUBLE PRECISION,
    zone_ecnote_parm VARCHAR(255),
    zone_origin VARCHAR(255),
    zone_name VARCHAR(255),
    zone_addr1 VARCHAR(255),
    zone_addr2 VARCHAR(255),
    zone_addr3 VARCHAR(255),
    zone_dp_flag VARCHAR(255),
    zone_latitude DOUBLE PRECISION,
    zone_longtitide DOUBLE PRECISION,
    zone_kota DOUBLE PRECISION,
    zone_kecamatan DOUBLE PRECISION,
    zone_provinsi DOUBLE PRECISION,
    zone_category DOUBLE PRECISION,
    lastupddtm DOUBLE PRECISION,
    lastupdby DOUBLE PRECISION,
    lastupdprocess DOUBLE PRECISION,
    zone_kelurahan DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: T_CROSSDOCK_AWD (31 rows, 3 columns)
DROP TABLE IF EXISTS raw.t_crossdock_awd CASCADE;
CREATE TABLE raw.t_crossdock_awd (
    id SERIAL PRIMARY KEY,
    awb_master VARCHAR(255),
    awb_child VARCHAR(255),
    create_date DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: T_GOTO (31 rows, 6 columns)
DROP TABLE IF EXISTS raw.t_goto CASCADE;
CREATE TABLE raw.t_goto (
    id SERIAL PRIMARY KEY,
    awb VARCHAR(255),
    crossdock_role VARCHAR(255),
    field_char1 DOUBLE PRECISION,
    field_number1 DOUBLE PRECISION,
    field_date1 DOUBLE PRECISION,
    create_date DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: T_MDT_CITY_ORIGIN (7422 rows, 6 columns)
DROP TABLE IF EXISTS raw.t_mdt_city_origin CASCADE;
CREATE TABLE raw.t_mdt_city_origin (
    id SERIAL PRIMARY KEY,
    city_branch VARCHAR(255),
    city_code VARCHAR(255),
    city_origin VARCHAR(255),
    city_mts VARCHAR(255),
    city_active VARCHAR(255),
    create_date DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================
-- Create indexes for performance
-- ============================================
CREATE INDEX IF NOT EXISTS idx_cnote_no ON raw.cms_cnote(cnote_no);
CREATE INDEX IF NOT EXISTS idx_cnote_date ON raw.cms_cnote(cnote_date);
CREATE INDEX IF NOT EXISTS idx_cnote_branch ON raw.cms_cnote(cnote_branch_id);
CREATE INDEX IF NOT EXISTS idx_manifest_no ON raw.cms_manifest(manifest_no);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA raw TO jne_user;
GRANT ALL PRIVILEGES ON SCHEMA staging TO jne_user;
GRANT ALL PRIVILEGES ON SCHEMA mart TO jne_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO jne_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO jne_user;