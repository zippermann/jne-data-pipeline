-- ============================================================
-- JNE Data Lineage - Populate lineage table
-- ============================================================
-- Documents the source of each column in unified_shipments
-- Based on unify_jne_tables_v2.sql join structure
-- ============================================================

-- Clear existing lineage data
TRUNCATE TABLE audit.data_lineage;

-- ============================================================
-- CMS_CNOTE (Base table - Join sequence 0)
-- ============================================================
INSERT INTO audit.data_lineage (target_column, source_table, source_column, source_alias, join_path, join_sequence, is_key_column, description)
VALUES 
    ('CNOTE_NO', 'CMS_CNOTE', 'CNOTE_NO', 'cn', 'Base table', 0, TRUE, 'Primary shipment identifier - Consignment Note Number'),
    ('CNOTE_DATE', 'CMS_CNOTE', 'CNOTE_DATE', 'cn', 'Base table', 0, FALSE, 'Shipment creation date'),
    ('CNOTE_ORIGIN', 'CMS_CNOTE', 'CNOTE_ORIGIN', 'cn', 'Base table', 0, FALSE, 'Origin branch/zone code'),
    ('CNOTE_DESTINATION', 'CMS_CNOTE', 'CNOTE_DESTINATION', 'cn', 'Base table', 0, FALSE, 'Destination branch/zone code'),
    ('CNOTE_BRANCH_ID', 'CMS_CNOTE', 'CNOTE_BRANCH_ID', 'cn', 'Base table', 0, FALSE, 'Branch ID where shipment was created'),
    ('CNOTE_SERVICES_CODE', 'CMS_CNOTE', 'CNOTE_SERVICES_CODE', 'cn', 'Base table', 0, FALSE, 'Service type code (REG, YES, CTC, etc.)'),
    ('CNOTE_ROUTE_CODE', 'CMS_CNOTE', 'CNOTE_ROUTE_CODE', 'cn', 'Base table', 0, FALSE, 'Route code for SLA lookup'),
    ('CNOTE_WEIGHT', 'CMS_CNOTE', 'CNOTE_WEIGHT', 'cn', 'Base table', 0, FALSE, 'Actual weight in kg'),
    ('CNOTE_QTY', 'CMS_CNOTE', 'CNOTE_QTY', 'cn', 'Base table', 0, FALSE, 'Number of pieces/items'),
    ('CNOTE_AMOUNT', 'CMS_CNOTE', 'CNOTE_AMOUNT', 'cn', 'Base table', 0, FALSE, 'Total shipment cost'),
    ('CNOTE_SHIPPER_NAME', 'CMS_CNOTE', 'CNOTE_SHIPPER_NAME', 'cn', 'Base table', 0, FALSE, 'Sender name'),
    ('CNOTE_RECEIVER_NAME', 'CMS_CNOTE', 'CNOTE_RECEIVER_NAME', 'cn', 'Base table', 0, FALSE, 'Recipient name');

-- ============================================================
-- CMS_CNOTE_POD (Join sequence 1)
-- ============================================================
INSERT INTO audit.data_lineage (target_column, source_table, source_column, source_alias, join_path, join_sequence, description)
VALUES 
    ('CNOTE_POD_STATUS', 'CMS_CNOTE_POD', 'CNOTE_POD_STATUS', 'pod', 'cn.CNOTE_NO = pod.CNOTE_POD_NO', 1, 'POD status code'),
    ('CNOTE_POD_DELIVERED', 'CMS_CNOTE_POD', 'CNOTE_POD_DELIVERED', 'pod', 'cn.CNOTE_NO = pod.CNOTE_POD_NO', 1, 'Delivery confirmation flag'),
    ('CNOTE_POD_DOC_NO', 'CMS_CNOTE_POD', 'CNOTE_POD_DOC_NO', 'pod', 'cn.CNOTE_NO = pod.CNOTE_POD_NO', 1, 'POD document number'),
    ('CNOTE_POD_CREATION_DATE', 'CMS_CNOTE_POD', 'CNOTE_POD_CREATION_DATE', 'pod', 'cn.CNOTE_NO = pod.CNOTE_POD_NO', 1, 'POD creation timestamp');

-- ============================================================
-- CMS_DRCNOTE (Join sequence 2)
-- ============================================================
INSERT INTO audit.data_lineage (target_column, source_table, source_column, source_alias, join_path, join_sequence, description)
VALUES 
    ('DRCNOTE_NO', 'CMS_DRCNOTE', 'DRCNOTE_NO', 'drc', 'cn.CNOTE_NO = drc.DRCNOTE_CNOTE_NO', 2, 'Receiving document number'),
    ('DRCNOTE_QTY', 'CMS_DRCNOTE', 'DRCNOTE_QTY', 'drc', 'cn.CNOTE_NO = drc.DRCNOTE_CNOTE_NO', 2, 'Quantity received'),
    ('DRCNOTE_REMARKS', 'CMS_DRCNOTE', 'DRCNOTE_REMARKS', 'drc', 'cn.CNOTE_NO = drc.DRCNOTE_CNOTE_NO', 2, 'Receiving remarks'),
    ('DRCNOTE_TDATE', 'CMS_DRCNOTE', 'DRCNOTE_TDATE', 'drc', 'cn.CNOTE_NO = drc.DRCNOTE_CNOTE_NO', 2, 'Receiving transaction date'),
    ('DRCNOTE_FLAG', 'CMS_DRCNOTE', 'DRCNOTE_FLAG', 'drc', 'cn.CNOTE_NO = drc.DRCNOTE_CNOTE_NO', 2, 'Receiving status flag'),
    ('DRCNOTE_DO', 'CMS_DRCNOTE', 'DRCNOTE_DO', 'drc', 'cn.CNOTE_NO = drc.DRCNOTE_CNOTE_NO', 2, 'Delivery order reference');

-- ============================================================
-- CMS_MRCNOTE (Join sequence 3 - via DRCNOTE)
-- ============================================================
INSERT INTO audit.data_lineage (target_column, source_table, source_column, source_alias, join_path, join_sequence, description)
VALUES 
    ('MRCNOTE_DATE', 'CMS_MRCNOTE', 'MRCNOTE_DATE', 'mrc', 'drc.DRCNOTE_NO = mrc.MRCNOTE_NO (deduped)', 3, 'Receiving master date'),
    ('MRCNOTE_BRANCH_ID', 'CMS_MRCNOTE', 'MRCNOTE_BRANCH_ID', 'mrc', 'drc.DRCNOTE_NO = mrc.MRCNOTE_NO (deduped)', 3, 'Receiving branch'),
    ('MRCNOTE_COURIER_ID', 'CMS_MRCNOTE', 'MRCNOTE_COURIER_ID', 'mrc', 'drc.DRCNOTE_NO = mrc.MRCNOTE_NO (deduped)', 3, 'Courier who received'),
    ('MRCNOTE_TYPE', 'CMS_MRCNOTE', 'MRCNOTE_TYPE', 'mrc', 'drc.DRCNOTE_NO = mrc.MRCNOTE_NO (deduped)', 3, 'Receiving type');

-- ============================================================
-- CMS_APICUST (Join sequence 4)
-- ============================================================
INSERT INTO audit.data_lineage (target_column, source_table, source_column, source_alias, join_path, join_sequence, description)
VALUES 
    ('APICUST_ORDER_ID', 'CMS_APICUST', 'APICUST_ORDER_ID', 'api', 'cn.CNOTE_NO = api.APICUST_CNOTE_NO', 4, 'API customer order ID'),
    ('APICUST_BRANCH', 'CMS_APICUST', 'APICUST_BRANCH', 'api', 'cn.CNOTE_NO = api.APICUST_CNOTE_NO', 4, 'API customer branch'),
    ('APICUST_COD_FLAG', 'CMS_APICUST', 'APICUST_COD_FLAG', 'api', 'cn.CNOTE_NO = api.APICUST_CNOTE_NO', 4, 'Cash on delivery flag'),
    ('APICUST_COD_AMOUNT', 'CMS_APICUST', 'APICUST_COD_AMOUNT', 'api', 'cn.CNOTE_NO = api.APICUST_CNOTE_NO', 4, 'COD amount'),
    ('APICUST_LATITUDE', 'CMS_APICUST', 'APICUST_LATITUDE', 'api', 'cn.CNOTE_NO = api.APICUST_CNOTE_NO', 4, 'Delivery latitude'),
    ('APICUST_LONGITUDE', 'CMS_APICUST', 'APICUST_LONGITUDE', 'api', 'cn.CNOTE_NO = api.APICUST_CNOTE_NO', 4, 'Delivery longitude');

-- ============================================================
-- CMS_DROURATE (Join sequence 5 - SLA reference)
-- ============================================================
INSERT INTO audit.data_lineage (target_column, source_table, source_column, source_alias, join_path, join_sequence, description)
VALUES 
    ('DROURATE_ZONE', 'CMS_DROURATE', 'DROURATE_ZONE', 'drou', 'cn.CNOTE_ROUTE_CODE = drou.DROURATE_CODE AND cn.CNOTE_SERVICES_CODE = drou.DROURATE_SERVICE', 5, 'Route zone category'),
    ('DROURATE_TRANSIT', 'CMS_DROURATE', 'DROURATE_TRANSIT', 'drou', 'Same as above', 5, 'Transit time in days'),
    ('DROURATE_DELIVERY', 'CMS_DROURATE', 'DROURATE_DELIVERY', 'drou', 'Same as above', 5, 'Delivery time in days'),
    ('DROURATE_ETD_FROM', 'CMS_DROURATE', 'DROURATE_ETD_FROM', 'drou', 'Same as above', 5, 'ETD start time'),
    ('DROURATE_ETD_THRU', 'CMS_DROURATE', 'DROURATE_ETD_THRU', 'drou', 'Same as above', 5, 'ETD end time');

-- ============================================================
-- CMS_DRSHEET (Join sequence 6)
-- ============================================================
INSERT INTO audit.data_lineage (target_column, source_table, source_column, source_alias, join_path, join_sequence, description)
VALUES 
    ('DRSHEET_NO', 'CMS_DRSHEET', 'DRSHEET_NO', 'drs', 'cn.CNOTE_NO = drs.DRSHEET_CNOTE_NO (deduped)', 6, 'Runsheet detail number'),
    ('DRSHEET_DATE', 'CMS_DRSHEET', 'DRSHEET_DATE', 'drs', 'cn.CNOTE_NO = drs.DRSHEET_CNOTE_NO (deduped)', 6, 'Runsheet date'),
    ('DRSHEET_STATUS', 'CMS_DRSHEET', 'DRSHEET_STATUS', 'drs', 'cn.CNOTE_NO = drs.DRSHEET_CNOTE_NO (deduped)', 6, 'Runsheet status'),
    ('DRSHEET_RECEIVER', 'CMS_DRSHEET', 'DRSHEET_RECEIVER', 'drs', 'cn.CNOTE_NO = drs.DRSHEET_CNOTE_NO (deduped)', 6, 'Receiver on runsheet');

-- ============================================================
-- CMS_MRSHEET (Join sequence 7 - via DRSHEET)
-- ============================================================
INSERT INTO audit.data_lineage (target_column, source_table, source_column, source_alias, join_path, join_sequence, description)
VALUES 
    ('MRSHEET_BRANCH', 'CMS_MRSHEET', 'MRSHEET_BRANCH', 'mrs', 'drs.DRSHEET_NO = mrs.MRSHEET_NO (deduped)', 7, 'Runsheet master branch'),
    ('MRSHEET_DATE', 'CMS_MRSHEET', 'MRSHEET_DATE', 'mrs', 'drs.DRSHEET_NO = mrs.MRSHEET_NO (deduped)', 7, 'Runsheet master date'),
    ('MRSHEET_COURIER_ID', 'CMS_MRSHEET', 'MRSHEET_COURIER_ID', 'mrs', 'drs.DRSHEET_NO = mrs.MRSHEET_NO (deduped)', 7, 'Delivery courier ID'),
    ('MRSHEET_ORIGIN', 'CMS_MRSHEET', 'MRSHEET_ORIGIN', 'mrs', 'drs.DRSHEET_NO = mrs.MRSHEET_NO (deduped)', 7, 'Runsheet origin');

-- ============================================================
-- CMS_MFCNOTE (Join sequence 17)
-- ============================================================
INSERT INTO audit.data_lineage (target_column, source_table, source_column, source_alias, join_path, join_sequence, description)
VALUES 
    ('MFCNOTE_MAN_NO', 'CMS_MFCNOTE', 'MFCNOTE_MAN_NO', 'mfc', 'cn.CNOTE_NO = mfc.MFCNOTE_NO (deduped)', 17, 'Manifest number for this CNOTE'),
    ('MFCNOTE_MAN_DATE', 'CMS_MFCNOTE', 'MFCNOTE_MAN_DATE', 'mfc', 'cn.CNOTE_NO = mfc.MFCNOTE_NO (deduped)', 17, 'Manifest creation date');

-- ============================================================
-- CMS_MANIFEST (Join sequence 18 - via MFCNOTE)
-- ============================================================
INSERT INTO audit.data_lineage (target_column, source_table, source_column, source_alias, join_path, join_sequence, description)
VALUES 
    ('MANIFEST_ORIGIN', 'CMS_MANIFEST', 'MANIFEST_ORIGIN', 'man', 'mfc.MFCNOTE_MAN_NO = man.MANIFEST_NO (deduped)', 18, 'Manifest origin'),
    ('MANIFEST_DESTINATION', 'CMS_MANIFEST', 'MANIFEST_DESTINATION', 'man', 'mfc.MFCNOTE_MAN_NO = man.MANIFEST_NO (deduped)', 18, 'Manifest destination'),
    ('MANIFEST_DATE', 'CMS_MANIFEST', 'MANIFEST_DATE', 'man', 'mfc.MFCNOTE_MAN_NO = man.MANIFEST_NO (deduped)', 18, 'Manifest date'),
    ('MANIFEST_TYPE', 'CMS_MANIFEST', 'MANIFEST_TYPE', 'man', 'mfc.MFCNOTE_MAN_NO = man.MANIFEST_NO (deduped)', 18, 'Manifest type (air/ground)'),
    ('MANIFEST_FLIGHT_NO', 'CMS_MANIFEST', 'MANIFEST_FLIGHT_NO', 'man', 'mfc.MFCNOTE_MAN_NO = man.MANIFEST_NO (deduped)', 18, 'Flight number if air shipment');

-- ============================================================
-- ORA_ZONE (Join sequence 34 - Reference)
-- ============================================================
INSERT INTO audit.data_lineage (target_column, source_table, source_column, source_alias, join_path, join_sequence, description)
VALUES 
    ('ORA_ZONE_BRANCH', 'ORA_ZONE', 'ZONE_BRANCH', 'ora', 'pra.DRSHEET_ZONE = ora.ZONE_CODE (deduped)', 34, 'Zone branch reference'),
    ('ORA_ZONE_DESC', 'ORA_ZONE', 'ZONE_DESC', 'ora', 'Same as above', 34, 'Zone description'),
    ('ORA_ZONE_KOTA', 'ORA_ZONE', 'ZONE_KOTA', 'ora', 'Same as above', 34, 'City name'),
    ('ORA_ZONE_KECAMATAN', 'ORA_ZONE', 'ZONE_KECAMATAN', 'ora', 'Same as above', 34, 'District name'),
    ('ORA_ZONE_PROVINSI', 'ORA_ZONE', 'ZONE_PROVINSI', 'ora', 'Same as above', 34, 'Province name'),
    ('ORA_ZONE_CATEGORY', 'ORA_ZONE', 'ZONE_CATEGORY', 'ora', 'Same as above', 34, 'Zone category');

-- ============================================================
-- LASTMILE_COURIER (Join sequence 33)
-- ============================================================
INSERT INTO audit.data_lineage (target_column, source_table, source_column, source_alias, join_path, join_sequence, description)
VALUES 
    ('COURIER_NAME', 'LASTMILE_COURIER', 'COURIER_NAME', 'cour', 'mrs.MRSHEET_COURIER_ID = cour.COURIER_ID (deduped)', 33, 'Courier full name'),
    ('COURIER_PHONE', 'LASTMILE_COURIER', 'COURIER_PHONE', 'cour', 'Same as above', 33, 'Courier phone number'),
    ('COURIER_BRANCH', 'LASTMILE_COURIER', 'COURIER_BRANCH', 'cour', 'Same as above', 33, 'Courier assigned branch'),
    ('COURIER_TYPE', 'LASTMILE_COURIER', 'COURIER_TYPE', 'cour', 'Same as above', 33, 'Courier type (employee/partner)');

-- Set updated timestamp
UPDATE audit.data_lineage SET updated_at = CURRENT_TIMESTAMP;

-- ============================================================
-- View the lineage
-- ============================================================
SELECT 
    target_column,
    source_table,
    source_column,
    source_alias,
    join_sequence,
    description
FROM audit.data_lineage
ORDER BY join_sequence, target_column;

-- ============================================================
-- Summary by source table
-- ============================================================
SELECT 
    source_table,
    COUNT(*) as columns_mapped,
    join_sequence,
    MIN(join_path) as join_path
FROM audit.data_lineage
GROUP BY source_table, join_sequence
ORDER BY join_sequence;
