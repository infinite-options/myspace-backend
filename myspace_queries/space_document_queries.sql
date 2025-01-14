-- CREATE TABLE space_dev.bills AS
-- SELECT * FROM pm.bills;
-- SELECT * FROM pm.leaseTenants;

SELECT * FROM space_dev.users;
SELECT * FROM space_dev.user_profile;

SELECT * FROM space_dev.ownerProfileInfo;
SELECT * FROM space_dev.property_owner;
SELECT * FROM space_dev.properties;
SELECT * FROM space_dev.appliances;
SELECT * FROM space_dev.maintenanceRequests;
SELECT * FROM space_dev.maintenanceQuotes;

SELECT * FROM space_dev.tenantProfileInfo;
SELECT * FROM space_dev.property_tenant;
SELECT * FROM space_dev.lease_tenant;
SELECT * FROM space_dev.leases;

SELECT * FROM space_dev.businessProfileInfo;
SELECT * FROM space_dev.contracts;
SELECT * FROM space_dev.contractFees;

SELECT * FROM space_dev.purchases;
SELECT * FROM space_dev.payments;
SELECT * FROM space_dev.bills;
SELECT * FROM space_dev.lists;
SELECT * FROM space_dev.property_utility;


SELECT a.purchase_uid, b.purchase_uid
FROM space_dev.purchases_org a
LEFT JOIN space_dev.purchases b ON a.purchase_uid = b.purchase_uid


-- TENANT DOCUMENTS
SELECT tenant_uid, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
	,lease_uid, lease_property_id, lease_end, lease_status
	,tenant_documents
    ,lease_documents
FROM space_dev.tenantProfileInfo
LEFT JOIN space_dev.lease_tenant ON tenant_uid = lt_tenant_id
LEFT JOIN space_dev.leases ON lease_uid = lt_lease_id
WHERE tenant_uid = "350-000002";


-- OWNER DOCUMENTS
SELECT property_owner_id, po_owner_percent
	, property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
    , contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
    , lease_uid, lease_start, lease_end, lease_status, lease_documents, lease_early_end_date, lease_renew_status
FROM space_dev.property_owner
LEFT JOIN space_dev.properties ON property_uid = property_id
LEFT JOIN space_dev.contracts ON property_uid = contract_property_id
LEFT JOIN space_dev.leases ON property_uid = lease_property_id
WHERE property_owner_id = "110-000003"