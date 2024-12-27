-- CREATE TABLE space_prod.bills AS
-- SELECT * FROM pm.bills;
-- SELECT * FROM pm.leaseTenants;

SELECT * FROM space_prod.users;
SELECT * FROM space_prod.user_profile;

SELECT * FROM space_prod.ownerProfileInfo;
SELECT * FROM space_prod.property_owner;
SELECT * FROM space_prod.properties;
SELECT * FROM space_prod.appliances;
SELECT * FROM space_prod.maintenanceRequests;
SELECT * FROM space_prod.maintenanceQuotes;

SELECT * FROM space_prod.tenantProfileInfo;
SELECT * FROM space_prod.property_tenant;
SELECT * FROM space_prod.lease_tenant;
SELECT * FROM space_prod.leases;

SELECT * FROM space_prod.businessProfileInfo;
SELECT * FROM space_prod.contracts;
SELECT * FROM space_prod.contractFees;

SELECT * FROM space_prod.purchases;
SELECT * FROM space_prod.payments;
SELECT * FROM space_prod.bills;
SELECT * FROM space_prod.lists;
SELECT * FROM space_prod.property_utility;


SELECT a.purchase_uid, b.purchase_uid
FROM space_prod.purchases_org a
LEFT JOIN space_prod.purchases b ON a.purchase_uid = b.purchase_uid


-- TENANT DOCUMENTS
SELECT tenant_uid, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
	,lease_uid, lease_property_id, lease_end, lease_status
	,tenant_documents
    ,lease_documents
FROM space_prod.tenantProfileInfo
LEFT JOIN space_prod.lease_tenant ON tenant_uid = lt_tenant_id
LEFT JOIN space_prod.leases ON lease_uid = lt_lease_id
WHERE tenant_uid = "350-000002";


-- OWNER DOCUMENTS
SELECT property_owner_id, po_owner_percent
	, property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
    , contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
    , lease_uid, lease_start, lease_end, lease_status, lease_documents, lease_early_end_date, lease_renew_status
FROM space_prod.property_owner
LEFT JOIN space_prod.properties ON property_uid = property_id
LEFT JOIN space_prod.contracts ON property_uid = contract_property_id
LEFT JOIN space_prod.leases ON property_uid = lease_property_id
WHERE property_owner_id = "110-000003"