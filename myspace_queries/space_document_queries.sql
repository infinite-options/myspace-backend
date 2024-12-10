-- CREATE TABLE space.bills AS
-- SELECT * FROM pm.bills;
-- SELECT * FROM pm.leaseTenants;

SELECT * FROM space.users;
SELECT * FROM space.user_profile;

SELECT * FROM space.ownerProfileInfo;
SELECT * FROM space.property_owner;
SELECT * FROM space.properties;
SELECT * FROM space.appliances;
SELECT * FROM space.maintenanceRequests;
SELECT * FROM space.maintenanceQuotes;

SELECT * FROM space.tenantProfileInfo;
SELECT * FROM space.property_tenant;
SELECT * FROM space.lease_tenant;
SELECT * FROM space.leases;

SELECT * FROM space.businessProfileInfo;
SELECT * FROM space.contracts;
SELECT * FROM space.contractFees;

SELECT * FROM space.purchases;
SELECT * FROM space.payments;
SELECT * FROM space.bills;
SELECT * FROM space.lists;
SELECT * FROM space.property_utility;


SELECT a.purchase_uid, b.purchase_uid
FROM space.purchases_org a
LEFT JOIN space.purchases b ON a.purchase_uid = b.purchase_uid


-- TENANT DOCUMENTS
SELECT tenant_uid, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
	,lease_uid, lease_property_id, lease_end, lease_status
	,tenant_documents
    ,lease_documents
FROM space.tenantProfileInfo
LEFT JOIN space.lease_tenant ON tenant_uid = lt_tenant_id
LEFT JOIN space.leases ON lease_uid = lt_lease_id
WHERE tenant_uid = "350-000002";


-- OWNER DOCUMENTS
SELECT property_owner_id, po_owner_percent
	, property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
    , contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
    , lease_uid, lease_start, lease_end, lease_status, lease_documents, lease_early_end_date, lease_renew_status
FROM space.property_owner
LEFT JOIN space.properties ON property_uid = property_id
LEFT JOIN space.contracts ON property_uid = contract_property_id
LEFT JOIN space.leases ON property_uid = lease_property_id
WHERE property_owner_id = "110-000003"