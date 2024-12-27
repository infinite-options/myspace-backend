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




-- OWNER PROFILE
SELECT * FROM space_prod.ownerProfileInfo
WHERE owner_uid = "110-000003";


-- TENANT PROFILE
SELECT * FROM space_prod.tenantProfileInfo
WHERE tenant_uid = "350-000002";