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




-- OWNER PROFILE
SELECT * FROM space_dev.ownerProfileInfo
WHERE owner_uid = "110-000003";


-- TENANT PROFILE
SELECT * FROM space_dev.tenantProfileInfo
WHERE tenant_uid = "350-000002";