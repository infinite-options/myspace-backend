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




-- OWNER PROFILE
SELECT * FROM space.ownerProfileInfo
WHERE owner_uid = "110-000003";


-- TENANT PROFILE
SELECT * FROM space.tenantProfileInfo
WHERE tenant_uid = "350-000002";