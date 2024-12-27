
-- Consider deleting S3 Buckets in AWS before deleting tables

-- Show Tables
SHOW TABLES IN space_x;

-- Show Views
SHOW FULL TABLES IN space WHERE TABLE_TYPE LIKE 'VIEW';

-- Show Stored Procedures
SELECT routine_name
FROM information_schema.ROUTINES
WHERE routine_schema = 'space_x'
AND routine_type = 'PROCEDURE';



-- From ChatGPT

-- Create a new Schema
CREATE SCHEMA space_x;

-- TEMPLATES
		-- Create a copy of the table structure
		CREATE TABLE space_x.table_name LIKE space.table_name;

		-- Copy the data from the old table to the new one
		INSERT INTO space_x.table_name SELECT * FROM space.table_name;


-- Step 1 CREATE TABLES
CREATE TABLE space_x.announcements LIKE space.announcements;
CREATE TABLE space_x.appliances LIKE space.appliances;
CREATE TABLE space_x.bills LIKE space.bills;
CREATE TABLE space_x.businessProfileInfo LIKE space.businessProfileInfo;
CREATE TABLE space_x.contracts LIKE space.contracts;
CREATE TABLE space_x.employees LIKE space.employees;
CREATE TABLE space_x.leaseFees LIKE space.leaseFees;
CREATE TABLE space_x.leases LIKE space.leases;
CREATE TABLE space_x.lease_tenant LIKE space.lease_tenant;
CREATE TABLE space_x.lists LIKE space.lists;
CREATE TABLE space_x.maintenanceQuotes LIKE space.maintenanceQuotes;
CREATE TABLE space_x.maintenanceRequests LIKE space.maintenanceRequests;
CREATE TABLE space_x.ownerProfileInfo LIKE space.ownerProfileInfo;
CREATE TABLE space_x.paymentMethods LIKE space.paymentMethods;
CREATE TABLE space_x.payments LIKE space.payments;
CREATE TABLE space_x.properties LIKE space.properties;
CREATE TABLE space_x.property_owner LIKE space.property_owner;
CREATE TABLE space_x.property_utility LIKE space.property_utility;
CREATE TABLE space_x.purchases LIKE space.purchases;
CREATE TABLE space_x.tenantProfileInfo LIKE space.tenantProfileInfo;
CREATE TABLE space_x.users LIKE space.users;


-- Step 2. INSERT INTO NEW TABLES
INSERT INTO space_x.announcements SELECT * FROM space.announcements;
INSERT INTO space_x.appliances SELECT * FROM space.appliances;
INSERT INTO space_x.bills SELECT * FROM space.bills;
INSERT INTO space_x.businessProfileInfo SELECT * FROM space.businessProfileInfo;
INSERT INTO space_x.contracts SELECT * FROM space.contracts;
INSERT INTO space_x.employees SELECT * FROM space.employees;
INSERT INTO space_x.leaseFees SELECT * FROM space.leaseFees;
INSERT INTO space_x.leases SELECT * FROM space.leases;
INSERT INTO space_x.lease_tenant SELECT * FROM space.lease_tenant;
INSERT INTO space_x.lists SELECT * FROM space.lists;
INSERT INTO space_x.maintenanceQuotes SELECT * FROM space.maintenanceQuotes;
INSERT INTO space_x.maintenanceRequests SELECT * FROM space.maintenanceRequests;
INSERT INTO space_x.ownerProfileInfo SELECT * FROM space.ownerProfileInfo;
INSERT INTO space_x.paymentMethods SELECT * FROM space.paymentMethods;
INSERT INTO space_x.payments SELECT * FROM space.payments;
INSERT INTO space_x.properties SELECT * FROM space.properties;
INSERT INTO space_x.property_owner SELECT * FROM space.property_owner;
INSERT INTO space_x.property_utility SELECT * FROM space.property_utility;
INSERT INTO space_x.purchases SELECT * FROM space.purchases;
INSERT INTO space_x.tenantProfileInfo SELECT * FROM space.tenantProfileInfo;
INSERT INTO space_x.users SELECT * FROM space.users;


-- Step 3. CLEAR OLD TABLES
TRUNCATE TABLE space_x.announcements;
TRUNCATE TABLE space_x.appliances;

TRUNCATE TABLE space_x.bills;
TRUNCATE TABLE space_x.businessProfileInfo;
TRUNCATE TABLE space_x.contracts;

TRUNCATE TABLE space_x.employees;
TRUNCATE TABLE space_x.leaseFees;
TRUNCATE TABLE space_x.leases;
TRUNCATE TABLE space_x.lease_tenant;

TRUNCATE TABLE space_x.maintenanceQuotes;
TRUNCATE TABLE space_x.maintenanceRequests;

TRUNCATE TABLE space_x.ownerProfileInfo;

TRUNCATE TABLE space_x.paymentMethods;
TRUNCATE TABLE space_x.payments;

TRUNCATE TABLE space_x.properties;
TRUNCATE TABLE space_x.property_owner;
TRUNCATE TABLE space_x.property_utility;

TRUNCATE TABLE space_x.purchases;

TRUNCATE TABLE space_x.tenantProfileInfo;

TRUNCATE TABLE space_x.users;