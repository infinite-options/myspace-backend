
-- Consider deleting S3 Buckets in AWS before deleting tables

-- Show Tables
SHOW TABLES IN space_dev;

-- Show Views
SHOW FULL TABLES IN space WHERE TABLE_TYPE LIKE 'VIEW';

-- Show Stored Procedures
SELECT routine_name
FROM information_schema.ROUTINES
WHERE routine_schema = 'space_dev'
AND routine_type = 'PROCEDURE';



-- From ChatGPT

-- Create a new Schema
CREATE SCHEMA space_dev;

-- TEMPLATES
		-- Create a copy of the table structure
		CREATE TABLE space_dev.table_name LIKE space.table_name;

		-- Copy the data from the old table to the new one
		INSERT INTO space_dev.table_name SELECT * FROM space.table_name;


-- Step 1 CREATE TABLES
CREATE TABLE space_dev.announcements LIKE space.announcements;
CREATE TABLE space_dev.appliances LIKE space.appliances;
CREATE TABLE space_dev.bills LIKE space.bills;
CREATE TABLE space_dev.businessProfileInfo LIKE space.businessProfileInfo;
CREATE TABLE space_dev.contracts LIKE space.contracts;
CREATE TABLE space_dev.employees LIKE space.employees;
CREATE TABLE space_dev.leaseFees LIKE space.leaseFees;
CREATE TABLE space_dev.leases LIKE space.leases;
CREATE TABLE space_dev.lease_tenant LIKE space.lease_tenant;
CREATE TABLE space_dev.lists LIKE space.lists;
CREATE TABLE space_dev.maintenanceQuotes LIKE space.maintenanceQuotes;
CREATE TABLE space_dev.maintenanceRequests LIKE space.maintenanceRequests;
CREATE TABLE space_dev.ownerProfileInfo LIKE space.ownerProfileInfo;
CREATE TABLE space_dev.paymentMethods LIKE space.paymentMethods;
CREATE TABLE space_dev.payments LIKE space.payments;
CREATE TABLE space_dev.properties LIKE space.properties;
CREATE TABLE space_dev.property_owner LIKE space.property_owner;
CREATE TABLE space_dev.property_utility LIKE space.property_utility;
CREATE TABLE space_dev.purchases LIKE space.purchases;
CREATE TABLE space_dev.tenantProfileInfo LIKE space.tenantProfileInfo;
CREATE TABLE space_dev.users LIKE space.users;


-- Step 2. INSERT INTO NEW TABLES
INSERT INTO space_dev.announcements SELECT * FROM space.announcements;
INSERT INTO space_dev.appliances SELECT * FROM space.appliances;
INSERT INTO space_dev.bills SELECT * FROM space.bills;
INSERT INTO space_dev.businessProfileInfo SELECT * FROM space.businessProfileInfo;
INSERT INTO space_dev.contracts SELECT * FROM space.contracts;
INSERT INTO space_dev.employees SELECT * FROM space.employees;
INSERT INTO space_dev.leaseFees SELECT * FROM space.leaseFees;
INSERT INTO space_dev.leases SELECT * FROM space.leases;
INSERT INTO space_dev.lease_tenant SELECT * FROM space.lease_tenant;
INSERT INTO space_dev.lists SELECT * FROM space.lists;
INSERT INTO space_dev.maintenanceQuotes SELECT * FROM space.maintenanceQuotes;
INSERT INTO space_dev.maintenanceRequests SELECT * FROM space.maintenanceRequests;
INSERT INTO space_dev.ownerProfileInfo SELECT * FROM space.ownerProfileInfo;
INSERT INTO space_dev.paymentMethods SELECT * FROM space.paymentMethods;
INSERT INTO space_dev.payments SELECT * FROM space.payments;
INSERT INTO space_dev.properties SELECT * FROM space.properties;
INSERT INTO space_dev.property_owner SELECT * FROM space.property_owner;
INSERT INTO space_dev.property_utility SELECT * FROM space.property_utility;
INSERT INTO space_dev.purchases SELECT * FROM space.purchases;
INSERT INTO space_dev.tenantProfileInfo SELECT * FROM space.tenantProfileInfo;
INSERT INTO space_dev.users SELECT * FROM space.users;


-- Step 3. CLEAR OLD TABLES
TRUNCATE TABLE announcements;
TRUNCATE TABLE appliances;

TRUNCATE TABLE bills;
TRUNCATE TABLE businessProfileInfo;
TRUNCATE TABLE contracts;

TRUNCATE TABLE employees;
TRUNCATE TABLE leaseFees;
TRUNCATE TABLE leases;
TRUNCATE TABLE lease_tenant;

TRUNCATE TABLE maintenanceQuotes;
TRUNCATE TABLE maintenanceRequests;

TRUNCATE TABLE ownerProfileInfo;

TRUNCATE TABLE paymentMethods;
TRUNCATE TABLE payments;

TRUNCATE TABLE properties;
TRUNCATE TABLE property_owner;
TRUNCATE TABLE property_utility;

TRUNCATE TABLE purchases;

TRUNCATE TABLE tenantProfileInfo;

TRUNCATE TABLE users;