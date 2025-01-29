
-- Consider deleting S3 Buckets in AWS before deleting tables

-- Show Tables
SHOW TABLES IN space_prod;

-- Show Views
SHOW FULL TABLES IN space WHERE TABLE_TYPE LIKE 'VIEW';

-- Show Stored Procedures
SELECT routine_name
FROM information_schema.ROUTINES
WHERE routine_schema = 'space_prod'
AND routine_type = 'PROCEDURE';



-- From ChatGPT

-- Create a new Schema
CREATE SCHEMA space_prod;

-- TEMPLATES
		-- Create a copy of the table structure
		CREATE TABLE space_prod.table_name LIKE space.table_name;

		-- Copy the data from the old table to the new one
		INSERT INTO space_prod.table_name SELECT * FROM space.table_name;


-- Step 1 CREATE TABLES
CREATE TABLE space_prod.announcements LIKE space.announcements;
CREATE TABLE space_prod.appliances LIKE space.appliances;
CREATE TABLE space_prod.bills LIKE space.bills;
CREATE TABLE space_prod.businessProfileInfo LIKE space.businessProfileInfo;
CREATE TABLE space_prod.contracts LIKE space.contracts;
CREATE TABLE space_prod.employees LIKE space.employees;
CREATE TABLE space_prod.leaseFees LIKE space.leaseFees;
CREATE TABLE space_prod.leases LIKE space.leases;
CREATE TABLE space_prod.lease_tenant LIKE space.lease_tenant;
CREATE TABLE space_prod.lists LIKE space.lists;
CREATE TABLE space_prod.maintenanceQuotes LIKE space.maintenanceQuotes;
CREATE TABLE space_prod.maintenanceRequests LIKE space.maintenanceRequests;
CREATE TABLE space_prod.ownerProfileInfo LIKE space.ownerProfileInfo;
CREATE TABLE space_prod.paymentMethods LIKE space.paymentMethods;
CREATE TABLE space_prod.payments LIKE space.payments;
CREATE TABLE space_prod.properties LIKE space.properties;
CREATE TABLE space_prod.property_owner LIKE space.property_owner;
CREATE TABLE space_prod.property_utility LIKE space.property_utility;
CREATE TABLE space_prod.purchases LIKE space.purchases;
CREATE TABLE space_prod.tenantProfileInfo LIKE space.tenantProfileInfo;
CREATE TABLE space_prod.users LIKE space.users;


-- Step 2. INSERT INTO NEW TABLES
INSERT INTO space_prod.announcements SELECT * FROM space.announcements;
INSERT INTO space_prod.appliances SELECT * FROM space.appliances;
INSERT INTO space_prod.bills SELECT * FROM space.bills;
INSERT INTO space_prod.businessProfileInfo SELECT * FROM space.businessProfileInfo;
INSERT INTO space_prod.contracts SELECT * FROM space.contracts;
INSERT INTO space_prod.employees SELECT * FROM space.employees;
INSERT INTO space_prod.leaseFees SELECT * FROM space.leaseFees;
INSERT INTO space_prod.leases SELECT * FROM space.leases;
INSERT INTO space_prod.lease_tenant SELECT * FROM space.lease_tenant;
INSERT INTO space_prod.lists SELECT * FROM space.lists;
INSERT INTO space_prod.maintenanceQuotes SELECT * FROM space.maintenanceQuotes;
INSERT INTO space_prod.maintenanceRequests SELECT * FROM space.maintenanceRequests;
INSERT INTO space_prod.ownerProfileInfo SELECT * FROM space.ownerProfileInfo;
INSERT INTO space_prod.paymentMethods SELECT * FROM space.paymentMethods;
INSERT INTO space_prod.payments SELECT * FROM space.payments;
INSERT INTO space_prod.properties SELECT * FROM space.properties;
INSERT INTO space_prod.property_owner SELECT * FROM space.property_owner;
INSERT INTO space_prod.property_utility SELECT * FROM space.property_utility;
INSERT INTO space_prod.purchases SELECT * FROM space.purchases;
INSERT INTO space_prod.tenantProfileInfo SELECT * FROM space.tenantProfileInfo;
INSERT INTO space_prod.users SELECT * FROM space.users;


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