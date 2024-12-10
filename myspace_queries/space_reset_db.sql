
-- Consider deleting S3 Buckets in AWS before deleting tables

-- Show Tables
SHOW TABLES IN space;

-- Show Views
SHOW FULL TABLES IN space WHERE TABLE_TYPE LIKE 'VIEW';


-- Schow Stored Procedures
SELECT routine_name
FROM information_schema.ROUTINES
WHERE routine_schema = 'space'
AND routine_type = 'PROCEDURE';



-- From ChatGPT

-- Create a new Schema
CREATE SCHEMA space_backup;

-- TEMPLATES
		-- Create a copy of the table structure
		CREATE TABLE space_backup.table_name LIKE space.table_name;

		-- Copy the data from the old table to the new one
		INSERT INTO space_backup.table_name SELECT * FROM space.table_name;


-- Step 1 CREATE TABLES
CREATE TABLE space_backup.announcements LIKE space.announcements;
CREATE TABLE space_backup.appliances LIKE space.appliances;
CREATE TABLE space_backup.bills LIKE space.bills;
CREATE TABLE space_backup.businessProfileInfo LIKE space.businessProfileInfo;
CREATE TABLE space_backup.contracts LIKE space.contracts;
CREATE TABLE space_backup.employees LIKE space.employees;
CREATE TABLE space_backup.leaseFees LIKE space.leaseFees;
CREATE TABLE space_backup.leases LIKE space.leases;
CREATE TABLE space_backup.lease_tenant LIKE space.lease_tenant;
CREATE TABLE space_backup.maintenanceQuotes LIKE space.maintenanceQuotes;
CREATE TABLE space_backup.maintenanceRequests LIKE space.maintenanceRequests;
CREATE TABLE space_backup.ownerProfileInfo LIKE space.ownerProfileInfo;
CREATE TABLE space_backup.paymentMethods LIKE space.paymentMethods;
CREATE TABLE space_backup.payments LIKE space.payments;
CREATE TABLE space_backup.properties LIKE space.properties;
CREATE TABLE space_backup.property_owner LIKE space.property_owner;
CREATE TABLE space_backup.property_utility LIKE space.property_utility;
CREATE TABLE space_backup.purchases LIKE space.purchases;
CREATE TABLE space_backup.tenantProfileInfo LIKE space.tenantProfileInfo;
CREATE TABLE space_backup.users LIKE space.users;


-- Step 2. INSERT INTO NEW TABLES
INSERT INTO space_backup.announcements SELECT * FROM space.announcements;
INSERT INTO space_backup.appliances SELECT * FROM space.appliances;
INSERT INTO space_backup.bills SELECT * FROM space.bills;
INSERT INTO space_backup.businessProfileInfo SELECT * FROM space.businessProfileInfo;
INSERT INTO space_backup.contracts SELECT * FROM space.contracts;
INSERT INTO space_backup.employees SELECT * FROM space.employees;
INSERT INTO space_backup.leaseFees SELECT * FROM space.leaseFees;
INSERT INTO space_backup.leases SELECT * FROM space.leases;
INSERT INTO space_backup.lease_tenant SELECT * FROM space.lease_tenant;
INSERT INTO space_backup.maintenanceQuotes SELECT * FROM space.maintenanceQuotes;
INSERT INTO space_backup.maintenanceRequests SELECT * FROM space.maintenanceRequests;
INSERT INTO space_backup.ownerProfileInfo SELECT * FROM space.ownerProfileInfo;
INSERT INTO space_backup.paymentMethods SELECT * FROM space.paymentMethods;
INSERT INTO space_backup.payments SELECT * FROM space.payments;
INSERT INTO space_backup.properties SELECT * FROM space.properties;
INSERT INTO space_backup.property_owner SELECT * FROM space.property_owner;
INSERT INTO space_backup.property_utility SELECT * FROM space.property_utility;
INSERT INTO space_backup.purchases SELECT * FROM space.purchases;
INSERT INTO space_backup.tenantProfileInfo SELECT * FROM space.tenantProfileInfo;
INSERT INTO space_backup.users SELECT * FROM space.users;


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