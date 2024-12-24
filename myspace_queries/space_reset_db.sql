
-- Consider deleting S3 Buckets in AWS before deleting tables

-- Show Tables
SHOW TABLES IN space_prod;

-- Show Views
SHOW FULL TABLES IN space_prod WHERE TABLE_TYPE LIKE 'VIEW';


-- Schow Stored Procedures
SELECT routine_name
FROM information_schema.ROUTINES
WHERE routine_schema = 'space'
AND routine_type = 'PROCEDURE';



-- From ChatGPT

-- Create a new Schema
CREATE SCHEMA space_prod_backup;

-- TEMPLATES
		-- Create a copy of the table structure
		CREATE TABLE space_prod_backup.table_name LIKE space_prod.table_name;

		-- Copy the data from the old table to the new one
		INSERT INTO space_prod_backup.table_name SELECT * FROM space_prod.table_name;


-- Step 1 CREATE TABLES
CREATE TABLE space_prod_backup.announcements LIKE space_prod.announcements;
CREATE TABLE space_prod_backup.appliances LIKE space_prod.appliances;
CREATE TABLE space_prod_backup.bills LIKE space_prod.bills;
CREATE TABLE space_prod_backup.businessProfileInfo LIKE space_prod.businessProfileInfo;
CREATE TABLE space_prod_backup.contracts LIKE space_prod.contracts;
CREATE TABLE space_prod_backup.employees LIKE space_prod.employees;
CREATE TABLE space_prod_backup.leaseFees LIKE space_prod.leaseFees;
CREATE TABLE space_prod_backup.leases LIKE space_prod.leases;
CREATE TABLE space_prod_backup.lease_tenant LIKE space_prod.lease_tenant;
CREATE TABLE space_prod_backup.maintenanceQuotes LIKE space_prod.maintenanceQuotes;
CREATE TABLE space_prod_backup.maintenanceRequests LIKE space_prod.maintenanceRequests;
CREATE TABLE space_prod_backup.ownerProfileInfo LIKE space_prod.ownerProfileInfo;
CREATE TABLE space_prod_backup.paymentMethods LIKE space_prod.paymentMethods;
CREATE TABLE space_prod_backup.payments LIKE space_prod.payments;
CREATE TABLE space_prod_backup.properties LIKE space_prod.properties;
CREATE TABLE space_prod_backup.property_owner LIKE space_prod.property_owner;
CREATE TABLE space_prod_backup.property_utility LIKE space_prod.property_utility;
CREATE TABLE space_prod_backup.purchases LIKE space_prod.purchases;
CREATE TABLE space_prod_backup.tenantProfileInfo LIKE space_prod.tenantProfileInfo;
CREATE TABLE space_prod_backup.users LIKE space_prod.users;


-- Step 2. INSERT INTO NEW TABLES
INSERT INTO space_prod_backup.announcements SELECT * FROM space_prod.announcements;
INSERT INTO space_prod_backup.appliances SELECT * FROM space_prod.appliances;
INSERT INTO space_prod_backup.bills SELECT * FROM space_prod.bills;
INSERT INTO space_prod_backup.businessProfileInfo SELECT * FROM space_prod.businessProfileInfo;
INSERT INTO space_prod_backup.contracts SELECT * FROM space_prod.contracts;
INSERT INTO space_prod_backup.employees SELECT * FROM space_prod.employees;
INSERT INTO space_prod_backup.leaseFees SELECT * FROM space_prod.leaseFees;
INSERT INTO space_prod_backup.leases SELECT * FROM space_prod.leases;
INSERT INTO space_prod_backup.lease_tenant SELECT * FROM space_prod.lease_tenant;
INSERT INTO space_prod_backup.maintenanceQuotes SELECT * FROM space_prod.maintenanceQuotes;
INSERT INTO space_prod_backup.maintenanceRequests SELECT * FROM space_prod.maintenanceRequests;
INSERT INTO space_prod_backup.ownerProfileInfo SELECT * FROM space_prod.ownerProfileInfo;
INSERT INTO space_prod_backup.paymentMethods SELECT * FROM space_prod.paymentMethods;
INSERT INTO space_prod_backup.payments SELECT * FROM space_prod.payments;
INSERT INTO space_prod_backup.properties SELECT * FROM space_prod.properties;
INSERT INTO space_prod_backup.property_owner SELECT * FROM space_prod.property_owner;
INSERT INTO space_prod_backup.property_utility SELECT * FROM space_prod.property_utility;
INSERT INTO space_prod_backup.purchases SELECT * FROM space_prod.purchases;
INSERT INTO space_prod_backup.tenantProfileInfo SELECT * FROM space_prod.tenantProfileInfo;
INSERT INTO space_prod_backup.users SELECT * FROM space_prod.users;


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