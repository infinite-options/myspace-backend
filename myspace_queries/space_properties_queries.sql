-- CREATE TABLE space_prod.bills AS
-- SELECT * FROM pm.bills;
-- SELECT * FROM pm.leaseTenants;

SELECT * FROM space_prod.users;
SELECT * FROM space_prod.user_profile;
SELECT * FROM space_prod.businessProfileInfo;
SELECT * FROM space_prod.tenantProfileInfo;
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



-- PROPERTIES BY OWNER
SELECT * FROM space_prod.p_details
WHERE  contract_status = 'ACTIVE'
	AND owner_uid = '110-000003';

    

-- PROPERTIES OWNED BY A USER
SELECT * FROM space_prod.ownerProfileInfo;
SELECT * FROM space_prod.property_owner;
SELECT * FROM space_prod.properties;

-- PROPERTIES BY OWNER
SELECT property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_type
	, property_num_beds, property_num_baths, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images
    , property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes
	, property_owner.property_owner_id
    , ownerProfileInfo.*
    , lease_start, lease_end, lease_status, lease_rent, lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, lease_actual_rent, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date
	, business_name, business_phone_number, business_email
    , purchases.*
    , payments.*
FROM space_prod.properties
LEFT JOIN space_prod.property_owner ON property_id = property_uid
LEFT JOIN space_prod.ownerProfileInfo ON property_owner_id = owner_uid
LEFT JOIN space_prod.leases ON lease_property_id = property_uid
LEFT JOIN space_prod.contracts ON contract_property_id = property_uid
LEFT JOIN space_prod.businessProfileInfo ON contract_business_id = business_uid
LEFT JOIN space_prod.purchases ON pur_property_id = property_uid
LEFT JOIN space_prod.payments ON pay_purchase_id = purchase_uid
WHERE property_owner_id = '110-000003';






-- OWNER ADDS NEW PROPERTY
	SELECT * FROM space_prod.properties;
	SELECT * FROM space_prod.property_owner;

-- ADDS RELATIONSHIP BETWEEN PROPERTY AND OWNER
INSERT INTO space_prod.property_owner
SET property_id = "200-000064"
	, property_owner_id = "100-000003"
	, po_owner_percent = NULL;

-- ADDS NEW PROPERTY DETAILS    
INSERT INTO space_prod.properties
SET property_uid = "200-000064"
	, property_available_to_rent = 0
	, property_active_date = CURRENT_TIMESTAMP()
	, property_address = "123 Test St"
	, property_unit = "101"
	, property_city = "Testarossa"
	, property_state = "TX"
	, property_zip = "11111"
	, property_type = "House"
	, property_num_beds = 1
	, property_num_baths = 1
	, property_area = 111
	, property_listed_rent = 1001
	, property_deposit = 1001
	, property_pets_allowed = 1
	, property_deposit_for_rent = 1
	, property_images = "[\"https://s3-us-west-1.amazonaws.com/io-pm/properties/200-000034/img_cover\"]"
	, property_taxes = NULL
	, property_mortgages = NULL
	, property_insurance = NULL
	, property_featured = NULL
	, property_description = "Test Property"
	, property_notes = "Test Again";

 -- ADDS NEW PROPERTY DETAILS 
INSERT INTO space_prod.properties
SET property_uid = '200-000065'
	, property_available_to_rent =  '0'
	, property_active_date = '8/1/2023'
	, property_address = '123 Add Property Street'
	, property_unit = ''
	, property_city = 'Tucson'
	, property_state = 'AZ'
	, property_zip = '85712'
	, property_type = 'Single Family Home'
	, property_num_beds = '1'
	, property_num_baths = '1'
	, property_area = '420'
	, property_listed_rent = '0'
	, property_deposit = '2000'
	, property_pets_allowed = '1'
	, property_deposit_for_rent = '0'
	, property_images = "[]"
	, property_taxes = '500'
	, property_mortgages = '0'
	, property_insurance = '0'
	, property_featured = '0'
	, property_description = 'This is a Tiny Home'
	, property_notes = 'This is a Really Tiny Home';
    
-- DELETE PROPERTY FROM PROPERTY-OWNER TABLE
	DELETE FROM space_prod.property_owner
	WHERE property_id = '200-000065'
		AND property_owner_id = '100-000003';  