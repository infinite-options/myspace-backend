SELECT * FROM space_prod.users;
SELECT * FROM space_prod.user_profile;

SELECT * FROM space_prod.ownerProfileInfo;
SELECT * FROM space_prod.property_owner;
SELECT * FROM space_prod.properties;
SELECT * FROM space_prod.appliances;
SELECT * FROM space_prod.maintenanceRequests;
SELECT * FROM space_prod.maintenanceQuotes;

SELECT * FROM space_prod.tenantProfileInfo;
SELECT * FROM space_prod.lease_tenant;
SELECT * FROM space_prod.leases;
SELECT * FROM space_prod.leaseFees;
SELECT * FROM space_prod.leaseDocuments;

SELECT * FROM space_prod.businessProfileInfo;
SELECT * FROM space_prod.contracts;
SELECT * FROM space_prod.contractFees;

SELECT * FROM space_prod.purchases;
SELECT * FROM space_prod.payments;
SELECT * FROM space_prod.bills;
SELECT * FROM space_prod.lists;
SELECT * FROM space_prod.property_utility;

SELECT * FROM space_prod.o_details;
SELECT * FROM space_prod.m_details;
SELECT * FROM space_prod.b_details;
SELECT * FROM space_prod.p_details;
SELECT * FROM space_prod.pp_details;
SELECT * FROM space_prod.pp_status;
SELECT * FROM space_prod.t_details;
SELECT * FROM space_prod.user_profiles;



--  FIND ALL MAINTENANCE COMPANIES
SELECT * FROM space_prod.businessProfileInfo
WHERE business_type = 'MAINTENANCE';



-- FIND ALL CURRENT BUSINESS CONTACTS
SELECT owner_uid AS contact_uid, "Owner" AS contact_type, owner_first_name AS contact_first_name, owner_last_name AS contact_last_name, owner_phone_number AS contact_phone_numnber, owner_email AS contact_email, owner_address AS contact_address, owner_unit AS contact_unit, owner_city AS contact_city, owner_state AS contact_state, owner_zip AS contact_zip
FROM space_prod.b_details AS b
LEFT JOIN space_prod.o_details ON b.contract_property_id = property_id
WHERE b.business_uid = "600-000003"
GROUP BY b.business_uid, owner_uid
UNION
SELECT tenant_uid AS contact_uid, "Tenant" AS contact_type, tenant_first_name AS contact_first_name, tenant_last_name AS contact_last_name, tenant_phone_number AS contact_phone_numnber, tenant_email AS contact_email, tenant_address AS contact_address, tenant_unit AS contact_unit, tenant_city AS contact_city, tenant_state AS contact_state, tenant_zip AS contact_zip
FROM space_prod.b_details AS b
LEFT JOIN space_prod.leases ON b.contract_property_id = lease_property_id
LEFT JOIN space_prod.t_details ON lease_uid = lt_lease_id
WHERE b.business_uid = "600-000003" AND lease_uid IS NOT NULL
GROUP BY b.business_uid, tenant_uid
UNION
SELECT m.business_uid AS contact_uid, "Business" AS contact_type, m.business_name AS contact_first_name, m.business_type AS contact_last_name, m.business_phone_number AS contact_phone_numnber, m.business_email AS contact_email, m.business_address AS contact_address, m.business_unit AS contact_unit, m.business_city AS contact_city, m.business_state AS contact_state, m.business_zip AS contact_zip
FROM space_prod.b_details AS b
LEFT JOIN space_prod.m_details ON contract_property_id = maintenance_property_id
LEFT JOIN space_prod.businessProfileInfo AS m ON quote_business_id = m.business_uid
WHERE b.business_uid = "600-000003" AND m.business_uid IS NOT NULL
GROUP BY b.business_uid, m.business_uid;







-- FIND ALL OWNER CONTACTS. (NOT USED)
SELECT -- *, 
	business_uid AS contact_uid, "Property Manager" AS contact_type, business_name AS contact_business_name, business_phone_number AS contact_phone_numnber, business_email AS contact_email, business_address AS contact_address, business_unit AS contact_unit, business_city AS contact_city, business_state AS contact_state, business_zip AS contact_zip
FROM (
	SELECT *
	FROM space_prod.o_details AS o
	LEFT JOIN space_prod.properties ON o.property_id = property_uid
	WHERE owner_uid = "110-000003"
) AS op
LEFT JOIN space_prod.b_details AS b ON b.contract_property_id = property_uid
WHERE b.contract_status IS NOT NULL
GROUP BY b.business_uid;


-- FIND ALL OWNER CONTACTS
SELECT -- *,
	business_uid AS contact_uid, "Property Manager" AS contact_type, business_name AS contact_business_name, business_phone_number AS contact_phone_numnber, business_email AS contact_email, business_address AS contact_address, business_unit AS contact_unit, business_city AS contact_city, business_state AS contact_state, business_zip AS contact_zip
	, business_ein_number, business_services_fees, business_locations, business_paypal, business_apple_pay, business_zelle, business_venmo, business_account_number, business_routing_number, business_documents
	-- property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, 
	, property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type
--     , property_num_beds, property_num_baths, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_owner_id-DNU, property_manager_id-DNU, property_appliances-DNU, property_appliance_id-DNU, property_utilities-DNU, property_utility_id-DNU
		, contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
FROM (
	SELECT *
	FROM space_prod.o_details AS o
	LEFT JOIN space_prod.properties ON o.property_id = property_uid
	WHERE owner_uid = "110-000003"
) AS op
LEFT JOIN space_prod.b_details AS b ON b.contract_property_id = property_uid
-- LEFT JOIN space_prod.contractFees ON contract_uid = contract_id
WHERE b.contract_status IS NOT NULL
GROUP BY b.business_uid, property_id;
