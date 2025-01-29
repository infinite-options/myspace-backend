-- CREATE TABLE space_prod.bills AS
-- SELECT * FROM pm.bills;
-- SELECT * FROM pm.leaseTenants;

SELECT * FROM space_prod.users;
SELECT * FROM space_prod.user_profiles;

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

SELECT * FROM space_prod.pp_status;
SELECT * FROM space_prod.pp_details;

-- MAINTENANCE STATUS BY OWNER
SELECT property_owner.property_owner_id
	, maintenanceRequests.maintenance_request_status
    -- , COUNT(maintenanceRequests.maintenance_request_status) AS num
FROM space_prod.properties
LEFT JOIN space_prod.property_owner ON property_id = property_uid
LEFT JOIN space_prod.maintenanceRequests ON maintenance_property_id = property_uid
WHERE property_owner_id = '110-000003'
GROUP BY maintenance_request_status;


-- MAINTENANCE STATUS BY OWNER
SELECT property_owner.property_owner_id
	, maintenanceRequests.maintenance_request_status
    , CASE maintenanceRequests.maintenance_request_status
           WHEN value1 THEN result1
           WHEN value2 THEN result2
           ...
           ELSE default_result
       END AS new_column
    , COUNT(maintenanceRequests.maintenance_request_status) AS num
FROM space_prod.properties
LEFT JOIN space_prod.property_owner ON property_id = property_uid
LEFT JOIN space_prod.maintenanceRequests ON maintenance_property_id = property_uid
WHERE property_owner_id = '110-000003'
GROUP BY maintenance_request_status;




-- MAINTENANCE PROJECTS BY PROPERTY        
SELECT -- *
	property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type, property_num_beds, property_num_baths, property_area
    , maintenance_request_uid, maintenance_property_id, maintenance_request_status, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
FROM space_prod.properties
LEFT JOIN space_prod.maintenanceRequests ON maintenance_property_id = property_uid
WHERE property_uid = '200-000029';







-- MAINTENANCE STATUS BY OWNER BY PROPERTY BY STATUS WITH ALL DETAILS
SELECT property_owner_id
	, property_uid, property_available_to_rent, property_active_date, property_address -- , property_unit, property_city, property_state, property_zip, property_type, property_num_beds, property_num_baths, property_area, property_listed_rent, property_images
    , maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
	, quote_earliest_availability, quote_event_duration, quote_notes, quote_total_estimate
FROM space_prod.maintenanceRequests 
LEFT JOIN space_prod.maintenanceQuotes ON quote_maintenance_request_id = maintenance_request_uid
LEFT JOIN space_prod.properties ON maintenance_property_id = property_uid	-- ASSOCIATE PROPERTY DETAILS WITH MAINTENANCE DETAILS
LEFT JOIN space_prod.property_owner ON property_id = property_uid 			-- SO WE CAN SORT BY OWNER
WHERE property_owner_id = '110-000003'
ORDER BY maintenance_request_status;

-- PROPERTY DETAILS INCLUDING MAINTENANCE      
SELECT property_uid, property_address
	, property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_type, property_num_beds, property_num_baths, property_area, property_listed_rent, property_images
    , maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
FROM space_prod.properties
LEFT JOIN space_prod.maintenanceRequests ON maintenance_property_id = property_uid		-- SO WE HAVE MAINTENANCE INFO
LEFT JOIN space_prod.property_owner ON property_id = property_uid 						-- SO WE CAN SORT BY OWNER
WHERE property_owner_id = '110-000003';







-- NEW MAINTENANCE STATUS QUERY
SELECT -- *
	maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, user_type, user_name, user_phone, user_email
    , maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status
    , maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
    , maintenance_quote_uid, quote_maintenance_request_id, quote_business_id, quote_services_expenses, quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes, quote_status, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
    , property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
    , o_details.*
FROM space_prod.m_details
LEFT JOIN space_prod.user_profiles ON maintenance_request_created_by = profile_uid
LEFT JOIN space_prod.properties ON property_uid = maintenance_property_id
LEFT JOIN space_prod.o_details ON maintenance_property_id = property_id
WHERE property_owner_id = "110-000003";



 -- NEW MAINTENANCE STATUS QUERY
SELECT -- *
	maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, user_type, user_name, user_phone, user_email
	, maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status
	, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
	, maintenance_quote_uid, quote_maintenance_request_id, quote_business_id, quote_services_expenses, quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes, quote_status, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
	, property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
FROM space_prod.m_details
LEFT JOIN space_prod.user_profiles ON maintenance_request_created_by = profile_uid
LEFT JOIN space_prod.properties ON property_uid = maintenance_property_id
WHERE property_owner_id = "110-000003";
-- ORDER BY maintenance_request_status;




SELECT * FROM space_prod.maintenanceRequests;

-- ADD NEW MAINTENACE REQUEST
INSERT INTO space_prod.maintenanceRequests
SET maintenance_request_uid = "800-000039"
,  maintenance_property_id = "200-000006"
,  maintenance_title = "SQL TEST"
,  maintenance_desc = "Testing the SQL query"
-- ,  maintenance_images = "[\"s3-us-west-1.amazonaws.com/io-pm/maintenanceRequests/800-000015/img_0\"]"
,  maintenance_images = "[\"s3-us-west-1.amazonaws.com/space-prod/maintenanceRequests/800-000015/img_0\"]"
,  maintenance_request_type = "Other"
,  maintenance_request_created_by = "600-000003"
,  maintenance_priority = "High"
,  maintenance_can_reschedule = "1"
,  maintenance_assigned_business = NULL
,  maintenance_assigned_worker = NULL
,  maintenance_scheduled_date = "800-000039"
,  maintenance_scheduled_time = NULL
,  maintenance_frequency = "One Time"
,  maintenance_notes = "Take Notes"
,  maintenance_request_status = "NEW"
,  maintenance_request_created_date = "2023-08-01"
,  maintenance_request_closed_date = NULL
,  maintenance_request_adjustment_date = NULL;


-- DELETE REUQEST FROM MAINTENANCE TABLE
DELETE FROM space_prod.maintenanceRequests
WHERE maintenance_request_uid = "800-000039";

-- THIS IS HOW TO FORMAT DATA USING THE NEW PYTHON ENTRY METHOD
INSERT INTO space_prod.maintenanceRequests 
(maintenance_property_id
, maintenance_title
, maintenance_desc
, maintenance_request_type
, maintenance_request_created_by
, maintenance_priority
, maintenance_can_reschedule
, maintenance_assigned_business
, maintenance_assigned_worker
, maintenance_scheduled_date
, maintenance_scheduled_time
, maintenance_frequency
, maintenance_notes
, maintenance_request_created_date
, maintenance_request_closed_date
, maintenance_request_adjustment_date
, maintenance_request_uid
, maintenance_request_status)
VALUES ('200-000006'
		, 'Testing New Maintenance Add Function'
        , 'Testing New Maintenance Add Function.  Adding the Maintenance item the old way first.'
        , 'Other'
        , '600-000003'
        , 'Low'
        , '1'
        , 'null'
        , 'null'
        , 'null'
        , 'null'
        , 'One Time'
        , 'null'
        , '2023-08-01'
        , 'null'
        , 'null'
        , '800-000051'
        , 'NEW')



;-- DEBUG maintenaceStatus

SELECT * FROM space_prod.maintenanceRequests;
SELECT * FROM space_prod.maintenanceQuotes;

SELECT -- *
	maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule
    , maintenance_assigned_business, maintenance_assigned_worker
    , maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
    , maintenance_callback_number, maintenance_estimated_cost
    , maintenance_quote_uid, quote_business_id, quote_services_expenses, quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes, quote_status, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
    -- , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type
    , bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
    , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
    , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
    , cf_month, cf_year
    , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email
    , initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email
    , payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email
    , property_address, property_unit, property_city, property_state, property_zip, property_type, property_images, property_description, property_notes
    , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_address, owner_unit, owner_city, owner_state, owner_zip
    , contract_start_date, contract_end_date, contract_status, business_name, business_phone_number, business_email, business_services_fees, business_locations, business_address, business_unit, business_city, business_state, business_zip
    , tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
-- 	business_uid,
-- 	property_uid, properties.property_address,
-- 	purchase_uid, purchase_status, purchase_type, 
-- 	payment_uid, pay_amount, payment_notes, payment_type,
-- 	maintenance_request_uid, maintenance_title, maintenance_desc, maintenance_request_type, maintenance_priority, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date,
-- 	maintenance_quote_uid, quote_services_expenses, quote_earliest_availability, quote_event_type, quote_notes, quote_status
FROM space_prod.m_details 
-- LEFT JOIN space_prod.properties ON maintenance_property_id = property_uid
LEFT JOIN space_prod.bills ON bill_maintenance_quote_id = maintenance_quote_uid
LEFT JOIN space_prod.pp_details ON pur_bill_id = bill_uid
WHERE quote_business_id = "600-000012" AND  quote_business_id IS NOT NULL
ORDER BY maintenance_request_created_date;




SELECT *
FROM space_prod.user_profiles;
SELECT * FROM space_prod.ownerProfileInfo;
SELECT * FROM space_prod.tenantProfileInfo;
SELECT * FROM space_prod.businessProfileInfo;


SELECT o.owner_uid, property_id, o.owner_user_id, o.owner_first_name, o.owner_last_name, o.owner_phone_number, o.owner_email, o.owner_address, o.owner_unit, o.owner_city, o.owner_state, o.owner_zip
	maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule
	, maintenance_assigned_business, maintenance_assigned_worker
	, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
	, maintenance_callback_number, maintenance_estimated_cost
	, maintenance_quote_uid, quote_business_id, quote_services_expenses, quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes, quote_status, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
	-- bill_uid,
	, purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
	, payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
	, cf_month, cf_year
	, receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email
	, initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email
	, payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email
	, property_address, property_unit, property_city, property_state, property_zip, property_type, property_images, property_description, property_notes
	, contract_start_date, contract_end_date, contract_status, business_name, business_phone_number, business_email, business_services_fees, business_locations, business_address, business_unit, business_city, business_state, business_zip
	, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
FROM o_details AS o
JOIN m_details ON maintenance_property_id = property_id
-- LEFT JOIN bills ON bill_maintenance_quote_id = maintenance_quote_uid
JOIN pp_details ON o.owner_uid = pp_details.owner_uid
WHERE o.owner_uid = -- \'""" + owner_uid + """\'
AND maintenance_quote_uid IS NOT NULL
GROUP BY maintenance_quote_uid
ORDER BY maintenance_request_created_date;


-- CHECK BUSINESS TYPE
SELECT -- *
	business_uid, business_type
FROM space_prod.businessProfileInfo
WHERE business_uid = "600-000003";

-- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
SELECT -- * -- bill_property_id,  maintenance_property_id,
	maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority
    , maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
    , maintenance_callback_number, maintenance_estimated_cost
    , maintenance_quote_uid, quote_maintenance_request_id, quote_business_id
    , quote_services_expenses -- WHERE DOES THIS COME FROM
    -- DO WE NEED PARTS INCLUDED? quote_parts (JSON Object), quote_parts_estimate ($), 
    , quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes
    , quote_status, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
    -- DO WE NEED FINAL INVOICE AMOUNTS OR DOES THAT GO INTO BILLS?
	, bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
    , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
    , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
    , cf_month, cf_year
    , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip
    , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date, business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_paypal, business_apple_pay, business_zelle, business_venmo, business_account_number, business_routing_number, business_documents, business_address, business_unit, business_city, business_state, business_zip
    , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_rent, lease_actual_rent, lt_lease_id, lt_tenant_id, lt_responsibility, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants
FROM space_prod.m_details
LEFT JOIN space_prod.bills ON bill_maintenance_quote_id = maintenance_quote_uid
LEFT JOIN space_prod.pp_status ON pur_bill_id = bill_uid
LEFT JOIN space_prod.o_details ON maintenance_property_id = property_id
LEFT JOIN space_prod.b_details ON maintenance_property_id = contract_property_id
LEFT JOIN space_prod.leases ON maintenance_property_id = lease_property_id
LEFT JOIN space_prod.t_details ON lt_lease_id = lease_uid
-- WHERE owner_uid = "110-000002"
-- WHERE business_uid = "600-000001"
-- WHERE tenant_uid = "350-000006"
WHERE quote_business_id = "600-000002"







; -- MAINTENANCE REQUEST BY OWNER, BUSINESS, TENENT OR PROPERTY
SELECT   * -- bill_property_id,  maintenance_property_id,
	-- maintenance_request_status, quote_status
	-- maintenanceRequests.*
	-- Properties
	--  property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_images
-- 	, property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
-- 	, business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, contract_status
-- 	, lease_uid, lease_status, lease_assigned_contacts,  tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
FROM space_prod.maintenanceRequests
LEFT JOIN space_prod.properties ON property_uid = maintenance_property_id
-- LEFT JOIN space_prod.bills ON bill_maintenance_quote_id = maintenance_quote_uid
-- LEFT JOIN space_prod.pp_status ON pur_bill_id = bill_uid
LEFT JOIN space_prod.o_details ON maintenance_property_id = property_id
LEFT JOIN (SELECT * FROM space_prod.b_details WHERE contract_status = "ACTIVE") AS b ON maintenance_property_id = contract_property_id
LEFT JOIN (SELECT * FROM space_prod.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
LEFT JOIN space_prod.t_details ON lt_lease_id = lease_uid
WHERE owner_uid = "110-000095"
-- WHERE owner_uid = \'""" + uid + """\'
-- WHERE business_uid = \'""" + uid + """\'
-- WHERE tenant_uid = \'""" + uid + """\'
-- WHERE quote_business_id = \'""" + uid + """\'
-- WHERE maintenance_property_id = "200-000076"
ORDER BY maintenance_request_created_date;
