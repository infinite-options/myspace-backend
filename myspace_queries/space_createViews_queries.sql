

-- VIEW CREATE PROPERTY-OWNER WITH OWNER PROFILE (po_details)
SELECT *
FROM space_prod.property_owner
LEFT JOIN space_prod.ownerProfileInfo ON property_owner_id = owner_uid;

-- VIEW CREATE CONTRACT WITH BUSINESS PROFILE (b_details)
SELECT * FROM space_prod.b_details;
SELECT *
FROM space_prod.contracts
LEFT JOIN space_prod.businessProfileInfo ON business_uid = contract_business_id
-- WHERE contract_status = 'ACTIVE' OR contract_status IS NULL;
WHERE contract_status != 'INACTIVE' OR contract_status IS NULL;

-- VIEW CREATE LEASE-TENANT WITH TENANT PROFILE (t_detail)
SELECT *
FROM space_prod.lease_tenant
LEFT JOIN space_prod.tenantProfileInfo ON tenant_uid = lt_tenant_id;


-- VIEW CREATE MAINTENANCE QUOTE & REQUESTS (m_detail)
SELECT *
FROM space_prod.maintenanceRequests
LEFT JOIN space_prod.maintenanceQuotes ON maintenance_request_uid = quote_maintenance_request_id;


-- VIEW CREATE PROPERTY-OWNER WITH OWNER PROFILE (o_detail)
SELECT *
FROM space_prod.property_owner
LEFT JOIN space_prod.ownerProfileInfo ON owner_uid = property_owner_id;



-- VIEW CREATION PURCHASE-PAYMENT-STATUS INCLUDING PAID LATE AND VACANT==> pp_status
-- pp_status is Puchases wth Summed Payments

-- SELECT *
-- 	, CASE
-- 		WHEN (total_paid >= pur_amount_due) AND (DATE(latest_date) > pur_due_date) THEN "PAID LATE"
--         WHEN (total_paid >= pur_amount_due)  THEN "PAID"
--         WHEN (total_paid = 0 OR ISNULL(total_paid)) THEN "UNPAID"
-- 		ELSE "PARTIALLY PAID"
-- 	  END AS payment_status
-- 	, IF(ISNULL(total_paid),pur_amount_due,(pur_amount_due - total_paid)) AS amt_remaining
--     , DATE_FORMAT(STR_TO_DATE(pur_due_date, '%m-%d-%Y'), "%M") AS cf_month
--     , DATE_FORMAT(STR_TO_DATE(pur_due_date, '%m-%d-%Y'), "%m") AS cf_month_num
--     , DATE_FORMAT(STR_TO_DATE(pur_due_date, '%m-%d-%Y'), "%Y") AS cf_year
-- FROM space_prod.purchases
-- LEFT JOIN (
-- 	SELECT *
-- 		, MAX(payment_date) AS latest_date
-- 		, SUM(pay_amount) AS total_paid
-- 	FROM space_prod.payments
-- 	GROUP BY pay_purchase_id
-- 	) pay  ON pay_purchase_id = purchase_uid
-- GROUP BY purchase_uid;


-- VIEW CREATION PURCHASE-PAYMENT-STATUS INCLUDING PAID LATE AND VACANT==> pp_status - UPDATED 9/20/2024
-- pp_status is Puchases wth Summed Payments.  No longer includes individual payment details
-- pp_status
SELECT *
	, CASE
		WHEN (total_paid >= pur_amount_due) AND (DATE(latest_date) > pur_due_date) THEN "PAID LATE"
        WHEN (total_paid >= pur_amount_due)  THEN "PAID"
        WHEN (total_paid = 0 OR ISNULL(total_paid)) THEN "UNPAID"
		ELSE "PARTIALLY PAID"
	  END AS payment_status
	, IF(ISNULL(total_paid),pur_amount_due,(pur_amount_due - total_paid)) AS amt_remaining
    , DATE_FORMAT(STR_TO_DATE(pur_due_date, '%m-%d-%Y'), "%M") AS cf_month
    , DATE_FORMAT(STR_TO_DATE(pur_due_date, '%m-%d-%Y'), "%m") AS cf_month_num
    , DATE_FORMAT(STR_TO_DATE(pur_due_date, '%m-%d-%Y'), "%Y") AS cf_year
FROM space_prod.purchases
LEFT JOIN (
	SELECT -- *, 
		pay_purchase_id
		, MAX(payment_date) AS latest_date
		, SUM(pay_amount) AS total_paid
        , IF( MIN(payment_verify) = 'verified', 'verified', 'unverified') AS verified
        , JSON_ARRAYAGG(payment_uid) AS payment_ids
	FROM space_prod.payments
	GROUP BY pay_purchase_id
	) pay  ON pay_purchase_id = purchase_uid
GROUP BY purchase_uid;

-- VIEW CREATION PURCHASE-PAYMENT-STATUS INCLUDING PAID LATE AND VACANT==> pp_status - UPDATED 9/20/2024
-- pp_status is Puchases wth Summed Payments.  No longer includes individual payment details
-- pp_status_updated
-- pp_details
SELECT *
	, JSON_LENGTH(payment_ids) AS num_payments
	, CASE
		WHEN (total_paid >= pur_amount_due) AND (DATE(latest_date) > pur_due_date) THEN "PAID LATE"
        WHEN (total_paid >= pur_amount_due)  THEN "PAID"
        WHEN (total_paid = 0 OR ISNULL(total_paid)) THEN "UNPAID"
		ELSE "PARTIALLY PAID"
	  END AS payment_status
	, IF(ISNULL(total_paid),pur_amount_due,(pur_amount_due - total_paid)) AS amt_remaining
    , DATE_FORMAT(STR_TO_DATE(pur_due_date, '%m-%d-%Y'), "%M") AS cf_month
    , DATE_FORMAT(STR_TO_DATE(pur_due_date, '%m-%d-%Y'), "%m") AS cf_month_num
    , DATE_FORMAT(STR_TO_DATE(pur_due_date, '%m-%d-%Y'), "%Y") AS cf_year
FROM space_prod.purchases
LEFT JOIN (
	SELECT -- *, 
		pay_purchase_id
		, MAX(payment_date) AS latest_date
		, SUM(pay_amount) AS total_paid
        , IF( MIN(payment_verify) = 'verified', 'verified', 'unverified') AS verified
        , JSON_ARRAYAGG(payment_uid) AS payment_ids_og
        , JSON_ARRAYAGG(
			JSON_OBJECT(
				'payment_id', payment_uid,
				'pay_amount', pay_amount,
                'payment_date', payment_date,
                'payment_verify', payment_verify,
                'payment_intent', payment_intent
			)
		) AS payment_ids
	FROM space_prod.payments
	GROUP BY pay_purchase_id
	) pay  ON pay_purchase_id = purchase_uid
GROUP BY purchase_uid;

-- VIEW CREATION PURCHASE-PAYMENT-STATUS ==> pp_details UPDATED
-- pp_details is pp_status (Purchases with summed Payments) with additional receiver, payer, property, owner business and tenant details

-- SELECT * FROM space_prod.user_profiles;

-- SELECT  -- *
-- 	purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
--     , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
--     , cf_month, cf_month_num, cf_year
--     , receiver.user_id AS receiver_user_id, receiver.profile_uid AS receiver_profile_uid, receiver.user_type AS receiver_user_type, receiver.user_name AS receiver_user_name, receiver.user_phone AS receiver_user_phone, receiver.user_email AS receiver_user_email
--     , initiator.user_id AS initiator_user_id, initiator.profile_uid AS initiator_profile_uid, initiator.user_type AS initiator_user_type, initiator.user_name AS initiator_user_name, initiator.user_phone AS initiator_user_phone, initiator.user_email AS initiator_user_email
--     , payer.user_id AS payer_user_id, payer.profile_uid AS payer_profile_uid, payer.user_type AS payer_user_type, payer.user_name AS payer_user_name, payer.user_phone AS payer_user_phone, payer.user_email AS payer_user_email
-- 	, property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
--     , property_images, property_description, property_notes
--     , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_address, owner_unit, owner_city, owner_state, owner_zip
--     , contract_start_date, contract_end_date, contract_status
--     , business_name, business_phone_number, business_email, business_services_fees, business_locations, business_address, business_unit, business_city, business_state, business_zip
--     , tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
-- FROM space_prod.pp_status
-- LEFT JOIN space_prod.user_profiles AS receiver ON pur_receiver = receiver.profile_uid
-- LEFT JOIN space_prod.user_profiles AS initiator ON pur_initiator = initiator.profile_uid
-- LEFT JOIN space_prod.user_profiles AS payer ON pur_payer = payer.profile_uid
-- LEFT JOIN space_prod.properties ON pur_property_id = property_uid
-- LEFT JOIN space_prod.o_details ON pur_property_id = property_id
-- LEFT JOIN space_prod.b_details ON pur_property_id = contract_property_id
-- LEFT JOIN space_prod.leases ON pur_property_id = lease_property_id
-- -- missing at least lease_fees
-- LEFT JOIN space_prod.t_details ON lt_lease_id = lease_uid
-- WHERE contract_status = "ACTIVE" OR ISNULL(contract_status);

-- RECREATED WITH EVERYTHING - SEEMS TO WORK 9/20/2024
-- SELECT  -- *
-- 	purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_description, pur_notes, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_receiver, pur_initiator, pur_payer, pur_late_Fee, pur_perDay_late_fee, pur_due_by, pur_late_by, pur_group, pur_leaseFees_id
--     , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, payment_intent, payment_method, payment_date_cleared, payment_client_secret, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_month_num, cf_year
--     -- , user_id, profile_uid, user_type, user_name, user_phone, user_email, user_id, profile_uid, user_type, user_name, user_phone, user_email, user_id, profile_uid, user_type, user_name, user_phone, user_email
--     , receiver.user_id AS receiver_user_id, receiver.profile_uid AS receiver_profile_uid, receiver.user_type AS receiver_user_type, receiver.user_name AS receiver_user_name, receiver.user_phone AS receiver_user_phone, receiver.user_email AS receiver_user_email
--     , initiator.user_id AS initiator_user_id, initiator.profile_uid AS initiator_profile_uid, initiator.user_type AS initiator_user_type, initiator.user_name AS initiator_user_name, initiator.user_phone AS initiator_user_phone, initiator.user_email AS initiator_user_email
--     , payer.user_id AS payer_user_id, payer.profile_uid AS payer_profile_uid, payer.user_type AS payer_user_type, payer.user_name AS payer_user_name, payer.user_phone AS payer_user_phone, payer.user_email AS payer_user_email
--     , property_uid, property_available_to_rent, property_active_date, property_listed_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image
--     , property_id, property_owner_id, po_owner_percent, po_start_date, po_end_date
--     , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
--     , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
--     , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
--     , lease_uid, lease_property_id, lease_application_date, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, lease_move_in_date, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_consent, lease_actual_rent, lease_end_notice_period, lease_end_reason
--     , lt_lease_id, lt_tenant_id, lt_responsibility
--     , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
-- FROM space_prod.pp_status
-- LEFT JOIN space_prod.user_profiles AS receiver ON pur_receiver = receiver.profile_uid
-- LEFT JOIN space_prod.user_profiles AS initiator ON pur_initiator = initiator.profile_uid
-- LEFT JOIN space_prod.user_profiles AS payer ON pur_payer = payer.profile_uid
-- LEFT JOIN space_prod.properties ON pur_property_id = property_uid
-- LEFT JOIN space_prod.o_details ON pur_property_id = property_id
-- LEFT JOIN space_prod.b_details ON pur_property_id = contract_property_id
-- LEFT JOIN space_prod.leases ON pur_property_id = lease_property_id
-- LEFT JOIN space_prod.t_details ON lt_lease_id = lease_uid
-- WHERE contract_status = "ACTIVE" OR ISNULL(contract_status);



-- VIEW CREATION PURCHASE-PAYMENT-STATUS ==> pp_details UPDATED 9/20/2024
-- Removed payment details -- Check remaining columns
-- SELECT  -- *
-- 	pp_status.*
--     , receiver.user_id AS receiver_user_id, receiver.profile_uid AS receiver_profile_uid, receiver.user_type AS receiver_user_type, receiver.user_name AS receiver_user_name, receiver.user_phone AS receiver_user_phone, receiver.user_email AS receiver_user_email
--     , initiator.user_id AS initiator_user_id, initiator.profile_uid AS initiator_profile_uid, initiator.user_type AS initiator_user_type, initiator.user_name AS initiator_user_name, initiator.user_phone AS initiator_user_phone, initiator.user_email AS initiator_user_email
--     , payer.user_id AS payer_user_id, payer.profile_uid AS payer_profile_uid, payer.user_type AS payer_user_type, payer.user_name AS payer_user_name, payer.user_phone AS payer_user_phone, payer.user_email AS payer_user_email
-- 	
--     , property_uid -- , property_available_to_rent, property_active_date, property_listed_date
--     , property_address, property_unit, property_city, property_state, property_zip -- , property_longitude, property_latitude
--     , property_type -- , property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent
--     , property_images -- , property_taxes, property_mortgages, property_insurance, property_featured
--     , property_description, property_notes -- , property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image
--     
--     , property_id, property_owner_id, po_owner_percent -- , po_start_date, po_end_date
--     , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email -- , owner_ein_number, owner_ssn
--     , owner_address, owner_unit, owner_city, owner_state, owner_zip -- , owner_documents, owner_photo_url
--     , contract_uid -- , contract_property_id, contract_business_id
--     , contract_start_date, contract_end_date -- , contract_fees, contract_assigned_contacts, contract_documents, contract_name
--     , contract_status -- , contract_early_end_date
--     , business_uid, business_user_id, business_type
--     , business_name, business_phone_number, business_email-- , business_ein_number
--     , business_services_fees, business_locations -- , business_documents
--     , business_address, business_unit, business_city, business_state, business_zip-- , business_photo_url
--     -- , lease_uid, lease_property_id, lease_application_date, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, lease_move_in_date, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_consent, lease_actual_rent, lease_end_notice_period, lease_end_reason
-- 	-- , lt_lease_id, lt_tenant_id, lt_responsibility
--     , tenant_uid, tenant_user_id
--     , tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url

-- FROM space_prod.pp_status
-- LEFT JOIN space_prod.user_profiles AS receiver ON pur_receiver = receiver.profile_uid
-- LEFT JOIN space_prod.user_profiles AS initiator ON pur_initiator = initiator.profile_uid
-- LEFT JOIN space_prod.user_profiles AS payer ON pur_payer = payer.profile_uid
-- LEFT JOIN space_prod.properties ON pur_property_id = property_uid
-- LEFT JOIN space_prod.o_details ON pur_property_id = property_id
-- LEFT JOIN (SELECT * FROM space_prod.b_details WHERE contract_status = 'ACTIVE') AS b ON pur_property_id = contract_property_id
-- LEFT JOIN (SELECT * FROM space_prod.leases WHERE lease_status = 'ACTIVE') AS l ON pur_property_id = lease_property_id
-- LEFT JOIN space_prod.t_details ON lt_lease_id = lease_uid












;

-- VIEW CREATION PROPERTY ==> p_details (UPDATED 6/19/2024)
SELECT *  
FROM space_prod.properties
LEFT JOIN space_prod.u_details ON property_uid = utility_property_id
LEFT JOIN space_prod.o_details ON property_uid = o_details.property_id
LEFT JOIN (SELECT * FROM space_prod.b_details WHERE contract_status = 'ACTIVE') AS pm ON property_uid = contract_property_id
LEFT JOIN (SELECT * FROM space_prod.leases WHERE lease_status = 'ACTIVE' OR lease_status = 'ACTIVE M2M') AS l ON property_uid = lease_property_id
LEFT JOIN space_prod.t_details ON lease_uid = lt_lease_id
LEFT JOIN (
	SELECT fees_lease_id,
	JSON_ARRAYAGG(
           JSON_OBJECT(
               'leaseFees_uid', leaseFees_uid,
               'fees_lease_id', fees_lease_id,
               'fee_name', fee_name,
               'fee_type', fee_type, 
               'charge', charge, 
               'due_by', due_by,
               'due_by_date', due_by_date,
               'late_by', late_by, 
               'late_fee', late_fee, 
               'perDay_late_fee', perDay_late_fee, 
               'frequency', frequency, 
               'available_topay', available_topay
           )
       ) AS lease_fees
	FROM space_prod.leaseFees
	GROUP BY fees_lease_id) AS f ON fees_lease_id = lease_uid
LEFT JOIN (
	SELECT appliance_property_id,
	JSON_ARRAYAGG(
		JSON_OBJECT(
			'appliance_uid', appliance_uid,
			'appliance_property_id', appliance_property_id,
			'appliance_type', appliance_type,
            'appliance_desc', appliance_desc,
			'appliance_url', appliance_url,
			'appliance_images', appliance_images,
            'appliance_favorite_image', appliance_favorite_image,
            'appliance_documents', appliance_documents,
			'appliance_available', appliance_available,
			'appliance_installed', appliance_installed,
			'appliance_model_num', appliance_model_num,
			'appliance_purchased', appliance_purchased,
			'appliance_serial_num', appliance_serial_num,
			'appliance_manufacturer', appliance_manufacturer,
			'appliance_warranty_info', appliance_warranty_info,
			'appliance_warranty_till', appliance_warranty_till,
			'appliance_purchased_from', appliance_purchased_from,
			'appliance_purchase_order', appliance_purchase_order,
            'appliance_category', list_category,
            'appliance_item', list_item
		)
    ) AS appliances
	FROM space_prod.appliances 
	LEFT JOIN space_prod.lists ON appliance_type = list_uid 
    GROUP BY appliance_property_id) AS a ON property_uid = a.appliance_property_id


;
SELECT * FROM space_prod.users;
SELECT * FROM space_prod.user_profile;
SELECT * FROM space_prod.ownerProfileInfo;
SELECT * FROM space_prod.tenantProfileInfo;
SELECT * FROM space_prod.businessProfileInfo;

SELECT 
	owner_user_id AS user_id
    ,owner_uid as profile_uid
	, "OWNER" AS user_type
    , CONCAT(owner_first_name, " ", owner_last_name) AS user_name
    , owner_phone_number AS user_phone
    , owner_email AS user_email
    FROM space_prod.ownerProfileInfo
UNION
SELECT 
	tenant_user_id AS user_id 
    , tenant_uid as profile_uid
    , "TENANT" AS user_type
    , CONCAT(tenant_first_name, " ", tenant_last_name) AS user_name
    , tenant_phone_number AS user_phone
    , tenant_email AS user_email
    FROM space_prod.tenantProfileInfo
UNION
SELECT 
	business_user_id AS user_id 
    , business_uid as profile_uid
    , business_type  AS user_type
    , business_name AS user_name
    , business_phone_number  AS user_phone
    , business_email AS user_email
    FROM space_prod.businessProfileInfo;



-- VIEW CREATION PROPERTY ==> doc_details
SELECT 
	td_uid AS doc_uid
	, td_tenant_id AS doc_party_id
	, td_created_date AS doc_created_data
	, td_type AS doc_type
	, td_name AS doc_name
	, td_description AS doc_desc
	, td_shared AS doc_shared
	, td_link AS doc_link
	FROM space_prod.tenantDocuments
UNION
SELECT
	ld_uid AS doc_uid
	, ld_lease_id AS doc_party_id
	, ld_created_date AS doc_created_data
	, ld_type AS doc_type
	, ld_name AS doc_name
	, ld_description AS doc_desc
	, ld_shared AS doc_shared
	, ld_link AS doc_link
FROM space_prod.leaseDocuments
UNION
SELECT
	'010-000011' AS doc_uid
	, '200-000001' AS doc_party_id
	, '2022-10-01' AS doc_created_data
	, 'PMCONTRACT' AS doc_type
	, 'Williams Blvd Contract - All Units' AS doc_name
	, 'Williams Blvd Contract - All Units' AS doc_desc
	, 'false' AS doc_shared
	-- , "https://s3-us-west-1.amazonaws.com/io-pm/contracts/010-000011/doc_0" AS doc_link;
	, "https://s3-us-west-1.amazonaws.com/space-prod/contracts/010-000011/doc_0" AS doc_link;

SELECT * FROM space_prod.doc_details;



-- VIEW CREATION UTILITIES ==> u_details
SELECT utility_property_id, 
		JSON_ARRAYAGG(
			JSON_OBJECT(
				'utility_type_id', utility_type_id,
				 'utility_desc', utility,
				 'utility_payer_id', utility_payer_id,
                 'utility_payer', responsible_party)
			) AS property_utilities
FROM (
	SELECT pu.*, l1.list_item AS utility , l2.list_item AS responsible_party
	FROM space_prod.property_utility pu
	LEFT JOIN space_prod.lists l1 ON utility_type_id = l1.list_uid
    LEFT JOIN space_prod.lists l2 ON utility_payer_id = l2.list_uid
	) AS u
GROUP BY utility_property_id
;




    
   -- VIEW CREATION UTILITIES ==> t_details 
SELECT *
FROM space_prod.lease_tenant
LEFT JOIN space_prod.tenantProfileInfo ON tenant_uid = lt_tenant_id;



-- VIEW CREATION QUOTE RANK ==> m_quote_rank
SELECT *,
			CASE
				WHEN max_quote_rank = "10" THEN "REQUESTED"
				WHEN max_quote_rank = "11" THEN "REFUSED"
				WHEN max_quote_rank = "20" THEN "SENT"
				WHEN max_quote_rank = "21" THEN "REJECTED"
				WHEN max_quote_rank = "22" THEN "WITHDRAWN"
				WHEN max_quote_rank = "30" THEN "ACCEPTED"
				WHEN max_quote_rank = "40" THEN "SCHEDULE"
				WHEN max_quote_rank = "50" THEN "SCHEDULED"
				WHEN max_quote_rank = "60" THEN "RESCHEDULED"
				WHEN max_quote_rank = "70" THEN "FINISHED"
				WHEN max_quote_rank = "80" THEN "COMPLETED"
				ELSE "0"
			END AS quote_status
		FROM 
		(
			SELECT -- maintenance_quote_uid, 
				quote_maintenance_request_id AS qmr_id
				-- , quote_status
				, MAX(quote_rank) AS max_quote_rank
			FROM (
				SELECT -- *,
					maintenance_quote_uid, quote_maintenance_request_id, quote_status,
					-- , quote_pm_notes, quote_business_id, quote_services_expenses, quote_earliest_available_date,quote_earliest_available_time, quote_event_type, quote_event_duration, quote_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
				CASE
					WHEN quote_status = "REQUESTED" THEN "10"
					WHEN quote_status = "REFUSED" THEN "11"
					WHEN quote_status = "SENT" THEN "20"
					WHEN quote_status = "REJECTED" THEN "21"
					WHEN quote_status = "WITHDRAWN"  THEN "22"
					WHEN quote_status = "ACCEPTED" THEN "30"
					WHEN quote_status = "SCHEDULE" THEN "40"
					WHEN quote_status = "SCHEDULED" THEN "50"
					WHEN quote_status = "RESCHEDULED" THEN "60"
					WHEN quote_status = "FINISHED" THEN "70"
					WHEN quote_status = "COMPLETED" THEN "80"     
					ELSE 0
				END AS quote_rank
				FROM space_prod.maintenanceQuotes
				) AS qr
			GROUP BY quote_maintenance_request_id
			) AS qr_quoterank
