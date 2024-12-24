-- SIMPLIFIED RENT STATUS
SELECT -- *,
	contract_business_id
	, rent_status
	, COUNT(rent_status) AS num
FROM (
SELECT -- *,
	property_uid
    , rs.*
    -- , property_available_to_rent, property_active_date, property_listed_date
    , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths
    -- , property_value, property_value_year
    , property_area
    -- , property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent
    , property_images
    -- , property_taxes, property_mortgages, property_insurance
    , property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image
	, property_utilities, po_owner_percent
    , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
    -- , owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents
    , owner_photo_url
    , contract_uid, contract_property_id, contract_business_id
    -- , contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name
    , contract_status
    -- , contract_early_end_date
    , business_uid, business_type, business_name, business_phone_number, business_email
    -- , business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip
    , business_photo_url
    , lease_uid, lease_start, lease_end, if(ISNULL(lease_status),'VACANT',lease_status) AS lease_status
    -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_rent_available_topay, lease_actual_rent, leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees
    , lt_lease_id, lt_tenant_id, lt_responsibility
    , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
    -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants
    , tenant_photo_url
    , CASE
			WHEN ISNULL(lease_status) THEN 'VACANT'
			ELSE purchase_status
		END AS rent_status
FROM space_dev.p_details
LEFT JOIN (
		-- PROPERTY RENT STATUS
		-- GROUP BY PROPERTY
		SELECT -- *
			-- purchase_uid, pur_timestamp, 
			pur_property_id, purchase_type, pur_cf_type
			-- , pur_bill_id, purchase_date, pur_due_date, pur_amount_due, 
			, purchase_status
			-- , pur_status_value, pur_notes, pur_description
			, pur_receiver, pur_initiator, pur_payer
			-- , pay_purchase_id
			, latest_pay_date, total_paid, amt_remaining
			, cf_month, cf_year
			, SUM(pur_amount_due) AS pur_amount_due
			, MIN(pur_status_value) AS pur_status_value
		FROM (
			-- GET PURCHASES AND AMOUNT REMAINING
			SELECT *
				, (pur_amount_due - total_paid) AS amt_remaining
				, MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y'))AS cf_month
				, YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y'))AS cf_year
			FROM space_dev.purchases
			LEFT JOIN (
				-- GET PAYMENTS BY PURCHASE ID
				SELECT pay_purchase_id
					-- , pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, payment_intent, payment_method, payment_date_cleared, payment_client_secret
					, MAX(payment_date) AS latest_pay_date
					, SUM(pay_amount) AS total_paid
				FROM space_dev.payments
				GROUP BY pay_purchase_id
				) pay  ON pay_purchase_id = purchase_uid
			) pp 
		WHERE purchase_type LIKE "%Rent%"
		GROUP BY pur_property_id, cf_month, cf_year
	) AS rs ON property_uid = pur_property_id
    
    WHERE owner_uid = "110-000003" AND ( cf_month = 3 OR ISNULL(cf_month))
    -- WHERE business_uid = "600-000003" AND ( cf_month = 3 OR ISNULL(cf_month))
	) AS r	
GROUP BY rent_status;
    
  -- ---------------------------------------------
  
-- FURTHER SIMPLIFIED RENT STATUS
SELECT -- *,
	rent_status
	, COUNT(rent_status) AS num
FROM (
	SELECT -- *,
		property_uid, owner_uid, po_start_date, po_end_date, contract_business_id, contract_status, contract_start_date, contract_end_date, contract_early_end_date , lease_status , rs.*
		, CASE
			WHEN ISNULL(lease_status) THEN 'VACANT'
			ELSE purchase_status
		END AS rent_status
	FROM space_dev.p_details
	LEFT JOIN (
			-- PROPERTY RENT STATUS
			-- GROUP BY PROPERTY
			SELECT -- *
				pur_property_id, purchase_type, pur_cf_type, purchase_status, pur_receiver, pur_initiator, pur_payer, latest_pay_date, cf_month, cf_year
				, SUM(pur_amount_due) AS pur_amount_due
				, SUM(total_paid) AS total_paid
				, MIN(pur_status_value) AS pur_status_value
			FROM (
				-- GET PURCHASES AND AMOUNT REMAINING
				SELECT *
					, MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y'))AS cf_month
					, YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y'))AS cf_year
				FROM space_dev.purchases
				LEFT JOIN (
					-- GET PAYMENTS BY PURCHASE ID
					SELECT pay_purchase_id
						-- , pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, payment_intent, payment_method, payment_date_cleared, payment_client_secret
						, MAX(payment_date) AS latest_pay_date
						, SUM(pay_amount) AS total_paid
					FROM space_dev.payments
					GROUP BY pay_purchase_id
					) pay  ON pay_purchase_id = purchase_uid
				) pp 
			WHERE purchase_type LIKE "%Rent%"
			GROUP BY pur_property_id, cf_month, cf_year
		) AS rs ON property_uid = pur_property_id
		-- WHERE owner_uid = "110-000003" AND ( cf_month = MONTH(CURRENT_DATE) OR ISNULL(cf_month))
		WHERE business_uid = "600-000003" AND ( cf_month = MONTH(CURRENT_DATE) OR ISNULL(cf_month))
	) AS r	
GROUP BY rent_status;

    