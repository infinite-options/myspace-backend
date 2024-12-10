-- CREATE TABLE space.bills AS
-- SELECT * FROM pm.bills;
-- SELECT * FROM pm.leaseTenants;

SELECT * FROM space.users;
SELECT * FROM space.user_profile;

SELECT * FROM space.ownerProfileInfo;
SELECT * FROM space.property_owner;
SELECT * FROM space.properties;
SELECT * FROM space.appliances;
SELECT * FROM space.maintenanceRequests;
SELECT * FROM space.maintenanceQuotes;

SELECT * FROM space.tenantProfileInfo;
SELECT * FROM space.property_tenant;
SELECT * FROM space.lease_tenant;
SELECT * FROM space.leases;

SELECT * FROM space.businessProfileInfo;
SELECT * FROM space.contracts;
SELECT * FROM space.contractFees;

SELECT * FROM space.purchases;
SELECT * FROM space.payments;
SELECT * FROM space.bills;
SELECT * FROM space.lists;
SELECT * FROM space.property_utility;

SELECT * FROM space.pp_details;
SELECT * FROM space.pp_status;

-- CASHFLOW BY BY OWNER BY MONTH
SELECT pp.*
	, property_owner_id -- , po_owner_percent
    , property_uid, property_address, property_unit, property_city, property_state, property_zip,  property_description, property_notes
FROM (
	SELECT *
		, sum(pay_amount) AS paid_amount
		, IF (pur_amount_due <= sum(pay_amount), "PAID", "UNPAID") AS payment_status
		, (pur_amount_due - sum(pay_amount)) AS delta
	FROM space.purchases
	LEFT JOIN space.payments ON pay_purchase_id = purchase_uid
	GROUP BY purchase_uid
    ) AS pp
LEFT JOIN space.properties ON property_uid = pur_property_id
LEFT JOIN space.property_owner ON property_uid = property_id
WHERE property_owner_id = "110-000003"
GROUP BY purchase_type;



-- ORIGINAL QUERY
 SELECT prop.owner_id, prop.property_uid, address, unit, city, state, zip, 
	pur.*, 
	DATE_FORMAT(next_payment, "%M") AS month, 
	DATE_FORMAT(next_payment, "%Y") AS year
FROM pm.properties prop
LEFT JOIN pm.purchases pur
ON pur_property_id LIKE CONCAT ('%',"200-000029", '%')
WHERE pur.receiver = "100-000003"   
AND pur.purchase_status <> 'DELETED'            
AND (YEAR(pur.next_payment) = "2023")
ORDER BY address,unit ASC;


SELECT prop.owner_id, prop.property_uid, address, unit, city,
	state, zip, 
	pur.*, 
	DATE_FORMAT(next_payment, "%M") AS month, 
	DATE_FORMAT(next_payment, "%Y") AS year
FROM pm.properties prop
LEFT JOIN pm.purchases pur
ON pur_property_id LIKE CONCAT ('%',prop.property_uid, '%')
WHERE pur.receiver = '110-000003'    
AND pur.purchase_status <> 'DELETED'            
AND (YEAR(pur.next_payment) = '2023')
ORDER BY address,unit ASC;

-- CASHFLOW BY OWNER BY YEAR
SELECT * FROM space.pp_status
WHERE pur_receiver = '110-000003'
	AND purchase_status != 'DELETED'
    AND cf_year = '2023';
    
    
-- WHAT DID AN OWNER RECEIVE
-- CASHFLOW BY OWNER BY MONTH, BY YEAR
SELECT * FROM space.pp_details; 

SELECT purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status
, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by
, sum_paid_amount, payment_status, amt_remaining, cf_month, cf_year
, property_address, property_unit, property_city, property_state, property_zip, property_type, property_images, property_description, property_notes, po_owner_percent
, owner_uid, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_address, owner_unit, owner_city, owner_state, owner_zip
, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts
, business_name, business_phone_number, business_email, business_services_fees, business_locations, business_address, business_unit, business_city, business_state, business_zip
, lease_start, lease_end, lease_status, lease_rent, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_early_end_date, lease_renew_status, lease_actual_rent, lease_effective_date, lt_responsibility
, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number 
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND purchase_status != 'DELETED'
    AND cf_year = '2023'
GROUP BY purchase_type;






-- ALL TRANSACTIONS AFFECTING A PARTICULAR OWNER
SELECT -- * , 
	cf_month, cf_year, pur_due_date
	, purchase_type, pur_cf_type
    , pur_amount_due, sum_paid_amount, payment_status, amt_remaining
    , property_address, property_unit
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND purchase_status != 'DELETED'
    AND cf_year = '2023';
    
    
-- OWNER CASHFLOW BY MONTH
SELECT -- * , 
	cf_month, cf_year, pur_due_date
	-- , purchase_type
    , pur_cf_type
    , SUM(pur_amount_due), SUM(sum_paid_amount), payment_status, SUM(amt_remaining)
    -- , property_address, property_unit
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND purchase_status != 'DELETED'
    AND cf_year = '2023'
GROUP BY pur_cf_type, cf_month, cf_year
ORDER BY pur_due_date ASC;

    
-- OWNER CASHFLOW BY MONTH BY PURCHASE TYPE   
SELECT -- * , 
	cf_month, cf_year, pur_due_date
	, purchase_type, pur_cf_type
    , SUM(pur_amount_due), SUM(sum_paid_amount), payment_status, SUM(amt_remaining)
    -- , property_address, property_unit
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND purchase_status != 'DELETED'
    AND cf_year = '2023'
GROUP BY purchase_type, pur_cf_type, cf_month, cf_year
ORDER BY pur_due_date ASC;





-- OWNER CASHFLOW BY PROPERTY
SELECT -- * , 
	cf_month, cf_year, pur_due_date
	, purchase_type
    , pur_cf_type
    , SUM(pur_amount_due), SUM(sum_paid_amount), payment_status, SUM(amt_remaining)
    , property_address, property_unit
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND purchase_status != 'DELETED'
    AND cf_year = '2023'
GROUP BY pur_cf_type, property_address, property_unit;


-- OWNER CASHFLOW BY PROPERTY BY MONTH
SELECT -- * , 
	cf_month, cf_year, pur_due_date
	, purchase_type
    , pur_cf_type
    , SUM(pur_amount_due), SUM(sum_paid_amount), payment_status, SUM(amt_remaining)
    , property_address, property_unit
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND purchase_status != 'DELETED'
    AND cf_year = '2023'
GROUP BY  pur_cf_type, property_address, property_unit, cf_month, cf_year
ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;





-- -------------------------------------------- BY REVENUE --------------------------------------------------------------


-- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER
SELECT -- * , 
	cf_year, cf_month, pur_due_date
	, pur_cf_type, purchase_type
    , pur_amount_due, sum_paid_amount, amt_remaining, payment_status
    , property_address, property_unit
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND cf_year = '2023'
	AND purchase_status != 'DELETED'
    AND pur_cf_type = 'revenue'
 ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;

-- ALL EXPENSES TRANSACTIONS AFFECTING A PARTICULAR OWNER
SELECT -- * , 
	cf_year, cf_month, pur_due_date
	, pur_cf_type, purchase_type
    , pur_amount_due, sum_paid_amount, amt_remaining, payment_status
    , property_address, property_unit
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND cf_year = '2023'
	AND purchase_status != 'DELETED'
    AND pur_cf_type = 'expense'
 ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
    
-- ALL OTHER TRANSACTIONS AFFECTING A PARTICULAR OWNER
SELECT -- * , 
	cf_year, cf_month, pur_due_date
	, pur_cf_type, purchase_type
    , pur_amount_due, sum_paid_amount, amt_remaining, payment_status
    , property_address, property_unit
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND cf_year = '2023'
	AND purchase_status != 'DELETED'
    AND (pur_cf_type != 'expense' AND pur_cf_type != 'revenue')
ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;





-- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY YEAR
SELECT -- * , 
	cf_year -- , cf_month, pur_due_date
	, pur_cf_type -- , purchase_type
    -- , sum(pur_amount_due), sum(sum_paid_amount), sum(amt_remaining) -- , payment_status
    , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
    , property_address, property_unit
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND cf_year = '2023'
	AND purchase_status != 'DELETED'
    AND pur_cf_type = 'revenue'
    GROUP BY property_address, property_unit
	ORDER BY property_address ASC, property_unit ASC;
    
-- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH
SELECT -- * , 
	cf_year, cf_month -- , pur_due_date
	, pur_cf_type -- , purchase_type
    , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
    , property_address, property_unit
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND cf_year = '2023'
	AND purchase_status != 'DELETED'
    AND pur_cf_type = 'revenue'
    GROUP BY property_address, property_unit, cf_month
    ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;

SELECT * FROM space.pp_details WHERE pur_property_id = "200-000006" AND purchase_type = "RENT" -- AND cf_month = "September";
   ;
   
-- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH BY PURCHASE TYPE
SELECT -- * , 
	cf_year, cf_month -- , pur_due_date
	, pur_cf_type, purchase_type
    , sum(pur_amount_due), sum(sum_paid_amount), sum(amt_remaining) -- , payment_status
    , property_address, property_unit
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND cf_year = '2023'
	AND purchase_status != 'DELETED'
    AND pur_cf_type = 'revenue'
    GROUP BY property_address, property_unit, cf_month,  purchase_type
    ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;






-- -------------------------------------------- END BY REVENUE --------------------------------------------------------------


-- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER
SELECT -- * , 
	cf_year, cf_month, pur_due_date
	, pur_cf_type, purchase_type
    , pur_amount_due, sum_paid_amount, amt_remaining, payment_status
    , property_address, property_unit
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND pur_due_date > DATE_SUB(NOW(), INTERVAL 365 DAY)
	AND purchase_status != 'DELETED'
    AND pur_cf_type = 'revenue'
 ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;














-- DASHBOARD QUERY
SELECT -- * , 
	cf_month, cf_year, pur_due_date
	, purchase_type, pur_cf_type
    , pur_amount_due, sum_paid_amount, payment_status, amt_remaining
    , property_address, property_unit
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND purchase_status != 'DELETED'
    AND cf_year = '2023'
GROUP BY purchase_type, pur_cf_type, cf_month, cf_year
ORDER BY pur_due_date ASC;


-- CF BY CATEGORY
SELECT * 
FROM space.pp_details
WHERE owner_uid = '110-000003'
	AND purchase_status != 'DELETED'
    AND cf_year = '2023'
GROUP BY pur_cf_type, cf_month, cf_year;



-- -------------------------------------------- REDUCED ENDPOINT --------------------------------------------------------------


SELECT -- *
	cf_month, cf_year
    , property_uid, property_address, property_unit, property_city, property_state, property_zip
    , purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, sum_paid_amount, payment_status
    , purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
FROM space.properties
LEFT JOIN space.property_owner ON property_uid = property_id
LEFT JOIN space.pp_status ON property_uid = pur_property_id;





-- -------------------------------------------- CURRENT LIVE ENDPOINTS --------------------------------------------------------------
-- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY YEAR
	SELECT -- * , 
		cf_year -- , cf_month, pur_due_date
		, pur_cf_type -- , purchase_type
		, sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
		, property_address, property_unit
	FROM space.pp_details
	WHERE receiver_profile_uid = '110-000003'
		-- receiver_profile_uid = \'""" + user_id + """\'
		AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
		AND purchase_status != 'DELETED'
		AND (pur_cf_type = 'REVENUE' OR pur_cf_type = 'revenue')
		GROUP BY property_address, property_unit
		ORDER BY property_address ASC, property_unit ASC;
                        
-- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH
	SELECT -- * , 
		cf_year, cf_month -- , pur_due_date
		, pur_cf_type -- , purchase_type
		, sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
		, property_address, property_unit
	FROM space.pp_details
	WHERE receiver_profile_uid = '110-000003'
		-- receiver_profile_uid = \'""" + user_id + """\'
		AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
		AND purchase_status != 'DELETED'
		AND (pur_cf_type = 'REVENUE' OR pur_cf_type = 'revenue')
		GROUP BY property_address, property_unit, cf_month, cf_year
		ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
					
                    
-- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH BY PURCHASE TYPE
	SELECT -- * , 
		cf_year, cf_month -- , pur_due_date
		, pur_cf_type, purchase_type
		, sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
		, property_address, property_unit
	FROM space.pp_details
	WHERE receiver_profile_uid = '110-000003'
		-- receiver_profile_uid = \'""" + user_id + """\'
		AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
		AND purchase_status != 'DELETED'
		AND (pur_cf_type = 'REVENUE' OR pur_cf_type = 'revenue')
		GROUP BY property_address, property_unit, cf_month, cf_year, purchase_type
		ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
        
        
 -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER
	SELECT -- * , 
		purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
        -- , pur_amount_paid-DNU, purchase_frequency-DNU, payment_frequency-DNU, linked_tenantpur_id-DNU
        , pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_year
        -- , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email
        -- , initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email
        -- , payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email
        , property_uid
        -- , property_available_to_rent, property_active_date
        , property_address, property_unit, property_city, property_state, property_zip
        -- , property_longitude, property_latitude
        -- , property_type, property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_id, property_owner_id, po_owner_percent
        -- , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_photo_url
        -- , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
        -- , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_paypal, business_apple_pay, business_zelle, business_venmo, business_account_number, business_routing_number, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
        -- , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_rent_available_topay, lease_actual_rent, leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees
        -- , lt_lease_id, lt_tenant_id, lt_responsibility
        -- , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_current_address-DNU, tenant_photo_url
        , bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
        -- , bill_requested_from-DNU, bill_pay_to-DNU, bill_to-DNU, bill_from-DNU
        
        -- , cf_year, cf_month, pur_due_date, pur_cf_type, purchase_type, pur_amount_due, total_paid, amt_remaining, payment_status, property_uid, property_address, property_unit, bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes, bill_requested_from-DNU, bill_pay_to-DNU, bill_to-DNU, bill_from-DNU
        
		-- cf_year, cf_month, pur_due_date
-- 		, pur_cf_type, purchase_type
-- 		, pur_amount_due, total_paid, amt_remaining, payment_status
-- 		, property_uid, property_address, property_unit
-- 		, space.bills.*
	FROM space.pp_details
	LEFT JOIN space.bills ON pur_bill_id = bill_uid AND pur_bill_id IS NOT NULL
	WHERE receiver_profile_uid = '110-000003'
		-- receiver_profile_uid = \'""" + user_id + """\'
		AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
		AND purchase_status != 'DELETED'
		AND (pur_cf_type = 'REVENUE' OR pur_cf_type = 'revenue')
	ORDER BY pur_timestamp DESC;
	-- ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
    
    
-- ALL EXPENSE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY YEAR
	SELECT -- * , 
		cf_year -- , cf_month, pur_due_date
		, pur_cf_type -- , purchase_type
		, sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
		, property_address, property_unit
	FROM space.pp_details
	WHERE pur_payer = '110-000003'
		-- pur_payer = \'""" + user_id + """\'
		AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
		AND purchase_status != 'DELETED'
		AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
		GROUP BY property_address, property_unit
		ORDER BY property_address ASC, property_unit ASC;


-- ALL EXPENSE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH
	SELECT -- * , 
		cf_year, cf_month -- , pur_due_date
		, pur_cf_type -- , purchase_type
		, sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
		, property_address, property_unit
	FROM space.pp_details
	WHERE pur_payer = '110-000003'
		-- pur_payer = \'""" + user_id + """\'
		AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
		AND purchase_status != 'DELETED'
		AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
		GROUP BY property_address, property_unit, cf_month
		ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;

-- ALL EXPENSE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH BY PURCHASE TYPE
	SELECT -- * , 
		cf_year, cf_month -- , pur_due_date
		, pur_cf_type, purchase_type
		, sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
		, property_address, property_unit
	FROM space.pp_details
	WHERE pur_payer = '110-000003'
		-- pur_payer = \'""" + user_id + """\'
		AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
		AND purchase_status != 'DELETED'
		AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
		GROUP BY property_address, property_unit, cf_month,  purchase_type
		ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    
                    
-- ALL EXPENSE TRANSACTIONS AFFECTING A PARTICULAR OWNER
	SELECT -- * , 
		cf_year, cf_month, pur_due_date
		, pur_cf_type, purchase_type
		, pur_amount_due, total_paid, amt_remaining, payment_status
		, property_address, property_unit
		, space.bills.*
	FROM space.pp_details
	LEFT JOIN space.bills ON pur_bill_id = bill_uid AND pur_bill_id IS NOT NULL
	WHERE pur_payer = '110-000003'
		-- pur_payer = \'""" + user_id + """\'
		AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
		AND purchase_status != 'DELETED'
		AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
	ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    
                    
-- ALL OTHER TRANSACTIONS AFFECTING A PARTICULAR OWNER
	SELECT -- * , 
		cf_year, cf_month, pur_due_date
		, pur_cf_type, purchase_type
		, pur_amount_due, total_paid, amt_remaining, payment_status
		, property_address, property_unit
	FROM space.pp_details
	WHERE receiver_profile_uid = '110-000003'
		-- receiver_profile_uid = \'""" + user_id + """\'
		AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
		AND purchase_status != 'DELETED'
		AND (pur_cf_type != 'expense' AND pur_cf_type != 'revenue' AND  pur_cf_type != 'EXPENSE' AND pur_cf_type != 'REVENUE')
	ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
    
    
-- ---------------------------------------------------------------------


-- OWNER CASHFLOW

-- EXPECTED REVENUE
SELECT pur_cf_type, pur_receiver, pur_payer
	, SUM(pur_amount_due) AS cf
    , "EXPECTED REVENUE" AS cf_type
FROM space.pp_details
WHERE pur_receiver = '110-000003'
  AND pur_cf_type = 'revenue'

UNION  
-- ACTUAL REVENUE
SELECT pur_cf_type, pur_receiver, pur_payer
	, SUM(pur_amount_due) AS cf
    , "ACTUAL REVENUE" AS cf_type
FROM space.pp_details
WHERE pur_receiver = '110-000003'
  AND pur_cf_type = 'revenue'
  AND payment_status IN ("PAID", "PARTIALLY PAID", "PAID LATE")
  
UNION  
-- EXPECTED EXPENSE
SELECT pur_cf_type, pur_receiver, pur_payer
	, SUM(pur_amount_due) AS cf
    , "EXPECTED EXPENSE" AS cf_type
FROM space.pp_details
WHERE pur_payer = '110-000003'
  AND pur_cf_type = 'expense'
  
UNION  
-- ACTUAL EXPENSE 
SELECT pur_cf_type, pur_receiver, pur_payer
	, SUM(pur_amount_due) AS cf
    , "ACTUAL EXPENSE" AS cf_type
FROM space.pp_details
WHERE pur_payer = '110-000003'
  AND pur_cf_type = 'expense'
  AND payment_status IN ("PAID", "PARTIALLY PAID", "PAID LATE")
;  
  
-- ---------------------------------------------------------------------
-- CASHFLOW SUMMARY


-- MANAGER CASHFLOW

-- EXPECTED REVENUE
SELECT pur_cf_type, pur_receiver, pur_payer
	, SUM(pur_amount_due) AS cf
    , "EXPECTED REVENUE" AS cf_type
FROM space.pp_details
WHERE pur_receiver = '600-000003'
  AND pur_cf_type = 'expense'

UNION  
-- ACTUAL REVENUE
SELECT pur_cf_type, pur_receiver, pur_payer
	, SUM(pur_amount_due) AS cf
    , "ACTUAL REVENUE" AS cf_type
FROM space.pp_details
WHERE pur_receiver = '600-000003'
  AND pur_cf_type = 'expense'
  AND payment_status IN ("PAID", "PARTIALLY PAID", "PAID LATE")
  
UNION  
-- PASSTHROUGH EXPENSE MAINTENANCE
SELECT pur_cf_type, pur_receiver, pur_payer
	, SUM(pur_amount_due) AS cf
    , "MAINTENANCE EXPECTED EXPENSE PAID" AS cf_type
FROM space.pp_details
WHERE pur_payer = '600-000003'
  AND pur_cf_type = 'expense'
  AND purchase_type = 'MAINTENANCE'
  
  
UNION  
-- PASSTHROUGH EXPENSE MAINTENANCE
SELECT pur_cf_type, pur_receiver, pur_payer
	, SUM(pur_amount_due) AS cf
    , "RENT EXPECTED EXPENSE PAID" AS cf_type
FROM space.pp_details
WHERE pur_payer = '600-000003'
  AND pur_cf_type = 'revenue'
  AND purchase_type = 'RENT'  
  
UNION  
-- PASSTHROUGH EXPENSE MAINTENANCE
SELECT pur_cf_type, pur_receiver, pur_payer
	, SUM(pur_amount_due) AS cf
    , "MAINTENANCE EXPECTED REVENUE RECEIVED" AS cf_type
FROM space.pp_details
WHERE pur_receiver = '600-000003'
  AND pur_cf_type = 'expense'
  AND purchase_type = 'MAINTENANCE'
  
  
UNION  
-- PASSTHROUGH EXPENSE MAINTENANCE
SELECT pur_cf_type, pur_receiver, pur_payer
	, SUM(pur_amount_due) AS cf
    , "RENT EXPECTED REVENUE RECEIVED" AS cf_type
FROM space.pp_details
WHERE pur_receiver = '600-000003'
  AND pur_cf_type = 'revenue'
  AND purchase_type = 'RENT'  
  
UNION  
-- PASSTHROUGH EXPENSE MAINTENANCE
SELECT pur_cf_type, pur_receiver, pur_payer
	, SUM(pur_amount_due) AS cf
    , "MAINTENANCE ACTUAL EXPENSE PAID" AS cf_type
FROM space.pp_details
WHERE pur_payer = '600-000003'
  AND pur_cf_type = 'expense'
  AND purchase_type = 'MAINTENANCE'
  AND payment_status IN ("PAID", "PARTIALLY PAID", "PAID LATE")
  
  
UNION  
-- PASSTHROUGH EXPENSE MAINTENANCE
SELECT pur_cf_type, pur_receiver, pur_payer
	, SUM(pur_amount_due) AS cf
    , "RENT ACTUAL EXPENSE PAID" AS cf_type
FROM space.pp_details
WHERE pur_payer = '600-000003'
  AND pur_cf_type = 'revenue'
  AND purchase_type = 'RENT'  
  AND payment_status IN ("PAID", "PARTIALLY PAID", "PAID LATE")
  
UNION  
-- PASSTHROUGH EXPENSE MAINTENANCE
SELECT pur_cf_type, pur_receiver, pur_payer
	, SUM(pur_amount_due) AS cf
    , "MAINTENANCE ACTUAL REVENUE RECEIVED" AS cf_type
FROM space.pp_details
WHERE pur_receiver = '600-000003'
  AND pur_cf_type = 'expense'
  AND purchase_type = 'MAINTENANCE'
  AND payment_status IN ("PAID", "PARTIALLY PAID", "PAID LATE")
  
  
UNION  
-- PASSTHROUGH EXPENSE MAINTENANCE
SELECT pur_cf_type, pur_receiver, pur_payer
	, SUM(pur_amount_due) AS cf
    , "RENT ACTUAL REVENUE RECEIVED" AS cf_type
FROM space.pp_details
WHERE pur_receiver = '600-000003'
  AND pur_cf_type = 'revenue'
  AND purchase_type = 'RENT'
  AND payment_status IN ("PAID", "PARTIALLY PAID", "PAID LATE")
;