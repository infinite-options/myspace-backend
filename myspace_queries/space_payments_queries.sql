SELECT * FROM space_prod.users;
SELECT * FROM space_prod.user_profile;

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
SELECT * FROM space_prod.leaseFees;

SELECT * FROM space_prod.businessProfileInfo;
SELECT * FROM space_prod.contracts;
SELECT * FROM space_prod.contractFees;

SELECT * FROM space_prod.purchases;
SELECT * FROM space_prod.payments;
SELECT * FROM space_prod.bills;
SELECT * FROM space_prod.lists;
SELECT * FROM space_prod.property_utility;

	
SELECT * FROM space_prod.o_details;
SELECT * FROM space_prod.p_details;
SELECT * FROM space_prod.pp_details;
SELECT * FROM space_prod.pp_status;
SELECT * FROM space_prod.t_details;




# ADD EXPENSE
# ADD REVENUE
# ADD UTILITY
# PAY BILL (ADD PAYMENT) - DONE
# REFLECT UPDATED AMOUNT IN PURCHASES
# DETERMINE IF RENT IS PAID AND RENT STATUS



# ADD PAYMENT

# GET UID FROM STORED PROCEDURE
INSERT INTO space_prod.payments
SET payment_uid = '500-000290' 
,pay_purchase_id = '400-000536'
, pay_amount = 0.10
, payment_notes = "PMTEST"
, pay_charge_id = "stripe transaction key"
, payment_type = "ZELLE"
, payment_date = NOW()
, payment_verify = "Unverified"
, paid_by = "110-000003";



# REFLECT UPDATED AMOUNT IN PURCHASES
SELECT *
	, sum(pay_amount) AS sum_paid_amount
-- 	, CASE
-- 		WHEN (pur_amount_due <= sum(pay_amount)) THEN "PAID"
--         WHEN (pur_amount_due <= sum(pay_amount)) THEN "PAID"
--         WHEN (sum(pay_amount) = 0 OR ISNULL(pay_amount)) THEN "UNPAID"
-- 		ELSE "PARTIALLY PAID"
-- 	  END AS payment_status
-- 	, (pur_amount_due - sum(pay_amount)) AS amt_remaining
-- 	, DATE_FORMAT(pur_due_date, "%M") AS cf_month
-- 	, DATE_FORMAT(pur_due_date, "%Y") AS cf_year
FROM space_prod.purchases
LEFT JOIN space_prod.payments ON pay_purchase_id = purchase_uid
GROUP BY purchase_uid, pay_purchase_id;


# EXERCISE TO FIND MAX PAYMENT DATE

SELECT -- * 
	p1.payment_uid
    , p1.pay_purchase_id
    , p1.pay_amount
    , p1.payment_date
--     , p2.latest_date
    , p2.total_amount
FROM space_prod.payments p1
JOIN ( 
    SELECT MAX(payment_date) AS latest_date, SUM(pay_amount) AS total_amount
    FROM space_prod.payments
    GROUP BY pay_purchase_id
 ) p2 ON p1.payment_date = p2.latest_date;

SELECT p1.payment_id, p1.amount, p1.payment_date, p2.total_amount
FROM payments p1
JOIN (
    SELECT MAX(payment_date) AS latest_date, SUM(amount) AS total_amount
    FROM payments
    GROUP BY payment_id
) p2 ON p1.payment_date = p2.latest_date;


SELECT *
	, MAX(payment_date) AS latest_date
    , SUM(pay_amount) AS total_paid
FROM space_prod.payments
GROUP BY pay_purchase_id;


SELECT *
	, CASE
		WHEN (total_paid >= pur_amount_due) AND (latest_date > pur_due_date) THEN "PAID LATE"
        WHEN (total_paid >= pur_amount_due) AND (latest_date <= pur_due_date) THEN "PAID"
        WHEN (total_paid = 0 OR ISNULL(total_paid)) THEN "UNPAID"
		ELSE "PARTIALLY PAID"
	  END AS payment_status
	, (pur_amount_due - total_paid) AS amt_remaining
	, DATE_FORMAT(pur_due_date, "%M") AS cf_month
	, DATE_FORMAT(pur_due_date, "%Y") AS cf_year
FROM space_prod.purchases
LEFT JOIN space_prod.leases ON pur_property_id = lease_property_id
LEFT JOIN space_prod.leaseFees ON fees_lease_id = lease_uid
LEFT JOIN (
	SELECT *
		, MAX(payment_date) AS latest_date
		, SUM(pay_amount) AS total_paid
	FROM space_prod.payments
	GROUP BY pay_purchase_id
	) pay  ON pay_purchase_id = purchase_uid
GROUP BY purchase_uid;









-- CREATE NEW BILL
SELECT * FROM space_prod.bills;

INSERT INTO space_prod.bills
SET bill_uid = '040-000061'
, bill_timestamp = CURRENT_TIMESTAMP()
, bill_description = 'PG&E'
, bill_amount = 200.01
, bill_created_by = '100-000003'
, bill_utility_type = 'electricity'
, bill_split = 'Uniform'
, bill_property_id = '[{"property_uid": "200-000041"}, {"property_uid": "200-000042"}]'
, bill_docs = NULL;

DELETE FROM space_prod.bills WHERE bill_uid = "040-000055";


 -- CREATE NEW BILL UID (STORED PROCEDURE)
CALL space_prod.new_bill_uid;




-- UTILITY PAYMENT REPOSONSIBILITY BY PROPERTY
SELECT responsible_party
FROM (
	SELECT u.*
		, list_item AS utility_type
		, CASE
			WHEN contract_status = "ACTIVE" AND utility_payer = "property manager"  			  	 	THEN contract_business_id
			WHEN lease_status = "ACTIVE" AND utility_payer = "tenant" 	THEN lt_tenant_id
			ELSE property_owner_id
		   END AS responsible_party
	FROM (
		SELECT -- *,
			property_uid, property_address, property_unit
			, utility_type_id, utility_payer_id
			, list_item AS utility_payer
			, property_owner_id
			, contract_business_id, contract_status, contract_start_date, contract_end_date
			, lease_status, lease_start, lease_end
			, lt_tenant_id, lt_responsibility 
		FROM space_prod.properties
		LEFT JOIN space_prod.property_utility ON property_uid = utility_property_id		-- TO FIND WHICH UTILITES TO PAY AND WHO PAYS THEM

		LEFT JOIN space_prod.lists ON utility_payer_id = list_uid				-- TO TRANSLATE WHO PAYS UTILITIES TO ENGLISH
		LEFT JOIN space_prod.property_owner ON property_uid = property_id		-- TO FIND PROPERTY OWNER
		LEFT JOIN space_prod.contracts ON property_uid = contract_property_id    -- TO FIND PROPERTY MANAGER
		LEFT JOIN space_prod.leases ON property_uid = lease_property_id			-- TO FIND CONTRACT START AND END DATES
		LEFT JOIN space_prod.lease_tenant ON lease_uid = lt_lease_id				-- TO FIND TENANT IDS AND RESPONSIBILITY PERCENTAGES
		WHERE contract_status = "ACTIVE"
		) u 

	LEFT JOIN space_prod.lists ON utility_type_id = list_uid					-- TO TRANSLATE WHICH UTILITY TO ENGLISH
    ) u_all

WHERE property_uid = "200-000029"
	AND utility_type = "gas";
    
-- MAINTENANCE REPOSONSIBILITY BY PROPERTY 
SELECT -- *
	property_owner_id
FROM space_prod.property_owner
WHERE property_id = "200-000029"
  
	
-- OPEN UP JSON OBJECT

-- THIS OPENS UP THE IMAGE_URL
SELECT *
FROM space_prod.properties, 
JSON_TABLE(property_images, '$[*]' 
	COLUMNS (
	a FOR ORDINALITY,
	image_url VARCHAR(1000)  PATH '$') ) p;
    
    
-- TRYING UTILITIES

--  THIS RETURNS EACH ITEM AS AN INDIVIDUAL COLUMN
SELECT property_uid, property_utilities, a, gas, wifi, water, electricity
FROM space_prod.properties, 
JSON_TABLE(properties.property_utilities, '$' 
	COLUMNS (
	a FOR ORDINALITY,
    gas VARCHAR(10) PATH '$.Gas',
    wifi VARCHAR(10) PATH '$.Wifi',
    water VARCHAR(10) PATH '$.Water',
    electricity VARCHAR(10) PATH '$.Electricity') ) p;    
 

-- UPDATE COMMAND TO UPDATE DATABASE
	-- SELECT * FROM space_prod.purchases
	-- WHERE ISNULL(pur_cf_type);

	-- UPDATE space_prod.purchases
	-- SET pur_cf_type = 'expense'
	-- WHERE purchase_type = "MANAGEMENT"
	-- 	AND pur_payer LIKE '100-%';