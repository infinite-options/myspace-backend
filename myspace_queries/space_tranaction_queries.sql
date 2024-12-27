-- CREATE TABLE space_dev.bills AS
-- SELECT * FROM pm.bills;
-- SELECT * FROM pm.leaseTenants;

SELECT * FROM space_dev.users;
SELECT * FROM space_dev.user_profiles;

SELECT * FROM space_dev.ownerProfileInfo;
SELECT * FROM space_dev.property_owner;
SELECT * FROM space_dev.properties;
SELECT * FROM space_dev.appliances;
SELECT * FROM space_dev.maintenanceRequests;
SELECT * FROM space_dev.maintenanceQuotes;

SELECT * FROM space_dev.tenantProfileInfo;
SELECT * FROM space_dev.property_tenant;
SELECT * FROM space_dev.lease_tenant;
SELECT * FROM space_dev.leases;

SELECT * FROM space_dev.businessProfileInfo;
SELECT * FROM space_dev.contracts;
SELECT * FROM space_dev.contractFees;

SELECT * FROM space_dev.purchases;
SELECT * FROM space_dev.payments;
SELECT * FROM space_dev.bills;
SELECT * FROM space_dev.lists;
SELECT * FROM space_dev.property_utility;

SELECT * FROM space_dev.pp_status;
SELECT * FROM space_dev.pp_details;



-- ALL PURCHASE TRANSACTIONS BY OWNER
SELECT * FROM space_dev.pp_details
WHERE owner_uid = '110-000003'
	AND pur_property_id = '200-000029';


-- TRANSACTIONS BY OWNER BY PROPERTY
SELECT * FROM space_dev.pp_details
WHERE owner_uid = '110-000003'
	AND pur_property_id = '200-000029';






-- ALL PURCHASE TRANSACTIONS BY OWNER
SELECT pp.*
	, property_owner_id -- , po_owner_percent
    , property_uid, property_address, property_unit, property_city, property_state, property_zip,  property_description, property_notes
FROM (
	SELECT *
		, sum(pay_amount) AS paid_amount
		, IF (pur_amount_due <= sum(pay_amount), "PAID", "UNPAID") AS payment_status
		, (pur_amount_due - sum(pay_amount)) AS delta
	FROM space_dev.purchases
	LEFT JOIN space_dev.payments ON pay_purchase_id = purchase_uid
	GROUP BY purchase_uid
    ) AS pp
LEFT JOIN space_dev.properties ON property_uid = pur_property_id
LEFT JOIN space_dev.property_owner ON property_uid = property_id
WHERE property_owner_id = "110-000003" AND
	(pur_receiver = "100-000003" OR
    pur_initiator = "100-000003"OR
    pur_payer = "100-000003") ;




            "pur_receiver": "600-000003",
            "pur_initiator": null,
            "pur_payer": "350-000038",




   
-- ALL TRANSACTIONS
SELECT -- *
	property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_type
    , property_listed_rent, property_deposit, property_images, property_taxes, property_mortgages, property_insurance, property_description, property_notes
    , property_owner_id, po_owner_percent
    , purchase_uid, pur_timestamp, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
FROM space_dev.properties
LEFT JOIN space_dev.property_owner ON property_id = property_uid 			-- SO WE CAN SORT BY OWNER
LEFT JOIN space_dev.purchases ON pur_property_id = property_uid
-- ADDITIONAL FILTERS
WHERE property_owner_id = "110-000003"											-- BY OWNER
	-- AND property_id = "200-000029" 									-- BY PROPERTY ID
GROUP BY MONTH(pur_due_date),
			YEAR(pur_due_date);

-- CF DASHBOARD
SELECT -- *
	property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_type
    , property_listed_rent, property_deposit, property_images, property_taxes, property_mortgages, property_insurance, property_description, property_notes
    , property_owner_id, po_owner_percent
    , purchase_uid, pur_timestamp, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
    -- , SUM(pay_amount) AS paid_amount
    -- , SUM(pay_amount)-pay_amount
    -- , payments.*
FROM space_dev.properties
LEFT JOIN space_dev.property_owner ON property_id = property_uid 			-- SO WE CAN SORT BY OWNER
LEFT JOIN space_dev.purchases ON pur_property_id = property_uid
LEFT JOIN space_dev.payments ON pay_purchase_id = purchase_uid
GROUP BY pay_purchase_id



-- ADDITIONAL FILTERS
WHERE property_owner_id = "110-000003"											-- BY OWNER
	-- AND property_id = "200-000029" 									-- BY PROPERTY ID
GROUP BY MONTH(pur_due_date),
			YEAR(pur_due_date),
            pur_cf_type;
 
 
 
-- NEW CASHFLOW --------------------------------------------------------------------------------

SELECT * FROM space_dev.payments;
SELECT * FROM space_dev.purchases;

-- ALL PURCHASE & PAYMENT TRANSACTIONS
SELECT *
-- 	, sum(pay_amount) AS paid_amount
--     , IF (pur_amount_due <= sum(pay_amount), "PAID", "UNPAID") AS payment_status
--     , (pur_amount_due - sum(pay_amount)) AS delta
FROM space_dev.purchases;




 

-- CASHFLOW BY MONTH
SELECT *
	, SUM(pur_amount_due) AS amount_due
    , SUM(paid_amount) AS paid_amount
FROM (
	SELECT *
		, sum(pay_amount) AS paid_amount
		, IF (pur_amount_due <= sum(pay_amount), "PAID", "UNPAID") AS payment_status
		, (pur_amount_due - sum(pay_amount)) AS delta
	FROM space_dev.purchases
	LEFT JOIN space_dev.payments ON pay_purchase_id = purchase_uid
	GROUP BY purchase_uid
    ) AS pp

GROUP BY MONTH(purchase_date),
			YEAR(purchase_date);


-- CASHFLOW BY MONTH
SELECT *
	, SUM(pur_amount_due) AS amount_due
    , SUM(paid_amount) AS paid_amount
FROM (
	SELECT *
		, sum(pay_amount) AS paid_amount
		, IF (pur_amount_due <= sum(pay_amount), "PAID", "UNPAID") AS payment_status
		, (pur_amount_due - sum(pay_amount)) AS delta
	FROM space_dev.purchases
	LEFT JOIN space_dev.payments ON pay_purchase_id = purchase_uid
	GROUP BY purchase_uid
    ) AS pp

GROUP BY MONTH(purchase_date),
			YEAR(purchase_date);


-- REVENUE AND EXPENSE BY MONTH
SELECT *
	, SUM(pur_amount_due) AS amount_due
    , SUM(paid_amount) AS paid_amount
FROM (
	SELECT *
		, sum(pay_amount) AS paid_amount
		, IF (pur_amount_due <= sum(pay_amount), "PAID", "UNPAID") AS payment_status
		, (pur_amount_due - sum(pay_amount)) AS delta
	FROM space_dev.purchases
	LEFT JOIN space_dev.payments ON pay_purchase_id = purchase_uid
	GROUP BY purchase_uid
    ) AS pp

GROUP BY MONTH(pur_due_date),
			YEAR(pur_due_date),
            pur_cf_type;


-- UPDATE space_dev.purchases
-- SET purchase_status = "COMPLETED"
-- WHERE purchase_status != "DELETED";
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 


-- CREATE NEW PURCHASE
SELECT * FROM space_dev.purchases;

INSERT INTO space_dev.purchases
SET purchase_uid = "400-000528"
	, pur_timestamp = CURRENT_TIMESTAMP()
    , pur_property_id = "200-000041"
    , purchase_type = "MANAGEMENT PAYMENT"
    , pur_cf_type = "expense"
    , pur_bill_id = NULL
    , purchase_date = CURRENT_DATE()
    , pur_due_date = DATE_ADD(LAST_DAY(CURRENT_DATE()), INTERVAL 1 DAY)
    , pur_amount_due = 100.00
    , purchase_status = "UNPAID"
    , pur_notes = "THIS IS A TEST"
    , pur_description = "THIS IS ONLY A TEST"
    , pur_receiver = "600-000003"
    , pur_initiator = "100-000003";
    
-- CREATE NEW BILL
SELECT * FROM space_dev.bills;

INSERT INTO space_dev.bills
SET bill_uid = '040-000061'
, bill_timestamp = CURRENT_TIMESTAMP()
, bill_description = 'PG&E'
, bill_amount = 200.01
, bill_created_by = '100-000003'
, bill_utility_type = 'electricity'
, bill_split = 'Uniform'
, bill_property_id = '[{"property_uid": "200-000041"}, {"property_uid": "200-000042"}]'
, bill_docs = NULL;

DELETE FROM space_dev.bills WHERE bill_uid = "040-000055";


 -- CREATE NEW BILL UID (STORED PROCEDURE)
CALL space_dev.new_bill_uid;




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
		FROM space_dev.properties
		LEFT JOIN space_dev.property_utility ON property_uid = utility_property_id		-- TO FIND WHICH UTILITES TO PAY AND WHO PAYS THEM

		LEFT JOIN space_dev.lists ON utility_payer_id = list_uid				-- TO TRANSLATE WHO PAYS UTILITIES TO ENGLISH
		LEFT JOIN space_dev.property_owner ON property_uid = property_id		-- TO FIND PROPERTY OWNER
		LEFT JOIN space_dev.contracts ON property_uid = contract_property_id    -- TO FIND PROPERTY MANAGER
		LEFT JOIN space_dev.leases ON property_uid = lease_property_id			-- TO FIND CONTRACT START AND END DATES
		LEFT JOIN space_dev.lease_tenant ON lease_uid = lt_lease_id				-- TO FIND TENANT IDS AND RESPONSIBILITY PERCENTAGES
		WHERE contract_status = "ACTIVE"
		) u 

	LEFT JOIN space_dev.lists ON utility_type_id = list_uid					-- TO TRANSLATE WHICH UTILITY TO ENGLISH
    ) u_all

WHERE property_uid = "200-000029"
	AND utility_type = "gas";

						





-- OPEN UP JSON OBJECT

-- THIS OPENS UP THE IMAGE_URL
SELECT *
FROM space_dev.properties, 
JSON_TABLE(property_images, '$[*]' 
	COLUMNS (
	a FOR ORDINALITY,
	image_url VARCHAR(1000)  PATH '$') ) p;
    
    
-- TRYING UTILITIES

--  THIS RETURNS EACH ITEM AS AN INDIVIDUAL COLUMN
SELECT property_uid, property_utilities, a, gas, wifi, water, electricity
FROM space_dev.properties, 
JSON_TABLE(properties.property_utilities, '$' 
	COLUMNS (
	a FOR ORDINALITY,
    gas VARCHAR(10) PATH '$.Gas',
    wifi VARCHAR(10) PATH '$.Wifi',
    water VARCHAR(10) PATH '$.Water',
    electricity VARCHAR(10) PATH '$.Electricity') ) p;    
 

-- UPDATE COMMAND TO UPDATE DATABASE
	SELECT * FROM space_dev.purchases
	WHERE ISNULL(pur_cf_type);

	UPDATE space_dev.purchases
	SET pur_payer = 'expense'
	WHERE purchase_type = "MANAGEMENT"
	AND pur_payer LIKE '100-%';

