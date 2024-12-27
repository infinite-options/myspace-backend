-- CREATE TABLE space_dev.bills AS
-- SELECT * FROM pm.bills;
-- SELECT * FROM pm.leaseTenants;

SELECT * FROM space_dev.users;
SELECT * FROM space_dev.user_profile;

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
SELECT * FROM space_dev.leaseFees;

SELECT * FROM space_dev.businessProfileInfo;
SELECT * FROM space_dev.contracts;
SELECT * FROM space_dev.contractFees;

SELECT * FROM space_dev.purchases;
SELECT * FROM space_dev.payments;
SELECT * FROM space_dev.bills;
SELECT * FROM space_dev.lists;
SELECT * FROM space_dev.property_utility;

	
SELECT * FROM space_dev.o_details;
SELECT * FROM space_dev.p_details;
SELECT * FROM space_dev.pp_details;
SELECT * FROM space_dev.pp_status;
SELECT * FROM space_dev.t_details;







-- MAINTENANCE STATUS BY OWNER
SELECT property_owner.property_owner_id
	, maintenanceRequests.maintenance_request_status
    , COUNT(maintenanceRequests.maintenance_request_status) AS num
FROM space_dev.properties
LEFT JOIN space_dev.property_owner ON property_id = property_uid
LEFT JOIN space_dev.maintenanceRequests ON maintenance_property_id = property_uid
WHERE property_owner_id = '110-000003'
GROUP BY maintenance_request_status;

-- LEASE STATUS BY OWNER
SELECT property_owner.property_owner_id
    , leases.lease_end
    , COUNT(lease_end) AS num
FROM space_dev.properties
LEFT JOIN space_dev.property_owner ON property_id = property_uid
LEFT JOIN space_dev.leases ON lease_property_id = property_uid
WHERE property_owner_id = '110-000003'
GROUP BY MONTH(lease_end),
		YEAR(lease_end);
        



-- PURCHASES BY PROPERTY
SELECT -- *
	property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
    , SUM(pur_amount_due) AS purchase_amount_due
	, purchases.*
FROM space_dev.properties
LEFT JOIN space_dev.purchases ON pur_property_id = property_uid 
WHERE property_uid = '200-000029'
GROUP BY MONTH(pur_due_date),
		YEAR(pur_due_date),
        purchase_type;
     

-- CASHFLOW BY PROPERTY BY MONTH.  CONSIDER ADDING pur_type WHEN THE PURCHASE IS LOGGED
SELECT * FROM space_dev.purchases;

SELECT *
	-- property_uid, property_address, property_unit, pur_type, pur_due_date
	, ROUND(SUM(pur_amount_due),2) AS cf
FROM (
	SELECT *
		-- property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
		-- , SUM(pur_amount_due) AS purchase_amount_due
		, purchases.*
        , IF (purchase_type LIKE "RENT" OR  purchase_type LIKE "DEPOSIT" OR  purchase_type LIKE "LATE FEE" OR  purchase_type LIKE "UTILITY" OR  purchase_type LIKE "EXTRA CHARGES", 1 , 0) AS cf_revenue
        , CASE
			WHEN (purchase_type LIKE "RENT" OR  purchase_type LIKE "LATE FEE" OR  purchase_type LIKE "UTILITY" OR  purchase_type LIKE "EXTRA CHARGES") THEN "revenue"
            WHEN (purchase_type LIKE "OWNER PAYMENT RENT" OR  purchase_type LIKE "OWNER PAYMENT LATE FEE" OR  purchase_type LIKE "OWNER PAYMENT EXTRA CHARGES" OR  purchase_type LIKE "MAINTENANCE") THEN "expense"
            WHEN (purchase_type LIKE "DEPOSIT") THEN "deposit"
            ELSE "other"
		  END AS pur_type
	FROM space_dev.properties
	LEFT JOIN space_dev.purchases ON pur_property_id = property_uid
    -- WHERE property_uid = "200-000001"
	-- WHERE MONTH(pur_due_date) = 4 AND YEAR(pur_due_date) = 2023
	GROUP BY MONTH(pur_due_date),
			YEAR(pur_due_date),
			purchase_type,
            property_uid
		) a
GROUP BY MONTH(pur_due_date),
			YEAR(pur_due_date),
            pur_type,
		property_uid;
   
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
GROUP BY pay_purchase_id;


 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 


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
	-- SELECT * FROM space_dev.purchases
	-- WHERE ISNULL(pur_cf_type);

	-- UPDATE space_dev.purchases
	-- SET pur_cf_type = 'expense'
	-- WHERE purchase_type = "MANAGEMENT"
	-- 	AND pur_payer LIKE '100-%';

