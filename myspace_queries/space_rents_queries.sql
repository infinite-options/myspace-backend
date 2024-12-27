# PROPERTY ENDPOINTS

# PROPERTY RENT STATUS

SELECT * FROM space_dev.p_details;

SELECT * FROM space_dev.properties;
SELECT * FROM space_dev.pp_status;
SELECT * FROM space_dev.pp_details;

SELECT * FROM space_dev.businessProfileInfo;
SELECT * FROM space_dev.ownerProfileInfo;



SELECT -- *
	property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
    , purchase_uid, purchase_type, payment_uid -- , sum_paid_amount, payment_status
    , owner_uid
FROM space_dev.properties
LEFT JOIN space_dev.pp_status ON property_uid = pur_property_id
LEFT JOIN space_dev.o_details ON property_uid = property_id
WHERE purchase_type = "RENT"
	AND cf_month = DATE_FORMAT(NOW(), '%M')
    AND cf_year = DATE_FORMAT(NOW(), '%Y')
    AND owner_uid = "110-000003"
	AND property_uid = "200-000001";
    
    
    DATE_FORMAT('2023-08-01', '%M')
    DATE_FORMAT(NOW(), '%M')