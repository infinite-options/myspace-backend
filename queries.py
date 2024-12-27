# from flask import request
from data_pm import connect, uploadImage, s3

def testQuery(user_id):
    print("In testQuery FUNCTION CALL")

    query = 'SELECT * FROM space_dev.purchases WHERE {column} LIKE %s'
    # like_pattern = '600%'

    if user_id.startswith("110"):
        query = query.format(column='pur_receiver')
        like_pattern = '110%'
    elif user_id.startswith("600"):
        query = query.format(column='pur_payer')
        like_pattern = '600%'
    else:
        print("Invalid condition type")
        # return None
        response = "No Such User"
        return response

    # print(query)

    try:
        # Run query to find Announcements Received
        with connect() as db:    
            response = db.execute(query, (like_pattern,))
            print("Function Query Complete")
            # print("This is the Function response: ", response)
            response["API Test"] = "Test Dev Env"
        return response
    except:
        print("Error in testQuery Query ")
        response = "Error in testQuery Query"
        return response

# RENT QUERIES
def LeaseDetailsQuery(user_id):
    print("In RentDetailsQuery FUNCTION CALL")

    # query = 'SELECT * FROM space_dev.purchases WHERE {column} LIKE %s'
    query = """
            -- OWNER, PROPERTY MANAGER, TENANT LEASES
            SELECT * 
            FROM (
                -- FIND ALL ACTIVE/ENDED LEASES WITH OR WITHOUT A MOVE OUT DATE
                SELECT *,
                DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) AS lease_days_remaining,
                CASE
                        WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) > DATEDIFF(LAST_DAY(DATE_ADD(NOW(), INTERVAL 11 MONTH)), NOW()) THEN 'FUTURE' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'FUTURE'
                        WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) < 0 THEN 'M2M' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'M2M'
                        ELSE MONTHNAME(STR_TO_DATE(LEFT(lease_end, 2), '%m'))
                END AS lease_end_month
                FROM space_dev.leases 
                {cond}
                -- WHERE lease_status = "ACTIVE" OR lease_status = "ACTIVE M2M" OR lease_status = "ENDED"
                ) AS l
            LEFT JOIN (
                SELECT fees_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                    ('leaseFees_uid', leaseFees_uid,
                    'fee_name', fee_name,
                    'fee_type', fee_type,
                    'charge', charge,
                    'due_by', due_by,
                    'late_by', late_by,
                    'late_fee', late_fee,
                    'perDay_late_fee', perDay_late_fee,
                    'frequency', frequency,
                    'available_topay', available_topay,
                    'due_by_date', due_by_date
                    )) AS lease_fees
                    FROM space_dev.leaseFees
                    GROUP BY fees_lease_id) as f ON lease_uid = fees_lease_id
            LEFT JOIN space_dev.properties ON property_uid = lease_property_id
            LEFT JOIN space_dev.o_details ON property_id = lease_property_id
            LEFT JOIN (
                SELECT lt_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                    ('tenant_uid', tenant_uid,
                    'lt_responsibility', if(lt_responsibility IS NOT NULL, lt_responsibility, "1"),
                    'tenant_first_name', tenant_first_name,
                    'tenant_last_name', tenant_last_name,
                    'tenant_phone_number', tenant_phone_number,
                    'tenant_email', tenant_email,
                    'tenant_drivers_license_number', tenant_drivers_license_number,
                    'tenant_drivers_license_state', tenant_drivers_license_state,
                    'tenant_ssn', tenant_ssn
                    )) AS tenants
                    FROM space_dev.t_details 
                    GROUP BY lt_lease_id) as t ON lease_uid = lt_lease_id
            LEFT JOIN (SELECT * FROM space_dev.b_details WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
            LEFT JOIN space_dev.u_details ON utility_property_id = lease_property_id
            -- WHERE owner_uid LIKE "%110-000003%"
            -- WHERE contract_business_id LIKE "%600-000003%"
            -- WHERE tenants LIKE "%350-000040%"
            -- WHERE owner_uid = \'""" + user_id + """\'
            -- WHERE contract_business_id = \'""" + user_id + """\'
            -- WHERE tenants LIKE '%""" + user_id + """%'
            WHERE {column} LIKE  \'%""" + user_id + """%\' 
                        ;
                        """
    # like_pattern = '600%'

    if user_id.startswith("110"):
        query = query.format(column='owner_uid', cond = 'WHERE lease_status = "ACTIVE" OR lease_status = "ACTIVE M2M" OR lease_status = "ENDED"')
    elif user_id.startswith("350"):
        query = query.format(column='tenants', cond = 'WHERE lease_uid LIKE "300%"')
    elif user_id.startswith("600"):
        query = query.format(column='contract_business_id', cond = 'WHERE lease_status = "ACTIVE" OR lease_status = "ACTIVE M2M" OR lease_status = "ENDED"')
    else:
        print("Invalid condition type")
        return None

    # print(query)

    try:
        # Run query
        with connect() as db:    
            response = db.execute(query)
            # response = db.execute(query, (like_pattern,))
            print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in RentDetailsQuery ")


def RentStatusQuery(user_id):
    print("In RentStatusQuery FUNCTION CALL")

    # query = 'SELECT * FROM space_dev.purchases WHERE {column} LIKE %s'
    query = """
            -- PROPERTY RENT STATUS FOR RENTS PAGE
            SELECT -- *,
                IF(ISNULL(cf_month), CONCAT(property_uid, "-VACANT"), CONCAT(property_uid, "-", cf_month, "-", cf_year)) AS rent_detail_index
                , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_favorite_image
                , owner_uid, contract_uid, contract_status, business_uid, lease_uid, lease_start, lease_end, lease_status, tenant_uid, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , rent_status
                , pur_property_id, purchase_type, pur_due_date, pur_amount_due
                , if(ISNULL(pur_status_value), "0", pur_status_value) AS pur_status_value
                , if(ISNULL(purchase_status), "UNPAID", purchase_status) AS purchase_status
                , pur_description, cf_month, cf_year
                , CASE
                    WHEN ISNULL(contract_uid) THEN "NO MANAGER"
                    WHEN ISNULL(lease_status) THEN "VACANT"
                    WHEN ISNULL(purchase_status) THEN "UNPAID"
                    ELSE purchase_status
                    END AS rent_status 
            FROM (
                -- Find number of properties
                SELECT -- *,
                        property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip -- , property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby
                        , property_favorite_image
                        -- , po_owner_percent, po_start_date, po_end_date
                        , owner_uid -- , owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
                        , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name
                        , contract_status -- , contract_early_end_date
                        , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                        , lease_uid, lease_start, lease_end
                        , lease_status -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
                        , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                        -- , if(ISNULL(lease_status), "VACANT", lease_status) AS rent_status 
                FROM space_dev.p_details
                -- WHERE tenant_uid = '350-000002'
                -- WHERE business_uid = "600-000043"
                -- WHERE owner_uid = "110-000003"
                -- WHERE owner_uid = \'""" + user_id + """\'
                -- WHERE business_uid = \'""" + user_id + """\'
                -- WHERE tenant_uid = \'""" + user_id + """\'
                WHERE {column} =  \'""" + user_id + """\'   
                ) AS p
            -- Link to rent status
            LEFT JOIN (
                SELECT  -- *,
                    pur_property_id
                    , purchase_type
                    , pur_due_date
                    , SUM(pur_amount_due) AS pur_amount_due
                    , MIN(pur_status_value) AS pur_status_value
                    , CASE
                            WHEN MIN(pur_status_value) = 0 THEN "UNPAID"
                            WHEN MIN(pur_status_value) = 1 THEN "PARTIALLY PAID"
                            WHEN MIN(pur_status_value) = 4 THEN "PAID LATE"
                            WHEN MIN(pur_status_value) = 5 THEN "PAID"
                            ELSE purchase_status
                        END AS purchase_status
                    , pur_description
                    , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_month
                        , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_year
                    FROM space_dev.purchases
                    WHERE LEFT(pur_payer, 3) = '350'
                        -- AND purchase_type = "Rent"
                        AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = MONTH(CURRENT_DATE)
                        AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = YEAR(CURRENT_DATE)
                GROUP BY pur_property_id -- , purchase_type
                ) AS pp
                ON property_uid = pur_property_id;
            """
    # like_pattern = '600%'

    if user_id.startswith("110"):
        query = query.format(column='owner_uid')
    elif user_id.startswith("350"):
        query = query.format(column='tenant_uid')
    elif user_id.startswith("600"):
        query = query.format(column='business_uid')
    else:
        print("Invalid condition type")
        return None

    # print(query)

    try:
        # Run query to find Announcements Received
        with connect() as db:    
            response = db.execute(query)
            # response = db.execute(query, (like_pattern,))
            print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in RentStatusQuery Query ")


def RentDetailsQuery(user_id):
    print("In RentDetailsQuery FUNCTION CALL")

    # query = 'SELECT * FROM space_dev.purchases WHERE {column} LIKE %s'
    query = """
            -- PROPERTY RENT DETAILS FOR RENT DETAILS PAGE
            SELECT -- *,
                IF(ISNULL(cf_month), CONCAT(property_uid, "-VACANT"), CONCAT(property_uid, "-", cf_month, "-", cf_year, "-", purchase_uid)) AS rent_detail_index
                , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_favorite_image
                , owner_uid, contract_uid, contract_status, business_uid, lease_uid, lease_start, lease_end, lease_status, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , rent_status
                , pur_property_id, purchase_type, pur_notes, pur_due_date, pur_amount_due
                , latest_date, total_paid, amt_remaining
                , if(ISNULL(pur_status_value), "0", pur_status_value) AS pur_status_value
                , if(ISNULL(purchase_status), "UNPAID", purchase_status) AS purchase_status
                , pur_description, pur_notes, cf_month, cf_year
                , CASE
                    WHEN ISNULL(contract_uid) THEN "NO MANAGER"
                    WHEN ISNULL(lease_status) THEN "VACANT"
                    WHEN ISNULL(purchase_status) THEN "UNPAID"
                    ELSE purchase_status
                    END AS rent_status
                , lf_purchase_uid
                , lf_purchase_type
                , lf_pur_due_date
                , lf_pur_amount_due
                , lf_pur_status_value
                , lf_purchase_status
                , lf_latest_date
                , lf_total_paid
                , lf_amt_remaining
                , lf_cf_month
                , lf_cf_year
            FROM (
                -- Find number of properties
                SELECT -- *,
                        property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip -- , property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby
                        , property_favorite_image
                        -- , po_owner_percent, po_start_date, po_end_date
                        , owner_uid -- , owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
                        , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name
                        , contract_status -- , contract_early_end_date
                        , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                        , lease_uid, lease_start, lease_end
                        , lease_status -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
                        , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                        -- , if(ISNULL(lease_status), "VACANT", lease_status) AS rent_status 
                FROM space_dev.p_details
                -- WHERE business_uid = "600-000043"
                -- WHERE owner_uid = "110-000003"
                -- WHERE tenant_uid = "350-000003"
                -- WHERE owner_uid = \'""" + user_id + """\'
                -- WHERE business_uid = \'""" + user_id + """\'
                -- WHERE tenant_uid = \'""" + user_id + """\' 
                WHERE {column} =  \'""" + user_id + """\' 
                ) AS p
            -- Link to rent status
            LEFT JOIN (
                SELECT  -- *,
                    pur_property_id
                    , purchase_uid
                    , purchase_type
                    , pur_notes
                    , pur_due_date
                    , SUM(pur_amount_due) AS pur_amount_due
                    , SUM(total_paid) AS total_paid
                    , SUM(amt_remaining) AS amt_remaining
                    , MIN(pur_status_value) AS pur_status_value
                    , CASE
                            WHEN MIN(pur_status_value) = 0 THEN "UNPAID"
                            WHEN MIN(pur_status_value) = 1 THEN "PARTIALLY PAID"
                            WHEN MIN(pur_status_value) = 4 THEN "PAID LATE"
                            WHEN MIN(pur_status_value) = 5 THEN "PAID"
                            ELSE purchase_status
                        END AS purchase_status
                    , pur_description
                    , latest_date -- , total_paid, amt_remaining
                    , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_month
                    , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_year
                    , lf_purchase_uid
                    , lf_purchase_type
                    , lf_pur_due_date
                    , lf_pur_amount_due
                    , lf_pur_status_value
                    , lf_purchase_status
                    , lf_latest_date
                    , lf_total_paid
                    , lf_amt_remaining
                    , lf_cf_month
                    , lf_cf_year
            --         SELECT *
                FROM (
                    SELECT  *
                    FROM space_dev.pp_status -- space_dev.purchases
                    WHERE LEFT(pur_payer, 3) = '350'
                        -- AND purchase_type = "Rent"
                ) AS ppr
                LEFT JOIN (
                    SELECT  purchase_uid AS lf_purchase_uid
                        , purchase_type AS lf_purchase_type
                        , pur_due_date AS lf_pur_due_date
                        , pur_amount_due AS lf_pur_amount_due
                        , pur_status_value AS lf_pur_status_value
                        , purchase_status AS lf_purchase_status
                        , latest_date AS lf_latest_date
                        , total_paid AS lf_total_paid
                        , amt_remaining AS lf_amt_remaining
                        , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS lf_cf_month
                        , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS lf_cf_year
                    FROM space_dev.pp_status -- space_dev.purchases
                    WHERE LEFT(pur_payer, 3) = '350'
                        AND purchase_type = "Late Fee"
                ) AS lf ON purchase_uid = lf_purchase_uid
            -- 		AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = MONTH(CURRENT_DATE)
            -- 		AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = YEAR(CURRENT_DATE)
                GROUP BY purchase_uid -- pur_property_id, pur_description, purchase_type, cf_month
            ) AS pp 
            ON property_uid = pur_property_id
            ORDER BY pur_due_date DESC
    """

    original_query = """
            -- PROPERTY RENT DETAILS FOR RENT DETAILS PAGE
            SELECT -- *,
                IF(ISNULL(cf_month), CONCAT(property_uid, "-VACANT"), CONCAT(property_uid, "-", cf_month, "-", cf_year, "-", purchase_uid)) AS rent_detail_index
                , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_favorite_image
                , owner_uid, contract_uid, contract_status, business_uid, lease_uid, lease_start, lease_end, lease_status, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , rent_status
                , pur_property_id, purchase_type, pur_notes, pur_due_date, pur_amount_due
                , latest_date, total_paid, amt_remaining
                , if(ISNULL(pur_status_value), "0", pur_status_value) AS pur_status_value
                , if(ISNULL(purchase_status), "UNPAID", purchase_status) AS purchase_status
                , pur_description, cf_month, cf_year
                , CASE
                    WHEN ISNULL(contract_uid) THEN "NO MANAGER"
                    WHEN ISNULL(lease_status) THEN "VACANT"
                    WHEN ISNULL(purchase_status) THEN "UNPAID"
                    ELSE purchase_status
                    END AS rent_status
                , lf_purchase_uid
                , lf_purchase_type
                , lf_pur_due_date
                , lf_pur_amount_due
                , lf_pur_status_value
                , lf_purchase_status
                , lf_latest_date
                , lf_total_paid
                , lf_amt_remaining
                , lf_cf_month
                , lf_cf_year
            FROM (
                -- Find number of properties
                SELECT -- *,
                        property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip -- , property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby
                        , property_favorite_image
                        -- , po_owner_percent, po_start_date, po_end_date
                        , owner_uid -- , owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
                        , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name
                        , contract_status -- , contract_early_end_date
                        , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                        , lease_uid, lease_start, lease_end
                        , lease_status -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
                        , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                        -- , if(ISNULL(lease_status), "VACANT", lease_status) AS rent_status 
                FROM space_dev.p_details
                -- WHERE business_uid = "600-000003"
                -- WHERE owner_uid = "110-000003"
                -- WHERE owner_uid = \'""" + user_id + """\'
                -- WHERE business_uid = \'""" + user_id + """\'
                -- WHERE tenant_uid = \'""" + user_id + """\' 
                WHERE {column} =  \'""" + user_id + """\' 
                ) AS p
            -- Link to rent status
            LEFT JOIN (
                SELECT  -- *,
                    pur_property_id
                    , purchase_uid
                    , purchase_type
                    , pur_notes
                    , pur_due_date
                    , SUM(pur_amount_due) AS pur_amount_due
                    , SUM(total_paid) AS total_paid
                    , SUM(amt_remaining) AS amt_remaining
                    , MIN(pur_status_value) AS pur_status_value
                    , CASE
                            WHEN MIN(pur_status_value) = 0 THEN "UNPAID"
                            WHEN MIN(pur_status_value) = 1 THEN "PARTIALLY PAID"
                            WHEN MIN(pur_status_value) = 4 THEN "PAID LATE"
                            WHEN MIN(pur_status_value) = 5 THEN "PAID"
                            ELSE purchase_status
                        END AS purchase_status
                    , pur_description
                    , latest_date -- , total_paid, amt_remaining
                    , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_month
                    , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_year
                    , lf_purchase_uid
                    , lf_purchase_type
                    , lf_pur_due_date
                    , lf_pur_amount_due
                    , lf_pur_status_value
                    , lf_purchase_status
                    , lf_latest_date
                    , lf_total_paid
                    , lf_amt_remaining
                    , lf_cf_month
                    , lf_cf_year

                FROM (
                    SELECT  *
                    FROM space_dev.pp_status -- space_dev.purchases
                    WHERE LEFT(pur_payer, 3) = '350'
                        -- AND purchase_type = "Rent"
                ) AS ppr
                LEFT JOIN (
                    SELECT  pur_description AS lf_purchase_uid
                        , purchase_type AS lf_purchase_type
                        , pur_due_date AS lf_pur_due_date
                        , pur_amount_due AS lf_pur_amount_due
                        , pur_status_value AS lf_pur_status_value
                        , purchase_status AS lf_purchase_status
                        , latest_date AS lf_latest_date
                        , total_paid AS lf_total_paid
                        , amt_remaining AS lf_amt_remaining
                        , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS lf_cf_month
                        , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS lf_cf_year
                    FROM space_dev.pp_status -- space_dev.purchases
                    WHERE LEFT(pur_payer, 3) = '350'
                        AND purchase_type = "Late Fee"
                ) AS lf ON purchase_uid = lf_purchase_uid
            -- 		AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = MONTH(CURRENT_DATE)
            -- 		AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = YEAR(CURRENT_DATE)
                GROUP BY pur_property_id, purchase_type, cf_month
            ) AS pp 
            ON property_uid = pur_property_id
            ORDER BY pur_due_date DESC
            """
    # like_pattern = '600%'

    if user_id.startswith("110"):
        query = query.format(column='owner_uid')
    elif user_id.startswith("350"):
        query = query.format(column='tenant_uid')
    elif user_id.startswith("600"):
        query = query.format(column='business_uid')
    else:
        print("Invalid condition type")
        return None

    # print(query)

    try:
        # Run query to find Announcements Received
        with connect() as db:    
            response = db.execute(query)
            # response = db.execute(query, (like_pattern,))
            print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in RentDetailsQuery ")


def RentDashboardQuery(user_id):
    print("In RentDashboardQuery FUNCTION CALL")

    # query = 'SELECT * FROM space_dev.purchases WHERE {column} LIKE %s'
    query = """
            -- PROPERTY RENT STATUS FOR DASHBOARD
            SELECT 
                rent_status
                , COUNT(rent_status) AS num
            FROM (
                SELECT -- *,
                    property_uid, owner_uid, contract_uid, contract_status, business_uid, lease_uid, lease_start, lease_end, lease_status, tenant_uid -- , rent_status
                    , pur_property_id, purchase_type, pur_due_date, pur_amount_due
                    , if(ISNULL(pur_status_value), "0", pur_status_value) AS pur_status_value
                    , if(ISNULL(purchase_status), "UNPAID", purchase_status) AS purchase_status
                    , pur_description, cf_month, cf_year
                    , CASE
                        WHEN ISNULL(contract_uid) THEN "NO MANAGER"
                        WHEN ISNULL(lease_status) THEN "VACANT"
                        WHEN ISNULL(purchase_status) THEN "UNPAID"
                        ELSE purchase_status
                        END AS rent_status 
                FROM (
                    -- Find number of properties
                    SELECT -- *,
                            property_uid -- , property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_utilities
                            -- , po_owner_percent, po_start_date, po_end_date
                            , owner_uid -- , owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
                            , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name
                            , contract_status -- , contract_early_end_date
                            , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                            , lease_uid, lease_start, lease_end
                            , lease_status -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
                            , tenant_uid -- , tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                            -- , if(ISNULL(lease_status), "VACANT", lease_status) AS rent_status 
                    FROM space_dev.p_details
                    -- WHERE business_uid = "600-000043"
                    -- WHERE owner_uid = "110-000003"
                    -- WHERE owner_uid = \'""" + user_id + """\'
                    -- WHERE business_uid = \'""" + user_id + """\'
                    -- WHERE tenant_uid = \'""" + user_id + """\'  
                    WHERE {column} =  \'""" + user_id + """\' 
                    ) AS p
                -- Link to rent status
                LEFT JOIN (
                    SELECT  -- *,
                        pur_property_id
                        , purchase_type
                        , pur_due_date
                        , SUM(pur_amount_due) AS pur_amount_due
                        , MIN(pur_status_value) AS pur_status_value
                        , CASE
                            WHEN MIN(pur_status_value) = 0 THEN "UNPAID"
                            WHEN MIN(pur_status_value) = 1 THEN "PARTIALLY PAID"
                            WHEN MIN(pur_status_value) = 4 THEN "PAID LATE"
                            WHEN MIN(pur_status_value) = 5 THEN "PAID"
                            ELSE purchase_status
                            END AS purchase_status
                        , pur_description
                        , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_month
                        , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_year
                    FROM space_dev.purchases
                    WHERE LEFT(pur_payer, 3) = '350'
                        -- AND purchase_type = "Rent"
                        AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = MONTH(CURRENT_DATE)
                        AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = YEAR(CURRENT_DATE)
                    GROUP BY pur_property_id
                    ) AS pp
                    ON property_uid = pur_property_id
                ) AS rs
            GROUP BY rent_status;
            """
    # like_pattern = '600%'

    if user_id.startswith("110"):
        query = query.format(column='owner_uid')
    elif user_id.startswith("350"):
        query = query.format(column='tenant_uid')
    elif user_id.startswith("600"):
        query = query.format(column='business_uid')
    else:
        print("Invalid condition type")
        return None

    # print(query)

    try:
        # Run query to find Announcements Received
        with connect() as db:    
            response = db.execute(query)
            # response = db.execute(query, (like_pattern,))
            print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in RentDashboardQuery ")


def RentPropertiesQuery(user_id):
    print("In RentPropertiesQuery FUNCTION CALL")

    query = """
            -- PROPERTY RENT STATUS FOR PROPERTIES
            SELECT 
                IF(ISNULL(cf_month), CONCAT(property_uid, "-VACANT"), CONCAT(property_uid, "-", cf_month, "-", cf_year)) AS rent_detail_index
                , p.*
                , pur_property_id, purchase_type, pur_due_date, pur_amount_due
                , if(ISNULL(pur_status_value), "0", pur_status_value) AS pur_status_value
                , if(ISNULL(purchase_status), "UNPAID", purchase_status) AS purchase_status
                , pur_description, cf_month, cf_year
                , CASE
                    WHEN ISNULL(contract_uid) THEN "NO MANAGER"
                    WHEN ISNULL(lease_status) THEN "VACANT"
                    WHEN ISNULL(purchase_status) THEN "UNPAID"
                    ELSE purchase_status
                    END AS rent_status  
            FROM (
                -- Find properties
                SELECT * FROM space_dev.p_details
                -- WHERE business_uid = "600-000043"
                -- WHERE owner_uid = "110-000012"
                -- WHERE owner_uid = \'""" + user_id + """\'
                -- WHERE business_uid = \'""" + user_id + """\'
                -- WHERE tenant_uid = \'""" + user_id + """\'  
                WHERE {column} =  \'""" + user_id + """\'
                ) AS p
            -- Link to rent status
            LEFT JOIN (
                SELECT -- *
                    pur_property_id
                    , purchase_type
                    , pur_due_date
                    , SUM(pur_amount_due) AS pur_amount_due
                    , MIN(pur_status_value) AS pur_status_value
                    , CASE
                            WHEN MIN(pur_status_value) = 0 THEN "UNPAID"
                            WHEN MIN(pur_status_value) = 1 THEN "PARTIALLY PAID"
                            WHEN MIN(pur_status_value) = 4 THEN "PAID LATE"
                            WHEN MIN(pur_status_value) = 5 THEN "PAID"
                            ELSE purchase_status
                        END AS purchase_status
                    , pur_description
                    , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_month
                    , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_year
                FROM space_dev.purchases
                WHERE LEFT(pur_payer, 3) = '350'
                    -- AND purchase_type = "Rent"
                    AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = MONTH(CURRENT_DATE)
                    AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = YEAR(CURRENT_DATE)
                GROUP BY pur_property_id -- , purchase_type
                ) AS pp
                ON property_uid = pur_property_id  
            """

    if user_id.startswith("110"):
        query = query.format(column='owner_uid')
    elif user_id.startswith("350"):
        query = query.format(column='tenant_uid')
    elif user_id.startswith("600"):
        query = query.format(column='business_uid')
    else:
        print("Invalid condition type")
        return None

    # print(query)

    try:
        # Run query to find Announcements Received
        with connect() as db:    
            response = db.execute(query)
            # response = db.execute(query, (like_pattern,))
            print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in RentPropertiesQuery ")


def AnnouncementReceiverQuery(user_id):
    print("In AnnouncementReceiverQuery FUNCTION CALL")

    try:
        # Run query to find Announcements Received
        with connect() as db:    
            response = db.execute("""
                    SELECT 
                        a.*,
                        COALESCE(b.business_name, c.owner_first_name, d.tenant_first_name) AS receiver_first_name,
                        COALESCE(c.owner_last_name,  d.tenant_last_name) AS receiver_last_name,
                        COALESCE(b.business_phone_number, c.owner_phone_number, d.tenant_phone_number) AS receiver_phone_number,
                        COALESCE(b.business_photo_url, c.owner_photo_url, d.tenant_photo_url) AS receiver_photo_url
                    , CASE
                            WHEN a.announcement_receiver LIKE '600%' THEN 'Business'
                            WHEN a.announcement_receiver LIKE '350%' THEN 'Tenant'
                            WHEN a.announcement_receiver LIKE '110%' THEN 'Owner'
                            ELSE 'Unknown'
                      END AS receiver_role
                    FROM space_dev.announcements a
                    LEFT JOIN space_dev.businessProfileInfo b ON a.announcement_receiver LIKE '600%' AND b.business_uid = a.announcement_receiver
                    LEFT JOIN space_dev.ownerProfileInfo c ON a.announcement_receiver LIKE '110%' AND c.owner_uid = a.announcement_receiver
                    LEFT JOIN space_dev.tenantProfileInfo d ON a.announcement_receiver LIKE '350%' AND d.tenant_uid = a.announcement_receiver
                    WHERE announcement_sender = \'""" + user_id + """\';
                    """)
            # print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in AnnouncementReceiverQuery Query ")


def AnnouncementSenderQuery(user_id):
    print("In AnnouncementSenderQuery FUNCTION CALL")

    try:
        # Run query to find Announcements Received
        with connect() as db:    
            response = db.execute("""
                    SELECT 
                        a.*,
                        COALESCE(b.business_name, c.owner_first_name, d.tenant_first_name) AS sender_first_name,
                        COALESCE(c.owner_last_name,  d.tenant_last_name) AS sender_last_name,
                        COALESCE(b.business_phone_number, c.owner_phone_number, d.tenant_phone_number) AS sender_phone_number,
                        COALESCE(b.business_photo_url, c.owner_photo_url, d.tenant_photo_url) AS sender_photo_url
                        , CASE
                            WHEN a.announcement_sender LIKE '600%' THEN 'Business'
                            WHEN a.announcement_sender LIKE '350%' THEN 'Tenant'
                            WHEN a.announcement_sender LIKE '110%' THEN 'Owner'
                            ELSE 'Unknown'
                        END AS sender_role
                    FROM 
                        space_dev.announcements a
                    LEFT JOIN space_dev.businessProfileInfo b ON a.announcement_sender LIKE '600%' AND b.business_uid = a.announcement_sender
                    LEFT JOIN space_dev.ownerProfileInfo c ON a.announcement_sender LIKE '110%' AND c.owner_uid = a.announcement_sender
                    LEFT JOIN space_dev.tenantProfileInfo d ON a.announcement_sender LIKE '350%' AND d.tenant_uid = a.announcement_sender
                    WHERE 
                        announcement_receiver = \'""" + user_id + """\';
                    """)
            # print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in AnnouncementSenderQuery Query ")


def DashboardCashflowQuery(user_id):
    # print("In DashboardCashflowQuery FUNCTION CALL")

    try:
        # Run query to find rents of ACTIVE leases
        with connect() as db:    
            # NOT SURE WHY THIS DOES NOT WORK
            # response = db.execute("""
                    # -- CASHFLOW FOR A PARTICULAR OWNER OR MANAGER
                    # SELECT pur_receiver, pur_payer
                    #     , SUM(pur_amount_due) AS pur_amount_due
                    #     , SUM(total_paid) AS total_paid
                    #     , cf_month, cf_month_num, cf_year
                    #     , pur_cf_type
                    # FROM space_dev.pp_status
                    # -- WHERE (pur_receiver = '110-000003' OR pur_payer = '110-000003')
                    # -- WHERE (pur_receiver = '600-000003' OR pur_payer = '600-000003')
                    # WHERE (pur_receiver = \'""" + user_id + """\' OR pur_payer = \'""" + user_id + """\')
                    # GROUP BY cf_month, cf_year, pur_cf_type
                    # ORDER BY cf_month_num
            #         """)
            response = db.execute("""
                    -- CASHFLOW FOR A PARTICULAR OWNER OR MANAGER
                    SELECT -- *,
                        -- IF(pur_receiver = '600-000003', '600-000003', "") AS pur_receiver,
                        -- IF(pur_receiver = '600-000003', "", "") AS pur_payer,
                        IF(pur_receiver = \'""" + user_id + """\', \'""" + user_id + """\', "") AS pur_receiver,
                        IF(pur_receiver = \'""" + user_id + """\', "", "") AS pur_payer,
                        SUM(pur_amount_due) AS pur_amount_due, SUM(total_paid) AS total_paid,
                        cf_month, cf_month_num, cf_year,
                        -- IF(pur_receiver = '600-000003', 'revenue', "") AS pur_cf_type
                        IF(pur_receiver = \'""" + user_id + """\', 'revenue', "") AS pur_cf_type
                    FROM space_dev.pp_status
                    -- WHERE pur_receiver = '600-000003' AND purchase_type != 'Deposit'
                    WHERE pur_receiver = \'""" + user_id + """\' AND purchase_type != 'Deposit'
                    GROUP BY cf_month, cf_year
                    UNION
                    SELECT -- *,
                        -- IF(pur_payer = '600-000003', "", "") AS pur_receiver,
                        -- IF(pur_payer = '600-000003', '600-000003', "") AS pur_payer,
                        IF(pur_payer = \'""" + user_id + """\', "", "") AS pur_receiver,
                        IF(pur_payer = \'""" + user_id + """\', \'""" + user_id + """\', "") AS pur_payer,
                        SUM(pur_amount_due) AS pur_amount_due, SUM(total_paid) AS total_paid,
                        cf_month, cf_month_num, cf_year,
                        -- IF(pur_payer = '600-000003', 'expense', "") AS pur_cf_type
                        IF(pur_payer = \'""" + user_id + """\', 'expense', "") AS pur_cf_type
                    FROM space_dev.pp_status
                    -- WHERE pur_payer = '600-000003' AND purchase_type != 'Deposit'
                    WHERE pur_payer = \'""" + user_id + """\' AND purchase_type != 'Deposit'
                    GROUP BY cf_month, cf_year                                  
                    """)
            # print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in DashboardCashflowQuery Query ")


def DashboardProfitQuery(user_id):
    # print("In DashboardCashflowQuery FUNCTION CALL")

    try:
        # Run query to find rents of ACTIVE leases
        with connect() as db:    
            # NOT SURE WHY THIS DOES NOT WORK
            # response = db.execute("""
                    # -- CASHFLOW FOR A PARTICULAR OWNER OR MANAGER
                    # SELECT pur_receiver, pur_payer
                    #     , SUM(pur_amount_due) AS pur_amount_due
                    #     , SUM(total_paid) AS total_paid
                    #     , cf_month, cf_month_num, cf_year
                    #     , pur_cf_type
                    # FROM space_dev.pp_status
                    # -- WHERE (pur_receiver = '110-000003' OR pur_payer = '110-000003')
                    # -- WHERE (pur_receiver = '600-000003' OR pur_payer = '600-000003')
                    # WHERE (pur_receiver = \'""" + user_id + """\' OR pur_payer = \'""" + user_id + """\')
                    # GROUP BY cf_month, cf_year, pur_cf_type
                    # ORDER BY cf_month_num
            #         """)
            response = db.execute("""
                    SELECT -- *
                        pur_receiver, pur_payer
                        , SUM(pur_amount_due) AS pur_amount_due
                        , SUM(total_paid) AS total_paid
                        , cf_month, cf_month_num, cf_year
                        , purchase_type
                        , pur_cf_type
                    FROM space_dev.pp_status
                    -- WHERE (pur_receiver = '600-000050' OR pur_payer = '600-000050')
                    WHERE (pur_receiver = \'""" + user_id + """\' OR pur_payer = \'""" + user_id + """\')
                    GROUP BY cf_month, cf_year, pur_cf_type, purchase_type
                    ORDER BY cf_month_num                                
                    """)
            # print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in DashboardCashflowQuery Query ")


def UnpaidRents():
    print("In Unpaid Rents Query FUNCTION CALL")

    try:
        # Run query to find rents of ACTIVE leases
        with connect() as db:    
            response = db.execute("""
                    -- DETERMINE WHICH RENTS ARE UNPAID OR PARTIALLY PAID
                    SELECT *
						, DATE_FORMAT(DATE_ADD(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i'), INTERVAL pur_late_by DAY), '%m-%d-%Y %H:%i') AS late_by_date
					FROM space_dev.purchases
					LEFT JOIN space_dev.contracts ON contract_property_id = pur_property_id
					LEFT JOIN space_dev.property_owner ON property_id = pur_property_id
					WHERE purchase_type = "RENT" AND
						  contract_status = "ACTIVE" AND
                          DATE_ADD(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i'), INTERVAL pur_late_by DAY) < CURDATE() AND
						  (purchase_status = "UNPAID" OR purchase_status = "PARTIALLY PAID") AND 
						  SUBSTRING(pur_payer, 1, 3) = '350';
                    """)
            # print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in UnpaidRents Query ")


def NextDueDate():
    print("In NextDueDate Query FUNCTION CALL")

    try:
        # Run query to find rents of ACTIVE leases
        with connect() as db:    
            response = db.execute("""
                    -- CALCULATE NEXT DUE DATE FOR RECURRING FEES
                    SELECT *
                    FROM (
                        SELECT lf.* 
                            , lease_uid, lease_property_id, lease_status, lease_assigned_contacts, lease_documents
                            , lt_lease_id, lt_tenant_id, lt_responsibility
                            , property_id, property_owner_id, po_owner_percent
                            , contract_uid, contract_property_id, contract_business_id, contract_fees, contract_status 
                            -- FIND NEXT DUE DATE
                            , DATE_FORMAT(
                                CASE 
                                    WHEN frequency = 'Monthly' THEN 
                                        IF(CURDATE() <= STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d'), 
                                            STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d'), 
                                            DATE_ADD(STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d'), INTERVAL 1 MONTH)
                                        )
                                    WHEN frequency = 'Semi-Monthly' THEN 
                                        CASE 
                                            WHEN CURDATE() <= STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d') THEN 
                                                STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d')
                                            WHEN CURDATE() <= STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by + 15), '%Y-%m-%d') THEN 
                                                STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by + 15), '%Y-%m-%d')
                                            ELSE 
                                                DATE_ADD(STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()) + 1, '-', due_by), '%Y-%m-%d'), INTERVAL 0 MONTH)
                                        END
                                    WHEN frequency = 'Quarterly' THEN 
                                        CASE 
                                            WHEN CURDATE() <= STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d') THEN 
                                                STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d')
                                            ELSE 
                                                DATE_ADD(STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d'), INTERVAL 3 MONTH)
                                        END
                                        
                                    WHEN frequency = 'Semi-Annually' THEN
                                        IF(CURDATE() <= STR_TO_DATE(due_by_date, '%m-%d-%Y %H:%i'), 
                                            STR_TO_DATE(due_by_date, '%m-%d-%Y %H:%i'), 
                                            DATE_ADD(STR_TO_DATE(due_by_date, '%m-%d-%Y %H:%i'), INTERVAL 6 MONTH)    
                                        )
                                    WHEN frequency = 'Annually' THEN
                                        IF(CURDATE() <= STR_TO_DATE(due_by_date, '%m-%d-%Y %H:%i'), 
                                            STR_TO_DATE(due_by_date, '%m-%d-%Y %H:%i'), 
                                            DATE_ADD(STR_TO_DATE(due_by_date, '%m-%d-%Y %H:%i'), INTERVAL 1 YEAR)
                                        )
                                    WHEN frequency = 'Weekly' THEN 
                                        DATE_ADD(CURDATE(), INTERVAL (due_by - DAYOFWEEK(CURDATE()) + 7) % 7 DAY)
                                    WHEN frequency = 'Bi-Weekly' THEN
                                        DATE_ADD(CURDATE(), INTERVAL (due_by - DAYOFWEEK(CURDATE()) + 14) % 14 DAY)
                                END, '%m-%d-%Y %H:%i') AS next_due_date
                        FROM (
                            SELECT * FROM space_dev.leases WHERE lease_status = 'ACTIVE'
                            ) AS l
                        LEFT JOIN (
                            SELECT * FROM space_dev.leaseFees WHERE frequency != 'One Time'
                            ) AS lf ON fees_lease_id = lease_uid  				-- get lease fees
                        LEFT JOIN space_dev.lease_tenant ON fees_lease_id = lt_lease_id            	-- get tenant responsible for rent
                        LEFT JOIN space_dev.property_owner ON lease_property_id = property_id      	-- get property owner and ownership percentage
                        LEFT JOIN (
                            SELECT * FROM space_dev.contracts WHERE contract_status = 'ACTIVE'
                            ) AS c ON lease_property_id = contract_property_id  				-- to make sure contract is active
                        ) AS ndd
                    LEFT JOIN space_dev.purchases ON lease_property_id = pur_property_id
                        AND fee_name = pur_notes
                        AND charge = pur_amount_due
                        AND lt_tenant_id = pur_payer
                        AND STR_TO_DATE(next_due_date, '%m-%d-%Y %H:%i') = STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')
                    """)
            
            # print("Function Query Complete")
            # print("This is the Function response: ", len(response["result"]))
        return response
    except:
        print("Error in NextDueDate Query ")


def ApprovedContracts():
    print("In Approved Contracts Query FUNCTION CALL")

    try:
        # Run query to find all APPROVED Contracts
        with connect() as db:    
            response = db.execute("""
                    SELECT * 
                    FROM space_dev.contracts
                    WHERE contract_status = 'APPROVED'
                        AND STR_TO_DATE(contract_start_date, '%m-%d-%Y') <= CURDATE();
                    """)

            # response = db.execute("""
            #         UPDATE space_dev.contracts AS c
            #         JOIN (
            #             SELECT contract_property_id
            #             FROM space_dev.contracts
            #             WHERE STR_TO_DATE(contract_start_date, '%m-%d-%Y') <= CURDATE()
            #             AND contract_status = 'APPROVED'
            #         ) AS approved_contracts
            #         ON c.contract_property_id = approved_contracts.contract_property_id
            #         SET c.contract_status = 'INACTIVE'
            #         WHERE c.contract_status = 'ACTIVE';

            #         SELECT * 
            #         FROM space_dev.contracts
            #         WHERE contract_status = 'APPROVED'
            #             AND STR_TO_DATE(contract_start_date, '%m-%d-%Y') <= CURDATE();
            #         """)

            print("Function Query Complete")
            print("This is the Function response: ", response)
        return response
    except:
        print("Error in ApprovedContracts Query ")


def ContractDetails(user_id):
    print("In Contracts Query FUNCTION CALL")

    query = """
                SELECT -- *,
                    -- property_id, property_unit, property_address, property_city, property_state, property_zip, property_owner_id, po_owner_percent
                    p.*
                    , owner_uid, owner_user_id, po_owner_percent, owner_first_name, owner_last_name, owner_phone_number, owner_email
                    -- , owner_address, owner_unit, owner_city, owner_state, owner_zip
                    , owner_photo_url
                    , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_renew_status, contract_early_end_date, contract_end_notice_period, contract_m2m
                    , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_services_fees
                    -- , business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                FROM space_dev.o_details o
                LEFT JOIN space_dev.properties p ON o.property_id =p.property_uid 
                LEFT JOIN space_dev.b_details b ON o.property_id = b.contract_property_id
                -- WHERE b.business_uid = '600-000011'
                -- WHERE o.owner_uid = '110-000003'
                -- WHERE o.owner_uid = \'""" + user_id + """\';
                -- WHERE b.business_uid = \'""" + user_id + """\';
                WHERE {column} =  \'""" + user_id + """\'  
                    """
    
    queryPM = """
            SELECT -- *,
                -- property_id, property_unit, property_address, property_city, property_state, property_zip, property_owner_id, po_owner_percent
                p.*
                , o.*
                , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_renew_status, contract_early_end_date, contract_end_notice_period, contract_m2m
                , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_services_fees
                -- , business_address, business_unit, business_city, business_state, business_zip, business_photo_url
            FROM space_dev.properties p
            LEFT JOIN (SELECT property_id, JSON_ARRAYAGG(JSON_OBJECT
                                ('owner_uid', owner_uid,
                                'owner_user_id', owner_user_id,
                                'po_owner_percent', po_owner_percent,
                                'owner_first_name', owner_first_name,
                                'owner_last_name', owner_last_name,
                                'owner_phone_number', owner_phone_number,
                                'owner_email', owner_email,
                                'owner_photo_url', owner_photo_url
                                )) AS owners
                                FROM space_dev.o_details
                                GROUP BY property_id) as o ON o.property_id = p.property_uid 
            LEFT JOIN space_dev.b_details b ON o.property_id = b.contract_property_id
            -- WHERE b.business_uid = '600-000003'
            -- WHERE o.owner_uid = '110-000003'
            -- WHERE o.owner_uid = \'""" + user_id + """\';
            -- WHERE b.business_uid = \'""" + user_id + """\';
            WHERE {column} =  \'""" + user_id + """\'  
            """

    if user_id.startswith("110"):
        query = query.format(column='owner_uid')
    elif user_id.startswith("600"):
        query = queryPM.format(column='business_uid')
    else:
        print("Invalid condition type")
        return None

    # print(query)


    try:
        # Run query to find Announcements Received
        with connect() as db:    
            response = db.execute(query)
            print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in Contracts Query ")


def MaintenanceRequests(user_id):
    print("In Maintenance Request Query FUNCTION CALL")

    query = """
            SELECT *
                -- quote_business_id, quote_status, maintenance_request_status, quote_total_estimate
                , CASE
                        WHEN maintenance_request_status = 'NEW' OR maintenance_request_status = 'INFO'              THEN "NEW REQUEST"
                        WHEN maintenance_request_status = "SCHEDULED"                                               THEN "SCHEDULED"
                        WHEN maintenance_request_status = 'CANCELLED' or quote_status = "FINISHED"                  THEN "COMPLETED"
                        WHEN quote_status = "SENT" OR quote_status = "REFUSED" OR quote_status = "REQUESTED"
                        OR quote_status = "REJECTED" OR quote_status = "WITHDRAWN" OR quote_status = "MORE INFO"    THEN "QUOTES REQUESTED"
                        WHEN quote_status = "ACCEPTED" OR quote_status = "SCHEDULE"                                 THEN "QUOTES ACCEPTED"
                        WHEN quote_status = "COMPLETED"                                                             THEN "PAID"     
                        ELSE quote_status
                    END AS maintenance_status
            FROM (
                SELECT * 
                FROM space_dev.maintenanceRequests
                LEFT JOIN (
                    SELECT *,
                        CASE
                            WHEN max_quote_rank = "10" THEN "REQUESTED"
                            WHEN max_quote_rank = "11" THEN "REFUSED"
                            WHEN max_quote_rank = "12" THEN "MORE INFO"     
                            WHEN max_quote_rank = "20" THEN "SENT"
                            WHEN max_quote_rank = "21" THEN "REJECTED"
                            WHEN max_quote_rank = "22" THEN "WITHDRAWN"
                            WHEN max_quote_rank = "30" THEN "ACCEPTED"
                            WHEN max_quote_rank = "40" THEN "SCHEDULE"
                            WHEN max_quote_rank = "50" THEN "SCHEDULED"
                            WHEN max_quote_rank = "60" THEN "RESCHEDULED"
                            WHEN max_quote_rank = "65" THEN "CANCELLED"
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
                                WHEN quote_status = "MORE INFO" THEN "12"
                                WHEN quote_status = "SENT" THEN "20"
                                WHEN quote_status = "REJECTED" THEN "21"
                                WHEN quote_status = "WITHDRAWN"  THEN "22"
                                WHEN quote_status = "ACCEPTED" THEN "30"
                                WHEN quote_status = "SCHEDULE" THEN "40"
                                WHEN quote_status = "SCHEDULED" THEN "50"
                                WHEN quote_status = "RESCHEDULED" THEN "60"
                                WHEN quote_status = "CANCELLED" THEN "65"
                                WHEN quote_status = "FINISHED" THEN "70"
                                WHEN quote_status = "COMPLETED" THEN "80"     
                                ELSE 0
                            END AS quote_rank
                            FROM space_dev.maintenanceQuotes
                            ) AS qr
                        GROUP BY quote_maintenance_request_id
                        ) AS qr_quoterank
                ) AS quote_summary ON maintenance_request_uid = qmr_id
            ) AS quotes
            LEFT JOIN ( SELECT * FROM space_dev.contracts WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
            LEFT JOIN space_dev.property_owner ON contract_property_id = property_id
            -- WHERE contract_business_id = \'""" + user_id + """\'
            -- WHERE contract_business_id = "600-000003"
            -- WHERE property_owner_id = "110-000003"
            -- WHERE owner_uid = \'""" + user_id + """\'
            WHERE {column} =  \'""" + user_id + """\'
                """

    if user_id.startswith("110"):
        query = query.format(column='property_owner_id')
    elif user_id.startswith("600"):
        query = query.format(column='contract_business_id')
    elif user_id.startswith("200"):
        query = query.format(column='maintenance_property_id')
    else:
        print("Invalid condition type")
        return None

    # print(query)

    try:
        # Run query to find Announcements Received
        with connect() as db:    
            response = db.execute(query)
            print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in Maintenance Query ")


def MaintenanceDetails(user_id):
    print("In Maintenance Details Query FUNCTION CALL")

    query = """
            SELECT *
            FROM (
                SELECT * 
                FROM space_dev.maintenanceRequests
                LEFT JOIN space_dev.property_owner ON maintenance_property_id = property_id
                LEFT JOIN ( SELECT * FROM space_dev.contracts WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                -- WHERE property_owner_id = '110-000007'
                -- WHERE maintenance_property_id = '200-000084'
                -- WHERE maintenance_assigned_business = '600-000010'  -- THIS DOESN"T WORK IN ACTUAL QUERY
                -- WHERE contract_business_id = '600-000043'
                WHERE {column} =  \'""" + user_id + """\' 
                ) AS mr

            LEFT JOIN (
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
                            FROM space_dev.maintenanceQuotes
                            ) AS qr
                        GROUP BY quote_maintenance_request_id
                        ) AS qr_quoterank
                ) AS quote_summary ON maintenance_request_uid = qmr_id 
                """

    if user_id.startswith("110"):
        query = query.format(column='property_owner_id')
    elif user_id.startswith("600"):
        query = query.format(column='contract_business_id')
    elif user_id.startswith("200"):
        query = query.format(column='maintenance_property_id')
    else:
        print("Invalid condition type")
        return None

    # print(query)

    try:
        # Run query to find Announcements Received
        with connect() as db:    
            response = db.execute(query)
            print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in Maintenance Query ")


def ProfileInfo(user_id):
    print("In Profile Info Query FUNCTION CALL")

    query = """
            SELECT * FROM {table} 
            LEFT JOIN (
                SELECT paymentMethod_profile_id, JSON_ARRAYAGG(JSON_OBJECT
                    ('paymentMethod_uid', paymentMethod_uid,
                    'paymentMethod_type', paymentMethod_type,
                    'paymentMethod_name', paymentMethod_name,
                    'paymentMethod_acct', paymentMethod_acct,
                    'paymentMethod_routing_number', paymentMethod_routing_number,
                    'paymentMethod_micro_deposits', paymentMethod_micro_deposits,
                    'paymentMethod_exp_date', paymentMethod_exp_date,
                    'paymentMethod_cvv', paymentMethod_cvv,
                    'paymentMethod_billingzip', paymentMethod_billingzip,
                    'paymentMethod_status', paymentMethod_status
                    )) AS paymentMethods
                    FROM space_dev.paymentMethods
                    GROUP BY paymentMethod_profile_id) as p ON {column} = paymentMethod_profile_id
            -- WHERE owner_uid = '110-000003'
            -- WHERE business_uid = '600-000003'
            -- WHERE tenant_uid = '350-000003'
            WHERE {column} = \'""" + user_id + """\'
            """
    # print(query)

    if user_id.startswith("110"):
        query = query.format(column='owner_uid', table = 'space_dev.ownerProfileInfo')
    elif user_id.startswith("600"):
        query = query.format(column='business_uid', table = 'space_dev.businessProfileInfo')
    elif user_id.startswith("350"):
        query = query.format(column='tenant_uid', table = 'space_dev.tenantProfileInfo')
    else:
        print("Invalid condition type")
        return None

    # print(query)

    try:
        # Run query to find Announcements Received
        with connect() as db:  
            print(query)
            response = db.execute(query)
            print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in Profile Info Query ")