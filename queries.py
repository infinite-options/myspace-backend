# from flask import request
from data_pm import connect, uploadImage, s3

def testQuery(user_id):
    print("In testQuery FUNCTION CALL")

    query = 'SELECT * FROM space.purchases WHERE {column} LIKE %s'
    # like_pattern = '600%'

    if user_id.startswith("110"):
        query = query.format(column='pur_receiver')
        like_pattern = '110%'
    elif user_id.startswith("600"):
        query = query.format(column='pur_payer')
        like_pattern = '600%'
    else:
        print("Invalid condition type")
        return None

    # print(query)

    try:
        # Run query to find Announcements Received
        with connect() as db:    
            response = db.execute(query, (like_pattern,))
            print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in testQuery Query ")


def RentStatusQuery(user_id):
    print("In RentStatusQuery FUNCTION CALL")

    # query = 'SELECT * FROM space.purchases WHERE {column} LIKE %s'
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
                FROM space.p_details
                -- WHERE tenant_uid = '350-000002'
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
                    , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_month
                    , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_year
                FROM space.purchases
                WHERE purchase_type = "Rent"
                    AND LEFT(pur_payer, 3) = '350'
                    AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = MONTH(CURRENT_DATE)
                    AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = YEAR(CURRENT_DATE)
                GROUP BY pur_property_id, purchase_type
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


def RentDashboardQuery(user_id):
    print("In RentDashboardQuery FUNCTION CALL")

    # query = 'SELECT * FROM space.purchases WHERE {column} LIKE %s'
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
                    FROM space.p_details
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
                    FROM space.purchases
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


def RentDetailsQuery(user_id):
    print("In RentDetailsQuery FUNCTION CALL")

    # query = 'SELECT * FROM space.purchases WHERE {column} LIKE %s'
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
                    FROM space.p_details
                    -- WHERE business_uid = "600-000003"
                    -- WHERE owner_uid = "110-000003"
                    -- WHERE owner_uid = \'""" + user_id + """\'
                    -- WHERE tenant_uid = '350-000003'
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
                    FROM space.purchases
                    WHERE LEFT(pur_payer, 3) = '350'
                        -- AND purchase_type = "Rent"
                        AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = MONTH(CURRENT_DATE)
                        AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = YEAR(CURRENT_DATE)
                    GROUP BY pur_property_id -- , purchase_type
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
        print("Error in RentDetailsQuery ")


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
                    FROM space.announcements a
                    LEFT JOIN space.businessProfileInfo b ON a.announcement_receiver LIKE '600%' AND b.business_uid = a.announcement_receiver
                    LEFT JOIN space.ownerProfileInfo c ON a.announcement_receiver LIKE '110%' AND c.owner_uid = a.announcement_receiver
                    LEFT JOIN space.tenantProfileInfo d ON a.announcement_receiver LIKE '350%' AND d.tenant_uid = a.announcement_receiver
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
                        space.announcements a
                    LEFT JOIN space.businessProfileInfo b ON a.announcement_sender LIKE '600%' AND b.business_uid = a.announcement_sender
                    LEFT JOIN space.ownerProfileInfo c ON a.announcement_sender LIKE '110%' AND c.owner_uid = a.announcement_sender
                    LEFT JOIN space.tenantProfileInfo d ON a.announcement_sender LIKE '350%' AND d.tenant_uid = a.announcement_sender
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
            #         -- CASHFLOW FOR A PARTICULAR OWNER OR MANAGER
            #         SELECT pur_receiver, pur_payer
            #             , SUM(pur_amount_due) AS pur_amount_due
            #             , SUM(total_paid) AS total_paid
            #             , cf_month, cf_month_num, cf_year
            #             , pur_cf_type
            #         FROM space.pp_status
            #         -- WHERE (pur_receiver = '110-000003' OR pur_payer = '110-000003')
            #         -- WHERE (pur_receiver = '600-000003' OR pur_payer = '600-000003')
            #         WHERE (pur_receiver = \'""" + user_id + """\' OR pur_payer = \'""" + user_id + """\')
            #         GROUP BY cf_month, cf_year, pur_cf_type
            #         ORDER BY cf_month_num
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
                    FROM space.pp_status
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
                    FROM space.pp_status
                    -- WHERE pur_payer = '600-000003' AND purchase_type != 'Deposit'
                    WHERE pur_payer = \'""" + user_id + """\' AND purchase_type != 'Deposit'
                    GROUP BY cf_month, cf_year                                  
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
                    -- DETERMINE WHICH RENTS ARE PAID OR PARTIALLY PAID
                    SELECT *
						, DATE_FORMAT(DATE_ADD(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i'), INTERVAL pur_late_by DAY), '%m-%d-%Y %H:%i') AS late_by_date
					FROM space.purchases
					LEFT JOIN space.contracts ON contract_property_id = pur_property_id
					LEFT JOIN space.property_owner ON property_id = pur_property_id
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
                                        DATE_ADD(CURDATE(), INTERVAL (due_by - DAYOFWEEK(CURDATE()) + 7) % 7 DAY)
                                END, '%m-%d-%Y %H:%i') AS next_due_date
                        FROM (
                            SELECT * FROM space.leases WHERE lease_status = 'ACTIVE'
                            ) AS l
                        LEFT JOIN (
                            SELECT * FROM space.leaseFees WHERE frequency != 'One Time'
                            ) AS lf ON fees_lease_id = lease_uid  				-- get lease fees
                        LEFT JOIN space.lease_tenant ON fees_lease_id = lt_lease_id            	-- get tenant responsible for rent
                        LEFT JOIN space.property_owner ON lease_property_id = property_id      	-- get property owner and ownership percentage
                        LEFT JOIN (
                            SELECT * FROM space.contracts WHERE contract_status = 'ACTIVE'
                            ) AS c ON lease_property_id = contract_property_id  				-- to make sure contract is active
                        ) AS ndd
                    LEFT JOIN space.purchases ON lease_property_id = pur_property_id
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
                    FROM space.contracts
                    WHERE contract_status = 'APPROVED'
                        AND STR_TO_DATE(contract_start_date, '%m-%d-%Y') <= CURDATE();
                    """)

            # response = db.execute("""
            #         UPDATE space.contracts AS c
            #         JOIN (
            #             SELECT contract_property_id
            #             FROM space.contracts
            #             WHERE STR_TO_DATE(contract_start_date, '%m-%d-%Y') <= CURDATE()
            #             AND contract_status = 'APPROVED'
            #         ) AS approved_contracts
            #         ON c.contract_property_id = approved_contracts.contract_property_id
            #         SET c.contract_status = 'INACTIVE'
            #         WHERE c.contract_status = 'ACTIVE';

            #         SELECT * 
            #         FROM space.contracts
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
                    , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                    -- , owner_address, owner_unit, owner_city, owner_state, owner_zip
                    , owner_photo_url
                    , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date, contract_end_notice_period, contract_m2m
                    , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_services_fees
                    -- , business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                FROM space.o_details o
                LEFT JOIN space.properties p ON o.property_id =p.property_uid 
                LEFT JOIN space.b_details b ON o.property_id = b.contract_property_id
                -- WHERE b.business_uid = '600-000011'
                -- WHERE o.owner_uid = '110-000003'
                -- WHERE o.owner_uid = \'""" + user_id + """\';
                -- WHERE b.business_uid = \'""" + user_id + """\';
                WHERE {column} =  \'""" + user_id + """\'  
                    """

    if user_id.startswith("110"):
        query = query.format(column='owner_uid')
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
            print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in Contracts Query ")


def MaintenanceDetails(user_id):
    print("In Maintenance Query FUNCTION CALL")

    query = """
            SELECT *
            FROM (
                SELECT * 
                FROM space.maintenanceRequests
                LEFT JOIN space.property_owner ON maintenance_property_id = property_id
                LEFT JOIN ( SELECT * FROM space.contracts WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                -- WHERE property_owner_id = '110-000007'
                -- WHERE maintenance_property_id = '200-000084'
                -- WHERE maintenance_assigned_business = '600-000010'  -- THIS DOESN"T WORK IN ACTUAL QUERY
                -- WHERE contract_business_id = '600-000043'
                -- WHERE {column} =  \'""" + user_id + """\' 
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
                            FROM space.maintenanceQuotes
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