# from flask import request
from data_pm import connect, uploadImage, s3



def DashboardCashflowQuery(user_id):
    print("In DashboardCashflowQuery FUNCTION CALL")

    try:
        # Run query to find rents of ACTIVE leases
        with connect() as db:    
            response = db.execute("""
                    -- CASHFLOW FOR A PARTICULAR OWNER OR MANAGER
                    SELECT pur_receiver, pur_payer
                        , SUM(pur_amount_due) AS pur_amount_due
                        , SUM(total_paid) AS total_paid
                        , cf_month, cf_month_num, cf_year
                        , pur_cf_type
                    FROM space.pp_status
                    -- WHERE (pur_receiver = '110-000003' OR pur_payer = '110-000003')
                    -- WHERE (pur_receiver = '600-000003' OR pur_payer = '600-000003')
                    WHERE (pur_receiver = \'""" + user_id + """\' OR pur_payer = \'""" + user_id + """\')
                    GROUP BY cf_month, cf_year, pur_cf_type
                    ORDER BY cf_month_num
                    """)
            # print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in UnpaidRents Query ")


def CashflowQuery(user_id):
    print("In Cashflow Query FUNCTION CALL", user_id)

    try:
        # Run query to find cashflow for a user by month, year, type and property
        with connect() as db:    
            response = db.execute("""
                    -- CASHFLOW GROUPED BY USER, BY YEAR, BY MONTH, BY CF_TYPE, BY PURCHASE CATEGORY
                    SELECT pur_receiver, pur_payer
                        , SUM(pur_amount_due) AS pur_amount_due
                        , SUM(total_paid) AS total_paid
                        , cf_month, cf_month_num, cf_year
                        , pur_cf_type
                        , purchase_type
                        -- , property_uid, property_address, property_unit, property_city, property_state, property_zip
                        , JSON_ARRAYAGG(JSON_OBJECT(
                            'property_uid',  property_uid,
                            'property_address', property_address,
                            'property_unit',  property_unit,
                            'property_city', property_city,
                            'property_state',  property_state,
                            'property_zip', property_zip,
                            'individual_purchase', individual_purchase
                            )) AS property
                    FROM (
                        SELECT pur_receiver, pur_payer
                            , SUM(pur_amount_due) AS pur_amount_due
                            , SUM(total_paid) AS total_paid
                            , cf_month, cf_month_num, cf_year
                            , pur_cf_type
                            , purchase_type
                            , property_uid, property_address, property_unit, property_city, property_state, property_zip
                            -- , purchase_uid, purchase_status, pur_amount_due
                            , JSON_ARRAYAGG(JSON_OBJECT(
                                'purchase_uid',  purchase_uid,
                                'pur_description', pur_description, 
                                'purchase_status', purchase_status,
                                'pur_amount_due', pur_amount_due,
                                'total_paid', total_paid,
                                'pur_leaseFees_id', pur_leaseFees_id,
                                'contract_uid,', contract_uid
                                )) AS individual_purchase
                            -- , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description
                        --     , pur_receiver, pur_initiator, pur_payer
                        --     , pur_group, pur_leaseFees_id, pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_month_num, cf_year
                        --     , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email
                        --     , initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email
                        --     , payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email
                        --     , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image
                        --     , property_id, property_owner_id, po_owner_percent
                        --     , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_photo_url
                        --     , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                        --     , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                        --     , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_actual_rent, leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lt_lease_id, lt_tenant_id, lt_responsibility
                        --     , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                        FROM space.pp_details
                        -- WHERE (pur_receiver = '110-000003' OR pur_payer = '110-000003') -- AND cf_month = 'JANUARY' AND cf_year = '2024'
                        -- WHERE (pur_receiver = '350-000003' OR pur_payer = '350-000003') -- AND cf_month = 'JANUARY' AND cf_year = '2024'
                        -- WHERE (pur_receiver = '600-000003' OR pur_payer = '600-000003') -- AND cf_month = 'SEPTEMBER' AND cf_year = '2024'
                        WHERE (pur_receiver = \'""" + user_id + """\' OR pur_payer = \'""" + user_id + """\')
                        GROUP BY cf_month, cf_year, pur_cf_type, purchase_type, property_uid 
                        ORDER BY cf_month_num, property_uid
                        ) AS p
                        GROUP BY cf_month, cf_year, pur_cf_type, purchase_type
                        ORDER BY cf_month_num
                    """)
            # print("Function Query Complete")
            # print("This is the Function response: ", response)
        return response
    except:
        print("Error in Cashflow Query ")


def UnpaidRents():
    print("In Unpaid Rents Query FUNCTION CALL")

    try:
        # Run query to find rents of ACTIVE leases
        with connect() as db:    
            response = db.execute("""
                    -- DETERMINE WHICH RENTS ARE PAID OR PARTIALLY PAID
                    SELECT *
						, DATE_FORMAT(DATE_ADD(STR_TO_DATE(pur_due_date, '%m-%d-%Y'), INTERVAL pur_late_by DAY), '%m-%d-%Y') AS late_by_date
					FROM space.purchases
					LEFT JOIN space.contracts ON contract_property_id = pur_property_id
					LEFT JOIN space.property_owner ON property_id = pur_property_id
					WHERE purchase_type = "RENT" AND
						  contract_status = "ACTIVE" AND
                          DATE_ADD(STR_TO_DATE(pur_due_date, '%m-%d-%Y'), INTERVAL pur_late_by DAY) < CURDATE() AND
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
                                        CASE 
                                            WHEN CURDATE() <= STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d') THEN 
                                                STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d')
                                            ELSE 
                                                DATE_ADD(STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d'), INTERVAL 6 MONTH)
                                        END
                                    WHEN frequency = 'Annually' THEN
                                        IF(CURDATE() <= STR_TO_DATE(due_by_date, '%m-%d-%Y'), 
                                            STR_TO_DATE(due_by_date, '%m-%d-%Y'), 
                                            DATE_ADD(STR_TO_DATE(due_by_date, '%m-%d-%Y'), INTERVAL 1 YEAR)
                                        )
                                    WHEN frequency = 'Weekly' THEN 
                                        DATE_ADD(CURDATE(), INTERVAL (due_by - DAYOFWEEK(CURDATE()) + 7) % 7 DAY)
                                    WHEN frequency = 'Bi-Weekly' THEN
                                        DATE_ADD(STR_TO_DATE(due_by_date, '%m-%d-%Y'), INTERVAL (FLOOR(DATEDIFF(CURDATE(), STR_TO_DATE(due_by_date, '%m-%d-%Y')) / 14) + 1) * 14 DAY)
                                END, '%m-%d-%Y') AS next_due_date
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
                        AND STR_TO_DATE(next_due_date, '%m-%d-%Y') = STR_TO_DATE(pur_due_date, '%m-%d-%Y')
                    """)
            
            # print("Function Query Complete")
            # print("THis is the Function response: ", response)
        return response
    except:
        print("Error in NextDueDate Query ")

