from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity


from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest

class Dashboard(Resource):
    def get(self, user_id):
        print('in Dashboard ', user_id)
        response = {}
        if user_id.startswith("600"):
            business_type = ""
            print('in Get Business Contacts')
            with connect() as db:
                print("in connect loop")
                query = db.execute(""" 
                    -- FIND ALL CURRENT BUSINESS CONTACTS
                        SELECT business_type
                        FROM space.businessProfileInfo
                        WHERE business_uid = \'""" + user_id + """\';
                        """)

            business_type = query['result'][0]['business_type']
            print(business_type)

            if business_type == "MAINTENANCE":
                with connect() as db:
                    print("in maintenance dashboard")
                    currentActivity = db.execute(""" 
                            -- CURRENT ACTIVITY FOR GRAPH AND TABLE
                            SELECT -- *,
                                quote_business_id
                                , maintenance_status
                                , COUNT(maintenance_status) AS num
                                , SUM(quote_total_estimate) AS total_estimate
                                , JSON_ARRAYAGG(JSON_OBJECT(
                                    'maintenance_request_uid', maintenance_request_uid,
                                    'maintenance_property_id', maintenance_property_id,
                                    'maintenance_title', maintenance_title,
                                    'maintenance_desc', maintenance_desc,
                                    'maintenance_images', maintenance_images,
                                    'maintenance_request_type', maintenance_request_type,
                                    'maintenance_priority', maintenance_priority,
                                    'maintenance_request_created_date', maintenance_request_created_date,
                                    'maintenance_scheduled_date', maintenance_scheduled_date,
                                    'maintenance_scheduled_time', maintenance_scheduled_time,
                                    'maintenance_frequency', maintenance_frequency,
                                    'property_address', property_address,
                                    'property_unit', property_unit,
                                    'property_city', property_city,
                                    'property_state', property_state,
                                    'property_zip', property_zip
                                    )) AS maintenance_request_info
                            FROM (
                                SELECT *
                                , CASE
                                        WHEN quote_status IN ("REQUESTED", "MORE INFO")                 THEN "REQUESTED"
                                        WHEN quote_status = "SENT" 	                                    THEN "SUBMITTED"
                                        WHEN quote_status IN ("ACCEPTED", "SCHEDULE")                   THEN "ACCEPTED"
                                        WHEN quote_status IN ("SCHEDULED" , "RESCHEDULE")               THEN "SCHEDULED"
                                        WHEN quote_status = "FINISHED"                                  THEN "FINISHED"
                                        WHEN quote_status = "COMPLETED"                                 THEN "PAID"   
                                        WHEN quote_status IN ("CANCELLED", "ARCHIVE", "NOT ACCEPTED","WITHDRAWN" ,"WITHDRAW", "REFUSED" ,"REJECTED")      THEN "ARCHIVE"
                                        ELSE quote_status
                                    END AS maintenance_status
                                FROM space.m_details
                                LEFT JOIN space.properties ON maintenance_property_id = property_uid
                                -- WHERE quote_business_id = '600-000012' 
                                WHERE quote_business_id = \'""" + user_id + """\'
                                ) AS ms
                            GROUP BY maintenance_status
                            ORDER BY maintenance_status;
                            """)

                    # print("Query: ", maintenanceQuery)
                    response["CurrentActivities"] = currentActivity

                    workOrders = db.execute(""" 
                            -- WORK ORDERS
                            SELECT *
                            FROM (
                                SELECT * -- , quote_business_id, quote_status, maintenance_request_status, quote_total_estimate
                                    , CASE
                                            WHEN quote_status = "SENT" OR quote_status = "WITHDRAWN" OR quote_status = "REFUSED" OR quote_status = "REJECTED"  THEN "SUBMITTED"
                                            WHEN quote_status = "ACCEPTED" OR quote_status = "SCHEDULE"   THEN "ACCEPTED"
                                            WHEN quote_status = "SCHEDULED" OR quote_status = "RESCHEDULE"   THEN "SCHEDULED"
                                            WHEN quote_status = "COMPLETED"   THEN "PAID"
                                            ELSE quote_status
                                        END AS maintenance_status
                                FROM space.m_details
                                WHERE quote_business_id = \'""" + user_id + """\'
                                    ) AS ms
                            ORDER BY maintenance_status;
                            """)

                    # print("Query: ", workOrders)
                    response["WorkOrders"] = workOrders


                    currentQuotes = db.execute(""" 
                            -- CURRENT QUOTES
                            SELECT m_details.*
                                , property_uid
                                , property_address, property_unit, property_city, property_state, property_zip
                                , business_uid
                                , business_name, business_phone_number, business_email 
                                , business_photo_url
                                , CASE
                                        WHEN quote_status = "REQUESTED"                                 THEN "REQUESTED"
                                        WHEN quote_status = "SENT" 	                                    THEN "SUBMITTED"
                                        WHEN quote_status IN ("ACCEPTED", "SCHEDULE")                   THEN "ACCEPTED"
                                        WHEN quote_status IN ("SCHEDULED" , "RESCHEDULE")               THEN "SCHEDULED"
                                        WHEN quote_status = "FINISHED"                                  THEN "FINISHED"
                                        WHEN quote_status = "COMPLETED"                                 THEN "PAID"   
                                        WHEN quote_status IN ("CANCELLED", "ARCHIVE", "NOT ACCEPTED","WITHDRAWN" ,"WITHDRAW", "REFUSED" ,"REJECTED")      THEN "ARCHIVE"
                                        ELSE quote_status
                                    END AS maintenance_status 
                            FROM space.m_details
                            LEFT JOIN space.properties ON property_uid = maintenance_property_id
                            LEFT JOIN space.b_details ON contract_property_id = maintenance_property_id
                            -- WHERE quote_business_id = '600-000012' 
                            WHERE quote_business_id = \'""" + user_id + """\'
                                AND quote_status IN ("ACCEPTED", "SCHEDULE", "SCHEDULED" , "RESCHEDULE", "FINISHED", "COMPLETED");
                            """)

                    # print("Query: ", workOrders)
                    response["CurrentQuotes"] = currentQuotes

                    return response
            elif business_type == "MANAGEMENT":
                with connect() as db:
                    print("in Manager dashboard")
                    print("in connect loop")

                    # PROFITABILITY
                    response["Profitability"] = {}

                    # PROFITABLITY REVENUE
                    revenue = db.execute(""" 
                                -- MONEY TO BE RECEIVED
                                SELECT -- *,
                                    -- purchase_uid, pur_timestamp, pur_property_id, purchase_type, 
                                    -- pur_cf_type -- , pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description, 
                                    pur_receiver -- , pur_initiator, pur_payer, pur_group, pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, 
                                    , cf_month, cf_month_num, cf_year -- , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email, initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email, payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email, property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_photo_url, contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date, business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url, lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_actual_rent, leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lt_lease_id, lt_tenant_id, lt_responsibility, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_current_address-DNU, tenant_photo_url
                                    , SUM(pur_amount_due) AS received_expected
                                    , SUM(pur_amount_due-amt_remaining) AS received_actual
                                FROM space.pp_details
                                WHERE cf_month = DATE_FORMAT(NOW(), '%M')
                                    AND cf_year = DATE_FORMAT(NOW(), '%Y')
                                    -- AND pur_receiver = '600-000003' 
                                    AND pur_receiver = \'""" + user_id + """\'
                                """)

                    response["Profitability"]["revenue"] = revenue
                
                    
                    # PROFITABLITY EXPENSE
                    expense = db.execute("""
                                -- MONEY TO BE PAID
                                SELECT -- *,
                                    -- purchase_uid, pur_timestamp, pur_property_id, purchase_type, 
                                    -- pur_cf_type -- , pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description, 
                                    pur_payer -- , pur_initiator, pur_payer, pur_group, pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, 
                                    , cf_month, cf_month_num, cf_year -- , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email, initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email, payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email, property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_photo_url, contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date, business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url, lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_actual_rent, leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lt_lease_id, lt_tenant_id, lt_responsibility, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_current_address-DNU, tenant_photo_url
                                    , SUM(pur_amount_due) AS paid_expected
                                    , SUM(pur_amount_due-amt_remaining) AS paid_actual
                                FROM space.pp_details
                                WHERE cf_month = DATE_FORMAT(NOW(), '%M')
                                    AND cf_year = DATE_FORMAT(NOW(), '%Y')
                                    -- AND pur_payer = '600-000003'
                                    AND pur_payer = \'""" + user_id + """\'
        	                    """)

                    response["Profitability"]["expense"] = expense

                    # print("Complete Profitability")


                    # HAPPINESS MATRIX - VACANCY AND CASHFLOW
                    response["HappinessMatrix"] = {}
                    matrix = db.execute("""
                                -- CASHFLOW ENDPOINT REWRITE TO INCLUDE BOTH VACANCY AND CASHFLOW
                                SELECT * 
                                    FROM (
                                        SELECT -- *,
                                            owner_uid, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_photo_url, business_uid -- , cf_month, cf_month_num, cf_year, expected_cashflow, actual_cashflow, delta_cashflow, percent_delta_cashflow
                                            , SUM(expected_cashflow) AS expected_cashflow
                                            , SUM(actual_cashflow) AS actual_cashflow
                                            , CAST((SUM(expected_cashflow) - SUM(actual_cashflow)) AS DECIMAL(10,2)) AS delta_cashflow
                                            , IF(SUM(expected_cashflow) = 0, 0, CAST(ABS(SUM(expected_cashflow) - SUM(actual_cashflow))*100/SUM(expected_cashflow) AS DECIMAL(10,2))) AS percent_delta_cashflow
                                        FROM (
                                            SELECT *,
                                                CAST((expected_cashflow - actual_cashflow) AS DECIMAL(10,2)) AS delta_cashflow
                                                , IF(expected_cashflow = 0, 0, CAST(ABS(expected_cashflow - actual_cashflow)/expected_cashflow AS DECIMAL(10,2))) AS percent_delta_cashflow
                                            FROM (
                                                SELECT -- *,
                                                    property_uid -- , property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_utilities, po_owner_percent, po_start_date, po_end_date
                                                    , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email -- , owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents
                                                    , owner_photo_url -- , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                                    , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                                                    -- , lease_uid, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
                                                    -- , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                                                    -- , pp_status.*
                                                    -- , purchase_uid, pur_timestamp
                                                    -- , pur_property_id, purchase_type, pur_cf_type -- , pur_bill_id, purchase_date, pur_due_date
                                                    -- , pur_amount_due -- , purchase_status, pur_status_value, pur_notes, pur_description
                                                    -- , pur_receiver, pur_initiator, pur_payer, pur_group -- , pay_purchase_id, latest_date
                                                    -- , total_paid, payment_status, amt_remaining
                                                    , cf_month, cf_month_num, cf_year
                                                    , SUM(CASE
                                                            WHEN pur_cf_type = 'revenue' AND owner_uid = pur_receiver THEN pur_amount_due
                                                            WHEN pur_cf_type = 'expense' AND owner_uid = pur_payer THEN -pur_amount_due
                                                            ELSE 0
                                                        END) AS expected_cashflow
                                                    , SUM(CASE
                                                            WHEN pur_cf_type = 'revenue' AND owner_uid = pur_receiver THEN total_paid
                                                            WHEN pur_cf_type = 'expense' AND owner_uid = pur_payer THEN -total_paid
                                                            ELSE 0
                                                        END) AS actual_cashflow
                                                FROM space.p_details
                                                LEFT JOIN space.pp_status ON property_uid = pur_property_id
                                                -- WHERE business_uid = '600-000003'
                                                WHERE business_uid = \'""" + user_id + """\'
                                                GROUP BY 
                                                    owner_uid
                                                    , cf_month
                                                    , cf_year
                                                ORDER BY owner_uid
                                                ) AS cf_details
                                            ) AS cfd  
                                            GROUP BY owner_uid
                                        ) AS cf
                                    LEFT JOIN (
                                    SELECT 
                                        property_owner_id,
                                        COUNT(CASE WHEN rent_status = 'VACANT' THEN 1 END) as vacancy_num, 
                                        COUNT(*) AS total_properties,
                                        cast(COUNT(CASE WHEN rent_status = 'VACANT' THEN 1 END)*-100/COUNT(*) as decimal) as vacancy_perc
                                    FROM (
                                        SELECT *,
                                            CASE
                                                WHEN (lease_status = 'ACTIVE' AND payment_status IS NOT NULL) THEN payment_status
                                                WHEN (lease_status = 'ACTIVE' AND payment_status IS NULL) THEN 'UNPAID'
                                                ELSE 'VACANT'
                                            END AS rent_status
                                        FROM (
                                            SELECT *
                                            FROM space.property_owner
                                            LEFT JOIN space.properties ON property_uid = property_id
                                            LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = 'ACTIVE') AS l ON property_uid = lease_property_id
                                            LEFT JOIN (SELECT * FROM space.contracts WHERE contract_status = 'ACTIVE') AS c ON contract_property_id = property_uid
                                            -- WHERE contract_business_id = '600-000003'
                                            WHERE contract_business_id = \'""" + user_id + """\'
                                        ) AS o
                                        LEFT JOIN (
                                            SELECT *
                                            FROM space.pp_status 
                                            WHERE (purchase_type = 'RENT' OR ISNULL(purchase_type))
                                                AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                                                AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                                        ) as r
                                        ON pur_property_id = property_id
                                        GROUP BY property_id
                                    ) AS rs
                                    GROUP BY property_owner_id
                                    ) AS v on cf.owner_uid = v.property_owner_id;
        	                    """)

                    response["HappinessMatrix"]["matrix_data"] = matrix

                    # print("Complete Profitability")

                    
                    # HAPPINESS MATRIX - VACANCY

                    vacancy = db.execute(""" 
                        SELECT 
                            property_owner_id as owner_uid,
                            COUNT(CASE WHEN rent_status = 'VACANT' THEN 1 END) as vacancy_num, 
                            COUNT(*) AS total_properties,
                            cast(COUNT(CASE WHEN rent_status = 'VACANT' THEN 1 END)*-100/COUNT(*) as decimal) as vacancy_perc
                        FROM (
                            SELECT *,
                                CASE
                                    WHEN (lease_status = 'ACTIVE' AND payment_status IS NOT NULL) THEN payment_status
                                    WHEN (lease_status = 'ACTIVE' AND payment_status IS NULL) THEN 'UNPAID'
                                    ELSE 'VACANT'
                                END AS rent_status
                            FROM (
                                SELECT *
                                FROM space.property_owner
                                LEFT JOIN space.properties ON property_uid = property_id
                                LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = 'ACTIVE') AS l ON property_uid = lease_property_id
                                LEFT JOIN (SELECT * FROM space.contracts WHERE contract_status = 'ACTIVE') AS c ON contract_property_id = property_uid
                                WHERE contract_business_id = \'""" + user_id + """\'
                            ) AS o
                            LEFT JOIN (
                                SELECT *
                                FROM space.pp_status 
                                WHERE (purchase_type = 'RENT' OR ISNULL(purchase_type))
                                    AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                                    AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                            ) as r
                            ON pur_property_id = property_id
                            GROUP BY property_id
                        ) AS rs
                        GROUP BY property_owner_id;
                                """)

                    response["HappinessMatrix"]["vacancy"] = vacancy
                    
                    for i in range(0,len(response["HappinessMatrix"]["vacancy"]["result"])):
                        response["HappinessMatrix"]["vacancy"]["result"][i]["vacancy_perc"] = float(response["HappinessMatrix"]["vacancy"]["result"][i]["vacancy_perc"])
                    

                    # HAPPINESS MATRIX - CASHFLOW DETAILS BY MONTH
                    delta_cashflow_details = db.execute("""
                                SELECT *,
                                    CAST((expected_cashflow - actual_cashflow) AS DECIMAL(10,2)) AS delta_cashflow
                                    , IF(expected_cashflow = 0, 0, CAST(ABS(expected_cashflow - actual_cashflow)/expected_cashflow AS DECIMAL(10,2))) AS percent_delta_cashflow
                                FROM (
                                    SELECT -- *,
                                        -- property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_utilities, po_owner_percent, po_start_date, po_end_date
                                        owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email -- , owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents
                                        , owner_photo_url -- , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                        , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                                        -- , lease_uid, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
                                        -- , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                                        -- , pp_status.*
                                        -- , purchase_uid, pur_timestamp
                                        -- , pur_property_id, purchase_type, pur_cf_type -- , pur_bill_id, purchase_date, pur_due_date
                                        -- , pur_amount_due -- , purchase_status, pur_status_value, pur_notes, pur_description
                                        -- , pur_receiver, pur_initiator, pur_payer, pur_group -- , pay_purchase_id, latest_date
                                        -- , total_paid, payment_status, amt_remaining
                                        , cf_month, cf_month_num, cf_year
                                        , SUM(CASE
                                                WHEN pur_cf_type = 'revenue' AND owner_uid = pur_receiver THEN pur_amount_due
                                                WHEN pur_cf_type = 'expense' AND owner_uid = pur_payer THEN -pur_amount_due
                                                ELSE 0
                                            END) AS expected_cashflow
                                        , SUM(CASE
                                                WHEN pur_cf_type = 'revenue' AND owner_uid = pur_receiver THEN total_paid
                                                WHEN pur_cf_type = 'expense' AND owner_uid = pur_payer THEN -total_paid
                                                ELSE 0
                                            END) AS actual_cashflow
                                    FROM space.p_details
                                    LEFT JOIN space.pp_status ON property_uid = pur_property_id
                                    -- WHERE business_uid = '600-000003'
                                    WHERE business_uid = \'""" + user_id + """\'
                                    GROUP BY 
                                        owner_uid
                                        , cf_month
                                        , cf_year
                                    ORDER BY owner_uid, cf_month_num DESC
                                    ) AS cf_details
                                """)

                    response["HappinessMatrix"]["delta_cashflow_details"] = delta_cashflow_details


                    # HAPPINESS MATRIX - CASHFLOW DETAILS BY PROPERTY
                    delta_cashflow_details = db.execute("""
                                SELECT *,
                                    CAST((expected_cashflow - actual_cashflow) AS DECIMAL(10,2)) AS delta_cashflow
                                    , IF(expected_cashflow = 0, 0, CAST(ABS(expected_cashflow - actual_cashflow)/expected_cashflow AS DECIMAL(10,2))) AS percent_delta_cashflow
                                FROM (
                                    SELECT -- *,
                                        property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip -- , property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_utilities, po_owner_percent, po_start_date, po_end_date
                                        , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email -- , owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents
                                        , owner_photo_url -- , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                        , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                                        -- , lease_uid, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
                                        -- , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                                        -- , pp_status.*
                                        -- , purchase_uid, pur_timestamp
                                        -- , pur_property_id, purchase_type, pur_cf_type -- , pur_bill_id, purchase_date, pur_due_date
                                        -- , pur_amount_due -- , purchase_status, pur_status_value, pur_notes, pur_description
                                        -- , pur_receiver, pur_initiator, pur_payer, pur_group -- , pay_purchase_id, latest_date
                                        -- , total_paid, payment_status, amt_remaining
                                        -- , cf_month, cf_month_num, cf_year
                                        , SUM(CASE
                                                WHEN pur_cf_type = 'revenue' AND owner_uid = pur_receiver THEN pur_amount_due
                                                WHEN pur_cf_type = 'expense' AND owner_uid = pur_payer THEN -pur_amount_due
                                                ELSE 0
                                            END) AS expected_cashflow
                                        , SUM(CASE
                                                WHEN pur_cf_type = 'revenue' AND owner_uid = pur_receiver THEN total_paid
                                                WHEN pur_cf_type = 'expense' AND owner_uid = pur_payer THEN -total_paid
                                                ELSE 0
                                            END) AS actual_cashflow
                                    FROM space.p_details
                                    LEFT JOIN space.pp_status ON property_uid = pur_property_id
                                    -- WHERE business_uid = '600-000003'
                                    WHERE business_uid = \'""" + user_id + """\'
                                    GROUP BY 
                                        owner_uid
                                        , property_uid
                                        -- , cf_month
                                        -- , cf_year
                                    ORDER BY owner_uid, property_uid
                                    ) AS cf_details
                                """)

                    response["HappinessMatrix"]["delta_cashflow_details_by_property"] = delta_cashflow_details


                    # HAPPINESS MATRIX - CASHFLOW DETAILS BY PROPERTY BY MONTH
                    delta_cashflow_details = db.execute("""
                                SELECT *,
                                    CAST((expected_cashflow - actual_cashflow) AS DECIMAL(10,2)) AS delta_cashflow
                                    , IF(expected_cashflow = 0, 0, CAST(ABS(expected_cashflow - actual_cashflow)/expected_cashflow AS DECIMAL(10,2))) AS percent_delta_cashflow
                                FROM (
                                    SELECT -- *,
                                        property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip -- , property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_utilities, po_owner_percent, po_start_date, po_end_date
                                        , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email -- , owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents
                                        , owner_photo_url -- , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                        , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                                        -- , lease_uid, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
                                        -- , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                                        -- , pp_status.*
                                        -- , purchase_uid, pur_timestamp
                                        -- , pur_property_id, purchase_type, pur_cf_type -- , pur_bill_id, purchase_date, pur_due_date
                                        -- , pur_amount_due -- , purchase_status, pur_status_value, pur_notes, pur_description
                                        -- , pur_receiver, pur_initiator, pur_payer, pur_group -- , pay_purchase_id, latest_date
                                        -- , total_paid, payment_status, amt_remaining
                                        , cf_month, cf_month_num, cf_year
                                        , SUM(CASE
                                                WHEN pur_cf_type = 'revenue' AND owner_uid = pur_receiver THEN pur_amount_due
                                                WHEN pur_cf_type = 'expense' AND owner_uid = pur_payer THEN -pur_amount_due
                                                ELSE 0
                                            END) AS expected_cashflow
                                        , SUM(CASE
                                                WHEN pur_cf_type = 'revenue' AND owner_uid = pur_receiver THEN total_paid
                                                WHEN pur_cf_type = 'expense' AND owner_uid = pur_payer THEN -total_paid
                                                ELSE 0
                                            END) AS actual_cashflow
                                    FROM space.p_details
                                    LEFT JOIN space.pp_status ON property_uid = pur_property_id
                                    -- WHERE business_uid = '600-000003'
                                    WHERE business_uid = \'""" + user_id + """\'
                                    GROUP BY 
                                        owner_uid
                                        , property_uid
                                        , cf_month
                                        , cf_year
                                    ORDER BY owner_uid, property_uid, cf_month_num DESC
                                    ) AS cf_details
                                """)

                    response["HappinessMatrix"]["delta_cashflow_details_by_property_by_month"] = delta_cashflow_details

                    # HAPPINESS MATRIX - CASHFLOW
                    delta_cashflow = db.execute("""
                                SELECT -- *,
                                    owner_uid, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_photo_url, business_uid -- , cf_month, cf_month_num, cf_year, expected_cashflow, actual_cashflow, delta_cashflow, percent_delta_cashflow
                                    , SUM(expected_cashflow) AS expected_cashflow
                                    , SUM(actual_cashflow) AS actual_cashflow
                                    , CAST((SUM(expected_cashflow) - SUM(actual_cashflow)) AS DECIMAL(10,2)) AS delta_cashflow
                                    , IF(SUM(expected_cashflow) = 0, 0, CAST(ABS(SUM(expected_cashflow) - SUM(actual_cashflow))/SUM(expected_cashflow) AS DECIMAL(10,2))) AS percent_delta_cashflow
                                FROM (
                                    SELECT *,
                                        CAST((expected_cashflow - actual_cashflow) AS DECIMAL(10,2)) AS delta_cashflow
                                        , IF(expected_cashflow = 0, 0, CAST(ABS(expected_cashflow - actual_cashflow)/expected_cashflow AS DECIMAL(10,2))) AS percent_delta_cashflow
                                    FROM (
                                        SELECT -- *,
                                            property_uid -- , property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_utilities, po_owner_percent, po_start_date, po_end_date
                                            , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email -- , owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents
                                            , owner_photo_url -- , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                            , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                                            -- , lease_uid, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
                                            -- , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                                            -- , pp_status.*
                                            -- , purchase_uid, pur_timestamp
                                            -- , pur_property_id, purchase_type, pur_cf_type -- , pur_bill_id, purchase_date, pur_due_date
                                            -- , pur_amount_due -- , purchase_status, pur_status_value, pur_notes, pur_description
                                            -- , pur_receiver, pur_initiator, pur_payer, pur_group -- , pay_purchase_id, latest_date
                                            -- , total_paid, payment_status, amt_remaining
                                            , cf_month, cf_month_num, cf_year
                                            , SUM(CASE
                                                    WHEN pur_cf_type = 'revenue' AND owner_uid = pur_receiver THEN pur_amount_due
                                                    WHEN pur_cf_type = 'expense' AND owner_uid = pur_payer THEN -pur_amount_due
                                                    ELSE 0
                                                END) AS expected_cashflow
                                            , SUM(CASE
                                                    WHEN pur_cf_type = 'revenue' AND owner_uid = pur_receiver THEN total_paid
                                                    WHEN pur_cf_type = 'expense' AND owner_uid = pur_payer THEN -total_paid
                                                    ELSE 0
                                                END) AS actual_cashflow
                                        FROM space.p_details
                                        LEFT JOIN space.pp_status ON property_uid = pur_property_id
                                        -- WHERE business_uid = '600-000003'
                                        WHERE business_uid = \'""" + user_id + """\'
                                        GROUP BY 
                                            owner_uid
                                            , cf_month
                                            , cf_year
                                        ORDER BY owner_uid
                                        ) AS cf_details
                                    ) AS cf  
                                    GROUP BY owner_uid
                                """)

                    response["HappinessMatrix"]["delta_cashflow"] = delta_cashflow
                    

                    print("Complete Happiness Matrix")

                    # MAINTENANCE     
                    maintenanceQuery = db.execute(""" 
                            -- MAINTENANCE STATUS BY MANAGER
                            SELECT contract_business_id
                                , maintenance_status
                                , COUNT(maintenance_status) AS num
                            FROM (
                                SELECT *
                                    -- quote_business_id, quote_status, maintenance_request_status, quote_total_estimate
                                    , CASE  
										WHEN quote_status = "COMPLETED"                                           			THEN "PAID" 
                                        WHEN maintenance_request_status IN ("NEW" ,"INFO")                                  THEN "NEW REQUEST"
                                        WHEN maintenance_request_status = "SCHEDULED"                                       THEN "SCHEDULED"
                                        WHEN maintenance_request_status = 'CANCELLED' or quote_status = "FINISHED"          THEN "COMPLETED"
                                        WHEN quote_status IN ("SENT" ,"REFUSED" , "REQUESTED", "REJECTED", "WITHDRAWN") 	THEN "QUOTES REQUESTED"
                                        WHEN quote_status IN ("ACCEPTED" , "SCHEDULE")                                  	THEN "QUOTES ACCEPTED"
                                        ELSE quote_status
                                    END AS maintenance_status
                                FROM (
                                    SELECT * 
                                    FROM space.maintenanceRequests
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
                                                    -- , quote_pm_notes, quote_business_id, quote_services_expenses,quote_earliest_available_date,quote_earliest_available_time , quote_event_type, quote_event_duration, quote_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
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
                                ) AS quotes
                            LEFT JOIN ( SELECT * FROM space.contracts WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                                WHERE contract_business_id = \'""" + user_id + """\'
                                -- WHERE contract_business_id = "600-000032"
                                ) AS ms
                            GROUP BY maintenance_status;
                            """)

                    print("Complete Maintenance Status")

                    # print("Query: ", maintenanceQuery)
                    response["MaintenanceStatus"] = maintenanceQuery

                    # LEASES
                    leaseQuery = db.execute("""
                            -- LEASE EXPRIATION BY MONTH FOR OWNER AND PM
                            SELECT lease_end_month
                                , lease_end_num
                                , COUNT(lease_end_month) AS leases_expiring
                                , COUNT(move_out_date) AS move_out
                            FROM (
                                SELECT -- *
                                    l.*
                                    , property_owner_id
                                    , contract_business_id
                                FROM (
                                    -- FIND ALL ACTIVE/ENDED LEASES WITH OR WITHOUT A MOVE OUT DATE
                                    SELECT -- *,
                                    lease_uid, lease_property_id , lease_application_date, lease_start, lease_end, lease_status 
                                    -- , lease_assigned_contacts, lease_documents, lease_early_end_date
                                    , lease_renew_status, move_out_date 
                                    -- , lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_rent_available_topay-DNU, lease_consent, lease_actual_rent, lease_move_in_date, lease_end_notice_period, lease_days_remaining, lease_end_month
                                    , DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) AS lease_days_remaining
                                    , CASE
                                            WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) > DATEDIFF(LAST_DAY(DATE_ADD(NOW(), INTERVAL 11 MONTH)), NOW()) THEN 'FUTURE' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'FUTURE'
                                            WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) < 0 THEN 'MTM' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'MTM'
                                            ELSE MONTHNAME(STR_TO_DATE(LEFT(lease_end, 2), '%m'))
                                    END AS lease_end_month
                                    , CAST(LEFT(lease_end, 2) AS UNSIGNED) AS lease_end_num
                                    FROM space.leases 
                                    WHERE (lease_status = "ACTIVE" OR lease_status = "ENDED")
                                    ) AS l
                                LEFT JOIN space.property_owner ON property_id = lease_property_id
                                LEFT JOIN (SELECT * FROM space.contracts WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
                                -- WHERE property_owner_id = "110-000003"
                                -- WHERE contract_business_id = "600-000003"
                                -- WHERE property_owner_id = \'""" + user_id + """\'
                                WHERE contract_business_id = \'""" + user_id + """\'
                                ) AS le
                            GROUP BY lease_end_month
                            ORDER BY lease_end_num ASC
                            """)

                    print("Complete Lease Status")

                    # print("lease Query: ", leaseQuery)
                    response["LeaseStatus"] = leaseQuery

                    # RENT STATUS
                    rentQuery = db.execute(""" 
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
                                    WHERE business_uid = \'""" + user_id + """\'
                                    -- WHERE tenant_uid = \'""" + user_id + """\'  
                                    ) AS p
                                -- Link to rent status
                                LEFT JOIN (
                                    SELECT  -- *,
                                        pur_property_id
                                        , purchase_type
                                        , pur_due_date
                                        , SUM(pur_amount_due) AS pur_amount_due
                                        , MIN(pur_status_value) AS pur_status_value
                                        , purchase_status
                                        , pur_description
                                        , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_month
                                        , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_year
                                    FROM space.purchases
                                    WHERE purchase_type = "Rent"
                                        AND LEFT(pur_payer, 3) = '350'
                                        AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = MONTH(CURRENT_DATE)
                                        AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = YEAR(CURRENT_DATE)
                                    GROUP BY pur_due_date, pur_property_id, purchase_type
                                    ) AS pp
                                    ON property_uid = pur_property_id
                                ) AS rs
                            GROUP BY rent_status;
                            """)
                    print("Complete Rent Status")

                    # print("rent Query: ", rentQuery)
                    response["RentStatus"] = rentQuery
                    # print(response)
                    
                    # PROPERTY LIST
                    print("In Property List")
                    propertyQuery = db.execute("""
                            SELECT -- *,
                                property_uid, property_address, property_unit
                            FROM space.contracts
                            LEFT JOIN space.properties ON contract_property_id = property_uid
                            -- WHERE contract_business_id = '600-000003' AND
                            WHERE contract_business_id = \'""" + user_id + """\' AND
                                contract_status = 'ACTIVE';
                        """)

                    # print("Property List: ", propertyQuery)
                    response["Properties"] = propertyQuery
                    # print(response)

                    # NEW PM REQUESTS
                    print("In New PM Requests")
                    contractsQuery = db.execute("""
                            -- NEW PROPERTIES FOR MANAGER
                            SELECT *, CASE WHEN announcements IS NULL THEN false ELSE true END AS announcements_boolean
                            FROM space.o_details
                            LEFT JOIN space.properties ON property_id = property_uid
                            LEFT JOIN space.b_details ON contract_property_id = property_uid
                            LEFT JOIN (
                            SELECT announcement_properties, JSON_ARRAYAGG(JSON_OBJECT
                            ('announcement_uid', announcement_uid,
                            'announcement_title', announcement_title,
                            'announcement_msg', announcement_msg,
                            'announcement_mode', announcement_mode,
                            'announcement_date', announcement_date,
                            'announcement_receiver', announcement_receiver
                            )) AS announcements
                            FROM space.announcements
                            GROUP BY announcement_properties) as t ON announcement_properties = property_uid
                            WHERE contract_business_id = \'""" + user_id + """\'  AND (contract_status = "NEW" OR contract_status = "SENT" OR contract_status = "REJECTED");
                        """)

                    # print("PM Request Query: ", contractsQuery)
                    response["NewPMRequests"] = contractsQuery
                    # print(response)

                    return response
            else:
                print("No Match")
                return("Not a valid option")

        elif user_id.startswith("110"):
            with connect() as db:
                # print("in owner dashboard")
                maintenanceQuery = db.execute(""" 
                        -- MAINTENANCE STATUS BY OWNER
                        SELECT -- * 
                            property_owner_id
                            , CASE
                                WHEN maintenance_request_status = 'NEW' THEN 'NEW REQUEST'
                                WHEN maintenance_request_status = 'INFO' THEN 'INFO REQUESTED'
                                ELSE maintenance_request_status
                            END AS maintenance_status
                            , COUNT(maintenance_request_status) AS num
                        FROM space.maintenanceRequests
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        WHERE owner_uid = \'""" + user_id + """\'
                        GROUP BY maintenance_request_status;
                        """)

                # print("Query: ", maintenanceQuery)
                response["MaintenanceStatus"] = maintenanceQuery

                leaseQuery = db.execute(""" 
                            -- LEASE EXPRIATION BY MONTH FOR OWNER AND PM
                            SELECT lease_end_month
                                , lease_end_num
                                , COUNT(lease_end_month) AS leases_expiring
                                , COUNT(move_out_date) AS move_out
                            FROM (
                                SELECT -- *
                                    l.*
                                    , property_owner_id
                                    , contract_business_id
                                FROM (
                                    -- FIND ALL ACTIVE/ENDED LEASES WITH OR WITHOUT A MOVE OUT DATE
                                    SELECT -- *,
                                    lease_uid, lease_property_id , lease_application_date, lease_start, lease_end, lease_status 
                                    -- , lease_assigned_contacts, lease_documents, lease_early_end_date
                                    , lease_renew_status, move_out_date 
                                    -- , lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_rent_available_topay-DNU, lease_consent, lease_actual_rent, lease_move_in_date, lease_end_notice_period, lease_days_remaining, lease_end_month
                                    , DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) AS lease_days_remaining
                                    , CASE
                                            WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) > DATEDIFF(LAST_DAY(DATE_ADD(NOW(), INTERVAL 11 MONTH)), NOW()) THEN 'FUTURE' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'FUTURE'
                                            WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) < 0 THEN 'MTM' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'MTM'
                                            ELSE MONTHNAME(STR_TO_DATE(LEFT(lease_end, 2), '%m'))
                                    END AS lease_end_month
                                    , CAST(LEFT(lease_end, 2) AS UNSIGNED) AS lease_end_num
                                    FROM space.leases 
                                    WHERE (lease_status = "ACTIVE" OR lease_status = "ENDED")
                                    ) AS l
                                LEFT JOIN space.property_owner ON property_id = lease_property_id
                                LEFT JOIN (SELECT * FROM space.contracts WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
                                -- WHERE property_owner_id = "110-000003"
                                -- WHERE contract_business_id = "600-000003"
                                WHERE property_owner_id = \'""" + user_id + """\'
                                -- WHERE contract_business_id = \'""" + user_id + """\'
                                ) AS le
                            GROUP BY lease_end_month
                            ORDER BY lease_end_num ASC
                        """)

                # print("Query: ", leaseQuery)
                response["LeaseStatus"] = leaseQuery

                rentQuery = db.execute(""" 
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
                                    WHERE owner_uid = \'""" + user_id + """\'
                                    -- WHERE business_uid = \'""" + user_id + """\'
                                    -- WHERE tenant_uid = \'""" + user_id + """\'  
                                    ) AS p
                                -- Link to rent status
                                LEFT JOIN (
                                    SELECT  -- *,
                                        pur_property_id
                                        , purchase_type
                                        , pur_due_date
                                        , SUM(pur_amount_due) AS pur_amount_due
                                        , MIN(pur_status_value) AS pur_status_value
                                        , purchase_status
                                        , pur_description
                                        , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_month
                                        , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_year
                                    FROM space.purchases
                                    WHERE purchase_type = "Rent"
                                        AND LEFT(pur_payer, 3) = '350'
                                        AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = MONTH(CURRENT_DATE)
                                        AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = YEAR(CURRENT_DATE)
                                    GROUP BY pur_due_date, pur_property_id, purchase_type
                                    ) AS pp
                                    ON property_uid = pur_property_id
                                ) AS rs
                            GROUP BY rent_status;
                        """)

                # print("Query: ", rentQuery)
                response["RentStatus"] = rentQuery


                cashFlow = db.execute(""" 
                            -- CASHFLOW FOR A PARTICULAR OWNER
                            SELECT pur_receiver, pur_payer
                                , SUM(pur_amount_due) AS pur_amount_due
                                , SUM(total_paid) AS total_paid
                                , cf_month, cf_month_num, cf_year
                                , pur_cf_type
                            FROM space.pp_details
                            -- WHERE (pur_receiver = '110-000003' OR pur_payer = '110-000003')
                            WHERE (pur_receiver = \'""" + user_id + """\' OR pur_payer = \'""" + user_id + """\')
                            GROUP BY cf_month, cf_year, pur_cf_type
                            ORDER BY cf_month_num, property_uid
                            """)

                # print("Query: ", cashFlow)
                response["CashflowStatus"] = cashFlow

                return response

        elif user_id.startswith("350"):
            with connect() as db:
                leaseQuery = db.execute(""" 
                        -- OWNER, PROPERTY MANAGER, TENANT LEASES
                        SELECT * 
                        FROM space.leases
                        LEFT JOIN space.properties ON property_uid = lease_property_id
                        LEFT JOIN space.o_details ON property_id = lease_property_id
                        LEFT JOIN (
                            SELECT lt_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                                ('tenant_uid', tenant_uid,
                                'lt_responsibility', lt_responsibility,
                                'tenant_first_name', tenant_first_name,
                                'tenant_last_name', tenant_last_name,
                                'tenant_phone_number', tenant_phone_number,
                                'tenant_email', tenant_email
                                )) AS tenants
                                FROM space.t_details 
                                GROUP BY lt_lease_id) as t ON lease_uid = lt_lease_id
                        LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
                        -- WHERE owner_uid = \'""" + user_id + """\'
                        -- WHERE owner_uid = "110-000003"
                        -- WHERE contract_business_id = \'""" + user_id + """\'
                        -- WHERE contract_business_id = "600-000003"
                        WHERE tenants LIKE '%""" + user_id + """%'
                        -- WHERE tenants LIKE "%350-000007%"
                        ; """)
                response["leaseDetails"] = leaseQuery
                

                property = db.execute("""
                        -- TENENT PROPERTY INFO
                        -- NEED TO WORK OUT THE CASE WHAT A TENANT RENTS MULITPLE PROPERTIES
                        SELECT -- *
                            -- SUM( CASE WHEN purchase_status = 'UNPAID' AND pur_payer = '350-000002' THEN pur_amount_due ELSE 0 END) as balance
                            SUM( CASE WHEN purchase_status = 'UNPAID' AND pur_payer = \'""" + user_id + """\'  THEN pur_amount_due ELSE 0 END) as balance
                            -- ,CAST(MIN(STR_TO_DATE(CASE WHEN purchase_status = 'UNPAID' AND pur_payer = '350-000002' THEN pur_due_date ELSE lease_end END, '%m-%d-%Y')) AS CHAR) AS earliest_due_date
                            ,CAST(MIN(STR_TO_DATE(CASE WHEN purchase_status = 'UNPAID' AND pur_payer = \'""" + user_id + """\' THEN pur_due_date ELSE lease_end END, '%m-%d-%Y')) AS CHAR) AS earliest_due_date
                            , lt_lease_id, lt_tenant_id, lt_responsibility, lease_uid, lease_property_id
                            , lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date
                            -- , lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, linked_application_id-DNU, lease_docuSign
                            -- , lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lease_actual_rent
                            , property_uid
                            -- , property_available_to_rent, property_active_date
                            , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                            , property_type, property_num_beds, property_num_baths, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent
                            , property_images, property_favorite_image
                            -- , property_taxes, property_mortgages, property_insurance, property_featured
                            , property_description, property_notes
                            , MAX(CASE WHEN lease_status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_priority
                        FROM space.lease_tenant
                        LEFT JOIN space.leases ON lease_uid = lt_lease_id
                        LEFT JOIN space.properties l ON lease_property_id = property_uid
                        -- LEFT JOIN space.purchases pur ON property_uid = pur_property_id
                        LEFT JOIN space.pp_status pur ON property_uid = pur_property_id
                        -- WHERE pur_payer = '350-000002' 
                        -- WHERE lt_tenant_id = '350-000002' 
                        WHERE lt_tenant_id = \'""" + user_id + """\' 
                            AND property_uid!=""
                        GROUP BY lease_uid
                        ORDER BY lease_status;
                        """)
                response["property"] = property

                # print(property['result'][0]['property_uid'])
                data = property['result']
                # print(data)
                

               # Extract property_uid values
                property_uids = [item['property_uid'] for item in data]

                # Print the property_uids in parentheses
                # print("(" + ", ".join(property_uids) + ")")




                # MONIES PAID
                moneyPaid = db.execute("""
                    -- MONEY PAID
                    SELECT * FROM space.pp_details
                    WHERE payment_status != 'UNPAID' 
                        -- AND pur_payer = '600-000003' 
                        AND pur_payer = \'""" + user_id + """\'
                    """)
                # print("Query: ", paidStatus)
                response["MoneyPaid"] = moneyPaid

                if len(property['result']) > 0 and property['result'][0]['property_uid']:
                    announcements = db.execute("""
                        -- TENENT ANNOUNCEMENTS
                        SELECT * FROM announcements
                        WHERE announcement_receiver LIKE '%""" + user_id + """%'
                        AND (announcement_mode = 'Tenants' OR announcement_mode = 'Properties')
                        AND announcement_properties LIKE  '%""" + property['result'][0]['property_uid'] + """%' """)
                    response["announcements"] = announcements
                    
                    maintenance = db.execute("""
                            -- TENENT MAINTENANCE TICKETS
                            SELECT  property_uid, owner_first_name, owner_last_name, owner_phone_number, owner_email,
                                p.business_name, p.business_phone_number, p.business_email,
                                mr.*
                            FROM space.maintenanceRequests mr
                                LEFT JOIN space.p_details p ON property_uid = mr.maintenance_property_id
                                LEFT JOIN space.businessProfileInfo b ON b.business_uid = p.business_uid
                            -- WHERE tenant_uid = '350-000162';
                            WHERE tenant_uid = \'""" + user_id + """\';
                            """)
                    response["maintenanceRequests"] = maintenance

                    maintenance = db.execute("""
                            -- TENENT MAINTENANCE TICKETS
                            SELECT -- *,
                                lt_tenant_id -- , maintenance_request_status
                                , CASE
                                    WHEN maintenance_request_status = 'NEW' THEN 'NEW REQUEST'
                                    WHEN maintenance_request_status = 'INFO' THEN 'INFO REQUESTED'
                                    ELSE maintenance_request_status
                                END AS maintenance_status
                                , COUNT(maintenance_request_status) AS num
                            FROM space.maintenanceRequests
                            LEFT JOIN (
                                SELECT * 
                                FROM space.leases 
                                WHERE lease_status = 'ACTIVE'
                                ) AS l ON lease_property_id = maintenance_property_id
                            LEFT JOIN space.lease_tenant ON lease_uid = lt_lease_id
                            LEFT JOIN space.businessProfileInfo ON business_uid = maintenance_assigned_business
                            -- WHERE lt_tenant_id = '350-000005'
                            WHERE lt_tenant_id = \'""" + user_id + """\'
                            GROUP BY maintenance_request_status;
                            """)
                    response["maintenanceRequestsNew"] = maintenance


                    maintenanceQuery = db.execute(""" 
                            -- MAINTENANCE STATUS BY TENANT
                            SELECT * 
                                , CASE
                                    WHEN maintenance_request_status = 'NEW' THEN 'NEW REQUEST'
                                    WHEN maintenance_request_status = 'INFO' THEN 'INFO REQUESTED'
                                    ELSE maintenance_request_status
                                END AS maintenance_status
                                -- , COUNT(maintenance_request_status) AS num
                            FROM space.maintenanceRequests
                            LEFT JOIN (
                                SELECT * 
                                FROM space.leases 
                                WHERE lease_status = 'ACTIVE'
                                ) AS l ON lease_property_id = maintenance_property_id
                            LEFT JOIN space.lease_tenant ON lease_uid = lt_lease_id
                            LEFT JOIN space.businessProfileInfo ON business_uid = maintenance_assigned_business
                            -- WHERE lt_tenant_id = '350-000005'
                            WHERE lt_tenant_id = \'""" + user_id + """\'
                            -- GROUP BY maintenance_request_status;
                            """)

                    # print("Query: ", maintenanceQuery)
                    response["MaintenanceStatus"] = maintenanceQuery







                return response
