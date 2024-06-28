
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar


class Contacts(Resource):
    # decorators = [jwt_required()]

    def get(self, uid):
        response = {}
        # conn = connect()

        #business contacts
        if uid.startswith("600"):
            business_type = ""
            # print('in Get Business Contacts')
            with connect() as db:
                # print("in connect loop")
                query = db.execute(""" 
                    -- FIND ALL CURRENT BUSINESS CONTACTS
                        SELECT business_type
                        FROM space.businessProfileInfo
                        WHERE business_uid = \'""" + uid + """\';
                        """)
                
            business_type = query['result'][0]['business_type']
            # print(business_type)

            if business_type == "MANAGEMENT":
                print("In Contacts - Get Management Contacts")
                response["management_contacts"] = {}
                with connect() as db:
                    print("in connect loop")
                   
                    # OWNERS
                    # print ('    -in Get Owner Contacts for Management - UPDATED')
                    profileQuery = db.execute(""" 
                            -- OWNER CONTACTS WITH RENTS, MAINTEANCE AND PAYMENT
                            SELECT *
                            FROM (
                                SELECT -- *,
                                    owner_uid, owner_user_id, po_owner_percent, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
                                    , SUM(CASE WHEN contract_status = 'NEW' THEN 1 ELSE 0 END) AS NEW_count
                                    , SUM(CASE WHEN contract_status = 'SENT' THEN 1 ELSE 0 END) AS SENT_count
                                    , SUM(CASE WHEN contract_status = 'ACTIVE' THEN 1 ELSE 0 END) AS PROPERTY_count
                                    , JSON_ARRAYAGG(JSON_OBJECT
                                        ('contract_uid', contract_uid,
                                        'contract_property_id', contract_property_id,
                                        'contract_start_date', contract_start_date,
                                        'contract_end_date', contract_end_date,
                                        'contract_documents', contract_documents,
                                        'contract_status', contract_status,
                                        'property_uid', property_uid,
                                        'property_available_to_rent', property_available_to_rent,
                                        'property_listed_date', property_listed_date,
                                        'property_address', property_address,
                                        'property_unit', property_unit,
                                        'property_city', property_city,
                                        'property_state', property_state,
                                        'property_zip', property_zip,
                                        'property_listed_rent', property_listed_rent,
                                        'property_favorite_image', property_favorite_image,
                                        'payment_status', payment_status,
                                        'cf_month', cf_month,
                                        'cf_month_num', cf_month_num,
                                        'cf_year', cf_year,
                                        'total_paid', total_paid,
                                        'pur_amount_due', pur_amount_due,
                                        'amt_remaining', amt_remaining,
                                        'maintenance_count', maintenance_count
                                        )) AS properties
                                FROM (
                                    SELECT * 
                                    FROM space.b_details
                                    LEFT JOIN space.properties ON property_uid = contract_property_id
                                    LEFT JOIN space.o_details ON contract_property_id = property_id
                                    -- ADD RENT STATUS
                                    LEFT JOIN (
                                        SELECT  -- *,
                                            pur_property_id, payment_status, cf_month, cf_month_num, cf_year
                                            , SUM(total_paid) AS total_paid
                                            , SUM(pur_amount_due) AS pur_amount_due
                                            , SUM(amt_remaining) AS amt_remaining
                                        FROM space.pp_status
                                        WHERE purchase_type = "Rent"
                                            AND cf_month_num = MONTH(CURRENT_DATE)
                                            AND cf_year = YEAR(CURRENT_DATE)
                                            AND LEFT(pur_payer,3) = '350'
                                        GROUP BY pur_property_id
                                        ) AS r ON pur_property_id = property_uid AND contract_status = 'ACTIVE'
                                    -- ADD MAINTENANCE ISSUES
                                    LEFT JOIN (
                                        SELECT -- *, 
                                            maintenance_property_id, COUNT(maintenance_property_id) AS maintenance_count
                                        FROM space.maintenanceRequests
                                        WHERE maintenance_request_status IN ('NEW','PROCESSING','SCHEDULED')
                                        GROUP BY maintenance_property_id
                                        ) AS m ON maintenance_property_id = property_uid
                                    -- WHERE contract_business_id = '600-000003'
                                    WHERE contract_business_id = \'""" + uid + """\'
                                ) AS owners
                                GROUP BY owner_uid
                            ) AS o
                            LEFT JOIN (
                                SELECT -- *,
                                    pur_receiver
                                    , JSON_ARRAYAGG(JSON_OBJECT
                                        ('total_amount_due', total_amount_due,
                                        'total_paid', total_paid,
                                        'cf_month', cf_month,
                                        'cf_month_num', cf_month_num,
                                        'cf_year', cf_year
                                        )) AS total_paid
                                FROM (        
                                SELECT -- *
                                    -- purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, 
                                    SUM(pur_amount_due) AS total_amount_due -- , purchase_status, pur_status_value, pur_notes, pur_description
                                    , pur_receiver -- , pur_initiator, pur_payer, pur_late_Fee, pur_perDay_late_fee, pur_due_by, pur_late_by, pur_group, payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, payment_intent, payment_method, payment_date_cleared, payment_client_secret, latest_date
                                    , SUM(total_paid) AS total_paid -- , payment_status, amt_remaining
                                    , cf_month, cf_month_num, cf_year
                                FROM space.pp_status
                                -- WHERE pur_payer = "600-000003"
                                WHERE pur_payer = \'""" + uid + """\'
                                GROUP BY pur_receiver, cf_month, cf_year
                                ) AS payments
                                GROUP BY pur_receiver
                            ) AS pp ON pur_receiver = owner_uid
                            LEFT JOIN (
                                SELECT -- *,
                                    paymentMethod_profile_id
                                    , JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'paymentMethod_type', paymentMethod_type,
                                        'paymentMethod_status', paymentMethod_status,
                                        'paymentMethod_name', paymentMethod_name
                                    )
                                ) AS payment_method
                                FROM space.paymentMethods
                                GROUP BY paymentMethod_profile_id
                                ) AS pm ON pm.paymentMethod_profile_id = owner_uid
                            """)
                    # print("Finished query")
                    
                    if len(profileQuery["result"]) > 0:
                        response["management_contacts"]["owners"] = profileQuery["result"]


                    # TENANTS
                    # print ('    -in Get Tenant Contacts for Management - UPDATED')
                    profileQuery = db.execute(""" 
                            -- TENANT CONTACTS WITH RENTS, MAINTEANCE AND PAYMENT
                            SELECT *
                                FROM (
                                SELECT tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_photo_url
                                    , JSON_ARRAYAGG(JSON_OBJECT
                                            ('contract_uid', contract_uid,
                                            'contract_property_id', contract_property_id,
                                            'contract_start_date', contract_start_date,
                                            'contract_end_date', contract_end_date,
                                            'contract_status', contract_status,
                                            'lease_uid', lease_uid,
                                            'lease_start', lease_start,
                                            'lease_end', lease_end,
                                            'lease_status', lease_status,
                                            'lease_documents', lease_documents,
                                            'lease_adults', lease_adults,
                                            'lease_children', lease_children,
                                            'lease_pets', lease_pets,
                                            'lease_documents', lease_documents,
                                            'lease_vehicles', lease_vehicles,
                                            'lease_referred', lease_referred,
                                            'lease_effective_date', lease_effective_date,
                                            'lt_responsibility', lt_responsibility,
                                            'property_available_to_rent', property_available_to_rent,
                                            'property_address', property_address,
                                            'property_unit', property_unit,
                                            'property_city', property_city,
                                            'property_state', property_state,
                                            'property_zip', property_zip,
                                            'property_favorite_image', property_favorite_image,
                                            'rent_payments', rent_payments,
                                            'maintenance_count', maintenance_count,
                                            'pur_payer', pur_payer
                                            )) AS property           
                                FROM (
                                    SELECT -- *,
                                        tenant_uid, lt_responsibility, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents
                                        -- , tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_pet_occupants, tenant_current_address-DNU
                                        , tenant_references, tenant_photo_url
                                        , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date
                                        , contract_fees, contract_assigned_contacts
                                        , contract_documents, contract_name, contract_status -- , contract_early_end_date
                                        -- , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                                        , lease_uid, lease_property_id, lease_application_date, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date
                                        , lease_actual_rent
                                        , property_uid, property_available_to_rent, property_active_date, property_listed_date, property_address, property_unit, property_city, property_state, property_zip
                                        , property_favorite_image
                                        , r.*
                                        , maintenance_count
                                    FROM space.b_details
                                    LEFT JOIN space.leases ON contract_property_id = lease_property_id
                                    LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                                    LEFT JOIN space.properties ON contract_property_id = property_uid

                                    -- ADD RENT BY PROPERTY BY TENANT
                                    LEFT JOIN (
                                        SELECT pur_property_id, pur_payer
                                            , JSON_ARRAYAGG(JSON_OBJECT
                                                ('pur_property_id', pur_property_id,
                                                'purchase_type', purchase_type,
                                                'pur_due_date', pur_due_date,
                                                'pur_amount_due', pur_amount_due,
                                                'purchase_status', purchase_status,
                                                'latest_date', latest_date,
                                                'total_paid', total_paid,
                                                'amt_remaining', amt_remaining,
                                                'cf_month', cf_month,
                                                'cf_year', cf_year
                                                )) AS rent_payments 
                                        FROM (
                                            SELECT  -- *,
                                                pur_property_id, pur_payer, purchase_type, pur_due_date, purchase_status, latest_date, cf_month, cf_month_num, cf_year
                                                , SUM(total_paid) AS total_paid
                                                , SUM(pur_amount_due) AS pur_amount_due
                                                , SUM(amt_remaining) AS amt_remaining
                                            FROM space.pp_status
                                            WHERE purchase_type = "Rent"
                                        -- 		AND cf_month_num = MONTH(CURRENT_DATE)
                                        -- 		AND cf_year = YEAR(CURRENT_DATE)
                                                AND LEFT(pur_payer,3) = '350'
                                            GROUP BY pur_property_id, pur_payer, cf_month, cf_year
                                            ORDER BY pur_property_id, pur_payer, cf_month_num
                                            ) AS rents
                                        GROUP BY pur_property_id, pur_payer
                                        ) AS r ON pur_property_id = property_uid AND tenant_uid = pur_payer AND lease_status = 'ACTIVE'

                                    -- ADD MAINTENANCE ISSUES
                                    LEFT JOIN (
                                        SELECT -- *, 
                                            maintenance_property_id, COUNT(maintenance_property_id) AS maintenance_count
                                        FROM space.maintenanceRequests
                                        WHERE maintenance_request_status IN ('NEW','PROCESSING','SCHEDULED')
                                        GROUP BY maintenance_property_id
                                        ) AS m ON maintenance_property_id = property_uid
                                    -- WHERE contract_business_id = '600-000003' 
                                    WHERE contract_business_id = \'""" + uid + """\'
                                        -- AND lease_status = 'ACTIVE'
                                    
                                    ) AS tenants
                                WHERE !ISNULL(tenant_user_id)
                                GROUP BY tenant_uid
                            ) AS pp
                            LEFT JOIN (
                                SELECT -- *,
                                    paymentMethod_profile_id
                                    , JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'paymentMethod_type', paymentMethod_type,
                                        'paymentMethod_status', paymentMethod_status,
                                        'paymentMethod_name', paymentMethod_name
                                    )
                                ) AS payment_method
                                FROM space.paymentMethods
                                GROUP BY paymentMethod_profile_id
                                ) AS pm ON pm.paymentMethod_profile_id = tenant_uid
                    """)
                    
                    if len(profileQuery["result"]) > 0:
                        response["management_contacts"]["tenants"] = profileQuery["result"]


                    # MAINTENANCE
                    # print ('    -in Get Maintenance Contacts for Management - UPDATED')
                    profileQuery = db.execute(""" 
                            -- MAINTENANCE CONTACTS FOR MANAGEMENT
                            SELECT -- *,
                                contract_business_id, quote_business_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_locations, business_address, business_unit, business_city, business_state, business_zip, business_photo_url, payment_method
                                , JSON_ARRAYAGG(JSON_OBJECT(
                                    'maintenance_status', maintenance_status, 
                                    'maintenance_request_info', maintenance_request_info,
                                    'num', num,
                                    'total_estimate', total_estimate
                                    )) AS maintenance_by_status
                            FROM (
                                SELECT -- *,
                                    contract_business_id
                                    , quote_business_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_locations, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                                    , payment_method
                                    , COUNT(maintenance_status) AS num
                                    , SUM(quote_total_estimate) AS total_estimate
                                    , maintenance_status
                                    , JSON_ARRAYAGG(JSON_OBJECT(
                                        'maintenance_request_uid', maintenance_request_uid,
                                        'maintenance_property_id', maintenance_property_id,
                                        'quote_business_id', quote_business_id,
                                        'contract_business_id', contract_business_id,
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
                                    LEFT JOIN space.properties ON maintenance_property_id = property_uid
                                    LEFT JOIN (SELECT * FROM space.contracts WHERE contract_status = "ACTIVE") AS C ON contract_property_id = property_uid
                                    LEFT JOIN space.businessProfileInfo ON business_uid = quote_business_id
                                    -- WHERE quote_business_id = '600-000012' 
                                    -- WHERE contract_business_id = '600-000051' 
                                    -- WHERE quote_business_id = \'""" + uid + """\'
                                    WHERE contract_business_id = \'""" + uid + """\'
                                    ) AS ms
                                LEFT JOIN (
                                    SELECT -- *,
                                        paymentMethod_profile_id
                                        , JSON_ARRAYAGG(
                                        JSON_OBJECT(
                                            'paymentMethod_type', paymentMethod_type,
                                            'paymentMethod_status', paymentMethod_status,
                                            'paymentMethod_name', paymentMethod_name
                                        )
                                    ) AS payment_method
                                    FROM space.paymentMethods
                                    GROUP BY paymentMethod_profile_id
                                    ) AS pm ON pm.paymentMethod_profile_id = quote_business_id
                                GROUP BY maintenance_status, quote_business_id
                                ORDER BY maintenance_status
                            ) AS mc
                            WHERE !ISNULL(quote_business_id)
                            GROUP BY quote_business_id
                            """)
                    
                    if len(profileQuery["result"]) > 0:
                        response["management_contacts"]["maintenance"] = profileQuery["result"]
                

                    # EMPLOYEES
                    # print ('    -in Get Employee Contacts for Management')
                    profileQuery = db.execute(""" 
                            -- EMPLOYEE CONTACTS FOR MANAGEMENT
                            SELECT * 
                            FROM space.employees
                            LEFT JOIN space.businessProfileInfo ON employee_business_id = business_uid
                            LEFT JOIN (
                                SELECT -- *,
                                    paymentMethod_profile_id
                                    , JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'paymentMethod_type', paymentMethod_type,
                                        'paymentMethod_status', paymentMethod_status,
                                        'paymentMethod_name', paymentMethod_name
                                    )
                                ) AS payment_method
                                FROM space.paymentMethods
                                GROUP BY paymentMethod_profile_id
                                ) AS pm ON pm.paymentMethod_profile_id = employee_uid
                            -- WHERE business_uid = '600-000012';
                            WHERE business_uid = \'""" + uid + """\'
                            """)
                    
                    if len(profileQuery["result"]) > 0:
                        response["management_contacts"]["employees"] = profileQuery["result"]

                    return response
                

            elif business_type == "MAINTENANCE":
                print("In Contacts - Get Maintenance Contacts")
                response["maintenance_contacts"] = {}

                with connect() as db:
                    print("in connect loop")

                    # TENANTS
                    # print('    -in Get Manager Contacts for Maintenance')
                    profileQuery = db.execute(f"""
                        -- TENANTS FOR MAINTENANCE
                        SELECT
                            tenant_uid as contact_uid,
                            "Tenant" as contact_type,
                            tenant_first_name,
                            tenant_last_name,
                            tenant_phone_number,
                            tenant_email,
                            tenant_address,
                            tenant_city,
                            tenant_state,
                            tenant_zip,
                            tenant_photo_url,
                            tenant_adult_occupants,
                            -- tenant_children_occupants,
                            tenant_pet_occupants,
                            -- tenant_vehicle_info,
                            tenant_drivers_license_number,
                            tenant_drivers_license_state,
                            JSON_ARRAYAGG( JSON_OBJECT(
                                        'property_uid', property_uid,
                                        'property_address', property_address,
                                        'property_unit', property_unit,
                                        'property_city', property_city,
                                        'property_state', property_state,
                                        'property_zip', property_zip,
                                        'property_favorite_image', property_favorite_image
                                    )
                                ) AS properties,
                            payment_method
                        FROM space.t_details as t 
                        LEFT JOIN space.leases as l ON t.lt_lease_id = l.lease_uid
                        LEFT JOIN space.properties ON lease_property_id = property_uid
                        LEFT JOIN space.m_details as m ON l.lease_property_id = m.maintenance_property_id
                        LEFT JOIN (
                                SELECT -- *,
                                    paymentMethod_profile_id
                                    , JSON_ARRAYAGG( JSON_OBJECT(
                                        'paymentMethod_type', paymentMethod_type,
                                        'paymentMethod_status', paymentMethod_status,
                                        'paymentMethod_name', paymentMethod_name
                                    )
                                ) AS payment_method
                                FROM space.paymentMethods
                                GROUP BY paymentMethod_profile_id
                                ) AS pm ON pm.paymentMethod_profile_id = tenant_uid
                        -- WHERE quote_business_id = '600-000012'
                        WHERE quote_business_id = \'""" + uid + """\'
                            AND lease_status = 'ACTIVE' 
                        GROUP BY tenant_uid;          
                    """)
                    
                    if len(profileQuery["result"]) > 0:
                        response["maintenance_contacts"]["tenants"] = profileQuery["result"]


                    # MANAGERS
                    # print('    -in Get Manager Contacts for Maintenance')
                    profileQuery = db.execute(f"""
                        SELECT 
                            business_uid,
                            business_name,
                            business_phone_number,
                            business_email,
                            business_address,
                            business_unit,
                            business_city,
                            business_state,
                            business_zip,
                            b.business_photo_url,
                            b.business_ein_number,
                            payment_method
                        FROM space.m_details as m
                        LEFT JOIN space.b_details as b ON m.maintenance_property_id = b.contract_property_id
                        LEFT JOIN (
                                SELECT -- *,
                                    paymentMethod_profile_id
                                    , JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'paymentMethod_type', paymentMethod_type,
                                        'paymentMethod_status', paymentMethod_status,
                                        'paymentMethod_name', paymentMethod_name
                                    )
                                ) AS payment_method
                                FROM space.paymentMethods
                                GROUP BY paymentMethod_profile_id
                                ) AS pm ON pm.paymentMethod_profile_id = business_uid
                        -- WHERE quote_business_id = '600-000012'
                        WHERE quote_business_id = \'""" + uid + """\'
                        GROUP BY business_uid;
                    """)

                    if len(profileQuery["result"]) > 0:
                        response["maintenance_contacts"]["managers"] = profileQuery["result"]


                    # Employees
                    # print ('    -in Get Employee Contacts for Maintenance')
                    profileQuery = db.execute(""" 
                            -- EMPLOYEE CONTACTS FOR MAINTENANCE
                            SELECT * 
                            FROM space.employees
                            LEFT JOIN space.businessProfileInfo ON employee_business_id = business_uid
                            -- WHERE business_uid = '600-000012';
                            WHERE business_uid = \'""" + uid + """\'
                    """)
                    
                    if len(profileQuery["result"]) > 0:
                        response["maintenance_contacts"]["employees"] = profileQuery["result"]

                    return response    


       
        #owner contacts
        elif uid.startswith("110"):
            # print('in Get Owner Contacts')
            print('in Contacts - Get Contacts for Owners')
            response["owner_contacts"] = {}

            
            with connect() as db:
                print("in connect loop")

                # MANAGERS
                # print('    -in Get Manager Contacts for Owner - UPDATED')
                profileQuery = db.execute(f"""
                    -- MANAGER CONTACTS WITH RENTS, MAINTEANCE AND PAYMENT
                    SELECT *
                    FROM (
                        SELECT -- *,
                            business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                            , SUM(CASE WHEN contract_status = 'NEW' THEN 1 ELSE 0 END) AS NEW_count
                            , SUM(CASE WHEN contract_status = 'SENT' THEN 1 ELSE 0 END) AS SENT_count
                            , JSON_ARRAYAGG(JSON_OBJECT
                                ('property_id', property_id,
                                'property_available_to_rent', property_available_to_rent,
                                'property_address', property_address,
                                'property_unit', property_unit,
                                'property_city', property_city,
                                'property_state', property_state,
                                'property_zip', property_zip,
                                'payment_status', payment_status,
                                'contract_status', contract_status,
                                'maintenance_count', maintenance_count
                                )) AS properties
                        FROM (
                            -- RENT, MAINTENANCE STATUS & PROPERTY MANAGER BY PROPERTY
                            SELECT * 
                            FROM space.o_details
                            LEFT JOIN space.properties ON property_id = property_uid
                            -- ADD RENT STATUS
                            LEFT JOIN (
                                SELECT  pur_property_id, payment_status, amt_remaining, cf_month, cf_month_num, cf_year
                                FROM space.pp_status
                                WHERE purchase_type = "Rent"
                                    AND cf_month_num = MONTH(CURRENT_DATE)
                                    AND cf_year = YEAR(CURRENT_DATE)
                                    AND LEFT(pur_payer,3) = '350' 
                                ) AS r ON pur_property_id = property_uid
                            -- ADD MAINTENANCE ISSUES
                            LEFT JOIN (
                                SELECT -- *, 
                                    maintenance_property_id, COUNT(maintenance_property_id) AS maintenance_count
                                FROM space.maintenanceRequests
                                WHERE maintenance_request_status IN ('NEW','PROCESSING','SCHEDULED')
                                GROUP BY maintenance_property_id
                                ) AS m ON maintenance_property_id = property_uid
                            -- ADD BUSINESS DETAILS
                            LEFT JOIN (
                                SELECT * 
                                FROM space.b_details
                                -- WHERE contract_status = 'ACTIVE'
                                ) AS b ON contract_property_id = property_uid
                            -- WHERE owner_uid = '110-000003'
                            WHERE owner_uid = \'""" + uid + """\' 
                        ) as prop
                        WHERE !ISNULL(business_uid)
                        GROUP BY business_uid
                    ) AS p
                    -- PROPERTY MANAGER PAYMENT METHODS
                    LEFT JOIN (
                        SELECT -- *,
                            paymentMethod_profile_id
                            , JSON_ARRAYAGG(JSON_OBJECT
                                ('paymentMethod_type', paymentMethod_type,
                                'paymentMethod_name', paymentMethod_name,
                                'paymentMethod_status', paymentMethod_status
                                )) AS payment_method
                        FROM space.paymentMethods
                        GROUP BY paymentMethod_profile_id
                    ) as pm ON paymentMethod_profile_id = business_uid
                    -- AGGREGATED PAYMENTS BY PROPERTY MANAGER 
                    LEFT JOIN (
                        SELECT -- *,
                            pur_payer
                            , JSON_ARRAYAGG(JSON_OBJECT
                                ('cf_month', cf_month,
                                'cf_year', cf_year,
                                'total_paid', total_paid,
                                'pur_amount_due', pur_amount_due
                                )) AS payments
                        FROM (
                            -- ACTUAL PAYMENTS BY MONTH BY PROPERTY MANAGER      
                            SELECT -- *,
                                pur_payer, cf_month, cf_year
                                , SUM(total_paid) AS total_paid
                                , SUM(pur_amount_due) AS pur_amount_due
                            FROM space.pp_status
                            -- WHERE pur_receiver = '110-000003'
                            WHERE pur_receiver = \'""" + uid + """\'
                            GROUP BY cf_month, cf_year, pur_payer
                            ORDER BY cf_month_num, cf_year
                        ) AS p
                        GROUP BY pur_payer
                    ) AS payments ON business_uid = pur_payer
                """)

                if len(profileQuery["result"]) > 0:
                    response["owner_contacts"]["managers"] = profileQuery["result"]

                # TENANTS
                # print('    -in Get Tenant Contacts for Owner - UPDATED')
                profileQuery = db.execute(f"""
                    -- ALL TENANTS & APPLICANTS FOR AN OWNER
                    SELECT -- *,
                        tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_references, tenant_photo_url
                        , payment_method
                        , JSON_ARRAYAGG(JSON_OBJECT
                            ('property_id', property_id,
                                'property_owner_id', property_owner_id,
                                'po_owner_percent', po_owner_percent,
                                'property_address', property_address,
                                'property_unit', property_unit,
                                'property_city', property_city,
                                'property_state', property_state,
                                'property_zip', property_zip,
                                'lease_uid', lease_uid,
                                'lease_start', lease_start,
                                'lease_end', lease_end,
                                'lease_status', lease_status,
                                'lease_documents', lease_documents,
                                'lease_adults', lease_adults,
                                'lease_children', lease_children,
                                'lease_pets', lease_pets,
                                'lease_vehicles', lease_vehicles,
                                'lease_referred', lease_referred,
                                'lease_effective_date', lease_effective_date,
                                'lease_actual_rent', lease_actual_rent,
                                'rents_paid', rents_paid
                                )) AS properties
                    FROM (
                        SELECT * 
                        FROM space.o_details
                        LEFT JOIN space.leases ON lease_property_id = property_id
                        LEFT JOIN space.properties ON property_uid = property_id
                        LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                        LEFT JOIN (
                            SELECT -- *,
                                paymentMethod_profile_id
                                , JSON_ARRAYAGG(JSON_OBJECT
                                    ('paymentMethod_type', paymentMethod_type,
                                    'paymentMethod_name', paymentMethod_name,
                                    'paymentMethod_status', paymentMethod_status
                                    )) AS payment_method
                            FROM space.paymentMethods
                            GROUP BY paymentMethod_profile_id
                            ) AS pm ON paymentMethod_profile_id = tenant_uid
                        -- ACTUAL PAYMENTS BY PROPERTY 
                        LEFT JOIN (
                            SELECT -- *,
                                pur_property_id, purchase_type, pur_payer
                                , JSON_ARRAYAGG(JSON_OBJECT
                                    ('payment_status', payment_status,
                                    'amt_remaining', amt_remaining,
                                    'cf_month', cf_month,
                                    'cf_month_num', cf_month_num,
                                    'cf_year', cf_year,
                                    'pur_amount_due', pur_amount_due,
                                    'total_paid', total_paid
                                    )) AS rents_paid
                            FROM (
                                SELECT -- *,
                                    -- purchase_uid, pur_timestamp, 
                                    pur_property_id, purchase_type -- , pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description, pur_receiver, pur_initiator
                                    , pur_payer -- , pur_late_Fee, pur_perDay_late_fee, pur_due_by, pur_late_by, pur_group, payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, payment_intent, payment_method, payment_date_cleared, payment_client_secret, latest_date, total_paid
                                    , payment_status
                                    , SUM(amt_remaining) AS amt_remaining, cf_month, cf_month_num, cf_year
                                    , SUM(total_paid) AS total_paid
                                    , SUM(pur_amount_due) AS pur_amount_due
                                FROM space.pp_status
                                WHERE LEFT(pur_payer,3) = '350'
                                GROUP BY pur_payer, pur_property_id, cf_month, cf_year
                                ORDER BY pur_property_id, cf_month_num, cf_year
                                ) AS r
                            GROUP BY pur_property_id
                            ) AS r ON pur_property_id = property_id AND tenant_uid = pur_payer
                        -- WHERE owner_uid = '110-000003'
                        WHERE owner_uid = \'""" + uid + """\'
                    ) as t
                    WHERE !ISNULL(tenant_user_id)
                    GROUP BY tenant_uid
                """)

                if len(profileQuery["result"]) > 0:
                    response["owner_contacts"]["tenants"] = profileQuery["result"]
                
                return response
            

        #all maintenance contacts
        elif uid.startswith("ALL"):
            # print('in Get All Maintenace Contacts')
            response = {}
            # conn = connect()


            with connect() as db:
                print("in connect loop")

                # ALL MAINTENANCE CONTACTS
                # print('    -in Get All Maintenance Contacts')
                profileQuery = db.execute(""" 
                        --  FIND ALL MAINTENANCE COMPANIES
                        SELECT -- *,
                            business_uid as contact_uid,
                            business_type as contact_type,
                            business_name as contact_first_name,
                            "MAINTENANCE" as contact_last_name,
                            business_phone_number as contact_phone_number,
                            business_email as contact_email,
                            business_address as contact_address,
                            business_unit as contact_unit,
                            business_city as contact_city,
                            business_state as contact_state,
                            business_zip as contact_zip,
                            business_photo_url AS contact_photo_url,
                            business_ein_number as contact_ein_number,
                            payment_method AS payment_method                             
                        FROM space.businessProfileInfo
                        LEFT JOIN (
                                SELECT -- *,
                                    paymentMethod_profile_id
                                    , JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'paymentMethod_type', paymentMethod_type,
                                        'paymentMethod_status', paymentMethod_status,
                                        'paymentMethod_name', paymentMethod_name
                                    )
                                ) AS payment_method
                                FROM space.paymentMethods
                                GROUP BY paymentMethod_profile_id
                                ) AS pm ON pm.paymentMethod_profile_id = business_uid
                        WHERE business_type = 'MAINTENANCE';
                        """)
                

                if len(profileQuery["result"]) > 0:
                    response["maintenance_businesses"] = profileQuery
                return response

        else:
            response = {'Error': 'Invalid UID'}
            return response