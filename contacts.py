
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



# class ContactsMaintenance(Resource):
#     # decorators = [jwt_required()]

#     def get(self):
#         print('in Get Maintenace Contacts')
#         response = {}
#         # conn = connect()


#         with connect() as db:
#             print("in connect loop")
#             profileQuery = db.execute(""" 
#                     --  FIND ALL MAINTENANCE COMPANIES
#                     SELECT * FROM space.businessProfileInfo
#                     WHERE business_type = 'MAINTENANCE';
#                     """)
            

#             # print("Query: ", profileQuery)
#             # items = execute(profileQuery, "get", conn)
#             # print(items)
#             # response["Profile"] = items["result"]

#             response["Maintenance_Contacts"] = profileQuery
#             return response
        

# class ContactsOwnerContactsDetails(Resource):
#     # decorators = [jwt_required()]

#     def get(self, owner_uid):
#         print('in Get Owner Contacts', owner_uid)
#         response = {}
#         # conn = connect()


#         with connect() as db:
#             print("in connect loop")
#             profileQuery = db.execute(""" 
#                     -- FIND ALL OWNER CONTACTS
#                     SELECT -- *,
#                         business_uid AS contact_uid, "Property Manager" AS contact_type, business_name AS contact_business_name, business_phone_number AS contact_phone_numnber, business_email AS contact_email, business_address AS contact_address, business_unit AS contact_unit, business_city AS contact_city, business_state AS contact_state, business_zip AS contact_zip
#                         , business_ein_number, business_services_fees, business_locations, business_paypal, business_apple_pay, business_zelle, business_venmo, business_account_number, business_routing_number, business_documents
#                         , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type
#                         , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
#                     FROM (
#                         SELECT *
#                         FROM space.o_details AS o
#                         LEFT JOIN space.properties ON o.property_id = property_uid
#                         WHERE owner_uid = \'""" + owner_uid + """\'
#                     ) AS op
#                     LEFT JOIN space.b_details AS b ON b.contract_property_id = property_uid
#                     -- LEFT JOIN space.contractFees ON contract_uid = contract_id
#                     WHERE b.contract_status IS NOT NULL
#                     GROUP BY b.business_uid, property_id;
#                     """)
            

#             # print("Query: ", profileQuery)
#             # items = execute(profileQuery, "get", conn)
#             # print(items)
#             # response["Profile"] = items["result"]

#             response["Owner_Contacts"] = profileQuery
#             return response
        



# class ContactsBusinessContacts(Resource):
#     # decorators = [jwt_required()]

#     def get(self, business_uid):
#         print('in Get Business Contacts')
#         response = {}
#         # conn = connect()


#         with connect() as db:
#             print("in connect loop")
#             profileQuery = db.execute(""" 
#                    -- FIND ALL CURRENT BUSINESS CONTACTS
#                     SELECT owner_uid AS contact_uid, "Owner" AS contact_type, owner_first_name AS contact_first_name, owner_last_name AS contact_last_name, owner_phone_number AS contact_phone_numnber, owner_email AS contact_email, owner_address AS contact_address, owner_unit AS contact_unit, owner_city AS contact_city, owner_state AS contact_state, owner_zip AS contact_zip
#                     FROM space.b_details AS b
#                     LEFT JOIN space.o_details ON b.contract_property_id = property_id
#                     WHERE b.business_uid = \'""" + business_uid + """\'
#                     GROUP BY b.business_uid, owner_uid
#                     UNION
#                     SELECT tenant_uid AS contact_uid, "Tenant" AS contact_type, tenant_first_name AS contact_first_name, tenant_last_name AS contact_last_name, tenant_phone_number AS contact_phone_numnber, tenant_email AS contact_email, tenant_address AS contact_address, tenant_unit AS contact_unit, tenant_city AS contact_city, tenant_state AS contact_state, tenant_zip AS contact_zip
#                     FROM space.b_details AS b
#                     LEFT JOIN space.leases ON b.contract_property_id = lease_property_id
#                     LEFT JOIN space.t_details ON lease_uid = lt_lease_id
#                     WHERE b.business_uid = \'""" + business_uid + """\' AND lease_uid IS NOT NULL
#                     GROUP BY b.business_uid, tenant_uid
#                     UNION
#                     SELECT m.business_uid AS contact_uid, "Business" AS contact_type, m.business_name AS contact_first_name, m.business_type AS contact_last_name, m.business_phone_number AS contact_phone_numnber, m.business_email AS contact_email, m.business_address AS contact_address, m.business_unit AS contact_unit, m.business_city AS contact_city, m.business_state AS contact_state, m.business_zip AS contact_zip
#                     FROM space.b_details AS b
#                     LEFT JOIN space.m_details ON contract_property_id = maintenance_property_id
#                     LEFT JOIN space.businessProfileInfo AS m ON quote_business_id = m.business_uid
#                     WHERE b.business_uid = \'""" + business_uid + """\' AND m.business_uid IS NOT NULL
#                     GROUP BY b.business_uid, m.business_uid;
#                     """)
            

#             # print("Query: ", profileQuery)
#             # items = execute(profileQuery, "get", conn)
#             # print(items)
#             # response["Profile"] = items["result"]

#             response["Business_Contacts"] = profileQuery
#             return response


# class ContactsBusinessContactsOwnerDetails(Resource):
#     # decorators = [jwt_required()]

#     def get(self, business_uid):
#         print('in Get Owner Contacts', business_uid)
#         response = {}
#         # conn = connect()


#         with connect() as db:
#             print("in connect loop")
#             profileQuery = db.execute(""" 
#                     -- FIND OWNER CONTACT DETAILS
#                     SELECT *
#                     -- owner_uid AS contact_uid, "Owner" AS contact_type, owner_first_name AS contact_first_name, owner_last_name AS contact_last_name, owner_phone_number AS contact_phone_numnber, owner_email AS contact_email, owner_address AS contact_address, owner_unit AS contact_unit, owner_city AS contact_city, owner_state AS contact_state, owner_zip AS contact_zip
#                     FROM space.b_details AS b
#                     LEFT JOIN space.o_details ON b.contract_property_id = property_id
#                     WHERE b.business_uid = \'""" + business_uid + """\';
#                     """)
            

#             # print("Query: ", profileQuery)
#             # items = execute(profileQuery, "get", conn)
#             # print(items)
#             # response["Profile"] = items["result"]

#             response["Owner_Details"] = profileQuery
#             return response

    
# class ContactsBusinessContactsTenantDetails(Resource):
#     # decorators = [jwt_required()]

#     def get(self, business_uid):
#         print('in Get Owner Contacts', business_uid)
#         response = {}
#         # conn = connect()


#         with connect() as db:
#             print("in connect loop")
#             profileQuery = db.execute(""" 
#                     -- FIND TENANT CONTACT DETAILS
#                     SELECT *
#                         -- tenant_uid AS contact_uid, "Tenant" AS contact_type, tenant_first_name AS contact_first_name, tenant_last_name AS contact_last_name, tenant_phone_number AS contact_phone_numnber, tenant_email AS contact_email, tenant_address AS contact_address, tenant_unit AS contact_unit, tenant_city AS contact_city, tenant_state AS contact_state, tenant_zip AS contact_zip
#                     FROM space.b_details AS b
#                     LEFT JOIN space.leases ON b.contract_property_id = lease_property_id
#                     LEFT JOIN space.t_details ON lease_uid = lt_lease_id
#                     WHERE b.business_uid = \'""" + business_uid + """\' AND lease_uid IS NOT NULL;
#                     """)
            

#             # print("Query: ", profileQuery)
#             # items = execute(profileQuery, "get", conn)
#             # print(items)
#             # response["Profile"] = items["result"]

#             response["Tenant_Details"] = profileQuery
#             return response
        
# class ContactsBusinessContactsMaintenanceDetails(Resource):
#     # decorators = [jwt_required()]

#     def get(self, business_uid):
#         print('in Get Owner Contacts', business_uid)
#         response = {}
#         # conn = connect()


#         with connect() as db:
#             print("in connect loop")
#             profileQuery = db.execute(""" 
#                     -- FIND MAINTENANCE CONTACT DETAILS
#                     SELECT *
#                         -- m.business_uid AS contact_uid, m.business_name AS contact_first_name, m.business_type AS contact_last_name, m.business_phone_number AS contact_phone_numnber, m.business_email AS contact_email, m.business_address AS contact_address, m.business_unit AS contact_unit, m.business_city AS contact_city, m.business_state AS contact_state, m.business_zip AS contact_zip
#                     FROM space.b_details AS b
#                     LEFT JOIN space.m_details ON contract_property_id = maintenance_property_id
#                     LEFT JOIN space.businessProfileInfo AS m ON quote_business_id = m.business_uid
#                     WHERE b.business_uid = \'""" + business_uid + """\';
#                     """)
            

#             # print("Query: ", profileQuery)
#             # items = execute(profileQuery, "get", conn)
#             # print(items)
#             # response["Profile"] = items["result"]

#             response["Maintenance_Details"] = profileQuery
#             return response
        
# class ContactsOwnerManagerDetails(Resource):
#     # decorators = [jwt_required()]

#     def get(self, owner_uid):
#         print('in Get Manager Contacts for Owners')
#         response = {}

#         with connect() as db:
#             print("in connect loop")
#             profileQuery = db.execute(f"""
#                 SELECT
#                 b.business_uid AS contact_uid,
#                 'Manager' AS contact_type,
#                 b.business_name AS contact_first_name,
#                 'Management' AS contact_last_name,
#                 b.business_phone_number AS contact_phone_number,
#                 b.business_email AS contact_email,
#                 b.business_address AS contact_address,
#                 b.business_city AS contact_city,
#                 b.business_state AS contact_state,
#                 b.business_zip AS contact_zip,
#                 COUNT(p.property_uid) AS property_count,
#                 GROUP_CONCAT(p.property_address) AS property_addresses
#                 FROM
#                     space.b_details AS b
#                 LEFT JOIN
#                     space.o_details AS o ON b.contract_property_id = o.property_id
#                 LEFT JOIN
#                     space.properties AS p ON o.property_id = p.property_uid
#                 WHERE
#                     o.property_owner_id = '{owner_uid}' AND contract_status = 'ACTIVE'
#                 GROUP BY
#                     b.business_uid;
#             """)
#             response["Owner_Contacts"] = profileQuery
#             return response
        
# class ContactsMaintenanceManagerDetails(Resource):
#     # decorators = [jwt_required()]

#     def get(self, business_uid):
#         print('in Get Manager Contacts for Maintenance')
#         response = {}

#         with connect() as db:
#             print("in connect loop")
#             profileQuery = db.execute(f"""
#                 SELECT 
#                     business_uid as contact_uid,
#                     "Property Manager" as contact_type,
#                     business_name as contact_first_name,
#                     "Management" as contact_last_name,
#                     business_phone_number as contact_phone_number,
#                     business_email as contact_email,
#                     business_address as contact_address,
#                     business_city as contact_city,
#                     business_state as contact_state,
#                     business_zip as contact_zip 
#                 FROM 
#                     space.m_details as m
#                 INNER JOIN 
#                     space.b_details as b ON m.maintenance_property_id = b.contract_property_id
#                 WHERE 
# 	                quote_business_id = '{business_uid}'
#                 GROUP BY 
#                     business_uid;
                
#             """)
#             response["Maintenance_Contacts_Managers"] = profileQuery
#             return response
        
# class ContactsMaintenanceTenantDetails(Resource):
#     # decorators = [jwt_required()]

#     def get(self, business_uid):
#         print('in Get Tenant Contacts for Maintenance')
#         response = {}

#         with connect() as db:
#             print("in connect loop")
#             profileQuery = db.execute(f"""
#                 SELECT
#                     tenant_uid as contact_uid,
#                     "Tenant" as contact_type,
#                     tenant_first_name as contact_first_name,
#                     tenant_last_name as contact_last_name,
#                     tenant_phone_number as contact_phone_number,
#                     tenant_email as contact_email,
#                     tenant_address as contact_address,
#                     tenant_city as contact_city,
#                     tenant_state as contact_state,
#                     tenant_zip as contact_zip 
#                 FROM 
#                     space.t_details as t 
#                 INNER JOIN 
#                     space.leases as l ON t.lt_lease_id = l.lease_uid
#                 INNER JOIN
#                     space.m_details as m ON l.lease_property_id = m.maintenance_property_id
#                 WHERE 
#                     lease_status = 'ACTIVE' AND quote_business_id = '{business_uid}'
#                 GROUP BY 
#                     tenant_uid;       
#             """)
#             response["Maintenance_Contacts_Tenants"] = profileQuery
#             return response

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
                   
                    #owners
                    # print ('    -in Get Owner Contacts for Management')
                    profileQuery = db.execute(""" 
                            -- OWNER CONTACTS WITH RENTS, MAINTEANCE AND PAYMENT
                            SELECT *
                            FROM (
                                SELECT -- *,
                                    owner_uid, owner_user_id, po_owner_percent, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
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
                                    WHERE contract_business_id = '600-000003'
                                    -- WHERE contract_business_id = \'""" + uid + """\'
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
                                WHERE pur_payer = "600-000003"
                                -- WHERE pur_payer = \'""" + uid + """\'
                                GROUP BY pur_receiver, cf_month, cf_year
                                ) AS payments
                                GROUP BY pur_receiver
                            ) AS pp ON pur_receiver = owner_uid
                            """)
                    # print("Finished query")
                    
                    if len(profileQuery["result"]) > 0:
                        response["management_contacts"]["owners"] = profileQuery["result"]



                    # tenants
                    ('    -in Get Tenant Contacts for Management')
                    profileQuery = db.execute(""" 
                            SELECT -- *,
                                contact_uid, contact_type, contact_first_name, contact_last_name, contact_phone_number, contact_email, contact_address, contact_unit, contact_city, contact_state, contact_zip, contact_photo_url, contact_adult_occupants, contact_children_occupants, contact_pet_occupants, contact_vehicle_info, contact_drivers_license_number, contact_drivers_license_state
                                , payment_method
                                , SUM(property_count) AS property_count
                                , JSON_ARRAYAGG(
                                        JSON_OBJECT(
                                            'agreement_status', agreement_status,
                                            'property_count', property_count,
                                            'properties', properties
                                        )
                                    ) AS entities
                            FROM (
                                SELECT 
                                lease_status AS agreement_status,
                                tenant_uid AS contact_uid,
                                "Tenant" AS contact_type,
                                tenant_first_name AS contact_first_name,
                                tenant_last_name AS contact_last_name,
                                tenant_phone_number AS contact_phone_number,
                                tenant_email AS contact_email, 
                                tenant_address AS contact_address,
                                tenant_unit AS contact_unit,
                                tenant_city AS contact_city,
                                tenant_state AS contact_state,
                                tenant_zip AS contact_zip,
                                tenant_photo_url as contact_photo_url,
                                tenant_adult_occupants as contact_adult_occupants,
                                tenant_children_occupants as contact_children_occupants,
                                tenant_pet_occupants as contact_pet_occupants,
                                tenant_vehicle_info as contact_vehicle_info,
                                tenant_drivers_license_number as contact_drivers_license_number,
                                tenant_drivers_license_state as contact_drivers_license_state,
                                payment_method AS payment_method
                                , COUNT(property_uid) AS property_count
                                , JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'property_address', p.property_address,
                                        'property_unit', p.property_unit,
                                        'property_city', p.property_city,
                                        'property_state', p.property_state,
                                        'property_zip', p.property_zip,
                                        'property_type', p.property_type
                                    )
                                ) AS properties
                                FROM space.b_details AS b
                                LEFT JOIN space.leases ON b.contract_property_id = lease_property_id
                                LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                                LEFT JOIN space.properties AS p ON b.contract_property_id = p.property_uid
                                LEFT JOIN (
                                    SELECT -- *,
                                        paymentMethod_profile_id
                                        , JSON_ARRAYAGG(
                                        JSON_OBJECT(
                                            'paymentMethod_type', paymentMethod_type,
                                            'paymentMethod_status', paymentMethod_status
                                        )
                                    ) AS payment_method
                                    FROM space.paymentMethods
                                    GROUP BY paymentMethod_profile_id
                                    ) AS pm ON pm.paymentMethod_profile_id = tenant_uid
                                -- WHERE b.business_uid = '600-000003'
                                WHERE b.business_uid = \'""" + uid + """\' 
                                    AND lease_uid IS NOT NULL
                                GROUP BY tenant_uid, lease_status
                            ) AS c
                            GROUP BY contact_uid
                    """)
                    
                    if len(profileQuery["result"]) > 0:
                        response["management_contacts"]["tenants"] = profileQuery["result"]

                    
                    profileQuery = db.execute(""" 
                            -- PROPERTY RENT DETAILS FOR RENT DETAILS PAGE
                            SELECT -- *,
                                IF(ISNULL(cf_month), CONCAT(property_uid, "-VACANT"), CONCAT(property_uid, "-", cf_month, "-", cf_year)) AS rent_detail_index
                                , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_favorite_image
                                , owner_uid, contract_uid, contract_status, business_uid, lease_uid, lease_start, lease_end, lease_status, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , rent_status
                                , pur_property_id, purchase_type, pur_due_date, pur_amount_due
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
                                -- WHERE business_uid = "600-000051"
                                -- WHERE owner_uid = "110-000003"
                                -- WHERE tenant_uid = '350-000002' 
                                -- WHERE owner_uid = \'""" + uid + """\'
                                WHERE business_uid = \'""" + uid + """\'
                                -- WHERE tenant_uid = \'""" + uid + """\'  
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
                                    , latest_date, total_paid, amt_remaining
                                    , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_month
                                    , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_year
                                FROM space.pp_status -- space.purchases
                                WHERE purchase_type = "Rent"
                                    AND LEFT(pur_payer, 3) = '350'
                            -- 		AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = MONTH(CURRENT_DATE)
                            -- 		AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = YEAR(CURRENT_DATE)
                                GROUP BY pur_due_date, pur_property_id, purchase_type
                                ) AS pp
                                ON property_uid = pur_property_id;    
                    """)
                    
                    if len(profileQuery["result"]) > 0:
                        response["management_contacts"]["tenants_rents"] = profileQuery["result"]


                    #maintenance
                    ('    -in Get Maintenance Contacts for Management')
                    profileQuery = db.execute(""" 
                            SELECT -- *,
                                maintenance_status
                                , quote_business_id
                                , b.*
                                , contract_business_id
                                , contract_property_id
                                , COUNT(quote_total_estimate)
                                , SUM(quote_total_estimate)
                            FROM (
                                SELECT *
                                    , CASE
                                        WHEN space.m_details.quote_status = "REQUESTED"                                                      								THEN "NEW"
                                        WHEN space.m_details.quote_status IN ("SENT") 	                                    												THEN "SUBMITTED"
                                        WHEN space.m_details.quote_status IN ("ACCEPTED", "SCHEDULE")                          												THEN "ACCEPTED"
                                        WHEN space.m_details.quote_status IN ("SCHEDULED" , "RESCHEDULE")                       											THEN "SCHEDULED"
                                        WHEN space.m_details.quote_status = "FINISHED"                                                       								THEN "FINISHED"
                                        WHEN space.m_details.quote_status = "COMPLETED"                                                      								THEN "PAID"   
                                        WHEN space.m_details.quote_status IN ("CANCELLED" , "ARCHIVE", "NOT ACCEPTED","WITHDRAWN" ,"WITHDRAW", "REFUSED" ,"REJECTED" )      THEN "ARCHIVE"
                                        ELSE space.m_details.quote_status
                                    END AS maintenance_status
                                FROM space.m_details
                                LEFT JOIN (
                                    SELECT * 
                                    FROM space.contracts
                                    WHERE contract_status = 'ACTIVE'
                                    ) AS c ON maintenance_property_id = contract_property_id 
                                -- WHERE contract_business_id = '600-000003'
                                WHERE contract_business_id = \'""" + uid + """\'   
                                ) AS m
                            LEFT JOIN space.businessProfileInfo b ON quote_business_id = b.business_uid 
                            GROUP BY maintenance_status, quote_business_id
                            ORDER BY quote_business_id
                    """)
                    
                    if len(profileQuery["result"]) > 0:
                        response["management_contacts"]["maintenance"] = profileQuery["result"]

                    return response

            elif business_type == "MAINTENANCE":
                print("In Contacts - Get Maintenance Contacts")
                response["maintenance_contacts"] = {}

                print('    -in Get Tenant Contacts for Maintenance')
                with connect() as db:
                    # print("in connect loop")
                    profileQuery = db.execute(f"""
                        SELECT
                            tenant_uid as contact_uid,
                            "Tenant" as contact_type,
                            tenant_first_name as contact_first_name,
                            tenant_last_name as contact_last_name,
                            tenant_phone_number as contact_phone_number,
                            tenant_email as contact_email,
                            tenant_address as contact_address,
                            tenant_city as contact_city,
                            tenant_state as contact_state,
                            tenant_zip as contact_zip,
                            tenant_photo_url as contact_photo_url,
                            tenant_adult_occupants as contact_adult_occupants,
                            -- tenant_children_occupants as contact_children_occupants,
                            tenant_pet_occupants as contact_pet_occupants,
                            -- tenant_vehicle_info as contact_vehicle_info,
                            tenant_drivers_license_number as contact_drivers_license_number,
                            tenant_drivers_license_state as contact_drivers_license_state,
                            payment_method AS payment_method
                        FROM space.t_details as t 
                        LEFT JOIN space.leases as l ON t.lt_lease_id = l.lease_uid
                        LEFT JOIN space.m_details as m ON l.lease_property_id = m.maintenance_property_id
                        LEFT JOIN (
                                SELECT -- *,
                                    paymentMethod_profile_id
                                    , JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'paymentMethod_type', paymentMethod_type,
                                        'paymentMethod_status', paymentMethod_status
                                    )
                                ) AS payment_method
                                FROM space.paymentMethods
                                GROUP BY paymentMethod_profile_id
                                ) AS pm ON pm.paymentMethod_profile_id = tenant_uid
                        -- WHERE quote_business_id = '600-000012'
                        WHERE quote_business_id = '{uid}'
                            AND lease_status = 'ACTIVE' 
                        GROUP BY tenant_uid;       
                    """)
                    if len(profileQuery["result"]) > 0:
                        response["maintenance_contacts"]["tenants"] = profileQuery["result"]



                print('    -in Get Manager Contacts for Maintenance')
                with connect() as db:
                    print("in connect loop")
                    profileQuery = db.execute(f"""
                        SELECT 
                            business_uid as contact_uid,
                            "Property Manager" as contact_type,
                            business_name as contact_first_name,
                            "Management" as contact_last_name,
                            business_phone_number as contact_phone_number,
                            business_email as contact_email,
                            business_address as contact_address,
                            business_unit as contact_unit,
                            business_city as contact_city,
                            business_state as contact_state,
                            business_zip as contact_zip,
                            b.business_photo_url AS contact_photo_url,
                            b.business_ein_number as contact_ein_number,
                            payment_method AS payment_method
                        FROM space.m_details as m
                        LEFT JOIN space.b_details as b ON m.maintenance_property_id = b.contract_property_id
                        LEFT JOIN (
                                SELECT -- *,
                                    paymentMethod_profile_id
                                    , JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'paymentMethod_type', paymentMethod_type,
                                        'paymentMethod_status', paymentMethod_status
                                    )
                                ) AS payment_method
                                FROM space.paymentMethods
                                GROUP BY paymentMethod_profile_id
                                ) AS pm ON pm.paymentMethod_profile_id = business_uid
                        -- WHERE quote_business_id = '600-000012'
                        WHERE quote_business_id = '{uid}'
                        GROUP BY business_uid;
                    """)
                    if len(profileQuery["result"]) > 0:
                        response["maintenance_contacts"]["managers"] = profileQuery["result"]
                    return response
                    
        #owner contacts
        elif uid.startswith("110"):
            # print('in Get Owner Contacts')
            print('in Contacts - Get Contacts for Owners')
            response["owner_contacts"] = {}

            
            with connect() as db:
                print("in connect loop")

                print('    -in Get Manager Contacts for Owner - UPDATED')
                profileQuery = db.execute(f"""
                    SELECT *
                    FROM (
                        SELECT -- *,
                            business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
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
                            WHERE owner_uid = '{uid}' 
                        ) as prop
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
                            WHERE pur_receiver = '{uid}'
                            GROUP BY cf_month, cf_year, pur_payer
                            ORDER BY cf_month_num, cf_year
                        ) AS p
                        GROUP BY pur_payer
                    ) AS payments ON business_uid = pur_payer
                """)

                if len(profileQuery["result"]) > 0:
                    response["owner_contacts"]["managers"] = profileQuery["result"]


                print('    -in Get Tenant Contacts for Owner - UPDATED')
                with connect() as db:
                    print("in connect loop")
                    #change query
                    profileQuery = db.execute(f"""
                        -- ALL TENANTS & APPLICANTS FOR AN OWNER
                        SELECT -- *,
                            tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_references, tenant_photo_url
                            , JSON_ARRAYAGG(JSON_OBJECT
                                ('property_id', property_id,
                                    'property_owner_id', property_owner_id,
                                    'po_owner_percent', po_owner_percent,
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
                                    'payment_method', payment_method,
                                    'rents_paid', rents_paid
                                    )) AS properties
                        FROM (
                            SELECT * 
                            FROM space.o_details
                            LEFT JOIN space.leases ON lease_property_id = property_id
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
                            WHERE owner_uid = '110-000003'
                            -- WHERE owner_uid = '{uid}'
                        ) as t
                        GROUP BY tenant_uid
                    """)

                    if len(profileQuery["result"]) > 0:
                        response["owner_contacts"]["tenants"] = profileQuery["result"]
                
                return response
            
        #all maintenance contacts
        elif uid.startswith("ALL"):
            print('in Get All Maintenace Contacts')
            response = {}
            # conn = connect()


            with connect() as db:
                print("in connect loop")
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
                                        'paymentMethod_status', paymentMethod_status
                                    )
                                ) AS payment_method
                                FROM space.paymentMethods
                                GROUP BY paymentMethod_profile_id
                                ) AS pm ON pm.paymentMethod_profile_id = business_uid
                        WHERE business_type = 'MAINTENANCE';
                        """)
                

                # print("Query: ", profileQuery)
                # items = execute(profileQuery, "get", conn)
                # print(items)
                # response["Profile"] = items["result"]

                response["maintenance_businesses"] = profileQuery
                return response

        else:
            response = {'Error': 'Invalid UID'}
            return response