
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
                    ('    -in Get Owner Contacts for Management')
                    profileQuery = db.execute(""" 
                            SELECT -- *,
                                contact_uid, contact_type, contact_first_name, contact_last_name, contact_phone_number, contact_email, contact_address, contact_unit, contact_city, contact_state, contact_zip, contact_photo_url, contact_ein_number
                                , SUM(property_count)
                                , JSON_ARRAYAGG(
                                        JSON_OBJECT(
                                            'agreement_status', agreement_status,
                                            'property_count', property_count,
                                            'properties', properties
                                        )
                                    ) AS entities
                            FROM (
                                SELECT 
                                    contract_status AS agreement_status,
                                    owner_uid AS contact_uid,
                                    "Owner" AS contact_type,
                                    owner_first_name AS contact_first_name,
                                    owner_last_name AS contact_last_name,
                                    owner_phone_number AS contact_phone_number,
                                    owner_email AS contact_email,
                                    owner_address AS contact_address,
                                    owner_unit AS contact_unit,
                                    owner_city AS contact_city,
                                    owner_state AS contact_state,
                                    owner_zip AS contact_zip,
                                    owner_photo_url as contact_photo_url,
                                    owner_ein_number AS contact_ein_number,
                                    COUNT(property_id) AS property_count,
                                    JSON_ARRAYAGG(
                                        JSON_OBJECT(
                                            'property_address', p.property_address,
                                            'property_city', p.property_city,
                                            'property_state', p.property_state,
                                            'property_zip', p.property_zip,
                                            'property_type', p.property_type
                                        )
                                    ) AS properties
                                FROM space.b_details AS b
                                LEFT JOIN space.o_details AS o ON b.contract_property_id = o.property_id
                                LEFT JOIN space.properties AS p ON o.property_id = p.property_uid
                                -- WHERE b.business_uid = '600-000003'
                                WHERE b.business_uid = \'""" + uid + """\'
                                GROUP BY owner_uid , contract_status
                                ) AS c
                            GROUP BY contact_uid
                    """)
                    
                    if len(profileQuery["result"]) > 0:
                        response["management_contacts"]["owners"] = profileQuery["result"]

                    # tenants
                    ('    -in Get Tenant Contacts for Management')
                    profileQuery = db.execute(""" 
                            SELECT -- *,
                                contact_uid, contact_type, contact_first_name, contact_last_name, contact_phone_number, contact_email, contact_address, contact_unit, contact_city, contact_state, contact_zip, contact_photo_url, contact_adult_occupants, contact_children_occupants, contact_pet_occupants, contact_vehicle_info, contact_drivers_license_number, contact_drivers_license_state
                                , SUM(property_count)
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
                                tenant_drivers_license_state as contact_drivers_license_state
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
                                -- WHERE b.business_uid = '600-000003'
                                WHERE b.business_uid = \'""" + uid + """\' 
                                    AND lease_uid IS NOT NULL
                                GROUP BY tenant_uid, lease_status
                            ) AS c
                            GROUP BY contact_uid
                    """)
                    
                    if len(profileQuery["result"]) > 0:
                        response["management_contacts"]["tenants"] = profileQuery["result"]


                    #maintenance
                    ('    -in Get Maintenance Contacts for Management')
                    profileQuery = db.execute(""" 
                            SELECT 
                                m.business_uid AS contact_uid,
                                "Maintenance" AS contact_type,
                                m.business_name AS contact_first_name,
                                m.business_type AS contact_last_name,
                                m.business_phone_number AS contact_phone_number,
                                m.business_email AS contact_email,
                                m.business_address AS contact_address,
                                m.business_unit AS contact_unit,
                                m.business_city AS contact_city, 
                                m.business_state AS contact_state,
                                m.business_zip AS contact_zip,
                                m.business_photo_url as contact_photo_url,
                                m.business_locations AS contact_business_locations                                             
                            FROM space.b_details AS b
                            LEFT JOIN space.m_details ON contract_property_id = maintenance_property_id
                            LEFT JOIN space.businessProfileInfo AS m ON quote_business_id = m.business_uid
                            WHERE b.business_uid = \'""" + uid + """\' AND m.business_uid IS NOT NULL AND m.business_type = 'MAINTENANCE'
                            GROUP BY b.business_uid, m.business_uid;
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
                            tenant_children_occupants as contact_children_occupants,
                            tenant_pet_occupants as contact_pet_occupants,
                            tenant_vehicle_info as contact_vehicle_info,
                            tenant_drivers_license_number as contact_drivers_license_number,
                            tenant_drivers_license_state as contact_drivers_license_state 
                        FROM 
                            space.t_details as t 
                        INNER JOIN 
                            space.leases as l ON t.lt_lease_id = l.lease_uid
                        INNER JOIN
                            space.m_details as m ON l.lease_property_id = m.maintenance_property_id
                        WHERE 
                            lease_status = 'ACTIVE' AND quote_business_id = '{uid}'
                        GROUP BY 
                            tenant_uid;       
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
                            business_city as contact_city,
                            business_state as contact_state,
                            business_zip as contact_zip,
                            b.business_photo_url AS contact_photo_url,
                            b.business_ein_number as contact_ein_number                         
                        FROM 
                            space.m_details as m
                        INNER JOIN 
                            space.b_details as b ON m.maintenance_property_id = b.contract_property_id
                        WHERE 
                            quote_business_id = '{uid}'
                        GROUP BY 
                            business_uid;
                        
                    """)
                    if len(profileQuery["result"]) > 0:
                        response["maintenance_contacts"]["managers"] = profileQuery["result"]
                    return response
                    
        #owner contacts
        elif uid.startswith("110"):
            # print('in Get Owner Contacts')
            print('in Contacts - Get Contacts for Owners')
            response["owner_contacts"] = {}

            print('    -in Get Manager Contacts for Owner')
            with connect() as db:
                print("in connect loop")
                profileQuery = db.execute(f"""
                    SELECT -- *,
                        contact_uid, contact_type, contact_first_name, contact_last_name, contact_phone_number, contact_email, contact_address, contact_unit, contact_city, contact_state, contact_zip, contact_photo_url, contact_ein_number
                        , SUM(property_count)
                        , JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'agreement_status', agreement_status,
                                    'property_count', property_count,
                                    'properties', properties
                                )
                            ) AS entities
                    FROM (
                        SELECT
                            contract_status AS agreement_status,
                            b.business_uid AS contact_uid,
                            'Manager' AS contact_type,
                            b.business_name AS contact_first_name,
                            'Management' AS contact_last_name,
                            b.business_phone_number AS contact_phone_number,
                            b.business_email AS contact_email,
                            b.business_address AS contact_address,
                            b.business_unit AS contact_unit,
                            b.business_city AS contact_city,
                            b.business_state AS contact_state,
                            b.business_zip AS contact_zip,
                            b.business_photo_url AS contact_photo_url,
                            b.business_ein_number as contact_ein_number,
                            COUNT(p.property_uid) AS property_count,
                            JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'property_address', p.property_address,
                                    'property_city', p.property_city,
                                    'property_state', p.property_state,
                                    'property_zip', p.property_zip
                                )
                            ) AS properties
                        FROM space.b_details AS b
                        LEFT JOIN space.o_details AS o ON b.contract_property_id = o.property_id
                        LEFT JOIN space.properties AS p ON o.property_id = p.property_uid
                        -- WHERE o.property_owner_id = '110-000003'
                        WHERE o.property_owner_id = '{uid}' 
                        GROUP BY b.business_uid, contract_status
                    ) AS c
                    GROUP BY contact_uid
                """)

                if len(profileQuery["result"]) > 0:
                    response["owner_contacts"]["managers"] = profileQuery["result"]


                print('    -in Get Tenant Contacts for Owner')
                with connect() as db:
                    print("in connect loop")
                    #change query
                    profileQuery = db.execute(f"""
                        SELECT -- *,
                            contact_uid, contact_type, contact_first_name, contact_last_name, contact_phone_number, contact_email, contact_address, contact_unit, contact_city, contact_state, contact_zip, contact_photo_url, contact_adult_occupants, contact_children_occupants, contact_pet_occupants, contact_vehicle_info, contact_drivers_license_number, contact_drivers_license_state
                            , SUM(property_count)
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
                                tenant_drivers_license_state as contact_drivers_license_state
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
                            FROM space.t_details as t
                            LEFT JOIN space.leases as l on t.lt_lease_id = l.lease_uid
                            LEFT JOIN space.properties as p on l.lease_property_id = p.property_uid
                            LEFT JOIN space.o_details as o on p.property_uid = o.property_id
                            -- WHERE o.property_owner_id = '110-000003'
                            WHERE o.property_owner_id = '{uid}'
                            GROUP BY t.tenant_uid, lease_status
                            ) AS c
                        GROUP BY contact_uid;
                    """)

                    if len(profileQuery["result"]) > 0:
                        response["owner_contacts"]["tenants"] = profileQuery["result"]

                
                return response

        else:
            response = {'Error': 'Invalid UID'}
            return response