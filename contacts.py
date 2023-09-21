
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



class ContactsMaintenance(Resource):
    # decorators = [jwt_required()]

    def get(self):
        print('in Get Maintenace Contacts')
        response = {}
        # conn = connect()


        with connect() as db:
            print("in connect loop")
            profileQuery = db.execute(""" 
                    --  FIND ALL MAINTENANCE COMPANIES
                    SELECT * FROM space.businessProfileInfo
                    WHERE business_type = 'MAINTENANCE';
                    """)
            

            # print("Query: ", profileQuery)
            # items = execute(profileQuery, "get", conn)
            # print(items)
            # response["Profile"] = items["result"]

            response["Maintenance_Contacts"] = profileQuery
            return response
        
        

class ContactsOwnerContactsDetails(Resource):
    # decorators = [jwt_required()]

    def get(self, owner_uid):
        print('in Get Owner Contacts', owner_uid)
        response = {}
        # conn = connect()


        with connect() as db:
            print("in connect loop")
            profileQuery = db.execute(""" 
                    -- FIND ALL OWNER CONTACTS
                    SELECT -- *,
                        business_uid AS contact_uid, "Property Manager" AS contact_type, business_name AS contact_business_name, business_phone_number AS contact_phone_numnber, business_email AS contact_email, business_address AS contact_address, business_unit AS contact_unit, business_city AS contact_city, business_state AS contact_state, business_zip AS contact_zip
                        , business_ein_number, business_services_fees, business_locations, business_paypal, business_apple_pay, business_zelle, business_venmo, business_account_number, business_routing_number, business_documents
                        , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type
                        , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                    FROM (
                        SELECT *
                        FROM space.o_details AS o
                        LEFT JOIN space.properties ON o.property_id = property_uid
                        WHERE owner_uid = \'""" + owner_uid + """\'
                    ) AS op
                    LEFT JOIN space.b_details AS b ON b.contract_property_id = property_uid
                    -- LEFT JOIN space.contractFees ON contract_uid = contract_id
                    WHERE b.contract_status IS NOT NULL
                    GROUP BY b.business_uid, property_id;
                    """)
            

            # print("Query: ", profileQuery)
            # items = execute(profileQuery, "get", conn)
            # print(items)
            # response["Profile"] = items["result"]

            response["Owner_Contacts"] = profileQuery
            return response
        



class ContactsBusinessContacts(Resource):
    # decorators = [jwt_required()]

    def get(self, business_uid):
        print('in Get Business Contacts')
        response = {}
        # conn = connect()


        with connect() as db:
            print("in connect loop")
            profileQuery = db.execute(""" 
                   -- FIND ALL CURRENT BUSINESS CONTACTS
                    SELECT owner_uid AS contact_uid, "Owner" AS contact_type, owner_first_name AS contact_first_name, owner_last_name AS contact_last_name, owner_phone_number AS contact_phone_numnber, owner_email AS contact_email, owner_address AS contact_address, owner_unit AS contact_unit, owner_city AS contact_city, owner_state AS contact_state, owner_zip AS contact_zip
                    FROM space.b_details AS b
                    LEFT JOIN space.o_details ON b.contract_property_id = property_id
                    WHERE b.business_uid = \'""" + business_uid + """\'
                    GROUP BY b.business_uid, owner_uid
                    UNION
                    SELECT tenant_uid AS contact_uid, "Tenant" AS contact_type, tenant_first_name AS contact_first_name, tenant_last_name AS contact_last_name, tenant_phone_number AS contact_phone_numnber, tenant_email AS contact_email, tenant_address AS contact_address, tenant_unit AS contact_unit, tenant_city AS contact_city, tenant_state AS contact_state, tenant_zip AS contact_zip
                    FROM space.b_details AS b
                    LEFT JOIN space.leases ON b.contract_property_id = lease_property_id
                    LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                    WHERE b.business_uid = \'""" + business_uid + """\' AND lease_uid IS NOT NULL
                    GROUP BY b.business_uid, tenant_uid
                    UNION
                    SELECT m.business_uid AS contact_uid, "Business" AS contact_type, m.business_name AS contact_first_name, m.business_type AS contact_last_name, m.business_phone_number AS contact_phone_numnber, m.business_email AS contact_email, m.business_address AS contact_address, m.business_unit AS contact_unit, m.business_city AS contact_city, m.business_state AS contact_state, m.business_zip AS contact_zip
                    FROM space.b_details AS b
                    LEFT JOIN space.m_details ON contract_property_id = maintenance_property_id
                    LEFT JOIN space.businessProfileInfo AS m ON quote_business_id = m.business_uid
                    WHERE b.business_uid = \'""" + business_uid + """\' AND m.business_uid IS NOT NULL
                    GROUP BY b.business_uid, m.business_uid;
                    """)
            

            # print("Query: ", profileQuery)
            # items = execute(profileQuery, "get", conn)
            # print(items)
            # response["Profile"] = items["result"]

            response["Business_Contacts"] = profileQuery
            return response


class ContactsBusinessContactsOwnerDetails(Resource):
    # decorators = [jwt_required()]

    def get(self, business_uid):
        print('in Get Owner Contacts', business_uid)
        response = {}
        # conn = connect()


        with connect() as db:
            print("in connect loop")
            profileQuery = db.execute(""" 
                    -- FIND OWNER CONTACT DETAILS
                    SELECT *
                    -- owner_uid AS contact_uid, "Owner" AS contact_type, owner_first_name AS contact_first_name, owner_last_name AS contact_last_name, owner_phone_number AS contact_phone_numnber, owner_email AS contact_email, owner_address AS contact_address, owner_unit AS contact_unit, owner_city AS contact_city, owner_state AS contact_state, owner_zip AS contact_zip
                    FROM space.b_details AS b
                    LEFT JOIN space.o_details ON b.contract_property_id = property_id
                    WHERE b.business_uid = \'""" + business_uid + """\';
                    """)
            

            # print("Query: ", profileQuery)
            # items = execute(profileQuery, "get", conn)
            # print(items)
            # response["Profile"] = items["result"]

            response["Owner_Details"] = profileQuery
            return response

    
class ContactsBusinessContactsTenantDetails(Resource):
    # decorators = [jwt_required()]

    def get(self, business_uid):
        print('in Get Owner Contacts', business_uid)
        response = {}
        # conn = connect()


        with connect() as db:
            print("in connect loop")
            profileQuery = db.execute(""" 
                    -- FIND TENANT CONTACT DETAILS
                    SELECT *
                        -- tenant_uid AS contact_uid, "Tenant" AS contact_type, tenant_first_name AS contact_first_name, tenant_last_name AS contact_last_name, tenant_phone_number AS contact_phone_numnber, tenant_email AS contact_email, tenant_address AS contact_address, tenant_unit AS contact_unit, tenant_city AS contact_city, tenant_state AS contact_state, tenant_zip AS contact_zip
                    FROM space.b_details AS b
                    LEFT JOIN space.leases ON b.contract_property_id = lease_property_id
                    LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                    WHERE b.business_uid = \'""" + business_uid + """\' AND lease_uid IS NOT NULL;
                    """)
            

            # print("Query: ", profileQuery)
            # items = execute(profileQuery, "get", conn)
            # print(items)
            # response["Profile"] = items["result"]

            response["Tenant_Details"] = profileQuery
            return response
        
class ContactsBusinessContactsMaintenanceDetails(Resource):
    # decorators = [jwt_required()]

    def get(self, business_uid):
        print('in Get Owner Contacts', business_uid)
        response = {}
        # conn = connect()


        with connect() as db:
            print("in connect loop")
            profileQuery = db.execute(""" 
                    -- FIND MAINTENANCE CONTACT DETAILS
                    SELECT *
                        -- m.business_uid AS contact_uid, m.business_name AS contact_first_name, m.business_type AS contact_last_name, m.business_phone_number AS contact_phone_numnber, m.business_email AS contact_email, m.business_address AS contact_address, m.business_unit AS contact_unit, m.business_city AS contact_city, m.business_state AS contact_state, m.business_zip AS contact_zip
                    FROM space.b_details AS b
                    LEFT JOIN space.m_details ON contract_property_id = maintenance_property_id
                    LEFT JOIN space.businessProfileInfo AS m ON quote_business_id = m.business_uid
                    WHERE b.business_uid = \'""" + business_uid + """\';
                    """)
            

            # print("Query: ", profileQuery)
            # items = execute(profileQuery, "get", conn)
            # print(items)
            # response["Profile"] = items["result"]

            response["Maintenance_Details"] = profileQuery
            return response
        

class ContactsOwnerContactsManagerDetails(Resource):
    # decorators = [jwt_required()]

    def get(self, owner_uid):
        print('in Get Manager Contacts for Owners')
        response = {}
    
        with connect() as db:
            print("in connect loop")
            profileQuery = db.execute(f"""
                SELECT
                b.business_uid AS contact_uid,
                'Manager' AS contact_type,
                b.business_name AS contact_first_name,
                'Management' AS contact_last_name,
                b.business_phone_number AS contact_phone_number,
                b.business_email AS contact_email,
                b.business_address AS contact_address,
                b.business_city AS contact_city,
                b.business_state AS contact_state,
                b.business_zip AS contact_zip,
                COUNT(p.property_uid) AS property_count,
                GROUP_CONCAT(p.property_address) AS property_addresses
                FROM
                    space.b_details AS b
                LEFT JOIN
                    space.o_details AS o ON b.contract_property_id = o.property_id
                LEFT JOIN
                    space.properties AS p ON o.property_id = p.property_uid
                WHERE
                    o.property_owner_id = '{owner_uid}' AND contract_status = 'ACTIVE'
                GROUP BY
                    b.business_uid;
            """)
            response["Owner_Contacts"] = profileQuery
            return response
        


class ContactsMaintenanceContactsManagerDetails(Resource):
    # decorators = [jwt_required()]

    def get(self, business_uid):
        print('in Get Tenant Contacts for Maintenance')
        response = {}
    
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
                    business_zip as contact_zip 
                FROM 
                    space.m_details as m
                INNER JOIN 
                    space.b_details as b ON m.maintenance_property_id = b.contract_property_id
                WHERE 
	                quote_business_id = '{business_uid}'
                GROUP BY 
                    business_uid;
                
            """)
            response["Maintenance_Contacts_Managers"] = profileQuery
            return response

class ContactsMaintenanceContactsTenantDetails(Resource):
    # decorators = [jwt_required()]

    def get(self, business_uid):
        print('in Get Tenant Contacts for Maintenance')
        response = {}
    
        with connect() as db:
            print("in connect loop")
            profileQuery = db.execute(f"""
                SELECT
                    tenant_uid as contact_uid,
                    "Active Tenant" as contact_type,
                    tenant_first_name as contact_first_name,
                    tenant_last_name as contact_last_name,
                    tenant_phone_number as contact_phone_number,
                    tenant_email as contact_email,
                    tenant_address as contact_address,
                    tenant_city as contact_city,
                    tenant_state as contact_state,
                    tenant_zip as contact_zip 
                FROM 
                    space.t_details as t 
                INNER JOIN 
                    space.leases as l ON t.lt_lease_id = l.lease_uid
                INNER JOIN
                    space.m_details as m ON l.lease_property_id = m.maintenance_property_id
                WHERE 
                    lease_status = 'ACTIVE' AND quote_business_id = '{business_uid}'
                GROUP BY 
                    tenant_uid;       
            """)
            response["Maintenance_Contacts_Tenants"] = profileQuery
            return response