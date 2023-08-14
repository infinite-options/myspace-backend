
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
            

            print("Query: ", profileQuery)
            # items = execute(profileQuery, "get", conn)
            # print(items)
            # response["Profile"] = items["result"]

            response["Maintenance Contacts"] = profileQuery
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
                    SELECT owner_uid AS contact_uid, owner_first_name AS contact_first_name, owner_last_name AS contact_last_name, owner_phone_number AS contact_phone_numnber, owner_email AS contact_email, owner_address AS contact_address, owner_unit AS contact_unit, owner_city AS contact_city, owner_state AS contact_state, owner_zip AS contact_zip
                    FROM space.b_details AS b
                    LEFT JOIN space.o_details ON b.contract_property_id = property_id
                    WHERE b.business_uid = \'""" + business_uid + """\'
                    GROUP BY b.business_uid, owner_uid
                    UNION
                    SELECT tenant_uid AS contact_uid, tenant_first_name AS contact_first_name, tenant_last_name AS contact_last_name, tenant_phone_number AS contact_phone_numnber, tenant_email AS contact_email, tenant_address AS contact_address, tenant_unit AS contact_unit, tenant_city AS contact_city, tenant_state AS contact_state, tenant_zip AS contact_zip
                    FROM space.b_details AS b
                    LEFT JOIN space.leases ON b.contract_property_id = lease_property_id
                    LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                    WHERE b.business_uid = \'""" + business_uid + """\'
                    GROUP BY b.business_uid, tenant_uid
                    UNION
                    SELECT m.business_uid AS contact_uid, m.business_name AS contact_first_name, m.business_type AS contact_last_name, m.business_phone_number AS contact_phone_numnber, m.business_email AS contact_email, m.business_address AS contact_address, m.business_unit AS contact_unit, m.business_city AS contact_city, m.business_state AS contact_state, m.business_zip AS contact_zip
                    FROM space.b_details AS b
                    LEFT JOIN space.m_details ON contract_property_id = maintenance_property_id
                    LEFT JOIN space.businessProfileInfo AS m ON quote_business_id = m.business_uid
                    WHERE b.business_uid = \'""" + business_uid + """\'
                    GROUP BY b.business_uid, m.business_uid;
                    """)
            

            print("Query: ", profileQuery)
            # items = execute(profileQuery, "get", conn)
            # print(items)
            # response["Profile"] = items["result"]

            response["Business Contacts"] = profileQuery
            return response