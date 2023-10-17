
# from myspace_api import get_new_propertyUID

from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest


# MAINTENANCE BY STATUS
#                           TENANT      OWNER     PROPERTY MANAGER     
# MAINTENANCE BY STATUS        Y           Y               Y
# BY MONTH    X           X               X
# BY YEAR     X           X               X


# def get_new_propertyUID(conn):
#     newPropertyQuery = execute("CALL space.new_property_uid;", "get", conn)
#     if newPropertyQuery["code"] == 280:
#         return newPropertyQuery["result"][0]["new_id"]
#     return "Could not generate new property UID", 500


class PropertiesByOwner(Resource):
    def get(self, owner_id):
        print('in Properties by Owner')
        response = {}
        # conn = connect()

        print("Property Owner UID: ", owner_id)

        with connect() as db:
            print("in connect loop")
            propertiesQuery = db.execute(""" 
                    -- PROPERTIES BY OWNER
                    SELECT * 
                    FROM p_details
                    LEFT JOIN space.pp_status ON pur_property_id = property_uid
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND (purchase_type = "RENT" OR ISNULL(purchase_type))
                        AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                        AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year));
                    """)
            

            # print("Query: ", propertiesQuery)
            # items = execute(propertiesQuery, "get", conn)
            response["Property"] = propertiesQuery
            return response


class PropertiesByManager(Resource):
    def get(self, owner_id, manager_business_id):
        print('in Properties by Manager')
        response = {}
        # conn = connect()

        print("Property Owner UID: ", owner_id)
        print("Property Manager UID: ", manager_business_id)

        with connect() as db:
            print("in connect loop")
            response = db.execute(""" 
                    -- PROPERTIES BY MANAGER
                    SELECT *
                    FROM space.property_owner
                    LEFT JOIN space.properties ON property_id = property_uid
                    LEFT JOIN space.b_details ON contract_property_id = property_uid
                    WHERE property_owner_id = \'""" + owner_id + """\' AND contract_business_id = \'""" + manager_business_id + """\';
                    """)
            

            # print("Query: ", propertiesQuery)
            # items = execute(propertiesQuery, "get", conn)
            return response

# 1. rent due date
# 2. lease expiring data
# 3. link to lease
# 4. current tenant
# 5. current tenant move-in date
# 6. current property manager
# 7. property value
# 8. rent payment status

class Properties(Resource):
    def get(self, uid):
        print('in Properties')
        response = {}
        # conn = connect()

        print("Property User UID: ", uid)

        if uid[:3] == '110':
            print("In Owner ID")
            with connect() as db:
                # print("in connect loop")
                propertiesQuery = db.execute("""  
                        -- PROPERTIES BY OWNER
                        SELECT *,
                            property_uid, p.property_address, p.property_unit, p.property_city, p.property_state, p.property_zip, p.property_type
                            , property_num_beds, property_num_baths, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, p.property_images, p.property_description, p.property_notes
                            , lease_uid, lease_start, lease_end, lease_status, lease_rent, lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, lease_actual_rent
                            , tenant_uid, p.tenant_first_name, p.tenant_last_name, p.tenant_email, p.tenant_phone_number
                            , p.owner_uid, p.owner_first_name, p.owner_last_name, p.owner_email, p.owner_phone_number
                            , contract_documents
                            , business_uid, p.business_name, p.business_email, p.business_phone_number
                            , latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_year
                            , num
                            , CASE
                                    WHEN (lease_status = 'ACTIVE' AND payment_status IS NOT NULL) THEN payment_status
                                    WHEN (lease_status = 'ACTIVE' AND payment_status IS NULL) THEN 'UNPAID'
                                    ELSE 'VACANT'
                                END AS rent_status
                        FROM (
                            SELECT * FROM space.p_details
                             WHERE owner_uid = \'""" + uid + """\'
                            ) as p
                        LEFT JOIN (
                            SELECT * 
                            FROM space.pp_details 
                            WHERE (purchase_type = "RENT" OR ISNULL(purchase_type))
                            AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                            AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                            ) AS r ON property_uid = pur_property_id
                        LEFT JOIN (
                            SELECT -- * 
                            property_owner_id
                            , maintenance_property_id
                            -- , maintenance_request_status
                            , COUNT(maintenance_property_id) AS num
                            FROM space.maintenanceRequests
                            LEFT JOIN space.o_details ON maintenance_property_id = property_id
                            WHERE maintenance_request_status != "COMPLETED" AND maintenance_request_status != "CANCELLED"
                            GROUP BY maintenance_property_id
                            ) AS m ON property_uid = maintenance_property_id;                      
                        """)

            # print("Query: ", propertiesQuery)
            response["Property"] = propertiesQuery
            return response
        
        elif uid[:3] == '600':
            print("In Business ID")
            with connect() as db:
                # print("in connect loop")
                propertiesQuery = db.execute(""" 
                    -- PROPERTIES BY MANAGER
                        SELECT *,
                            property_uid, p.property_address, p.property_unit, p.property_city, p.property_state, p.property_zip, p.property_type
                            , property_num_beds, property_num_baths, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, p.property_images, p.property_description, p.property_notes
                            , lease_uid, lease_start, lease_end, lease_status, lease_rent, lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, lease_actual_rent
                            , tenant_uid, p.tenant_first_name, p.tenant_last_name, p.tenant_email, p.tenant_phone_number
                            , p.owner_uid, p.owner_first_name, p.owner_last_name, p.owner_email, p.owner_phone_number
                            , contract_documents
                            , business_uid, p.business_name, p.business_email, p.business_phone_number
                            , latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_year
                            , num
                            , CASE
                                    WHEN (lease_status = 'ACTIVE' AND payment_status IS NOT NULL) THEN payment_status
                                    WHEN (lease_status = 'ACTIVE' AND payment_status IS NULL) THEN 'UNPAID'
                                    ELSE 'VACANT'
                                END AS rent_status
                        FROM (
                            SELECT * FROM space.p_details
                             WHERE business_uid = \'""" + uid + """\'
                            ) as p
                        LEFT JOIN (
                            SELECT * 
                            FROM space.pp_details 
                            WHERE (purchase_type = "RENT" OR ISNULL(purchase_type))
                            AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                            AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                            ) AS r ON property_uid = pur_property_id
                        LEFT JOIN (
                            SELECT -- * 
                            property_owner_id
                            , maintenance_property_id
                            -- , maintenance_request_status
                            , COUNT(maintenance_property_id) AS num
                            FROM space.maintenanceRequests
                            LEFT JOIN space.o_details ON maintenance_property_id = property_id
                            WHERE maintenance_request_status != "COMPLETED" AND maintenance_request_status != "CANCELLED"
                            GROUP BY maintenance_property_id
                            ) AS m ON property_uid = maintenance_property_id; 
                        """)  

            # print("Query: ", propertiesQuery)
            response["Property"] = propertiesQuery
            return response
        
        elif uid[:3] == '350':
            print("In Tenant ID")
            with connect() as db:
                # print("in connect loop")
                propertiesQuery = db.execute(""" 
                        -- PROPERTIES BY TENANT
                        SELECT -- *,
                            property_uid, p.property_address, p.property_unit, p.property_city, p.property_state, p.property_zip, p.property_type
                            , latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_year
                            , num
                            , CASE
                                    WHEN (lease_status = 'ACTIVE' AND payment_status IS NOT NULL) THEN payment_status
                                    WHEN (lease_status = 'ACTIVE' AND payment_status IS NULL) THEN 'UNPAID'
                                    ELSE 'VACANT'
                                END AS rent_status
                        FROM (
                            SELECT * FROM space.p_details
                            WHERE tenant_uid = \'""" + uid + """\'
                            ) as p
                        LEFT JOIN (
                            SELECT * 
                            FROM space.pp_details 
                            WHERE  (purchase_type = "RENT" OR ISNULL(purchase_type))
                            AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                            AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                            ) AS r ON property_uid = pur_property_id
                        LEFT JOIN (
                            SELECT -- * 
                            property_owner_id
                            , maintenance_property_id
                            -- , maintenance_request_status
                            , COUNT(maintenance_property_id) AS num
                            FROM space.maintenanceRequests
                            LEFT JOIN space.o_details ON maintenance_property_id = property_id
                            WHERE  maintenance_request_status != "COMPLETED" AND maintenance_request_status != "CANCELLED"
                            GROUP BY maintenance_property_id
                            ) AS m ON property_uid = maintenance_property_id;
                        """)  

            # print("Query: ", propertiesQuery)
            response["Property"] = propertiesQuery
            return response
        
        else:
            print("UID Not found")
            response["Property"] = "UID Not Found"
            return response

    
    def post(self):
        print("In add Property")
        response = {}

        with connect() as db:
            data = request.form
            fields = [
                "property_owner_id"
                , "po_owner_percent"
                , 'property_available_to_rent'
                , "property_active_date"
                , 'property_address'
                , "property_unit"
                , "property_city"
                , "property_state"
                , "property_zip"
                , "property_type"
                , "property_num_beds"
                , "property_num_baths"
                , "property_area"
                , "property_listed_rent"
                , "property_deposit"
                , "property_pets_allowed"
                , "property_deposit_for_rent"
                , "property_taxes"
                , "property_mortgages"
                , "property_insurance"
                , "property_featured"
                , "property_description"
                , "property_notes"
            ]

            newRequest = {}
            newProperty = {}

            # print("Property Type: ", data.get("property_type"))
            # print("Property Address: ", request.form.get('property_address'))

            for field in fields:
                # print("Field: ", field)
                # print("Form Data: ", data.get(field))
                newProperty[field] = data.get(field)
                # print("New Property Field: ", newProperty[field])
            # print("Current newProperty", newProperty, type(newProperty))

            keys_to_remove = ["property_owner_id", "po_owner_percent"]
            newRequest = {key: newProperty.pop(key) for key in keys_to_remove if key in newProperty}
            # print("Current newProperty", newProperty, type(newProperty))
            # print("Current newRequest", newRequest, type(newRequest))
            
            # newRequest['property_owner_id'] = request.form.get("property_owner_id")
            # newRequest['po_owner_percent'] = request.form.get("po_owner_percent")
            # print(newRequest)


            # # GET NEW UID
            # print("Get New Property UID")
            newRequestID = db.call('new_property_uid')['result'][0]['new_id']
            newRequest['property_id'] = newRequestID
            newProperty['property_uid'] = newRequestID
            print(newRequestID)

            images = []
            i = -1
            # WHILE WHAT IS TRUE?
            while True:
                # print("In while loop")
                filename = f'img_{i}'
                # print("Filename: ", filename)
                if i == -1:
                    filename = 'img_cover'
                file = request.files.get(filename)
                # print("File: ", file)
                if file:
                    key = f'properties/{newRequestID}/{filename}'
                    image = uploadImage(file, key, '')
                    images.append(image)
                    # print("Images: ", images)
                else:
                    break
                i += 1
            newProperty['property_images'] = json.dumps(images)
            # print("Images to be uploaded: ", newProperty['property_images'])

            # print(newRequest)
            response = db.insert('property_owner', newRequest)
            response['property_owner'] = "Added"

            # print(newProperty, type(newProperty))
            response = db.insert('properties', newProperty)
            response['property_UID'] = newRequestID
            response['images'] = newProperty['property_images']

        return response
    
    
    def put(self):
        response = {}
        payload = request.form.to_dict()
        if payload.get('property_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'property_uid': payload.pop('property_uid')}
        with connect() as db:
            response = db.update('properties', key, payload)
        return response


    def delete(self):
        print("In delete Property")
        response = {}

        with connect() as db:
            data = request.get_json(force=True)
            print(data)

            property_owner_id = data["property_owner_id"]
            property_id = data["property_id"]
            

            # Query
            delPropertyQuery = (""" 
                    -- DELETE PROPERTY FROM PROPERTY-OWNER TABLE
                    DELETE FROM space.property_owner
                    WHERE property_id = \'""" + property_id + """\'
                        AND property_owner_id = \'""" + property_owner_id + """\';         
                    """)

            # print("Query: ", delPropertyQuery)
            response_po = db.delete(delPropertyQuery) 
            # print("Query out", response_po["code"])
            # if response_po["code"] == 200:
            #     response["Deleted property_uid"] = property_id
            # else:
            #     response["Deleted property_uid"] = "Not successful"


            delPropertyQuery = (""" 
                    -- DELETE PROPERTY FROM PROPERTIES TABLE
                    DELETE FROM space.properties 
                    WHERE property_uid = \'""" + property_id + """\';         
                    """)

            # print("Query: ", delPropertyQuery)
            response = db.delete(delPropertyQuery) 
            # print("Query out", response["code"])
            
            if response["code"] == 200:
                response["Deleted property_uid"] = property_id
            else:
                response["Deleted property_uid"] = "Not successful"

            if response_po["code"] == 200:
                response["Deleted po property_id"] = property_id
            else:
                response["Deleted po property_id"] = "Not successful"


            return response

class PropertyDashboardByOwner (Resource):
    def get(self, owner_id):
        print('in Property Dashboard by Owner')
        response = {}
        # conn = connect()

        print("Property Owner UID: ", owner_id)

        with connect() as db:
            print("in connect loop")
            propertiesQuery = db.execute(""" 
                    -- MANTENANCE AND RENT STATUS BY PROPERTY BY OWNER
                    SELECT * 
                    FROM (
                        SELECT * 
                        FROM space.o_details
                        LEFT JOIN space.properties ON property_uid = property_id
                        WHERE owner_uid = "110-000003"
                        ) AS p
                    LEFT JOIN (
                        -- OPEN MAINTENANCE (not paid, not completed, not cancelled) BY PROPERTY
                        SELECT  -- *, 
                            maintenance_property_id, COUNT(maintenance_request_status) AS num_open_maintenace_req
                        FROM space.m_details
                        WHERE maintenance_request_status != "PAID" AND maintenance_request_status != "COMPLETED" AND maintenance_request_status != "CANCELLED"
                        GROUP BY maintenance_property_id
                        ) AS m ON property_id = maintenance_property_id
                    LEFT JOIN (
                        -- RENT STATUS BY PROPERTY FOR OWNER PAGE (USED IN RENTS ALSO)
                        SELECT property_id
                            -- , property_owner_id, po_owner_percent
                            -- , property_address, property_unit, property_city, property_state, property_zip
                            -- , pp_status.*
                            , IF (ISNULL(payment_status), "VACANT", payment_status) AS rent_status
                        FROM space.property_owner
                        LEFT JOIN space.properties ON property_uid = property_id
                        LEFT JOIN space.pp_status ON pur_property_id = property_id
                        WHERE property_owner_id = "110-000003"
                            AND (purchase_type = "RENT" OR ISNULL(purchase_type))
                            AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                            AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                        GROUP BY property_id
                        ORDER BY rent_status
                        ) AS r ON p.property_id = r.property_id;
                    """)
            

            # print("Query: ", propertiesQuery)
            # items = execute(propertiesQuery, "get", conn)
            response["Property_Dashboard"] = propertiesQuery
            return response