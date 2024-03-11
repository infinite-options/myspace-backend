
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
import ast


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

def updateImages(imageFiles, property_uid):
    content = []
    
    for filename in imageFiles:
    
        if type(imageFiles[filename]) == str:
    
            bucket = 'io-pm'
            key = imageFiles[filename].split('/io-pm/')[1]
            data = s3.get_object(
                Bucket=bucket,
                Key=key
            )
            imageFiles[filename] = data['Body']
            content.append(data['ContentType'])            
        else:
            content.append('')

    
    
    s3Resource = boto3.resource('s3')
    bucket = s3Resource.Bucket('io-pm')
    bucket.objects.filter(Prefix=f'properties/{property_uid}/').delete()
    images = []
    for i in range(len(imageFiles.keys())):
    
        filename = f'img_{i-1}'
        if i == 0:
            filename = 'img_cover'
        key = f'properties/{property_uid}/{filename}'
        image = uploadImage(
            imageFiles[filename], key, content[i])
            # imageFiles[filename], key, '')
    
        images.append(image)
    return images

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

        print("In Find Applications")
        with connect() as db:
            # print("in connect loop")
            applicationQuery = db.execute("""
                -- FIND APPLICATIONS CURRENTLY IN PROGRESS
                SELECT property_uid
                    , leases.*
                    , lease_fees
                    , t_details.*
                FROM space.properties
                LEFT JOIN space.leases ON property_uid = lease_property_id
                LEFT JOIN (SELECT fees_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                        ('leaseFees_uid', leaseFees_uid,
                        'fee_name', fee_name,
                        'fee_type', fee_type,
                        'charge', charge,
                        'due_by', due_by,
                        'late_by', late_by,
                        'late_fee',late_fee,
                        'perDay_late_fee', perDay_late_fee,
                        'frequency', frequency,
                        'available_topay', available_topay,
                        'due_by_date', due_by_date
                        )) AS lease_fees
                        FROM space.leaseFees
                        GROUP BY fees_lease_id) AS lf ON fees_lease_id = lease_uid
                LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                WHERE (leases.lease_status = "NEW" OR leases.lease_status = "SENT" OR leases.lease_status = "REJECTED" OR leases.lease_status = "REFUSED" OR leases.lease_status = "PROCESSING" OR leases.lease_status = "TENANT APPROVED");
                 """)

        # print("Query: ", propertiesQuery)
        response["Applications"] = applicationQuery


        if uid[:3] == '110':
            print("In Owner ID")
            with connect() as db:
                # print("in connect loop")
                propertiesQuery = db.execute("""  
                    -- PROPERTIES BY OWNER
                    SELECT    
                        property_uid, property_available_to_rent, property_active_date
                        , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                        , property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area
                        , property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_favorite_image
                        , property_taxes, property_mortgages, property_insurance
                        , property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_utilities
                        , po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
                        , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                        , business_uid, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                        , lease_uid, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lease_actual_rent, lt_lease_id, lt_tenant_id, lt_responsibility
                        , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                        , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer -- , pur_amount_paid-DNU, purchase_frequency-DNU, payment_frequency-DNU, linked_tenantpur_id-DNU
                        , pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_year
                        , property_owner_id, maintenance_property_id, num_open_maintenace_req
                        , CASE
                                WHEN ISNULL(contract_uid) THEN "NO MANAGER"
                                WHEN (lease_status = 'ACTIVE' AND payment_status IS NOT NULL) THEN payment_status
                                WHEN (lease_status = 'ACTIVE' AND payment_status IS NULL) THEN 'UNPAID'
                                ELSE 'VACANT'
                            END AS rent_status 
                    --     , contract_uid AS actual_contract_uid
                    --     , lease_status AS actual_lease_status
                    FROM (
                        SELECT * FROM space.p_details
                        -- WHERE business_uid = "600-000003"
                        -- WHERE owner_uid = "110-000003"
                        WHERE owner_uid = \'""" + uid + """\'
                        -- WHERE business_uid = \'""" + uid + """\' AND contract_status = 'ACTIVE' AND contract_end_date < curdate()
                        -- WHERE tenant_uid = \'""" + uid + """\'
                        ) as p
                    LEFT JOIN (
                        SELECT * 
                        FROM space.pp_status
                        WHERE  (purchase_type = "RENT" OR ISNULL(purchase_type))
                        AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                        AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                        ) AS r ON p.property_uid = pur_property_id
                    LEFT JOIN (
                        SELECT -- * 
                            property_owner_id
                            , maintenance_property_id
                            , COUNT(maintenance_property_id) AS num_open_maintenace_req
                        FROM space.maintenanceRequests
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        WHERE  maintenance_request_status != "COMPLETED" AND maintenance_request_status != "CANCELLED"
                        GROUP BY maintenance_property_id
                        ) AS m ON p.property_uid = maintenance_property_id
                        order by lease_status;                      
                        """)

            # print("Query: ", propertiesQuery)
            response["Property"] = propertiesQuery
            # return response
        
        elif uid[:3] == '600':
            print("In Business ID")
            with connect() as db:
                # print("in connect loop")
                propertiesQuery = db.execute(""" 
                    -- PROPERTIES BY MANAGER
                    SELECT    
                        property_uid, property_available_to_rent, property_active_date
                        , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                        , property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area
                        , property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_favorite_image
                        , property_taxes, property_mortgages, property_insurance
                        , property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_utilities
                        , po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
                        , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                        , business_uid, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                        , lease_uid, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lease_actual_rent, lt_lease_id, lt_tenant_id, lt_responsibility
                        , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                        , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer -- , pur_amount_paid-DNU, purchase_frequency-DNU, payment_frequency-DNU, linked_tenantpur_id-DNU
                        , pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_year
                        , property_owner_id, maintenance_property_id, num_open_maintenace_req
                        , CASE
                                WHEN ISNULL(contract_uid) THEN "NO MANAGER"
                                WHEN (lease_status = 'ACTIVE' AND payment_status IS NOT NULL) THEN payment_status
                                WHEN (lease_status = 'ACTIVE' AND payment_status IS NULL) THEN 'UNPAID'
                                ELSE 'VACANT'
                            END AS rent_status 
                    --     , contract_uid AS actual_contract_uid
                    --     , lease_status AS actual_lease_status
                    FROM (
                        SELECT * FROM space.p_details
                        -- WHERE business_uid = "600-000003"
                        -- WHERE owner_uid = "110-000003"
                        -- WHERE owner_uid = \'""" + uid + """\'
                        WHERE business_uid = \'""" + uid + """\' AND contract_status = 'ACTIVE' AND contract_end_date < curdate()
                        -- WHERE tenant_uid = \'""" + uid + """\'
                        ) as p
                    LEFT JOIN (
                        SELECT * 
                        FROM space.pp_status
                        WHERE  (purchase_type = "RENT" OR ISNULL(purchase_type))
                        AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                        AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                        ) AS r ON p.property_uid = pur_property_id
                    LEFT JOIN (
                        SELECT -- * 
                            property_owner_id
                            , maintenance_property_id
                            , COUNT(maintenance_property_id) AS num_open_maintenace_req
                        FROM space.maintenanceRequests
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        WHERE  maintenance_request_status != "COMPLETED" AND maintenance_request_status != "CANCELLED"
                        GROUP BY maintenance_property_id
                        ) AS m ON p.property_uid = maintenance_property_id
                        order by lease_status; 
                        """)  

            # print("Query: ", propertiesQuery)
            response["Property"] = propertiesQuery
            # return response


            print("In New PM Requests")
            with connect() as db:
            # print("in connect loop")
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
                    WHERE contract_business_id = \'""" + uid + """\'  AND (contract_status = "NEW" OR contract_status = "SENT" OR contract_status = "REJECTED");
                    """)

            # print("Query: ", propertiesQuery)
            response["NewPMRequests"] = contractsQuery
        
        elif uid[:3] == '350':
            print("In Tenant ID")
            with connect() as db:
                # print("in connect loop")
                propertiesQuery = db.execute(""" 
                    -- PROPERTIES BY TENANT
                    SELECT    
                        property_uid, property_available_to_rent, property_active_date
                        , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                        , property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area
                        , property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_favorite_image
                        , property_taxes, property_mortgages, property_insurance
                        , property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_utilities
                        , po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
                        , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                        , business_uid, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                        , lease_uid, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lease_actual_rent, lt_lease_id, lt_tenant_id, lt_responsibility
                        , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                        , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer -- , pur_amount_paid-DNU, purchase_frequency-DNU, payment_frequency-DNU, linked_tenantpur_id-DNU
                        , pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_year
                        , property_owner_id, maintenance_property_id, num_open_maintenace_req
                        , CASE
                                WHEN ISNULL(contract_uid) THEN "NO MANAGER"
                                WHEN (lease_status = 'ACTIVE' AND payment_status IS NOT NULL) THEN payment_status
                                WHEN (lease_status = 'ACTIVE' AND payment_status IS NULL) THEN 'UNPAID'
                                ELSE 'VACANT'
                            END AS rent_status 
                    --     , contract_uid AS actual_contract_uid
                    --     , lease_status AS actual_lease_status
                    FROM (
                        SELECT * FROM space.p_details
                        -- WHERE business_uid = "600-000003"
                        -- WHERE owner_uid = "110-000003"
                        -- WHERE owner_uid = \'""" + uid + """\'
                        -- WHERE business_uid = \'""" + uid + """\'
                        WHERE tenant_uid = \'""" + uid + """\' AND contract_end_date < curdate()
                        ) as p
                    LEFT JOIN (
                        SELECT * 
                        FROM space.pp_status
                        WHERE  (purchase_type = "RENT" OR ISNULL(purchase_type))
                        AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                        AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                        ) AS r ON p.property_uid = pur_property_id
                    LEFT JOIN (
                        SELECT -- * 
                            property_owner_id
                            , maintenance_property_id
                            , COUNT(maintenance_property_id) AS num_open_maintenace_req
                        FROM space.maintenanceRequests
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        WHERE  maintenance_request_status != "COMPLETED" AND maintenance_request_status != "CANCELLED"
                        GROUP BY maintenance_property_id
                        ) AS m ON p.property_uid = maintenance_property_id
                        order by lease_status; 
                        """)  

            # print("Query: ", propertiesQuery)
            response["Property"] = propertiesQuery
            # return response
        
        elif uid[:3] == '200':
            print("In Property ID")
            with connect() as db:
                response = db.select('properties', {"property_uid": uid})
            # return response
        
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
                , "property_value"
                , "property_value_year"
                , "property_area"
                , "property_description"
                , "property_notes"
                , "property_amenities_unit"
                , "property_amenities_community"
                , "property_amenities_nearby"
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

            # image upload - old
            # images = []
            # i = -1
            # # WHILE WHAT IS TRUE?
            # while True:
            #     # print("In while loop")
            #     filename = f'img_{i}'
            #     # print("Filename: ", filename)
            #     if i == -1:
            #         filename = 'img_cover'
            #     file = request.files.get(filename)
            #     # print("File: ", file)
            #     if file:
            #         key = f'properties/{newRequestID}/{filename}'
            #         image = uploadImage(file, key, '')
            #         images.append(image)
            #         # print("Images: ", images)
            #     else:
            #         break
            #     i += 1
            # newProperty['property_images'] = json.dumps(images)
            # # print("Images to be uploaded: ", newProperty['property_images'])

            # image upload - new
            images = []
            i = 0
            imageFiles = {}
            favorite_image = data.get("img_favorite")
            while True:
                filename = f'img_{i}'                
                file = request.files.get(filename)
                s3Link = data.get(filename)
                if file:
                    imageFiles[filename] = file
                    unique_filename = filename + "_" + datetime.utcnow().strftime('%Y%m%d%H%M%SZ')
                    key = f'properties/{newRequestID}/{unique_filename}'
                    image = uploadImage(file, key, '')
                    images.append(image)

                    if filename == favorite_image:
                        newProperty["property_favorite_image"] = image

                elif s3Link:
                    imageFiles[filename] = s3Link
                    images.append(s3Link)

                    if filename == favorite_image:
                        newProperty["property_favorite_image"] = s3Link
                else:
                    break
                i += 1
            
            newProperty['property_images'] = json.dumps(images)        


            # print(newRequest)
            response = db.insert('property_owner', newRequest)
            response['property_owner'] = "Added"

            # print(newProperty, type(newProperty))
            response = db.insert('properties', newProperty)
            response['property_UID'] = newRequestID
            response['images'] = newProperty['property_images']

        return response
    
    
    def put(self):
        print("In update Property")
        # response = {}
        # payload = request.form.to_dict()
        # print(payload)
        # if payload.get('property_uid') is None:
        #     raise BadRequest("Request failed, no UID in payload.")
        # key = {'property_uid': payload.pop('property_uid')}
        # with connect() as db:
        #     response = db.update('properties', key, payload)
        # return response

        data = request.form
        # print(data)
        property_uid = data.get('property_uid')
        fields = [
            'property_available_to_rent'
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
            , "property_value"
            , "property_value_year"
            , "property_area"
            , "property_description"
            , "property_notes"
            , "property_amenities_unit"
            , "property_amenities_community"
            , "property_amenities_nearby"
        ]

        newProperty = {}
        for field in fields:
            fieldValue = data.get(field)
            # print(field, fieldValue)
            if fieldValue:
                newProperty[field] = data.get(field)

        images = []
        # i = -1
        i = 0
        imageFiles = {}
        favorite_image = data.get("img_favorite")
        while True:
            filename = f'img_{i}'
            # if i == -1:
            #     filename = 'img_cover'
            file = request.files.get(filename)
            s3Link = data.get(filename)
            if file:
                imageFiles[filename] = file
                unique_filename = filename + "_" + datetime.utcnow().strftime('%Y%m%d%H%M%SZ')
                key = f'properties/{property_uid}/{unique_filename}'
                image = uploadImage(file, key, '')
                images.append(image)

                if filename == favorite_image:
                    newProperty["property_favorite_image"] = image

            elif s3Link:
                imageFiles[filename] = s3Link
                images.append(s3Link)

                if filename == favorite_image:
                    newProperty["property_favorite_image"] = s3Link
            else:
                break
            i += 1
        # print("image_files", images)
        # images = updateImages(imageFiles, property_uid)
        # newProperty['property_images'] = json.dumps(images)
        # print("images",images)
        # if len(imageLinks) > 0:
        #     for item in images:
        #         imageLinks.append(item)
        #     newProperty['property_images'] = json.dumps(imageLinks)

        # else:
        
        newProperty['property_images'] = json.dumps(images)        

        # delete images from s3
        deleted_images_str = data.get("deleted_images")
        deleted_images = []
        
        if deleted_images_str is not None and isinstance(deleted_images_str, str):
            try:                
                deleted_images = ast.literal_eval(deleted_images_str)                                
            except (ValueError, SyntaxError) as e:
                print(f"Error parsing the deleted_images string: {e}")
                
        
        s3Client = boto3.client('s3')
        

        response = {'s3_delete_responses': []}
        if(deleted_images):
            try:                
                objects_to_delete = []
                for img in deleted_images:                    
                    key = "properties/" + img.split("properties/")[-1]
                    objects_to_delete.append(key)               

                for obj_key in objects_to_delete:                    
                    delete_response = s3Client.delete_object(Bucket='io-pm', Key=f'{obj_key}')
                    response['s3_delete_responses'].append({obj_key: delete_response})

            except Exception as e:
                print(f"Deletion from s3 failed: {str(e)}")
                response['s3_delete_error'] = f"Deletion from s3 failed: {str(e)}"




        # print("new_Property", newProperty)
        key = {'property_uid': property_uid}
        with connect() as db:
            response['database_response'] = db.update('properties', key, newProperty)
        response['images'] = newProperty['property_images']
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

