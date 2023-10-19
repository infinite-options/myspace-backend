
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img, uploadImage
from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest
import ast

from maintenance_mapper import mapMaintenanceForOwner, mapMaintenanceForTenant, mapMaintenanceForProperty, \
    mapMaintenanceForMaintenance, mapMaintenanceForPropertyManager


# MAINTENANCE BY STATUS
#                           TENANT      OWNER     PROPERTY MANAGER     
# MAINTENANCE BY STATUS        Y           Y               Y
# BY MONTH    X           X               X
# BY YEAR     X           X               X


# def get_new_maintenanceUID(conn):
#     print("In new UID request")
    
#     with connect() as db:
#         newMaintenanceQuery = db.execute("CALL space.new_request_uid;", "get", conn)
#         if newMaintenanceQuery["code"] == 280:
#             return newMaintenanceQuery["result"][0]["new_id"]
#     return "Could not generate new property UID", 500


def updateImages(imageFiles, maintenance_request_uid):
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
    bucket.objects.filter(
        Prefix=f'maintenanceRequests/{maintenance_request_uid}/').delete()
    images = []
    for i in range(len(imageFiles.keys())):

        filename = f'img_{i-1}'
        if i == 0:
            filename = 'img_cover'
        key = f'maintenanceRequests/{maintenance_request_uid}/{filename}'
        image = uploadImage(
            imageFiles[filename], key, content[i])

        images.append(image)
    return images


class MaintenanceByProperty(Resource):
    def get(self, property_id):
        print('in Maintenance By Property')
        response = {}
        
        # print("Property UID: ", property_id)

        with connect() as db:
            print("in connect loop")
            maintenanceQuery = db.execute(""" 
                    -- MAINTENANCE PROJECTS BY PROPERTY        
                    SELECT -- *
                        property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type, property_num_beds, property_num_baths, property_area
                        , maintenance_request_uid, maintenance_property_id, maintenance_request_status, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by
                        , maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency
                        , maintenance_notes, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date, maintenance_callback_number, maintenance_estimated_cost
                    FROM space.properties
                    LEFT JOIN space.maintenanceRequests ON maintenance_property_id = property_uid
                    WHERE property_uid = \'""" + property_id + """\';
                    """)
            

            # print("Query: ", maintenanceQuery)
            response["MaintenanceProjects"] = maintenanceQuery
            return response


class MaintenanceRequests(Resource):
    def get(self, uid):
        print('in Maintenance Request')
        response = {}

        print("UID: ", uid)

        if uid[:3] == '110':
            print("In Owner ID")

            with connect() as db:
                print("in connect loop")
                maintenanceRequests = db.execute(""" 
                        -- MAINTENANCE REQUEST BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT  -- * -- bill_property_id,  maintenance_property_id,
                            -- maintenance_request_status, quote_status
                            maintenanceRequests.*
                            -- Properties
                            , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_images
                            , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                            , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, contract_status
                            , lease_uid, lease_status, lease_assigned_contacts,  tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                        FROM space.maintenanceRequests
                        LEFT JOIN space.properties ON property_uid = maintenance_property_id
                        -- LEFT JOIN space.bills ON bill_maintenance_quote_id = maintenance_quote_uid
                        -- LEFT JOIN space.pp_status ON pur_bill_id = bill_uid
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        LEFT JOIN space.b_details ON maintenance_property_id = contract_property_id
                        LEFT JOIN space.leases ON maintenance_property_id = lease_property_id
                        LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                        WHERE owner_uid = \'""" + uid + """\'
                        -- WHERE business_uid = \'""" + uid + """\'
                        -- WHERE tenant_uid = \'""" + uid + """\'
                        -- WHERE quote_business_id = \'""" + uid + """\'
                        -- WHERE maintenance_property_id = = \'""" + uid + """\'
                        ORDER BY maintenance_request_created_date;
                        """)

            if maintenanceRequests.get('code') == 200:
                return mapMaintenanceForOwner(maintenanceRequests)

            response["maintenanceRequests"] = maintenanceRequests
            return response


        elif uid[:3] == '350':
            print("In Tenant ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceRequests = db.execute(""" 
                        -- MAINTENANCE REQUEST BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT  -- * -- bill_property_id,  maintenance_property_id,
                            -- maintenance_request_status, quote_status
                            maintenanceRequests.*
                            -- Properties
                            , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_images
                            , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                            , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, contract_status
                            , lease_uid, lease_status, lease_assigned_contacts,  tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                        FROM space.maintenanceRequests
                        LEFT JOIN space.properties ON property_uid = maintenance_property_id
                        -- LEFT JOIN space.bills ON bill_maintenance_quote_id = maintenance_quote_uid
                        -- LEFT JOIN space.pp_status ON pur_bill_id = bill_uid
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        LEFT JOIN space.b_details ON maintenance_property_id = contract_property_id
                        LEFT JOIN space.leases ON maintenance_property_id = lease_property_id
                        LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                        -- WHERE owner_uid = \'""" + uid + """\'
                        -- WHERE business_uid = \'""" + uid + """\'
                        WHERE tenant_uid = \'""" + uid + """\'
                        -- WHERE quote_business_id = \'""" + uid + """\'
                        -- WHERE maintenance_property_id = = \'""" + uid + """\'
                        ORDER BY maintenance_request_created_date;
                        """)

            if maintenanceRequests.get('code') == 200:
                return mapMaintenanceForTenant(maintenanceRequests)

            response["maintenanceRequests"] = maintenanceRequests
            return response

        elif uid[:3] == '200':
            print("In Property ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceRequests = db.execute(""" 
                        -- MAINTENANCE REQUEST BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT  -- * -- bill_property_id,  maintenance_property_id,
                            -- maintenance_request_status, quote_status
                            maintenanceRequests.*
                            -- Properties
                            , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_images
                            , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                            , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, contract_status
                            , lease_uid, lease_status, lease_assigned_contacts,  tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                        FROM space.maintenanceRequests
                        LEFT JOIN space.properties ON property_uid = maintenance_property_id
                        -- LEFT JOIN space.bills ON bill_maintenance_quote_id = maintenance_quote_uid
                        -- LEFT JOIN space.pp_status ON pur_bill_id = bill_uid
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        LEFT JOIN space.b_details ON maintenance_property_id = contract_property_id
                        LEFT JOIN space.leases ON maintenance_property_id = lease_property_id
                        LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                        -- WHERE owner_uid = \'""" + uid + """\'
                        -- WHERE business_uid = \'""" + uid + """\'
                        -- WHERE tenant_uid = \'""" + uid + """\'
                        -- WHERE quote_business_id = \'""" + uid + """\'
                        WHERE maintenance_property_id = \'""" + uid + """\'
                        ORDER BY maintenance_request_created_date;
                        """)

            if maintenanceRequests.get('code') == 200:
                return mapMaintenanceForProperty(maintenanceRequests)

            response["maintenanceRequests"] = maintenanceRequests
            return response

        else:
            print("UID Not found")
            response["MaintenanceStatus"] = "UID Not Found"
            return response

    def post(self):
        print("In Maintenace Requests")
        response = {}
        with connect() as db:
            data = request.form
            fields = [
                'maintenance_property_id'
                , 'maintenance_title'
                , 'maintenance_desc'
                , 'maintenance_request_type'
                , 'maintenance_request_created_by'
                , 'maintenance_priority'
                , 'maintenance_can_reschedule'
                , 'maintenance_assigned_business'
                , 'maintenance_assigned_worker'
                , 'maintenance_scheduled_date'
                , 'maintenance_scheduled_time'
                , 'maintenance_frequency'
                , 'maintenance_notes'
                , 'maintenance_request_created_date'
                , 'maintenance_request_closed_date'
                , 'maintenance_request_adjustment_date'
                , 'maintenance_callback_number'
                , 'maintenance_estimated_cost'
            ]

            newRequest = {}
            for field in fields:
                newRequest[field] = data.get(field)
                # print(field, " = ", newRequest[field])


            # # GET NEW UID
            print("Get New Request UID")
            newRequestID = db.call('new_request_uid')['result'][0]['new_id']
            newRequest['maintenance_request_uid'] = newRequestID
            print(newRequestID)

            images = []
            i = -1
            # WHILE WHAT IS TRUE?
            while True:
                print("In while loop")
                filename = f'img_{i}'
                # print("Filename: ", filename)
                if i == -1:
                    filename = 'img_cover'
                file = request.files.get(filename)
                # print("File: ", file)
                if file:
                    key = f'maintenanceRequests/{newRequestID}/{filename}'
                    image = uploadImage(file, key, '')
                    images.append(image)
                else:
                    break
                i += 1
            newRequest['maintenance_images'] = json.dumps(images)
            # print("Images to be uploaded: ", newRequest['maintenance_images'])

            newRequest['maintenance_request_status'] = 'NEW'
            # newRequest['frequency'] = 'One time'
            # newRequest['can_reschedule'] = False

            # print(newRequest)

            response = db.insert('maintenanceRequests', newRequest)
            response['Maintenance_UID'] = newRequestID
            response['images'] = newRequest['maintenance_images']

        return response
    
    def put(self):
        response = {}
        payload = request.get_json()
        print(payload)
        if payload.get('maintenance_request_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'maintenance_request_uid': payload.pop('maintenance_request_uid')}
        with connect() as db:
            print(key, payload)
            response = db.update('maintenanceRequests', key, payload)
        return response


class MaintenanceQuotes(Resource):
    def get(self):
        where = request.args.to_dict()
        with connect() as db:
            response = db.select('maintenanceQuotes', where)
        return response

    def post(self):
        response = []
        payload = request.form
        quote_maintenance_request_id = payload.get("quote_maintenance_request_id")
        quote_maintenance_contacts = payload.getlist("quote_maintenance_contacts")
        quote_pm_notes = payload["quote_pm_notes"]
        with connect() as db:
            for quote_business_id in quote_maintenance_contacts:
                quote = {}
                quote["maintenance_quote_uid"] = db.call('space.new_quote_uid')['result'][0]['new_id']
                quote["quote_business_id"] = quote_business_id
                quote["quote_maintenance_request_id"] = quote_maintenance_request_id
                quote["quote_status"] = "REQUESTED"
                quote["quote_pm_notes"] = quote_pm_notes
                images = []
                i = 0
                while True:
                    filename = f'img_{i}'
                    file = request.files.get(filename)
                    if file:
                        key = f'maintenanceQuotes/{quote["maintenance_quote_uid"]}/{filename}'
                        image = uploadImage(file, key, '')
                        images.append(image)
                    else:
                        break
                    i += 1
                quote["quote_maintenance_images"] = json.dumps(images)
                query_response = db.insert('maintenanceQuotes', quote)
                response.append(query_response)
        return response

    def put(self):
        print('in MaintenanceQuotes')
        payload = request.form
        if payload.get('maintenance_quote_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'maintenance_quote_uid': payload['maintenance_quote_uid']}
        print("Key: ", key)
        quote = {k: v for k, v in payload.items()}
        print("KV Pairs: ", quote)

        with connect() as db:
            query = db.select('maintenanceQuotes', key)
        try:
            quote_from_db = query.get('result')[0]
            images = quote_from_db.get("quote_maintenance_images")
            images = ast.literal_eval(images) if images else []  # convert to list of URLs
            print('type: ', type(images))
            print(f'previously saved images: {images}')
        except IndexError as e:
            print(e)
            raise BadRequest("Request failed, no such Maintenance Quote in the database.")

        i = 0
        s3_i = len(images) if len(images) else 0
        while True:
            filename = f'img_{i}'
            s3_filename = f'img_{s3_i}'
            print("Filename: ", filename)
            file = request.files.get(filename)
            print("File: ", file)
            if file:
                S3key = f'maintenanceQuotes/{quote["maintenance_quote_uid"]}/{s3_filename}'
                print("S3 Key: ", S3key)
                image = uploadImage(file, S3key, '')
                images.append(image)
            else:
                break
            i += 1
            s3_i += 1
        print("Images: ",images)    
        quote["quote_maintenance_images"] = json.dumps(images)

        with connect() as db:
            print("In actual PUT")
            response = db.update('maintenanceQuotes', key, quote)
        return response


class MaintenanceQuotesByUid(Resource):
    def get(self, maintenance_quote_uid):
        print('in MaintenanceQuotesByUid')
        with connect() as db:
            response = db.select('maintenanceQuotes', {"maintenance_quote_uid": maintenance_quote_uid})
        return response


class MaintenanceStatus(Resource): 
    def get(self, uid):
        print('in Maintenance Status')
        response = {}

        print("UID: ", uid)

        if uid[:3] == '110':
            print("In Owner ID")

            with connect() as db:
                # print("in connect loop")
                maintenanceStatus = db.execute(""" 
                        -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT -- * -- bill_property_id,  maintenance_property_id,
                            -- maintenance_request_status, quote_status
                            maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority
                            , maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                            , maintenance_callback_number, maintenance_estimated_cost, maintenance_pm_notes
                            , maintenance_quote_uid, quote_maintenance_request_id, quote_business_id
                            , quote_services_expenses -- WHERE DOES THIS COME FROM
                            -- DO WE NEED PARTS INCLUDED? quote_parts (JSON Object), quote_parts_estimate ($), 
                            , quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes
                            , quote_status, quote_pm_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                            -- Properties
                            , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                            -- DO WE NEED FINAL INVOICE AMOUNTS OR DOES THAT GO INTO BILLS?
                            , bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                            , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
                            , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
                            , cf_month, cf_year
                            -- Properties
                            , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_images
                            , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                            , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, contract_status
                            , lease_uid, lease_status, lease_assigned_contacts,  tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                        FROM space.m_details
                        LEFT JOIN space.properties ON property_uid = maintenance_property_id
                        LEFT JOIN space.bills ON bill_maintenance_quote_id = maintenance_quote_uid
                        LEFT JOIN space.pp_status ON pur_bill_id = bill_uid
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        LEFT JOIN space.b_details ON maintenance_property_id = contract_property_id
                        LEFT JOIN space.leases ON maintenance_property_id = lease_property_id
                        LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                        WHERE owner_uid = \'""" + uid + """\'
                        -- WHERE business_uid = \'""" + uid + """\'
                        -- WHERE tenant_uid = \'""" + uid + """\'
                        -- WHERE quote_business_id = \'""" + uid + """\'
                        -- WHERE maintenance_property_id = = \'""" + uid + """\'
                        ORDER BY maintenance_request_created_date;
                        """)

            if maintenanceStatus.get('code') == 200:
                return mapMaintenanceForOwner(maintenanceStatus)

            response["MaintenanceStatus"] = maintenanceStatus
            return response
        
        elif uid[:3] == '600':
            print("In Business ID")

            # CHECK IF MAINTENANCE OR MANAGEMENT
            with connect() as db:
                # print("in check loop")
                businessType = db.execute(""" 
                        -- CHECK BUSINESS TYPE
                        SELECT -- *
                            business_uid, business_type
                        FROM space.businessProfileInfo
                        WHERE business_uid = \'""" + uid + """\';
                        """)
            
            # print(businessType)
            print(businessType["result"])
            # print(businessType["result"][0]["business_type"])

            # if businessType["result"] == "()":
            if len(businessType["result"]) == 0:
                print("BUSINESS UID Not found")
                response["MaintenanceStatus"] = "BUSINESS UID Not Found"
                return response

            elif businessType["result"][0]["business_type"] == "MANAGEMENT":

                with connect() as db:
                    print("in MANAGEMENT")
                    maintenanceStatus = db.execute(""" 
                            -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                            SELECT -- * -- bill_property_id,  maintenance_property_id,
                                -- maintenance_request_status, quote_status
                                maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority
                                , maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                                , maintenance_callback_number, maintenance_estimated_cost, maintenance_pm_notes
                                , maintenance_quote_uid, quote_maintenance_request_id, quote_business_id
                                , quote_services_expenses -- WHERE DOES THIS COME FROM
                                -- DO WE NEED PARTS INCLUDED? quote_parts (JSON Object), quote_parts_estimate ($), 
                                , quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes
                                , quote_status, quote_pm_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                                -- Properties
                                , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                                -- DO WE NEED FINAL INVOICE AMOUNTS OR DOES THAT GO INTO BILLS?
                                , bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                                , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
                                , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
                                , cf_month, cf_year
                                -- Properties
                                , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_images
                                , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                                , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, contract_status
                                , lease_uid, lease_status, lease_assigned_contacts,  tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                            FROM space.m_details
                            LEFT JOIN space.properties ON property_uid = maintenance_property_id
                            LEFT JOIN space.bills ON bill_maintenance_quote_id = maintenance_quote_uid
                            LEFT JOIN space.pp_status ON pur_bill_id = bill_uid
                            LEFT JOIN space.o_details ON maintenance_property_id = property_id
                            LEFT JOIN space.b_details ON maintenance_property_id = contract_property_id
                            LEFT JOIN space.leases ON maintenance_property_id = lease_property_id
                            LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                            -- WHERE owner_uid = \'""" + uid + """\'
                            WHERE business_uid = \'""" + uid + """\'
                            -- WHERE tenant_uid = \'""" + uid + """\'
                            -- WHERE quote_business_id = \'""" + uid + """\'
                            -- WHERE maintenance_property_id = \'""" + uid + """\'
                            ORDER BY maintenance_request_created_date;
                            """)

                if maintenanceStatus.get('code') == 200:
                    return mapMaintenanceForPropertyManager(maintenanceStatus)

                response["MaintenanceStatus"] = maintenanceStatus
                return response
            
            elif businessType["result"][0]["business_type"] == "MAINTENANCE":

                with connect() as db:
                    print("in MAINTENANCE")
                    maintenanceStatus = db.execute(""" 
                            -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                            -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                            SELECT -- * -- bill_property_id,  maintenance_property_id,
                                -- maintenance_request_status, quote_status
                                maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority
                                , maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                                , maintenance_callback_number, maintenance_estimated_cost, maintenance_pm_notes
                                , maintenance_quote_uid, quote_maintenance_request_id, quote_business_id
                                , quote_services_expenses -- WHERE DOES THIS COME FROM
                                -- DO WE NEED PARTS INCLUDED? quote_parts (JSON Object), quote_parts_estimate ($), 
                                , quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes
                                , quote_status, quote_pm_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                                -- Properties
                                , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                                -- DO WE NEED FINAL INVOICE AMOUNTS OR DOES THAT GO INTO BILLS?
                                , bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                                , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
                                , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
                                , cf_month, cf_year
                                -- Properties
                                , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_images
                                , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                                , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, contract_status
                                , lease_uid, lease_status, lease_assigned_contacts,  tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                            FROM space.m_details
                            LEFT JOIN space.properties ON property_uid = maintenance_property_id
                            LEFT JOIN space.bills ON bill_maintenance_quote_id = maintenance_quote_uid
                            LEFT JOIN space.pp_status ON pur_bill_id = bill_uid
                            LEFT JOIN space.o_details ON maintenance_property_id = property_id
                            LEFT JOIN space.b_details ON maintenance_property_id = contract_property_id
                            LEFT JOIN space.leases ON maintenance_property_id = lease_property_id
                            LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                            -- WHERE owner_uid = \'""" + uid + """\'
                            -- WHERE business_uid = \'""" + uid + """\'
                            -- WHERE tenant_uid = \'""" + uid + """\'
                            WHERE quote_business_id = \'""" + uid + """\'
                            -- WHERE maintenance_property_id = \'""" + uid + """\'
                            ORDER BY maintenance_request_created_date;
                            """)

                if maintenanceStatus.get('code') == 200:
                    return mapMaintenanceForMaintenance(maintenanceStatus)

                response["MaintenanceStatus"] = maintenanceStatus
                return response
            
            else:
                print("BUSINESS TYPE Not found")
                response["MaintenanceStatus"] = "BUSINESS TYPE Not Found"
                return response
        
        elif uid[:3] == '350':
            print("In Tenant ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceStatus = db.execute(""" 
                        -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT -- * -- bill_property_id,  maintenance_property_id,
                            -- maintenance_request_status, quote_status
                            maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority
                            , maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                            , maintenance_callback_number, maintenance_estimated_cost, maintenance_pm_notes
                            , maintenance_quote_uid, quote_maintenance_request_id, quote_business_id
                            , quote_services_expenses -- WHERE DOES THIS COME FROM
                            -- DO WE NEED PARTS INCLUDED? quote_parts (JSON Object), quote_parts_estimate ($), 
                            , quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes
                            , quote_status, quote_pm_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                            -- Properties
                            , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                            -- DO WE NEED FINAL INVOICE AMOUNTS OR DOES THAT GO INTO BILLS?
                            , bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                            , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
                            , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
                            , cf_month, cf_year
                            -- Properties
                            , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_images
                            , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                            , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, contract_status
                            , lease_uid, lease_status, lease_assigned_contacts,  tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                        FROM space.m_details
                        LEFT JOIN space.properties ON property_uid = maintenance_property_id
                        LEFT JOIN space.bills ON bill_maintenance_quote_id = maintenance_quote_uid
                        LEFT JOIN space.pp_status ON pur_bill_id = bill_uid
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        LEFT JOIN space.b_details ON maintenance_property_id = contract_property_id
                        LEFT JOIN space.leases ON maintenance_property_id = lease_property_id
                        LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                        -- WHERE owner_uid = \'""" + uid + """\'
                        -- WHERE business_uid = \'""" + uid + """\'
                        WHERE tenant_uid = \'""" + uid + """\'
                        -- WHERE quote_business_id = \'""" + uid + """\'
                        -- WHERE maintenance_property_id = \'""" + uid + """\'
                        ORDER BY maintenance_request_created_date;
                        """)

            if maintenanceStatus.get('code') == 200:
                return mapMaintenanceForTenant(maintenanceStatus)

            response["MaintenanceStatus"] = maintenanceStatus
            return response

        elif uid[:3] == '200':
            print("In Property ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceStatus = db.execute(""" 
                        -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT -- * -- bill_property_id,  maintenance_property_id,
                            -- maintenance_request_status, quote_status
                            maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority
                            , maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                            , maintenance_callback_number, maintenance_estimated_cost, maintenance_pm_notes
                            , maintenance_quote_uid, quote_maintenance_request_id, quote_business_id
                            , quote_services_expenses -- WHERE DOES THIS COME FROM
                            -- DO WE NEED PARTS INCLUDED? quote_parts (JSON Object), quote_parts_estimate ($), 
                            , quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes
                            , quote_status, quote_pm_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                            -- Properties
                            , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                            -- DO WE NEED FINAL INVOICE AMOUNTS OR DOES THAT GO INTO BILLS?
                            , bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                            , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
                            , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
                            , cf_month, cf_year
                            -- Properties
                            , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_images
                            , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                            , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, contract_status
                            , lease_uid, lease_status, lease_assigned_contacts,  tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                        FROM space.m_details
                        LEFT JOIN space.properties ON property_uid = maintenance_property_id
                        LEFT JOIN space.bills ON bill_maintenance_quote_id = maintenance_quote_uid
                        LEFT JOIN space.pp_status ON pur_bill_id = bill_uid
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        LEFT JOIN space.b_details ON maintenance_property_id = contract_property_id
                        LEFT JOIN space.leases ON maintenance_property_id = lease_property_id
                        LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                        -- WHERE owner_uid = \'""" + uid + """\'
                        -- WHERE business_uid = \'""" + uid + """\'
                        -- WHERE tenant_uid = \'""" + uid + """\'
                        -- WHERE quote_business_id = \'""" + uid + """\'
                        WHERE maintenance_property_id = \'""" + uid + """\'
                        ORDER BY maintenance_request_created_date;
                        """)

            if maintenanceStatus.get('code') == 200:
                return mapMaintenanceForProperty(maintenanceStatus)

            response["MaintenanceStatus"] = maintenanceStatus
            return response

        else:
            print("UID Not found")
            response["MaintenanceStatus"] = "UID Not Found"
            return response
