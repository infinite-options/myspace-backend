
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

from maintenance_mapper import mapMaintenanceStatusByUserType, mapMaintenanceStatusForOwner, \
    mapMaintenanceStatusForPropertyManager, mapMaintenanceStatusForMaintenancePerson, mapMaintenanceStatusForTenant


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


class MaintenanceStatusByProperty(Resource):
    # decorators = [jwt_required()]

    def get(self, property_id):
        print('in Maintenance Status by Property')
        response = {}

        # print("Property UID: ", property_id)

        with connect() as db:
            print("in connect loop")
            maintenanceQuery = db.execute(""" 
                    -- MAINTENANCE STATUS BY PROPERTY        
                    SELECT property_uid, property_address
                        , maintenanceRequests.maintenance_request_status
                        , COUNT(maintenanceRequests.maintenance_request_status) AS num
                    FROM space.properties
                    LEFT JOIN space.maintenanceRequests ON maintenance_property_id = property_uid
                    WHERE property_uid = \'""" + property_id + """\'
                    GROUP BY maintenance_request_status;
                    """)

            
            # print("Query: ", maintenanceQuery)
            response["MaintenanceProjectStatus"] = maintenanceQuery
            return response


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




# class OwnerMaintenanceByStatus(Resource):
class MaintenanceStatusByOwner(Resource): 
    def get(self, owner_id):
        print('in New Owner Maintenance Dashboard')
        response = {}

        # print("Owner UID: ", owner_id)

        with connect() as db:
            print("in connect loop")
            maintenanceQuery = db.execute(""" 
                    -- NEW MAINTENANCE STATUS QUERY
                    SELECT -- *
                        maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, user_type, user_name, user_phone, user_email
                        , maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status
                        , maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                        , maintenance_quote_uid, quote_maintenance_request_id, quote_business_id, quote_services_expenses, quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes, quote_status, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                        , property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
                        , o_details.*
                    FROM space.m_details
                    LEFT JOIN space.user_profiles ON maintenance_request_created_by = profile_uid
                    LEFT JOIN space.properties ON property_uid = maintenance_property_id
                    LEFT JOIN space.o_details ON maintenance_property_id = property_id
                    WHERE property_owner_id = \'""" + owner_id + """\'
                    ORDER BY maintenance_request_status;
                    """)

            # print("Query: ", maintenanceQuery)  # This is a list
            # # FOR DEBUG ONLY - THESE STATEMENTS ALLOW YOU TO CHECK THAT THE QUERY WORKS
            # response["MaintenanceProjects"] = maintenanceQuery
            # return response
        

            # Format Output to be a dictionary of lists
            property_dict = {}
            for item in maintenanceQuery["result"]:
                # print("item: ", item)
                maintenance_status = item["maintenance_request_status"]
                # print("maintenance_status: ", maintenance_status)
                property_info = {k: v for k, v in item.items() if k != 'maintenance_request_status'}
                
                if maintenance_status in property_dict:
                    property_dict[maintenance_status].append(property_info)
                else:
                    property_dict[maintenance_status] = [property_info]

            # Print the resulting dictionary
            # print(property_dict)
            return property_dict
        

class MaintenanceRequestsByOwner(Resource):
    def get(self, owner_id):
        print('in New Owner Maintenance Dashboard')
        response = {}

        # print("Owner UID: ", owner_id)

        with connect() as db:
            print("in connect loop")
            maintenanceQuery = db.execute(""" 
                    -- NEW MAINTENANCE STATUS QUERY
                    SELECT -- *
                        maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, user_type, user_name, user_phone, user_email
                        , maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status
                        , maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date, maintenance_callback_number, maintenance_estimated_cost
                        , maintenance_quote_uid, quote_maintenance_request_id, quote_business_id, quote_services_expenses, quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes, quote_status, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                        , property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
                        , o_details.*
                    FROM space.m_details
                    LEFT JOIN space.user_profiles ON maintenance_request_created_by = profile_uid
                    LEFT JOIN space.properties ON property_uid = maintenance_property_id
                    LEFT JOIN space.o_details ON maintenance_property_id = property_id
                    WHERE property_owner_id = \'""" + owner_id + """\'
                    ORDER BY maintenance_request_status;
                    """)

            response["MaintenanceProjects"] = maintenanceQuery

            maintenanceNum = db.execute(""" 
                                -- MAINTENANCE STATUS BY USER
                                SELECT property_owner.property_owner_id
                                    , maintenanceRequests.maintenance_request_status
                                    , COUNT(maintenanceRequests.maintenance_request_status) AS num
                                FROM space.properties
                                LEFT JOIN space.property_owner ON property_id = property_uid
                                LEFT JOIN space.maintenanceRequests ON maintenance_property_id = property_uid
                                WHERE property_owner_id = \'""" + owner_id + """\'
                                GROUP BY maintenance_request_status;
                                """)
            response["MaintenanceNum"] = maintenanceNum
            return response


class MaintenanceReq(Resource):
    def get(self, uid):
        print('in Maintenance Request')
        response = {}

        print("UID: ", uid)

        if uid[:3] == '110':
            print("In Owner ID")

            with connect() as db:
                print("in connect loop")
                maintenanceStatus = db.execute(""" 
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

            response["MaintenanceStatus"] = maintenanceStatus
            return response


        elif uid[:3] == '350':
            print("In Tenant ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceStatus = db.execute(""" 
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

            response["MaintenanceStatus"] = maintenanceStatus
            return response

        elif uid[:3] == '200':
            print("In Property ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceStatus = db.execute(""" 
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

            response["MaintenanceStatus"] = maintenanceStatus
            return response

        else:
            print("UID Not found")
            response["MaintenanceStatus"] = "UID Not Found"
            return response

class MaintenanceRequests(Resource):
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
        if payload.get('maintenance_request_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'maintenance_request_uid': payload.pop('maintenance_request_uid')}
        with connect() as db:
            response = db.update('maintenanceRequests', key, payload)
        return response


class MaintenanceQuotes(Resource):
    def get(self):
        where = request.args.to_dict()
        with connect() as db:
            response = db.select('maintenanceQuotes', where)
        return response

    def post(self):
        print('in MaintenanceQuotes')
        payload = request.get_json()
        with connect() as db:
            response = db.insert('maintenanceQuotes', payload)
        return response

    def put(self):
        print('in MaintenanceQuotes')
        payload = request.form
        if payload.get('maintenance_quote_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'maintenance_quote_uid': payload['maintenance_quote_uid']}
        quote = {k: v for k, v in payload.items()}
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

        with connect() as db:
            response = db.update('maintenanceQuotes', key, quote)
        return response


class MaintenanceQuotesByUid(Resource):
    def get(self, maintenance_quote_uid):
        print('in MaintenanceQuotesByUid')
        with connect() as db:
            response = db.select('maintenanceQuotes', {"maintenance_quote_uid": maintenance_quote_uid})
        return response



class MaintenanceSummaryByOwner(Resource): 
    def get(self, owner_id):
        print('in New Owner Maintenance Dashboard')
        response = {}

        # print("Owner UID: ", owner_id)

        with connect() as db:
            print("in connect loop")
            maintenanceQuery = db.execute(""" 
                    -- MAINTENANCE STATUS BY OWNER
                    SELECT property_owner.property_owner_id
                        , maintenanceRequests.maintenance_request_status
                        , COUNT(maintenanceRequests.maintenance_request_status) AS num
                    FROM space.properties
                    LEFT JOIN space.property_owner ON property_id = property_uid
                    LEFT JOIN space.maintenanceRequests ON maintenance_property_id = property_uid
                    WHERE property_owner_id = \'""" + owner_id + """\'
                    GROUP BY maintenance_request_status;
                    """)

            # print("Query: ", maintenanceQuery)  # This is a list
            # # FOR DEBUG ONLY - THESE STATEMENTS ALLOW YOU TO CHECK THAT THE QUERY WORKS
            response["MaintenanceSummary"] = maintenanceQuery
            return response


# class MaintenanceStatusByProfile(Resource):
#     def get(self, profile_uid):
#         print('in MaintenanceStatusByProfile')
#         with connect() as db:
#             query = db.select('user_profiles', {"profile_uid": profile_uid})
#         try:
#             user = query.get('result')[0]
#             business_user_id = user['user_id']
#             user_type = user['user_type']
#         except (IndexError, KeyError) as e:
#             print(e)
#             raise BadRequest("Request failed, no such user_profile record in the database.")

#         with connect() as db:
#             response = db.execute("""SELECT business_uid,
#                 property_uid, properties.property_address,
#                 purchase_uid, purchase_status, purchase_type, 
#                 payment_uid, pay_amount, payment_notes, payment_type,
#                 maintenance_request_uid, maintenance_title, maintenance_desc, maintenance_request_type, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date,
#                 maintenance_quote_uid, quote_services_expenses, quote_earliest_availability, quote_event_type, quote_notes, quote_status
#                 FROM b_details 
#                 JOIN properties ON contract_property_id = property_uid
#                 JOIN pp_details ON property_uid = pur_property_id
#                 JOIN m_details ON property_uid = maintenance_property_id
#                 WHERE business_user_id = \'""" + business_user_id + """\'
#                 ORDER BY maintenance_request_created_date;""")
#         if response.get('code') == 200 and response.get('result'):
#             return mapMaintenanceStatusByUserType(response, user_type)
#         return response

class MaintenanceStatusByProfile(Resource):
    def get(self, profile_uid):
        print('in MaintenanceStatusByProfile')
        print(profile_uid)
        with connect() as db:
            query = db.select('user_profiles', {"profile_uid": profile_uid})
        try:
            user = query.get('result')[0]
            business_user_id = user['user_id']
            user_type = user['user_type']
        except (IndexError, KeyError) as e:
            print(e)
            raise BadRequest("Request failed, no such user_profile record in the database.")

        with connect() as db:
            response = db.execute("""
                                  SELECT -- *
                                    maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule
                                    , maintenance_assigned_business, maintenance_assigned_worker
                                    , maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                                    , maintenance_callback_number, maintenance_estimated_cost
                                    , maintenance_quote_uid, quote_business_id, quote_services_expenses, quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes, quote_status, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                                    -- , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type
                                    , bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                                    , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
                                    , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
                                    , cf_month, cf_year
                                    , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email
                                    , initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email
                                    , payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email
                                    , property_address, property_unit, property_city, property_state, property_zip, property_type, property_images, property_description, property_notes
                                    , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_address, owner_unit, owner_city, owner_state, owner_zip
                                    , contract_start_date, contract_end_date, contract_status, business_name, business_phone_number, business_email, business_services_fees, business_locations, business_address, business_unit, business_city, business_state, business_zip
                                    , tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                                FROM space.m_details 
                                -- LEFT JOIN space.properties ON maintenance_property_id = property_uid
                                LEFT JOIN space.bills ON bill_maintenance_quote_id = maintenance_quote_uid
                                LEFT JOIN space.pp_details ON pur_bill_id = bill_uid
                                WHERE quote_business_id = \'""" + profile_uid + """\' AND  quote_business_id IS NOT NULL
                                ORDER BY maintenance_request_created_date;
                                  """)


        if response.get('code') == 200 and response.get('result'):
            return mapMaintenanceStatusByUserType(response, user_type)
        return response


class MaintenanceStatusByOwnerSimplified(Resource): 
    def get(self, owner_id):
        print('in New Owner Maintenance Dashboard')
        response = {}

        # print("Owner UID: ", owner_id)

        with connect() as db:
            print("in connect loop")
            maintenanceQuery = db.execute(""" 
                    -- MAINTENANCE STATUS BY OWNER BY PROPERTY BY STATUS WITH LIMITED DETAILS FOR FLUTTERFLOW
                    SELECT property_owner_id
                        , property_uid, property_address, property_unit -- , property_city, property_state, property_zip, property_type, property_num_beds, property_num_baths, property_area, property_listed_rent, property_images
                        , maintenance_request_uid, maintenance_title
                        , if (ISNULL(JSON_UNQUOTE(JSON_EXTRACT(maintenance_images, '$[0]'))),"", JSON_UNQUOTE(JSON_EXTRACT(maintenance_images, '$[0]'))) AS image
                        , maintenance_request_type, maintenance_request_status, maintenance_request_created_date
                    FROM space.maintenanceRequests 
                    LEFT JOIN space.maintenanceQuotes ON quote_maintenance_request_id = maintenance_request_uid
                    LEFT JOIN space.properties ON maintenance_property_id = property_uid	-- ASSOCIATE PROPERTY DETAILS WITH MAINTENANCE DETAILS
                    LEFT JOIN space.property_owner ON property_id = property_uid 			-- SO WE CAN SORT BY OWNER
                    WHERE property_owner_id = \'""" + owner_id + """\'
                    ORDER BY maintenance_request_status;
                    """)

            # print("Query: ", maintenanceQuery)  # This is a list
            # # FOR DEBUG ONLY - THESE STATEMENTS ALLOW YOU TO CHECK THAT THE QUERY WORKS
            response["MaintenanceSummary"] = maintenanceQuery
            return response
        
class MaintenanceDashboard(Resource):
    def post(self):
        response = {}

        with connect() as db:

            requested = db.execute(""" 
                            SELECT SUM(quote_total_estimate) FROM space.maintenanceQuotes WHERE quote_status = 'REQUESTED'
                            """)
            sum_requested = requested["result"][0]["SUM(quote_total_estimate)"]

        with connect() as db:

            accepted = db.execute(""" 
                            SELECT SUM(quote_total_estimate) FROM space.maintenanceQuotes WHERE quote_status = 'ACCEPTED'
                            """)
            sum_accepted = accepted["result"][0]["SUM(quote_total_estimate)"]
        with connect() as db:

            scheduled = db.execute(""" 
                            SELECT SUM(quote_total_estimate) FROM space.maintenanceQuotes WHERE quote_status = 'SCHEDULED'
                            """)
            sum_scheduled = scheduled["result"][0]["SUM(quote_total_estimate)"]
        with connect() as db:

            completed = db.execute(""" 
                            SELECT SUM(quote_total_estimate) FROM space.maintenanceQuotes WHERE quote_status = 'COMPLETED'
                            """)
            sum_completed = completed["result"][0]["SUM(quote_total_estimate)"]

        with connect() as db:

            requested = db.execute(""" 
                            SELECT COUNT(*) FROM space.maintenanceQuotes WHERE quote_status = 'REQUESTED'
                            """)
            num_requested = requested["result"][0]["COUNT(*)"]

        with connect() as db:

            accepted = db.execute(""" 
                            SELECT COUNT(*) FROM space.maintenanceQuotes WHERE quote_status = 'ACCEPTED'
                            """)
            num_accepted = accepted["result"][0]["COUNT(*)"]
        with connect() as db:

            scheduled = db.execute(""" 
                            SELECT COUNT(*) FROM space.maintenanceQuotes WHERE quote_status = 'SCHEDULED'
                            """)
            num_scheduled = scheduled["result"][0]["COUNT(*)"]
        with connect() as db:

            completed = db.execute(""" 
                            SELECT COUNT(*) FROM space.maintenanceQuotes WHERE quote_status = 'COMPLETED'
                            """)
            num_completed = completed["result"][0]["COUNT(*)"]

        json_num = {"Accepted": num_accepted,
                "Requested": num_requested,
                "Scheduled": num_scheduled,
                "Completed": num_completed}

        json_sum = {"Accepted": sum_accepted,
                    "Requested": sum_requested,
                    "Scheduled": sum_scheduled,
                    "Completed": sum_completed}

        return json_sum
        
class MaintenanceSummaryAndStatusByOwner(Resource): 
    def get(self, owner_id):
        print('in Maintenance Summary and Status by Owner')
        response = {}

        # print("Owner UID: ", owner_id)

        with connect() as db:
            print("in connect loop")
            maintenanceQuery = db.execute(""" 
                    -- NEW QUERY TO SIMPLIFY FLUTTERFLOW EXPANDABLE WIDGET WITH JSON OBJECT
                    SELECT
                        maintenanceRequests.maintenance_request_status
                        , COUNT(maintenanceRequests.maintenance_request_status) AS num
                        , JSON_ARRAYAGG(
                            JSON_OBJECT(
                                'owner_id', property_owner_id,
                                'property_id', property_uid,
                                'property_address', property_address,
                                'property_unit', property_unit,
                                'incident_date', maintenance_request_created_date,
                                'maintenance_request_type', maintenance_request_type,
                                'priority', maintenance_priority,
                                'title', maintenance_title,
                                'description', maintenance_desc,
                                'images', maintenance_images,
                                'estimated_cost', quote_total_estimate
                            ) 
                        ) AS individual_incidents
                    FROM space.maintenanceRequests 
                    LEFT JOIN space.maintenanceQuotes ON quote_maintenance_request_id = maintenance_request_uid
                    LEFT JOIN space.properties ON maintenance_property_id = property_uid	-- ASSOCIATE PROPERTY DETAILS WITH MAINTENANCE DETAILS
                    LEFT JOIN space.property_owner ON property_id = property_uid 			-- SO WE CAN SORT BY OWNER
                    WHERE property_owner_id = '110-000003'
                    GROUP BY maintenance_request_status;
                    """)

            # print("Query: ", maintenanceQuery)  # This is a list
            # # FOR DEBUG ONLY - THESE STATEMENTS ALLOW YOU TO CHECK THAT THE QUERY WORKS
            response["MaintenanceSummary"] = maintenanceQuery
            return response


class MaintenanceStatus(Resource): 
    def get(self, uid):
        print('in Maintenance Status')
        response = {}

        print("UID: ", uid)

        if uid[:3] == '110':
            print("In Owner ID")

            with connect() as db:
                print("in connect loop")
                maintenanceStatus = db.execute(""" 
                        -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT -- * -- bill_property_id,  maintenance_property_id,
                            maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority
                            , maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                            , maintenance_callback_number, maintenance_estimated_cost, maintenance_pm_notes
                            , maintenance_quote_uid, quote_maintenance_request_id, quote_business_id
                            , quote_services_expenses -- WHERE DOES THIS COME FROM
                            -- DO WE NEED PARTS INCLUDED? quote_parts (JSON Object), quote_parts_estimate ($), 
                            , quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes
                            , quote_status, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                            -- Properties
                            , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                            -- DO WE NEED FINAL INVOICE AMOUNTS OR DOES THAT GO INTO BILLS?
                            , bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                            , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
                            , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
                            , cf_month, cf_year
                            , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip
                            , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date, business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_paypal, business_apple_pay, business_zelle, business_venmo, business_account_number, business_routing_number, business_documents, business_address, business_unit, business_city, business_state, business_zip
                            , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_rent, lease_actual_rent, lt_lease_id, lt_tenant_id, lt_responsibility, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants
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
                return mapMaintenanceStatusForOwner(maintenanceStatus)

            response["MaintenanceStatus"] = maintenanceStatus
            return response
        
        elif uid[:3] == '600':
            print("In Business ID")

            # CHECK IF MAINTENANCE OR MANAGEMENT
            with connect() as db:
                print("in check loop")
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
                    print("in connect loop")
                    maintenanceStatus = db.execute(""" 
                            -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                            SELECT -- * -- bill_property_id,  maintenance_property_id,
                                maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority
                                , maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                                , maintenance_callback_number, maintenance_estimated_cost, maintenance_pm_notes
                                , maintenance_quote_uid, quote_maintenance_request_id, quote_business_id
                                , quote_services_expenses -- WHERE DOES THIS COME FROM
                                -- DO WE NEED PARTS INCLUDED? quote_parts (JSON Object), quote_parts_estimate ($), 
                                , quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes
                                , quote_status, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                                -- Properties
                                , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                                -- DO WE NEED FINAL INVOICE AMOUNTS OR DOES THAT GO INTO BILLS?
                                , bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                                , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
                                , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
                                , cf_month, cf_year
                                , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip
                                , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date, business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_paypal, business_apple_pay, business_zelle, business_venmo, business_account_number, business_routing_number, business_documents, business_address, business_unit, business_city, business_state, business_zip
                                , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_rent, lease_actual_rent, lt_lease_id, lt_tenant_id, lt_responsibility, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants
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
                    return mapMaintenanceStatusForPropertyManager(maintenanceStatus)

                response["MaintenanceStatus"] = maintenanceStatus
                return response
            
            elif businessType["result"][0]["business_type"] == "MAINTENANCE":

                with connect() as db:
                    print("in connect loop")
                    maintenanceStatus = db.execute(""" 
                            -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                            SELECT -- * -- bill_property_id,  maintenance_property_id,
                                maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority
                                , maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                                , maintenance_callback_number, maintenance_estimated_cost, maintenance_pm_notes
                                , maintenance_quote_uid, quote_maintenance_request_id, quote_business_id
                                , quote_services_expenses -- WHERE DOES THIS COME FROM
                                -- DO WE NEED PARTS INCLUDED? quote_parts (JSON Object), quote_parts_estimate ($), 
                                , quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes
                                , quote_status, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                                -- Properties
                                , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                                -- DO WE NEED FINAL INVOICE AMOUNTS OR DOES THAT GO INTO BILLS?
                                , bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                                , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
                                , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
                                , cf_month, cf_year
                                , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip
                                , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date, business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_paypal, business_apple_pay, business_zelle, business_venmo, business_account_number, business_routing_number, business_documents, business_address, business_unit, business_city, business_state, business_zip
                                , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_rent, lease_actual_rent, lt_lease_id, lt_tenant_id, lt_responsibility, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants
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
                    return mapMaintenanceStatusForMaintenancePerson(maintenanceStatus)

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
                            maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority
                            , maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                            , maintenance_callback_number, maintenance_estimated_cost, maintenance_pm_notes
                            , maintenance_quote_uid, quote_maintenance_request_id, quote_business_id
                            , quote_services_expenses -- WHERE DOES THIS COME FROM
                            -- DO WE NEED PARTS INCLUDED? quote_parts (JSON Object), quote_parts_estimate ($), 
                            , quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes
                            , quote_status, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                            -- Properties
                            , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                            -- DO WE NEED FINAL INVOICE AMOUNTS OR DOES THAT GO INTO BILLS?
                            , bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                            , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
                            , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
                            , cf_month, cf_year
                            , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip
                            , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date, business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_paypal, business_apple_pay, business_zelle, business_venmo, business_account_number, business_routing_number, business_documents, business_address, business_unit, business_city, business_state, business_zip
                            , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_rent, lease_actual_rent, lt_lease_id, lt_tenant_id, lt_responsibility, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants
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
                return mapMaintenanceStatusForTenant(maintenanceStatus)

            response["MaintenanceStatus"] = maintenanceStatus
            return response

        elif uid[:3] == '200':
            print("In Property ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceStatus = db.execute(""" 
                        -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT -- * -- bill_property_id,  maintenance_property_id,
                            maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority
                            , maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                            , maintenance_callback_number, maintenance_estimated_cost, maintenance_pm_notes
                            , maintenance_quote_uid, quote_maintenance_request_id, quote_business_id
                            , quote_services_expenses -- WHERE DOES THIS COME FROM
                            -- DO WE NEED PARTS INCLUDED? quote_parts (JSON Object), quote_parts_estimate ($), 
                            , quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes
                            , quote_status, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                            -- Properties
                            , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                            -- DO WE NEED FINAL INVOICE AMOUNTS OR DOES THAT GO INTO BILLS?
                            , bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                            , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
                            , payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by, latest_date, total_paid, payment_status, amt_remaining
                            , cf_month, cf_year
                            , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip
                            , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date, business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_paypal, business_apple_pay, business_zelle, business_venmo, business_account_number, business_routing_number, business_documents, business_address, business_unit, business_city, business_state, business_zip
                            , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_rent, lease_actual_rent, lt_lease_id, lt_tenant_id, lt_responsibility, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants
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
                return mapMaintenanceStatusForTenant(maintenanceStatus)

            response["MaintenanceStatus"] = maintenanceStatus
            return response

        else:
            print("UID Not found")
            response["MaintenanceStatus"] = "UID Not Found"
            return response

   
        

