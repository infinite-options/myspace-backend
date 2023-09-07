
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

            # print("Query: ", maintenanceQuery)  # This is a list
            # # FOR DEBUG ONLY - THESE STATEMENTS ALLOW YOU TO CHECK THAT THE QUERY WORKS
            response["MaintenanceProjects"] = maintenanceQuery
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
                        , maintenance_request_created_date, maintenance_request_uid, maintenance_title, maintenance_images, maintenance_request_type, maintenance_request_status
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