
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

from data import connect, disconnect, execute, helper_upload_img, helper_icon_img, uploadImage
from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar




# MAINTENANCE BY STATUS
#                           TENANT      OWNER     PROPERTY MANAGER     
# MAINTENANCE BY STATUS        Y           Y               Y
# BY MONTH    X           X               X
# BY YEAR     X           X               X


def get_new_maintenanceUID(conn):
    print("In new UID request")
    newMaintenanceQuery = execute("CALL space.new_request_uid;", "get", conn)
    if newMaintenanceQuery["code"] == 280:
        return newMaintenanceQuery["result"][0]["new_id"]
    return "Could not generate new property UID", 500


class MaintenanceStatusByProperty(Resource):
    def get(self, property_id):
        print('in New Owner Dashboard new')
        response = {}
        conn = connect()

        print("Property UID: ", property_id)

        try:
            maintenanceQuery = (""" 
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
            items = execute(maintenanceQuery, "get", conn)
            response["MaintenanceProjectStatus"] = items["result"]


            return response

        except:
            print("Error in Maintenance Query")
        finally:
            disconnect(conn)


class MaintenanceByProperty(Resource):
    def get(self, property_id):
        print('in Maintenance By Property')
        response = {}
        conn = connect()

        print("Property UID: ", property_id)

        try:
            maintenanceQuery = (""" 
                    -- MAINTENANCE PROJECTS BY PROPERTY        
                    SELECT -- *
                        property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type, property_num_beds, property_num_baths, property_area
                        , maintenance_request_uid, maintenance_property_id, maintenance_request_status, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                    FROM space.properties
                    LEFT JOIN space.maintenanceRequests ON maintenance_property_id = property_uid
                    WHERE property_uid = \'""" + property_id + """\';
                    """)
            

            # print("Query: ", maintenanceQuery)
            items = execute(maintenanceQuery, "get", conn)
            print("here")
            print(items)
            response["MaintenanceProjects"] = items["result"]


            return response

        except:
            print("Error in Maintenance Query")
        finally:
            disconnect(conn)


# class OwnerMaintenanceByStatus(Resource):
class MaintenanceStatusByOwner(Resource): 
    def get(self, owner_id):
        print('in New Owner Maintenance Dashboard')
        response = {}
        conn = connect()

        # print("Owner UID: ", owner_id)


        try:
            # maintenanceQuery = (""" 
            #         -- MAINTENANCE STATUS BY OWNER BY PROPERTY BY STATUS WITH ALL DETAILS
            #         SELECT property_owner_id
            #             , property_uid, property_available_to_rent, property_active_date, property_address -- , property_unit, property_city, property_state, property_zip, property_type, property_num_beds, property_num_baths, property_area, property_listed_rent, property_images
            #             , maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
            #             , quote_earliest_availability, quote_event_duration, quote_notes, quote_total_estimate
            #         FROM space.maintenanceRequests 
            #         LEFT JOIN space.maintenanceQuotes ON quote_maintenance_request_id = maintenance_request_uid
            #         LEFT JOIN space.properties ON maintenance_property_id = property_uid	-- ASSOCIATE PROPERTY DETAILS WITH MAINTENANCE DETAILS
            #         LEFT JOIN space.property_owner ON property_id = property_uid 			-- SO WE CAN SORT BY OWNER
            #         WHERE property_owner_id = \'""" + owner_id + """\'
            #         ORDER BY maintenance_request_status;
            #         """)


            maintenanceQuery = (""" 
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

            # print("Query: ", maintenanceQuery)
            items = execute(maintenanceQuery, "get", conn)
            # print(type(items), items)  # This is a Dictionary
            # print(type(items["result"]), items["result"])  # This is a list

            property_list = items["result"]

            # Format Output to be a dictionary of lists
            property_dict = {}
            for item in property_list:
                maintenance_status = item['maintenance_request_status']
                # property_id = item['property_uid']
                property_info = {k: v for k, v in item.items() if k != 'maintenance_request_status'}
                
                if maintenance_status in property_dict:
                    property_dict[maintenance_status].append(property_info)
                else:
                    property_dict[maintenance_status] = [property_info]

            # Print the resulting dictionary
            # print(property_dict)
            return property_dict
            

        except:
            print("Error in Maintenance Status Query")
        finally:
            disconnect(conn)



# ADD MAINTENACE ITEM
class MaintenanceRequests_works(Resource):
    def post(self):

        try:
            print("In Maintenance Requests POST")
            conn = connect()
            response = {}
            response['message'] = []
            data = request.form
            print("Imported data; ", data)


            # GET NEW UID
            new_maintenance_request_uid = get_new_maintenanceUID(conn)
            # newRequestID = db.call('new_request_id')['result'][0]['new_id']
            print("New ID: ", new_maintenance_request_uid)


            # ASSIGN VALUE
            maintenance_property_id = data["maintenance_property_id"]
            maintenance_title = data["maintenance_title"]
            maintenance_desc = data["maintenance_desc"]
            print(maintenance_desc)
            maintenance_images = data["maintenance_images"]
            print(maintenance_images)
            maintenance_request_type = data["maintenance_request_type"]
            maintenance_request_created_by = data["maintenance_request_created_by"]
            maintenance_priority = data["maintenance_priority"]
            maintenance_can_reschedule = data["maintenance_can_reschedule"]
            print(maintenance_can_reschedule)
            maintenance_assigned_business = data["maintenance_assigned_business"]
            maintenance_assigned_worker = data["maintenance_assigned_worker"]
            maintenance_scheduled_date = data["maintenance_scheduled_date"]
            maintenance_scheduled_time = data["maintenance_scheduled_time"]
            maintenance_frequency = data["maintenance_frequency"]
            print(maintenance_frequency)
            maintenance_notes = data["maintenance_notes"]
            maintenance_request_status = data["maintenance_request_status"]
            maintenance_request_created_date = data["maintenance_request_created_date"]
            maintenance_request_closed_date = data["maintenance_request_closed_date"]
            maintenance_request_adjustment_date = data["maintenance_request_adjustment_date"]
            print(maintenance_request_created_date)


            
            # DEFINE QUERY  
            maintenanceQuery = (""" 
                    -- ADD NEW MAINTENACE REQUEST
                    INSERT INTO space.maintenanceRequests
                    SET maintenance_request_uid = "800-000039"
                    ,  maintenance_property_id = "200-000006"
                    ,  maintenance_title = "SQL TEST"
                    ,  maintenance_desc = "Testing the SQL query"
                    ,  maintenance_request_type = "Other"
                    ,  maintenance_request_created_by = "600-000003"
                    ,  maintenance_priority = "High"
                    ,  maintenance_can_reschedule = "1"
                    ,  maintenance_assigned_business = NULL
                    ,  maintenance_assigned_worker = NULL
                    ,  maintenance_scheduled_date = "800-000039"
                    ,  maintenance_scheduled_time = NULL
                    ,  maintenance_frequency = "One Time"
                    ,  maintenance_notes = "Take Notes"
                    ,  maintenance_request_status = "NEW"
                    ,  maintenance_request_created_date = "2023-08-01"
                    ,  maintenance_request_closed_date = NULL
                    ,  maintenance_request_adjustment_date = NULL;
                    """)
            
            print("Query: ", maintenanceQuery)

            # RUN QUERY
            response = execute(maintenanceQuery, "post", conn) 
            print("Query out", response["code"])
            response["maintenance_request_uid"] = new_maintenance_request_uid
            return response

        except:
            print("Error in Add Maintenance Request Query")
        finally:
            disconnect(conn)

# ADD MAINTENACE ITEM
# class MaintenanceRequests(Resource):
#     def post(self):

#         try:
#             print("In Maintenance Requests POST")
#             conn = connect()
#             response = {}
#             response['message'] = []
#             data = request.form
#             print("Imported data; ", data)
#             fields = ['property_uid', 'title', 'description',
#                       'priority', 'request_created_by', 'request_type']


#             # GET NEW UID
#             new_maintenance_request_uid = get_new_maintenanceUID(conn)
#             # newRequestID = db.call('new_request_id')['result'][0]['new_id']
#             print("New ID: ", new_maintenance_request_uid)


#             fields = [
#                 'maintenance_property_id'
#                 , 'maintenance_title'
#                 , 'maintenance_desc'
#                 , 'maintenance_images'
#                 , 'maintenance_request_type'
#                 , 'maintenance_request_created_by'
#                 , 'maintenance_priority'
#                 , 'maintenance_can_reschedule'
#                 , 'maintenance_assigned_business'
#                 , 'maintenance_assigned_worker'
#                 , 'maintenance_scheduled_date'
#                 , 'maintenance_scheduled_time'
#                 , 'maintenance_frequency'
#                 , 'maintenance_notes'

#                 , 'maintenance_request_created_date'
#                 , 'maintenance_request_closed_date'
#                 , 'maintenance_request_adjustment_date'
#             ]
#             newRequest = {}
#             newRequest['maintenance_request_status'] = 'NEW'
#             for field in fields:
#                 newRequest[field] = data[field]
#                 print(field, " = ", newRequest[field])


#             # ASSIGN VALUE
#             maintenance_property_id = data["maintenance_property_id"]
#             maintenance_title = data["maintenance_title"]
#             maintenance_desc = data["maintenance_desc"]
#             print(maintenance_desc)
#             maintenance_images = data["maintenance_images"]
#             print(maintenance_images)
#             maintenance_request_type = data["maintenance_request_type"]
#             maintenance_request_created_by = data["maintenance_request_created_by"]
#             maintenance_priority = data["maintenance_priority"]
#             maintenance_can_reschedule = data["maintenance_can_reschedule"]
#             print(maintenance_can_reschedule)
#             maintenance_assigned_business = data["maintenance_assigned_business"]
#             maintenance_assigned_worker = data["maintenance_assigned_worker"]
#             maintenance_scheduled_date = data["maintenance_scheduled_date"]
#             maintenance_scheduled_time = data["maintenance_scheduled_time"]
#             maintenance_frequency = data["maintenance_frequency"]
#             print(maintenance_frequency)
#             maintenance_notes = data["maintenance_notes"]
#             # maintenance_request_status = data["maintenance_request_status"]
#             maintenance_request_created_date = data["maintenance_request_created_date"]
#             maintenance_request_closed_date = data["maintenance_request_closed_date"]
#             maintenance_request_adjustment_date = data["maintenance_request_adjustment_date"]
#             print(maintenance_request_created_date)






            
#             # DEFINE QUERY  
#             maintenanceQuery = (""" 
#                     -- ADD NEW MAINTENACE REQUEST
#                     INSERT INTO space.maintenanceRequests
#                     SET maintenance_request_uid = \'""" + new_maintenance_request_uid + """\'
#                     ,  maintenance_property_id = "200-000006"
#                     ,  maintenance_title = "SQL TEST"
#                     ,  maintenance_desc = "Testing the SQL query"
#                     ,  maintenance_request_type = "Other"
#                     ,  maintenance_request_created_by = "600-000003"
#                     ,  maintenance_priority = "High"
#                     ,  maintenance_can_reschedule = "1"
#                     ,  maintenance_assigned_business = NULL
#                     ,  maintenance_assigned_worker = NULL
#                     ,  maintenance_scheduled_date = "800-000039"
#                     ,  maintenance_scheduled_time = NULL
#                     ,  maintenance_frequency = "One Time"
#                     ,  maintenance_notes = "Take Notes"
#                     ,  maintenance_request_status = "NEW"
#                     ,  maintenance_request_created_date = "2023-08-01"
#                     ,  maintenance_request_closed_date = NULL
#                     ,  maintenance_request_adjustment_date = NULL;
#                     """)
            
#             print("Query: ", maintenanceQuery)

#             # RUN QUERY
#             response = execute(maintenanceQuery, "post", conn) 
#             print("Query out", response["code"])
#             response["maintenance_request_uid"] = new_maintenance_request_uid
#             return response

#         except:
#             print("Error in Add Maintenance Request Query")
#         finally:
#             disconnect(conn)


class MaintenanceRequests(Resource):
    def post(self):
        response = {}
        with connect() as db:
            data = request.form
            # fields = ['property_uid', 'title', 'description',
            #           'priority', 'request_created_by', 'request_type']

            fields = [
                'maintenance_property_id'
                , 'maintenance_title'
                , 'maintenance_desc'
                , 'maintenance_images'
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
            ]

            newRequest = {}
            for field in fields:
                newRequest[field] = data.get(field)
                print(field, " = ", newRequest[field])


            # # GET NEW UID
            # new_maintenance_request_uid = get_new_maintenanceUID(conn)
            # # newRequestID = db.call('new_request_id')['result'][0]['new_id']
            # print("New ID: ", new_maintenance_request_uid)

            print("Get New Request UID")
            newRequestID = db.call('new_request_id')['result'][0]['new_id']
            newRequest['maintenance_request_uid'] = newRequestID
            print(newRequestID)

            images = []
            i = -1
            # WHILE WHAT IS TRUE?
            while True:
                print("In while loop")
                filename = f'img_{i}'
                print("Filename: ", filename)
                if i == -1:
                    filename = 'img_cover'
                file = request.files.get(filename)
                print("File: ", file)
                if file:

                    key = f'maintenanceRequests/{newRequestID}/{filename}'
                    image = uploadImage(file, key, '')
                    images.append(image)
                else:
                    break
                i += 1
            newRequest['maintenance_images'] = json.dumps(images)
            print("Images to be uploaded: ", newRequest['maintenance_images'])

            newRequest['maintenance_request_status'] = 'NEW'
            # newRequest['frequency'] = 'One time'
            # newRequest['can_reschedule'] = False

            print(newRequest)

            response = db.insert('maintenanceRequests', newRequest)
        return response
