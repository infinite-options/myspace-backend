
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
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


class OwnerMaintenanceByStatus(Resource):
    def get(self, owner_id):
        print('in New Owner Maintenance Dashboard')
        response = {}
        conn = connect()

        # print("Owner UID: ", owner_id)


        try:
            maintenanceQuery = (""" 
                    -- MAINTENANCE STATUS BY OWNER BY PROPERTY BY STATUS WITH ALL DETAILS
                    SELECT property_owner_id
                        , property_uid, property_available_to_rent, property_active_date, property_address -- , property_unit, property_city, property_state, property_zip, property_type, property_num_beds, property_num_baths, property_area, property_listed_rent, property_images
                        , maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                        , quote_earliest_availability, quote_event_duration, quote_notes, quote_total_estimate
                    FROM space.maintenanceRequests 
                    LEFT JOIN space.maintenanceQuotes ON quote_maintenance_request_id = maintenance_request_uid
                    LEFT JOIN space.properties ON maintenance_property_id = property_uid	-- ASSOCIATE PROPERTY DETAILS WITH MAINTENANCE DETAILS
                    LEFT JOIN space.property_owner ON property_id = property_uid 			-- SO WE CAN SORT BY OWNER
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

