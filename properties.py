
# from myspace_api import get_new_propertyUID

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


def get_new_propertyUID(conn):
    newPropertyQuery = execute("CALL space.new_property_uid;", "get", conn)
    if newPropertyQuery["code"] == 280:
        return newPropertyQuery["result"][0]["new_id"]
    return "Could not generate new property UID", 500


class PropertiesByOwner(Resource):
    def get(self, owner_id):
        print('in Properties by Owner')
        response = {}
        conn = connect()

        print("Property Owner UID: ", owner_id)

        try:
            propertiesQuery = (""" 
                    -- PROPERTIES BY OWNER
                    SELECT * FROM space.prop_details
                    WHERE  contract_status = 'ACTIVE'
                        AND owner_uid = \'""" + owner_id + """\';
                    """)
            

            # print("Query: ", propertiesQuery)
            items = execute(propertiesQuery, "get", conn)
            response["Property"] = items["result"]


            return response

        except:
            print("Error in Property Query")
        finally:
            disconnect(conn)


# 1. rent due date
# 2. lease expiring data
# 3. link to lease
# 4. current tenant
# 5. current tenant move-in date
# 6. current property manager
# 7. property value
# 8. rent payment status

class Properties(Resource):
    def get(self):
        print('in Properties')
        response = {}
        conn = connect()

        try:
            propertiesQuery = (""" 
                    -- PROPERTIES
                    SELECT * FROM space.prop_details
                    WHERE contract_status = 'ACTIVE'
                    """)
            

            # print("Query: ", propertiesQuery)
            items = execute(propertiesQuery, "get", conn)
            response["Property"] = items["result"]


            return response

        except:
            print("Error in Property Query")
        finally:
            disconnect(conn)
    
    def post(self):
        print("In add Property")

        try:
            conn = connect()
            response = {}
            response['message'] = []
            data = request.get_json(force=True)
            print(data)

            #  Get New Bill UID
            new_property_uid = get_new_propertyUID(conn)
            print(new_property_uid)


            # Set Variables from JSON OBJECT
            property_owner_id = data["property_owner_id"]
            po_owner_percent = data["po_owner_percent"]
            print(po_owner_percent)
            property_available_to_rent = data["available_to_rent"]
            property_active_date = data["active_date"]
            property_address = data["address"]
            property_unit = data["unit"]
            property_city = data["city"]
            property_state = data["state"]
            property_zip = data["zip"]
            print(property_zip)
            property_type = data["property_type"]
            property_num_beds = data["bedrooms"]
            property_num_baths = data["bathrooms"]
            property_area = data["property_area"]
            property_listed_rent = data["listed"]
            property_deposit = data["deposit"]
            print(property_deposit)
            property_pets_allowed = data["pets_allowed"]
            property_deposit_for_rent = data["deposit_for_rent"]
            property_images = data["property_images"]
            print(property_images)
            property_taxes = data["property_taxes"]
            property_mortgages = data["property_mortgages"]
            property_insurance = data["property_insurance"]
            property_featured = data["property_featured"]
            property_description = data["property_description"]
            property_notes = data["property_notes"]
            print(property_notes)


            propertyQuery = (""" 
                    -- ADDS RELATIONSHIP BETWEEN PROPERTY AND OWNER
                    INSERT INTO space.property_owner
                    SET property_id = \'""" + new_property_uid + """\'
                        , property_owner_id = \'""" + property_owner_id + """\'
                        , po_owner_percent = \'""" + str(po_owner_percent) + """\';
                    """)
            
            # print("Query: ", propertyQuery)
            response = execute(propertyQuery, "post", conn) 
            # print("Query out", response["code"])
            # response["property_uid"] = new_property_uid

            propertyQuery = (""" 
                    -- ADDS NEW PROPERTY DETAILS 
                    INSERT INTO space.properties
                    SET property_uid = \'""" + new_property_uid + """\'
                        , property_available_to_rent =  \'""" + property_available_to_rent + """\'
                        , property_active_date = \'""" + property_active_date + """\'
                        , property_address = \'""" + property_address + """\'
                        , property_unit = \'""" + property_unit + """\'
                        , property_city = \'""" + property_city + """\'
                        , property_state = \'""" + property_state + """\'
                        , property_zip = \'""" + property_zip + """\'
                        , property_type = \'""" + property_type + """\'
                        , property_num_beds = \'""" + str(property_num_beds) + """\'
                        , property_num_baths = \'""" + str(property_num_baths) + """\'
                        , property_area = \'""" + str(property_area) + """\'
                        , property_listed_rent = \'""" + str(property_listed_rent) + """\'
                        , property_deposit = \'""" + str(property_deposit) + """\'
                        , property_pets_allowed = \'""" + str(property_pets_allowed) + """\'
                        , property_deposit_for_rent = \'""" + str(property_deposit_for_rent) + """\'
                        , property_images = "[]"
                        , property_taxes = \'""" + str(property_taxes)  + """\'
                        , property_mortgages = \'""" + str(property_mortgages) + """\'
                        , property_insurance = \'""" + str(property_insurance) + """\'
                        , property_featured = \'""" + str(property_featured) + """\'
                        , property_description = \'""" + property_description + """\'
                        , property_notes = \'""" + property_notes + """\';
                    """)
            
            # print("Query: ", propertyQuery)
            response = execute(propertyQuery, "post", conn) 
            # print("Query out", response["code"])
            response["property_uid"] = new_property_uid

            return response

        except:
            print("Error in Add Property Query")
        finally:
            disconnect(conn)

    def delete(self):
        print("In delete Bill")

        try:
            conn = connect()
            response = {}
            response['message'] = []
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

            print("Query: ", delPropertyQuery)
            response = execute(delPropertyQuery, "del", conn) 
            print("Query out", response["code"])
            response["Deleted property_uid"] = property_id


            delPropertyQuery = (""" 
                    -- DELETE PROPERTY FROM PROPERTIES TABLE
                    DELETE FROM space.properties 
                    WHERE property_uid = \'""" + property_id + """\';         
                    """)

            print("Query: ", delPropertyQuery)
            response = execute(delPropertyQuery, "del", conn) 
            print("Query out", response["code"])
            response["Deleted bill_uid"] = property_id


            return response

        except:
            print("Error in Delete Property Query")
        finally:
            disconnect(conn)

