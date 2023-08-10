# MANIFEST MY SPACE (PROPERTY MANAGEMENT) BACKEND PYTHON FILE
# https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/<enter_endpoint_details>

# To run program:  python3 myspace_api.py

# README:  if conn error make sure password is set properly in RDS PASSWORD section

# README:  Debug Mode may need to be set to False when deploying live (although it seems to be working through Zappa)

# README:  if there are errors, make sure you have all requirements are loaded
# pip3 install -r requirements.txt


# SECTION 1:  IMPORT FILES AND FUNCTIONS

# from bills import Bills, DeleteUtilities
# from dashboard import ownerDashboard

from properties import Properties, PropertiesByOwner
from transactions import AllTransactions, TransactionsByOwner, TransactionsByOwnerByProperty
from cashflow import CashflowByOwner
from profiles import OwnerProfile, TenantProfile
from documents import OwnerDocuments, TenantDocuments
from maintenance import MaintenanceStatusByProperty, MaintenanceByProperty, OwnerMaintenanceByStatus
from data import connect, disconnect, execute, helper_upload_img, helper_icon_img

import os
import boto3
import json
from datetime import datetime as dt
from datetime import timezone as dtz
import time
from datetime import datetime, date, timedelta
from pytz import timezone as ptz  #Not sure what the difference is

import stripe

from flask import Flask, request, render_template, url_for, redirect
from flask_restful import Resource, Api
from flask_cors import CORS
from flask_mail import Mail, Message  # used for email
# from flask_jwt_extended import JWTManager

# used for serializer email and error handling
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadTimeSignature

from werkzeug.exceptions import BadRequest, NotFound
# NEED to figure out where the NotFound or InternalServerError is displayed
# from werkzeug.exceptions import BadRequest, InternalServerError

#  NEED TO SOLVE THIS
# from NotificationHub import Notification
# from NotificationHub import NotificationHub

from decimal import Decimal
from hashlib import sha512


# BING API KEY
# Import Bing API key into bing_api_key.py

#  NEED TO SOLVE THIS
# from env_keys import BING_API_KEY, RDS_PW
# from dotenv import load_dotenv

import sys
import pytz
import pymysql
import requests


# OTHER IMPORTS NOT IN NITYA
from oauth2client import GOOGLE_REVOKE_URI, GOOGLE_TOKEN_URI, client
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from urllib.parse import urlparse
import urllib.request
import base64
from io import BytesIO
from dateutil.relativedelta import relativedelta
import math
from dateutil.relativedelta import *
from math import ceil
import string
import random
import hashlib
import binascii
import csv

# regex
import re
# from env_keys import BING_API_KEY, RDS_PW



# from env_file import RDS_PW, S3_BUCKET, S3_KEY, S3_SECRET_ACCESS_KEY
s3 = boto3.client('s3')


app = Flask(__name__)
CORS(app)
# CORS(app, resources={r'/api/*': {'origins': '*'}})
# Set this to false when deploying to live application
app.config['DEBUG'] = True





# SECTION 2:  UTILITIES AND SUPPORT FUNCTIONS
# EMAIL INFO
#app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_SERVER'] = 'smtp.mydomain.com'
app.config['MAIL_PORT'] = 465

app.config['MAIL_USERNAME'] = 'support@manifestmy.space'
app.config['MAIL_PASSWORD'] = 'Support4MySpace'
app.config['MAIL_DEFAULT_SENDER'] = 'support@manifestmy.space'


app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = True
# app.config['MAIL_DEBUG'] = True
# app.config['MAIL_SUPPRESS_SEND'] = False
# app.config['TESTING'] = False
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg'])
mail = Mail(app)
s = URLSafeTimedSerializer('thisisaverysecretkey')
# API
api = Api(app)


# convert to UTC time zone when testing in local time zone
utc = pytz.utc
# These statment return Day and Time in GMT
# def getToday(): return datetime.strftime(datetime.now(utc), "%Y-%m-%d")
# def getNow(): return datetime.strftime(datetime.now(utc),"%Y-%m-%d %H:%M:%S")

# These statment return Day and Time in Local Time - Not sure about PST vs PDT
def getToday(): return datetime.strftime(datetime.now(), "%Y-%m-%d")
def getNow(): return datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")


# NOTIFICATIONS - NEED TO INCLUDE NOTIFICATION HUB FILE IN SAME DIRECTORY
# from NotificationHub import AzureNotification
# from NotificationHub import AzureNotificationHub
# from NotificationHub import Notification
# from NotificationHub import NotificationHub
# For Push notification
# isDebug = False
# NOTIFICATION_HUB_KEY = os.environ.get('NOTIFICATION_HUB_KEY')
# NOTIFICATION_HUB_NAME = os.environ.get('NOTIFICATION_HUB_NAME')

# Twilio settings
# from twilio.rest import Client

# TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
# TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')











# -- Stored Procedures start here -------------------------------------------------------------------------------


# RUN STORED PROCEDURES

def get_new_billUID(conn):
    newBillQuery = execute("CALL space.new_bill_uid;", "get", conn)
    if newBillQuery["code"] == 280:
        return newBillQuery["result"][0]["new_id"]
    return "Could not generate new bill UID", 500


def get_new_purchaseUID(conn):
    newPurchaseQuery = execute("CALL space.new_purchase_uid;", "get", conn)
    if newPurchaseQuery["code"] == 280:
        return newPurchaseQuery["result"][0]["new_id"]
    return "Could not generate new bill UID", 500

# def get_new_propertyUID(conn):
#     newPropertyQuery = execute("CALL space.new_property_uid;", "get", conn)
#     if newPropertyQuery["code"] == 280:
#         return newPropertyQuery["result"][0]["new_id"]
#     return "Could not generate new property UID", 500




# -- SPACE Queries start here -------------------------------------------------------------------------------

class ownerDashboard(Resource):
    def get(self, owner_id):
        print('in New Owner Dashboard new')
        response = {}
        conn = connect()

        # print("Owner UID: ", owner_id)

        try:
            maintenanceQuery = (""" 
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
            

            # print("Query: ", maintenanceQuery)
            items = execute(maintenanceQuery, "get", conn)
            response["MaintenanceStatus"] = items["result"]


            leaseQuery = (""" 
                    -- LEASE STATUS BY USER
                    SELECT property_owner.property_owner_id
                        , leases.lease_end
                        , COUNT(lease_end) AS num
                    FROM space.properties
                    LEFT JOIN space.property_owner ON property_id = property_uid
                    LEFT JOIN space.leases ON lease_property_id = property_uid
                    WHERE property_owner_id = \'""" + owner_id + """\'
                    GROUP BY MONTH(lease_end), YEAR(lease_end);
                    """)

            # print("Query: ", leaseQuery)
            items = execute(leaseQuery, "get", conn)
            response["LeaseStatus"] = items["result"]


            return response

        except:
            print("Error in Maintenance or Lease Query")
        finally:
            disconnect(conn)






class ownerDashboardProperties(Resource):
    def get(self, owner_id):
        print('in New Owner Maintenance Dashboard')
        response = {}
        conn = connect()

        # print("Owner UID: ", owner_id)


        try:
            maintenanceQuery = (""" 
                    -- PROPERTY DETAILS INCLUDING MAINTENANCE      
                    SELECT property_uid, property_address
                        , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_type, property_num_beds, property_num_baths, property_area, property_listed_rent, property_images
                        , maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                    FROM space.properties
                    LEFT JOIN space.maintenanceRequests ON maintenance_property_id = property_uid		-- SO WE HAVE MAINTENANCE INFO
                    LEFT JOIN space.property_owner ON property_id = property_uid 						-- SO WE CAN SORT BY OWNER
                    WHERE property_owner_id = \'""" + owner_id + """\';
                    """)

            # print("Query: ", maintenanceQuery)
            items = execute(maintenanceQuery, "get", conn)
            # print(type(items), items)  # This is a Dictionary
            # print(type(items["result"]), items["result"])  # This is a list

            property_list = items["result"]

            # Format Output to be a dictionary of lists
            property_dict = {}
            for item in property_list:
                property_id = item['property_uid']
                property_info = {k: v for k, v in item.items() if k != 'property_uid'}
                
                if property_id in property_dict:
                    property_dict[property_id].append(property_info)
                else:
                    property_dict[property_id] = [property_info]

            # Print the resulting dictionary
            # print(property_dict)
            return property_dict
            

        except:
            print("Error in Maintenance Status Query")
        finally:
            disconnect(conn)



class Bills(Resource):
    def post(self):
        print("In add Bill")

        try:
            conn = connect()
            response = {}
            response['message'] = []
            data = request.get_json(force=True)
            print(data)

            #  Get New Bill UID
            new_bill_uid = get_new_billUID(conn)
            print(new_bill_uid)


            # Set Variables from JSON OBJECT
            bill_description = data["bill_description"]
            bill_amount = data["bill_amount"]
            bill_created_by = data["bill_created_by"]
            bill_utility_type = data["bill_utility_type"]
            bill_split = data["bill_split"]
            bill_property_id = data["bill_property_id"]
            print(str(json.dumps(bill_property_id)))
            bill_docs = data["bill_docs"]

            billQuery = (""" 
                    -- CREATE NEW BILL
                    INSERT INTO space.bills
                    SET bill_uid = \'""" + new_bill_uid + """\'
                    , bill_timestamp = CURRENT_TIMESTAMP()
                    , bill_description = \'""" + bill_description + """\'
                    , bill_amount = \'""" + str(bill_amount) + """\'
                    , bill_created_by = \'""" + bill_created_by + """\'
                    , bill_utility_type = \'""" + bill_utility_type + """\'
                    , bill_split = \'""" + bill_split + """\'
                    , bill_property_id = \'""" + json.dumps(bill_property_id, sort_keys=False) + """\'
                    , bill_docs = \'""" + json.dumps(bill_docs, sort_keys=False) + """\';          
                    """)

            # print("Query: ", billQuery)
            response = execute(billQuery, "post", conn) 
            # print("Query out", response["code"])
            response["bill_uid"] = new_bill_uid

            # Works to this point


            try:
                print("made it here", bill_property_id)
                split_num = len(bill_property_id)
                print(split_num)
                split_bill_amount = round(bill_amount/split_num,2)
                for data_dict in bill_property_id:
                    for key, value in data_dict.items():
                        # print(f"{key}: {value}")
                        # print(value)
                        pur_property_id = value
                        print("Input to Find Responsible Party Query:  ", pur_property_id, bill_utility_type)

                        # For each property ID and utility, identify the responsible party

                        responsibleQuery = (""" 
                            -- UTILITY PAYMENT REPOSONSIBILITY BY PROPERTY
                            SELECT responsible_party
                            FROM (
                                SELECT u.*
                                    , list_item AS utility_type
                                    , CASE
                                        WHEN contract_status = "ACTIVE" AND utility_payer = "property manager" THEN contract_business_id
                                        WHEN lease_status = "ACTIVE" AND utility_payer = "tenant" THEN lt_tenant_id
                                        ELSE property_owner_id
                                    END AS responsible_party
                                FROM (
                                    SELECT -- *,
                                        property_uid, property_address, property_unit
                                        , utility_type_id, utility_payer_id
                                        , list_item AS utility_payer
                                        , property_owner_id
                                        , contract_business_id, contract_status, contract_start_date, contract_end_date
                                        , lease_status, lease_start, lease_end
                                        , lt_tenant_id, lt_responsibility 
                                    FROM space.properties
                                    LEFT JOIN space.property_utility ON property_uid = utility_property_id		-- TO FIND WHICH UTILITES TO PAY AND WHO PAYS THEM

                                    LEFT JOIN space.lists ON utility_payer_id = list_uid				-- TO TRANSLATE WHO PAYS UTILITIES TO ENGLISH
                                    LEFT JOIN space.property_owner ON property_uid = property_id		-- TO FIND PROPERTY OWNER
                                    LEFT JOIN space.contracts ON property_uid = contract_property_id    -- TO FIND PROPERTY MANAGER
                                    LEFT JOIN space.leases ON property_uid = lease_property_id			-- TO FIND CONTRACT START AND END DATES
                                    LEFT JOIN space.lease_tenant ON lease_uid = lt_lease_id				-- TO FIND TENANT IDS AND RESPONSIBILITY PERCENTAGES
                                    WHERE contract_status = "ACTIVE"
                                    ) u 

                                LEFT JOIN space.lists ON utility_type_id = list_uid					-- TO TRANSLATE WHICH UTILITY TO ENGLISH
                                ) u_all

                            WHERE property_uid = \'""" + pur_property_id + """\'
                                AND utility_type = \'""" + bill_utility_type + """\';
         
                            """)

                        # print("Query: ", responsibleQuery)
                        queryResponse = execute(responsibleQuery, "get", conn)
                        print("queryResponse is: ", queryResponse)
                        responsibleArray = queryResponse['result'][0]
                        print("Responsible Party is: ", responsibleArray)


                        # THESE STATEMENTS DO THE SAME THING
                        responsibleParty = queryResponse['result'][0]['responsible_party']
                        print("Responsible Party is: ", responsibleParty)
                        responsibleParty = responsibleArray['responsible_party']
                        print("Responsible Party is: ", responsibleParty)

                        # STILL NEED TO ADD A LOOP FOR EACH RESPONSIBLE PARTY   

                        # for data_dict2 in responsibleArray:
                        #     for key, value in data_dict2.items():
                        #         print(f"{key}: {value}")
                        #         print(value)
                
                        # post a Purchase for each property

                        #  Get New Bill UID
                        new_purchase_uid = get_new_purchaseUID(conn)
                        print(new_purchase_uid)

                        # Determine if this is a revenue or expense
                        if responsibleParty[:3] == "350": pur_cf_type = "revenue"
                        else: pur_cf_type = "expense"


                        purchaseQuery = (""" 
                            INSERT INTO space.purchases
                            SET purchase_uid = \'""" + new_purchase_uid + """\'
                                , pur_timestamp = CURRENT_TIMESTAMP()
                                , pur_property_id = \'""" + pur_property_id  + """\'
                                , purchase_type = "BILL POSTING"
                                , pur_cf_type = \'""" + pur_cf_type  + """\'
                                , pur_bill_id = \'""" + new_bill_uid + """\'
                                , purchase_date = CURRENT_DATE()
                                , pur_due_date = DATE_ADD(LAST_DAY(CURRENT_DATE()), INTERVAL 1 DAY)
                                , pur_amount_due = \'""" + str(split_bill_amount) + """\'
                                , purchase_status = "UNPAID"
                                , pur_notes = "THIS IS A TEST"
                                , pur_description = "THIS IS ONLY A TEST"
                                , pur_receiver = "600-000003"
                                , pur_payer = \'""" + responsibleParty + """\'
                                , pur_initiator = \'""" + bill_created_by + """\';
                            """)

                        print("Query: ", purchaseQuery)
                        queryResponse = execute(purchaseQuery, "post", conn)
                        print("queryResponse is: ", queryResponse)
                        # responsibleArray = queryResponse['result'][0]
                        # print("Responsible Party is: ", responsibleArray)


                        # # THESE STATEMENTS DO THE SAME THING
                        # responsibleParty = queryResponse['result'][0]['responsible_party']
                        # print("Responsible Party is: ", responsibleParty)
                        # responsibleParty = responsibleArray['responsible_party']
                        # print("Responsible Party is: ", responsibleParty)



                        continue

            except json.JSONDecodeError:
                print("Invalid JSON format.")


            return response

        except:
            print("Error in Add Bill Query")
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

            #  Get Bill UID
            bill_uid = data["bill_uid"]
            print(bill_uid)

            # Query
            delBillQuery = (""" 
                    -- DELETE BILL
                    DELETE FROM space.bills 
                    WHERE bill_uid = \'""" + bill_uid + """\';         
                    """)

            # print("Query: ", delBillQuery)
            response = execute(delBillQuery, "del", conn) 
            # print("Query out", response["code"])
            response["Deleted bill_uid"] = bill_uid


            return response

        except:
            print("Error in Add Bill Query")
        finally:
            disconnect(conn)







        


#  -- ACTUAL ENDPOINTS    -----------------------------------------

# New APIs, uses connect() and disconnect()
# Create new api template URL
# api.add_resource(TemplateApi, '/api/v2/templateapi')

# Run on below IP address and port
# Make sure port number is unused (i.e. don't use numbers 0-1023)

# GET requests


api.add_resource(ownerDashboard, '/ownerDashboard/<string:owner_id>')
api.add_resource(ownerDashboardProperties, '/ownerDashboardProperties/<string:owner_id>')
api.add_resource(OwnerMaintenanceByStatus, '/ownerMaintenanceByStatus/<string:owner_id>')
api.add_resource(Properties, '/properties')
api.add_resource(PropertiesByOwner, '/propertiesByOwner/<string:owner_id>')
api.add_resource(MaintenanceByProperty, '/maintenanceByProperty/<string:property_id>')
api.add_resource(MaintenanceStatusByProperty, '/maintenanceStatusByProperty/<string:property_id>')
api.add_resource(Bills, '/bills')

api.add_resource(CashflowByOwner, '/cashflowByOwner/<string:owner_id>/<string:year>')

api.add_resource(TransactionsByOwner, '/transactionsByOwner/<string:owner_id>')
api.add_resource(TransactionsByOwnerByProperty, '/transactionsByOwnerByProperty/<string:owner_id>/<string:property_id>')
api.add_resource(AllTransactions, '/allTransactions')


api.add_resource(OwnerProfile, '/ownerProfile/<string:owner_id>')
api.add_resource(TenantProfile, '/tenantProfile/<string:tenant_id>')

api.add_resource(OwnerDocuments, '/ownerDocuments/<string:owner_id>')
api.add_resource(TenantDocuments, '/tenantDocuments/<string:tenant_id>')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000)
