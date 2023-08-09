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





# SECTION 3: DATABASE FUNCTIONALITY
# RDS for AWS SQL 5.7
# RDS_HOST = 'pm-mysqldb.cxjnrciilyjq.us-west-1.rds.amazonaws.com'
# RDS for AWS SQL 8.0
RDS_HOST = 'io-mysqldb8.cxjnrciilyjq.us-west-1.rds.amazonaws.com'
RDS_PORT = 3306
RDS_USER = 'admin'
RDS_DB = 'space'
RDS_PW="prashant"   # Not sure if I need this
# RDS_PW = os.environ.get('RDS_PW')
S3_BUCKET = "manifest-image-db"
# S3_BUCKET = os.environ.get('S3_BUCKET')
# S3_KEY = os.environ.get('S3_KEY')
# S3_SECRET_ACCESS_KEY = os.environ.get('S3_SECRET_ACCESS_KEY')


# CONNECT AND DISCONNECT TO MYSQL DATABASE ON AWS RDS (API v2)
# Connect to MySQL database (API v2)
def connect():
    global RDS_PW
    global RDS_HOST
    global RDS_PORT
    global RDS_USER
    global RDS_DB

    # print("Trying to connect to RDS (API v2)...")
    try:
        conn = pymysql.connect(host=RDS_HOST,
                               user=RDS_USER,
                               port=RDS_PORT,
                               passwd=RDS_PW,
                               db=RDS_DB,
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)
        # print("Successfully connected to RDS. (API v2)")
        return conn
    except:
        print("Could not connect to RDS. (API v2)")
        raise Exception("RDS Connection failed. (API v2)")

# Disconnect from MySQL database (API v2)
def disconnect(conn):
    try:
        conn.close()
        # print("Successfully disconnected from MySQL database. (API v2)")
    except:
        print("Could not properly disconnect from MySQL database. (API v2)")
        raise Exception("Failure disconnecting from MySQL database. (API v2)")

# Execute an SQL command (API v2)
# Set cmd parameter to 'get' or 'post'
# Set conn parameter to connection object
# OPTIONAL: Set skipSerialization to True to skip default JSON response serialization
def execute(sql, cmd, conn, skipSerialization=False):
    response = {}
    # print("==> Execute Query: ", cmd,sql)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cmd == 'get':
                print("in get")
                result = cur.fetchall()
                response['message'] = 'Successfully executed SQL query.'
                # Return status code of 280 for successful GET request
                response['code'] = 280
                if not skipSerialization:
                    print("Serialized Response")
                    result = serializeResponse(result)
                print("Not Serialized Response")
                response['result'] = result
            elif cmd == 'post':
                print("in post")
                conn.commit()
                response['message'] = 'Successfully committed SQL command.'
                # Return status code of 281 for successful POST request
                response['code'] = 281
            elif cmd == 'del':
                print("in del")
                conn.commit()
                response['message'] = 'Successfully committed SQL command.'
                # Return status code of 281 for successful POST request
                response['code'] = 281
            else:
                response['message'] = 'Request failed. Unknown or ambiguous instruction given for MySQL command.'
                # Return status code of 480 for unknown HTTP method
                response['code'] = 480
    except:
        response['message'] = 'Request failed, could not execute MySQL command.'
        # Return status code of 490 for unsuccessful HTTP request
        response['code'] = 490
    finally:
        # response['sql'] = sql
        return response

# Serialize JSON
def serializeResponse(response):
    try:
        for row in response:
            for key in row:
                if type(row[key]) is Decimal:
                    row[key] = float(row[key])
                elif (type(row[key]) is date or type(row[key]) is datetime) and row[key] is not None:
                # Change this back when finished testing to get only date
                    row[key] = row[key].strftime("%Y-%m-%d")
                    # row[key] = row[key].strftime("%Y-%m-%d %H-%M-%S")
                # elif is_json(row[key]):
                #     row[key] = json.loads(row[key])
                elif isinstance(row[key], bytes):
                    row[key] = row[key].decode()
        return response
    except:
        raise Exception("Bad query JSON")


# RUN STORED PROCEDURES

        # MOVE STORED PROCEDURES HERE


# Function to upload image to s3
def allowed_file(filename):
    # Checks if the file is allowed to upload
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def helper_upload_img(file):
    bucket = S3_BUCKET
    # creating key for image name
    salt = os.urandom(8)
    dk = hashlib.pbkdf2_hmac('sha256',  (file.filename).encode(
        'utf-8'), salt, 100000, dklen=64)
    key = (salt + dk).hex()

    if file and allowed_file(file.filename):

        # image link
        filename = 'https://s3-us-west-1.amazonaws.com/' \
                   + str(bucket) + '/' + str(key)

        # uploading image to s3 bucket
        upload_file = s3.put_object(
            Bucket=bucket,
            Body=file,
            Key=key,
            ACL='public-read',
            ContentType='image/jpeg'
        )
        return filename
    return None

# Function to upload icons
def helper_icon_img(url):

    bucket = S3_BUCKET
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        raw_data = response.content
        url_parser = urlparse(url)
        file_name = os.path.basename(url_parser.path)
        key = 'image' + "/" + file_name

        try:

            with open(file_name, 'wb') as new_file:
                new_file.write(raw_data)

            # Open the server file as read mode and upload in AWS S3 Bucket.
            data = open(file_name, 'rb')
            upload_file = s3.put_object(
                Bucket=bucket,
                Body=data,
                Key=key,
                ACL='public-read',
                ContentType='image/jpeg')
            data.close()

            file_url = 'https://%s/%s/%s' % (
                's3-us-west-1.amazonaws.com', bucket, key)

        except Exception as e:
            print("Error in file upload %s." % (str(e)))

        finally:
            new_file.close()
            os.remove(file_name)
    else:
        print("Cannot parse url")

    return file_url





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

def get_new_propertyUID(conn):
    newPropertyQuery = execute("CALL space.new_property_uid;", "get", conn)
    if newPropertyQuery["code"] == 280:
        return newPropertyQuery["result"][0]["new_id"]
    return "Could not generate new property UID", 500




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

class Properties(Resource):
    def get(self):
        print('in Properties')
        response = {}
        conn = connect()

        try:
            propertiesQuery = (""" 
                    -- PROPERTIES BY OWNER
                    SELECT property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_type
                        , property_num_beds, property_num_baths, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images
                        , property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes
                        , property_owner.property_owner_id
                        , ownerProfileInfo.*
                    FROM space.properties
                    LEFT JOIN space.property_owner ON property_id = property_uid
                    LEFT JOIN space.ownerProfileInfo ON property_owner_id = owner_uid;
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

class PropertiesByOwner(Resource):
    def get(self, owner_id):
        print('in Properties by Owner')
        response = {}
        conn = connect()

        print("Property Owner UID: ", owner_id)

        try:
            propertiesQuery = (""" 
                    -- PROPERTIES BY OWNER
                    SELECT property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_type
                        , property_num_beds, property_num_baths, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images
                        , property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes
                        , property_owner.property_owner_id
                        , ownerProfileInfo.*
                    FROM space.properties
                    LEFT JOIN space.property_owner ON property_id = property_uid
                    LEFT JOIN space.ownerProfileInfo ON property_owner_id = owner_uid
                    WHERE property_owner_id = \'""" + owner_id + """\';
                    """)
            

            # print("Query: ", propertiesQuery)
            items = execute(propertiesQuery, "get", conn)
            response["Property"] = items["result"]


            return response

        except:
            print("Error in Property Query")
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

class ownerDashboardMaintenanceByStatus(Resource):
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
                    FROM space.maintenanceRequests
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

class CashFlow(Resource):
    def get(self):
        print('in CashFlow')
        response = {}
        conn = connect()

        try:
            cashFlowQuery = (""" 
                    -- CASHFLOW BY PROPERTY BY MONTH.  CONSIDER ADDING pur_type WHEN THE PURCHASE IS LOGGED
                    SELECT -- *
                        property_uid, property_address, property_unit, pur_type, pur_due_date
                        , ROUND(SUM(pur_amount_due),2) AS cf
                    FROM (
                        SELECT -- *
                            property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
                            -- , SUM(pur_amount_due) AS purchase_amount_due
                            , purchases.*
                            , IF (purchase_type LIKE "RENT" OR  purchase_type LIKE "DEPOSIT" OR  purchase_type LIKE "LATE FEE" OR  purchase_type LIKE "UTILITY" OR  purchase_type LIKE "EXTRA CHARGES", 1 , 0) AS cf_revenue
                            , CASE
                                WHEN (purchase_type LIKE "RENT" OR  purchase_type LIKE "LATE FEE" OR  purchase_type LIKE "UTILITY" OR  purchase_type LIKE "EXTRA CHARGES") THEN "revenue"
                                WHEN (purchase_type LIKE "OWNER PAYMENT RENT" OR  purchase_type LIKE "OWNER PAYMENT LATE FEE" OR  purchase_type LIKE "OWNER PAYMENT EXTRA CHARGES" OR  purchase_type LIKE "MAINTENANCE") THEN "expense"
                                WHEN (purchase_type LIKE "DEPOSIT") THEN "deposit"
                                ELSE "other"
                            END AS pur_type
                        FROM space.properties
                        LEFT JOIN space.purchases ON pur_property_id = property_uid
                        -- WHERE property_uid = "200-000001"
                        -- WHERE MONTH(pur_due_date) = 4 AND YEAR(pur_due_date) = 2023
                        GROUP BY MONTH(pur_due_date),
                                YEAR(pur_due_date),
                                purchase_type,
                                property_uid
                            ) a
                    GROUP BY MONTH(pur_due_date),
                                YEAR(pur_due_date),
                                pur_type,
                            property_uid;
                    """)
            

            # print("Query: ", cashFlowQuery)
            items = execute(cashFlowQuery, "get", conn)
            response["Property"] = items["result"]


            return response

        except:
            print("Error in Cash Flow Query")
        finally:
            disconnect(conn)



class TransactionsByOwner(Resource):
    def get(self, owner_id):
        print('in Transactions By Owner')
        response = {}
        conn = connect()

        print("Property Owner UID: ", owner_id)

        try:
            transactionQuery = (""" 
                    -- ALL TRANSACTIONS
                    SELECT -- *
                        property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_type
                        , property_listed_rent, property_deposit, property_images, property_taxes, property_mortgages, property_insurance, property_description, property_notes
                        , property_owner_id, po_owner_percent
                        , purchase_uid, pur_timestamp, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
                    FROM space.properties
                    LEFT JOIN space.property_owner ON property_id = property_uid 			-- SO WE CAN SORT BY OWNER
                    LEFT JOIN space.purchases ON pur_property_id = property_uid
                    WHERE property_owner_id = \'""" + owner_id + """\';
                    """)
            

            # print("Query: ", TransactionQuery)
            items = execute(transactionQuery, "get", conn)
            response["Transactions"] = items["result"]


            return response

        except:
            print("Error in Transaction Query")
        finally:
            disconnect(conn)

class TransactionsByOwnerByProperty(Resource):
    def get(self, owner_id, property_id):
        print('in Transactions By Owner')
        response = {}
        conn = connect()

        print("Property Owner UID: ", owner_id)

        try:
            transactionQuery = (""" 
                    -- TRANSACTIONS BY OWNER BY PROPERTY
                    SELECT -- *
                        property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_type
                        , property_listed_rent, property_deposit, property_images, property_taxes, property_mortgages, property_insurance, property_description, property_notes
                        , property_owner_id, po_owner_percent
                        , purchase_uid, pur_timestamp, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
                    FROM space.properties
                    LEFT JOIN space.property_owner ON property_id = property_uid 			-- SO WE CAN SORT BY OWNER
                    LEFT JOIN space.purchases ON pur_property_id = property_uid
                    WHERE property_owner_id = \'""" + owner_id + """\'
                        AND property_uid = \'""" + property_id + """\';
                    """)
            

            # print("Query: ", TransactionQuery)
            items = execute(transactionQuery, "get", conn)
            response["Transactions"] = items["result"]


            return response

        except:
            print("Error in Transaction Query")
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
api.add_resource(ownerDashboardMaintenanceByStatus, '/ownerDashboardMaintenanceByStatus/<string:owner_id>')
api.add_resource(Properties, '/properties')
api.add_resource(PropertiesByOwner, '/propertiesByOwner/<string:owner_id>')
api.add_resource(MaintenanceByProperty, '/maintenanceByProperty/<string:property_id>')
api.add_resource(MaintenanceStatusByProperty, '/maintenanceStatusByProperty/<string:property_id>')
api.add_resource(Bills, '/bills')
api.add_resource(CashFlow, '/cashFlow')
api.add_resource(TransactionsByOwner, '/transactionsByOwner/<string:owner_id>')
api.add_resource(TransactionsByOwnerByProperty, '/transactionsByOwnerByProperty/<string:owner_id>/<string:property_id>')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000)
