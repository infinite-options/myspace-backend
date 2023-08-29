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

from rents import Rents, RentDetails
from payments import Payments
from properties import Properties, PropertiesByOwner
from transactions import AllTransactions, TransactionsByOwner, TransactionsByOwnerByProperty
from cashflow import CashflowByOwner
from profiles import OwnerProfile, TenantProfile
from documents import OwnerDocuments, TenantDocuments
from leases import LeaseDetails
from purchases import Bills
from maintenance import MaintenanceStatusByProperty, MaintenanceByProperty, MaintenanceStatusByOwner, MaintenanceRequestsByOwner, MaintenanceRequests
from contacts import ContactsMaintenance, ContactsBusinessContacts, ContactsOwnerContactsDetails
from settings import BankAccount
# from refresh import Refresh
# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, uploadImage, s3

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
# from google_auth_oauthlib.flow import InstalledAppFlow
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

# def get_new_billUID(conn):
#     newBillQuery = execute("CALL space.new_bill_uid;", "get", conn)
#     if newBillQuery["code"] == 280:
#         return newBillQuery["result"][0]["new_id"]
#     return "Could not generate new bill UID", 500


# def get_new_purchaseUID(conn):
#     newPurchaseQuery = execute("CALL space.new_purchase_uid;", "get", conn)
#     if newPurchaseQuery["code"] == 280:
#         return newPurchaseQuery["result"][0]["new_id"]
#     return "Could not generate new bill UID", 500

# def get_new_propertyUID(conn):
#     newPropertyQuery = execute("CALL space.new_property_uid;", "get", conn)
#     if newPropertyQuery["code"] == 280:
#         return newPropertyQuery["result"][0]["new_id"]
#     return "Could not generate new property UID", 500




# -- SPACE Queries start here -------------------------------------------------------------------------------

class ownerDashboard(Resource):
    def get(self, owner_id):
        print('in Owner Dashboard')
        response = {}

        # print("Owner UID: ", owner_id)

        with connect() as db:
            print("in owner dashboard")
            maintenanceQuery = db.execute(""" 
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
            response["MaintenanceStatus"] = maintenanceQuery


            leaseQuery = db.execute(""" 
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
            response["LeaseStatus"] = leaseQuery


            rentQuery = db.execute(""" 
                    -- RENT STATUS BY PROPERTY FOR OWNER DASHBOARD
                    SELECT -- *,
                        property_owner_id
                        , rent_status
                        , COUNT(rent_status) AS num
                    FROM (
                        SELECT property_id, property_owner_id, po_owner_percent
                            , property_address, property_unit, property_city, property_state, property_zip
                            , pp_status.*
                            , IF (ISNULL(payment_status), "VACANT", payment_status) AS rent_status
                        FROM space.property_owner
                        LEFT JOIN space.properties ON property_uid = property_id
                        LEFT JOIN space.pp_status ON pur_property_id = property_id
                        WHERE property_owner_id = \'""" + owner_id + """\'
                            AND (purchase_type = "RENT" OR ISNULL(purchase_type))
                            AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                            AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                        GROUP BY property_id
                        ) AS rs
                    GROUP BY rent_status
                    """)

            # print("Query: ", leaseQuery)
            response["RentStatus"] = rentQuery


            return response






class ownerDashboardProperties(Resource):
    def get(self, owner_id):
        print('in Owner Dashboard Properties')
        response = {}

        # print("Owner UID: ", owner_id)


        with connect() as db:
            print("in owner dashboard properties")
            property_list = db.execute(""" 
                    -- PROPERTY DETAILS INCLUDING MAINTENANCE      
                    SELECT property_uid, property_address
                        , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_type, property_num_beds, property_num_baths, property_area, property_listed_rent, property_images
                        , maintenance_request_uid, maintenance_property_id, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_status, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date
                    FROM space.properties
                    LEFT JOIN space.maintenanceRequests ON maintenance_property_id = property_uid		-- SO WE HAVE MAINTENANCE INFO
                    LEFT JOIN space.property_owner ON property_id = property_uid 						-- SO WE CAN SORT BY OWNER
                    WHERE property_owner_id = \'""" + owner_id + """\';
                    """)

            # print("Query: ", maintenanceQuery, type(maintenanceQuery))
            # items = execute(maintenanceQuery, "get", conn)
            # print(type(items), items)  # This is a Dictionary
            # print(type(items["result"]), items["result"])  # This is a list

            # property_list = items["result"]

            print(type(property_list))
            print(type(property_list["result"]))

            # Format Output to be a dictionary of lists
            property_dict = {}
            for item in property_list["result"]:
                property_id = item['property_uid']
                property_info = {k: v for k, v in item.items() if k != 'property_uid'}
                
                if property_id in property_dict:
                    property_dict[property_id].append(property_info)
                else:
                    property_dict[property_id] = [property_info]

            # Print the resulting dictionary
            # print(property_dict)
            return property_dict



class TenantDashboard(Resource):
    def get(self, tenant_id):
        response = {}
        with connect() as db:
            property = db.execute("""
                    SELECT p.property_uid, p.property_address, p.property_unit
                    FROM space.properties p
                        INNER JOIN space.leases l ON l.lease_property_id = p.property_uid
                        INNER JOIN space.lease_tenant lt ON lt.lt_lease_id = l.lease_uid
                    WHERE lt.lt_tenant_id = \'""" + tenant_id + """\';
                    """)
            response["property"] = property
            maintenance = db.execute("""
                    SELECT mr.maintenance_images, mr.maintenance_title,
                        mr.maintenance_request_status, mr.maintenance_priority,
                        mr.maintenance_scheduled_date, mr.maintenance_scheduled_time
                    FROM space.maintenanceRequests mr
                        INNER JOIN space.properties p ON p.property_uid = mr.maintenance_property_id
                    WHERE p.property_uid = \'""" + property['result'][0]['property_uid'] + """\';
                    """)
            response["maintenanceRequests"] = maintenance
            announcements = db.execute("""
                SELECT * FROM announcements
                WHERE announcement_receiver LIKE '%""" + tenant_id + """%'
                AND (announcement_mode = 'Tenants' OR announcement_mode = 'Properties')
                AND announcement_properties LIKE  '%""" + property['result'][0]['property_uid'] + """%' """)
            response["announcements"] = announcements
            return response


#  -- ACTUAL ENDPOINTS    -----------------------------------------

# New APIs, uses connect() and disconnect()
# Create new api template URL
# api.add_resource(TemplateApi, '/api/v2/templateapi')

# Run on below IP address and port
# Make sure port number is unused (i.e. don't use numbers 0-1023)

# GET requests


api.add_resource(ownerDashboard, '/ownerDashboard/<string:owner_id>')
api.add_resource(ownerDashboardProperties, '/ownerDashboardProperties/<string:owner_id>')

api.add_resource(Rents, '/rents/<string:owner_id>')
api.add_resource(RentDetails, '/rentDetails/<string:owner_id>')

api.add_resource(Properties, '/properties')
api.add_resource(PropertiesByOwner, '/propertiesByOwner/<string:owner_id>')

api.add_resource(MaintenanceRequests, '/maintenanceRequests')
api.add_resource(MaintenanceRequestsByOwner, '/maintenanceRequestsByOwner/<string:owner_id>')
api.add_resource(MaintenanceByProperty, '/maintenanceByProperty/<string:property_id>')
api.add_resource(MaintenanceStatusByProperty, '/maintenanceStatusByProperty/<string:property_id>')
api.add_resource(MaintenanceStatusByOwner, '/maintenanceStatusByOwner/<string:owner_id>')

api.add_resource(Bills, '/bills')

api.add_resource(CashflowByOwner, '/cashflowByOwner/<string:owner_id>/<string:year>')

api.add_resource(TransactionsByOwner, '/transactionsByOwner/<string:owner_id>')
api.add_resource(TransactionsByOwnerByProperty, '/transactionsByOwnerByProperty/<string:owner_id>/<string:property_id>')
api.add_resource(AllTransactions, '/allTransactions')

api.add_resource(Payments, '/makePayment')


api.add_resource(OwnerProfile, '/ownerProfile/<string:owner_id>')
api.add_resource(TenantProfile, '/tenantProfile/<string:tenant_id>')

api.add_resource(OwnerDocuments, '/ownerDocuments/<string:owner_id>')
api.add_resource(TenantDocuments, '/tenantDocuments/<string:tenant_id>')

api.add_resource(LeaseDetails, '/leaseDetails/<string:filter_id>')

api.add_resource(ContactsMaintenance, '/contactsMaintenance')
api.add_resource(ContactsBusinessContacts, '/contactsBusinessContacts/<string:business_uid>')
api.add_resource(ContactsOwnerContactsDetails, '/contactsOwnerContactsDetails/<string:owner_uid>')

api.add_resource(BankAccount, '/bankAccount/<string:business_id>')
api.add_resource(TenantDashboard, '/tenantDashboard/<string:tenant_id>')


# refresh
# api.add_resource(Refresh, '/refresh')

# socialLogin
# api.add_resource(UserSocialLogin, '/userSocialLogin/<string:email>')
# api.add_resource(UserSocialSignup, '/userSocialSignup')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000)
