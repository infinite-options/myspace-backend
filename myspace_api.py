# MANIFEST MY SPACE (PROPERTY MANAGEMENT) BACKEND PYTHON FILE
# https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/<enter_endpoint_details>
# from announcements import AnnouncementsByUserId # , Announcements,
# from profiles import RolesByUserid
from password import Password
# To run program:  python3 myspace_api.py

# README:  if conn error make sure password is set properly in RDS PASSWORD section

# README:  Debug Mode may need to be set to False when deploying live (although it seems to be working through Zappa)

# README:  if there are errors, make sure you have all requirements are loaded
# pip3 install -r requirements.txt


# SECTION 1:  IMPORT FILES AND FUNCTIONS

# from bills import Bills, DeleteUtilities
# from dashboard import ownerDashboard

from dashboard import Dashboard # , ownerDashboard, managerDashboard, tenantDashboard, maintenanceDashboard
from appliances import Appliances, RemoveAppliance
from rents import Rents, RentDetails
from payments import Payments, PaymentOwner, NewPayments, PaymentStatus, PaymentMethod, RequestPayment
from properties import Properties
from transactions import AllTransactions
from cashflow import CashflowByOwner
from cashflow import Cashflow, CashflowSimplified, HappinessMatrix, CashflowSummary
from employees import Employee, EmployeeVerification
from profiles import Profile, BusinessProfileList, OwnerProfile, TenantProfile, BusinessProfile, OwnerProfileByOwnerUid, TenantProfileByTenantUid,  BusinessProfileByUid, BusinessProfileWeb
from documents import OwnerDocuments, TenantDocuments, QuoteDocuments
from documents import Documents
from leases import LeaseDetails, LeaseApplication
from purchases import Bills, AddExpense, AddRevenue, RentPurchase
from maintenance import MaintenanceStatus, MaintenanceByProperty, MaintenanceRequests, MaintenanceQuotes, MaintenanceQuotesByUid
# from purchases import Bills, AddExpense, AddRevenue
from cron import ExtendLease, MonthlyRentPurchase_CLASS, MonthlyRentPurchase_CRON, LateFees_CLASS
# from maintenance import MaintenanceStatusByProperty, MaintenanceByProperty,  \
#     MaintenanceRequestsByOwner, MaintenanceRequests, MaintenanceSummaryByOwner, \
#     MaintenanceSummaryAndStatusByOwner, MaintenanceQuotes, MaintenanceQuotesByUid, MaintenanceDashboardPOST
from contacts import Contacts  #, ContactsMaintenance, ContactsOwnerContactsDetails, ContactsBusinessContacts, ContactsBusinessContactsOwnerDetails, ContactsBusinessContactsTenantDetails, ContactsBusinessContactsMaintenanceDetails, ContactsOwnerManagerDetails, ContactsMaintenanceManagerDetails, ContactsMaintenanceTenantDetails
from contracts import Contracts
from settings import Account
from lists import List
from listings import Listings
from managers import SearchManager
from status_update import StatusUpdate
# from quotes import QuotesByBusiness, QuotesStatusByBusiness, QuotesByRequest, Quotes
from utilities import Utilities
from cron import MonthlyRent_CLASS
from users import UserInfo

# from refresh import Refresh
# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, uploadImage, s3
from twilio.rest import Client
import os
import boto3
import json
from datetime import datetime as dt
from datetime import timezone as dtz
import time
from datetime import datetime, date, timedelta
from pytz import timezone as ptz  # Not sure what the difference is

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

#from dotenv import load_dotenv

import sys
import pytz
import pymysql
import requests


# OTHER IMPORTS NOT IN NITYA
from oauth2client import GOOGLE_REVOKE_URI, GOOGLE_TOKEN_URI, client
# from google_auth_oauthlib.flow import InstalledAppFlow
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
# app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_SERVER'] = 'smtp.mydomain.com'
app.config['MAIL_PORT'] = 465

app.config['MAIL_USERNAME'] = 'support@manifestmy.space'
app.config['MAIL_PASSWORD'] = 'Support4MySpace'
app.config['MAIL_DEFAULT_SENDER'] = 'support@manifestmy.space'

sender = os.getenv('SUPPORT_EMAIL')


TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')

# TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
# TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')

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
# NOTIFICATION_HUB_NAME = os.environ.get('NOTIFICATION_HUB_NAME'
def sendEmail(recipient, subject, body):
    with app.app_context():
        # print("In sendEmail: ", recipient, subject, body)
        sender="support@manifestmy.space"
        # print("sender: ", sender)
        msg = Message(
            sender=sender,
            recipients=[recipient],
            subject=subject,
            body=body
        )
        # print("sender: ", sender)
        # print("Email message: ", msg)
        mail.send(msg)
        # print("email sent")

# app.sendEmail = sendEmail

    
class SendEmail(Resource):
    def post(self):
        payload = request.get_json()
        print(payload)

        # Check if each field in the payload is not null
        if all(field is not None for field in payload.values()):
            sendEmail(payload["receiver"], payload["email_subject"], payload["email_body"])
            return "Email Sent"
        else:
            return "Some fields are missing in the payload", 400


class SendEmailCRON_CLASS(Resource):

    def get(self):
        print("In Send EMail get")
        try:
            conn = connect()

            recipient = ["pmarathay@gmail.com"]
            subject = "MySpace Email sent"
            body = (
                "MySpace Email sent2")
            # mail.send(msg)
            sendEmail(recipient, subject, body)

            return "Email Sent", 200

        except:
            raise BadRequest("Request failed, please try again later.")
        finally:
            print("exit SendEmail")

def Send_Twilio_SMS(message, phone_number):
    items = {}
    numbers = phone_number
    message = message
    numbers = list(set(numbers.split(',')))
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    for destination in numbers:
        message = client.messages.create(
            body=message,
            from_='+19254815757',
            to="+1" + destination
        )
    items['code'] = 200
    items['Message'] = 'SMS sent successfully to the recipient'
    return items


class Announcements(Resource):
    def post(self, user_id):
        print("In Announcements POST")
        payload = request.get_json()
        # print("Post Announcement Payload: ", payload)
        manager_id = user_id
        if isinstance(payload["announcement_receiver"], list):
            receivers = payload["announcement_receiver"]
        else:
            receivers = [payload["announcement_receiver"]]        

        if isinstance(payload["announcement_properties"], list):
            properties = payload["announcement_properties"]
        else:
            properties = [payload["announcement_properties"]]        

        receiverPropertiesMap = {}

        for propertyString in properties:
            propertyObj = json.loads(propertyString)
            # print("Property: ", propertyObj)

            for key, value in propertyObj.items():
                receiverPropertiesMap[key] = value        

        with connect() as db:
            for i in range(len(receivers)):
                newRequest = {}
                newRequest['announcement_title'] = payload["announcement_title"]
                newRequest['announcement_msg'] = payload["announcement_msg"]
                newRequest['announcement_sender'] = manager_id
                newRequest['announcement_mode'] = payload["announcement_mode"]
                newRequest['announcement_properties'] = json.dumps(receiverPropertiesMap.get(receivers[i], []))
                newRequest['announcement_receiver'] = receivers[i]

                # Get the current date and time
                current_datetime = datetime.now().strftime("%m-%d-%Y %H:%M:%S")
    
                # Insert or update the "announcement_read" key with the current date and time
                newRequest['announcement_date'] = current_datetime
                # print("Announcement Date: ", newRequest['announcement_date'])

                user_query = None                    
                if(receivers[i][:3] == '350'):                    
                    user_query = db.execute(""" 
                                        -- Find the user details
                                        SELECT tenant_email as email, tenant_phone_number as phone_number 
                                        FROM space.tenantProfileInfo AS t
                                        WHERE t.tenant_uid = \'""" + receivers[i] + """\';
                                        """)                    
                elif(receivers[i][:3] == '110'):                                        
                    user_query = db.execute(""" 
                                        -- Find the user details
                                        SELECT owner_email as email, owner_phone_number as phone_number 
                                        FROM space.ownerProfileInfo AS o
                                        WHERE o.owner_uid = \'""" + receivers[i] + """\';
                                        """)
                elif(receivers[i][:3] == '600'):                                        
                    user_query = db.execute(""" 
                                        -- Find the user details
                                        SELECT business_email as email, business_phone_number as phone_number
                                        FROM space.businessProfileInfo AS b
                                        WHERE b.business_uid = \'""" + receivers[i] + """\';
                                        """)                                        
                for j in range(len(payload["announcement_type"])):
                    if payload["announcement_type"][j] == "Email":
                        newRequest['Email'] = "1"
                        user_email = user_query['result'][0]['email']
                        sendEmail(user_email, payload["announcement_title"], payload["announcement_msg"])
                    if payload["announcement_type"][j] == "Text":
                        newRequest['Text'] = "1"
                        user_phone = user_query['result'][0]['phone_number']
                        msg = payload["announcement_title"]+"\n" + payload["announcement_msg"]
                        Send_Twilio_SMS(msg, user_phone)
                    # if payload["announcement_type"][j] == "App":
                    #     newRequest['App'] = "1"
                newRequest['App'] = "1"                
                response = db.insert('announcements', newRequest)

        return response                

    def get(self, user_id):
        print("In Announcements GET")
        response = {}
        with connect() as db:
            # if user_id.startswith("600-"):
            sentQuery = db.execute("""
                    SELECT 
                        a.*,
                        COALESCE(b.business_name, c.owner_first_name, d.tenant_first_name) AS receiver_first_name,
                        COALESCE(c.owner_last_name,  d.tenant_last_name) AS receiver_last_name,
                        COALESCE(b.business_phone_number, c.owner_phone_number, d.tenant_phone_number) AS receiver_phone_number,
                        COALESCE(b.business_photo_url, c.owner_photo_url, d.tenant_photo_url) AS receiver_photo_url
                    , CASE
                            WHEN a.announcement_receiver LIKE '600%' THEN 'Business'
                            WHEN a.announcement_receiver LIKE '350%' THEN 'Tenant'
                            WHEN a.announcement_receiver LIKE '110%' THEN 'Owner'
                            ELSE 'Unknown'
                      END AS receiver_role
                    FROM space.announcements a
                    LEFT JOIN space.businessProfileInfo b ON a.announcement_receiver LIKE '600%' AND b.business_uid = a.announcement_receiver
                    LEFT JOIN space.ownerProfileInfo c ON a.announcement_receiver LIKE '110%' AND c.owner_uid = a.announcement_receiver
                    LEFT JOIN space.tenantProfileInfo d ON a.announcement_receiver LIKE '350%' AND d.tenant_uid = a.announcement_receiver
                    WHERE announcement_sender = \'""" + user_id + """\';
            """)

            response["sent"] = sentQuery

            receivedQuery = db.execute("""
                    SELECT 
                        a.*,
                        COALESCE(b.business_name, c.owner_first_name, d.tenant_first_name) AS sender_first_name,
                        COALESCE(c.owner_last_name,  d.tenant_last_name) AS sender_last_name,
                        COALESCE(b.business_phone_number, c.owner_phone_number, d.tenant_phone_number) AS sender_phone_number,
                        COALESCE(b.business_photo_url, c.owner_photo_url, d.tenant_photo_url) AS sender_photo_url
                        , CASE
                            WHEN a.announcement_sender LIKE '600%' THEN 'Business'
                            WHEN a.announcement_sender LIKE '350%' THEN 'Tenant'
                            WHEN a.announcement_sender LIKE '110%' THEN 'Owner'
                            ELSE 'Unknown'
                        END AS sender_role
                    FROM 
                        space.announcements a
                    LEFT JOIN 
                        space.businessProfileInfo b ON a.announcement_sender LIKE '600%' AND b.business_uid = a.announcement_sender
                    LEFT JOIN 
                        space.ownerProfileInfo c ON a.announcement_sender LIKE '110%' AND c.owner_uid = a.announcement_sender
                    LEFT JOIN 
                        space.tenantProfileInfo d ON a.announcement_sender LIKE '350%' AND d.tenant_uid = a.announcement_sender
                    WHERE 
                        announcement_receiver = \'""" + user_id + """\';

            """)

            response["received"] = receivedQuery

            # else:
            #     response = db.execute("""
            #                             -- Find the user details
            #                             SELECT *
            #                             FROM space.announcements AS a
            #                             WHERE a.announcement_receiver = \'""" + user_id + """\'
            #                             AND a.App = '1'
            #                             ORDER BY a.announcement_date DESC;
            #                             """)
        return response

    def put(self):
        print("In Announcements PUT")
        response = {}
        payload = request.get_json()
        if payload.get('announcement_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        # print("Announcement Payload: ", payload)

        # Get the current date and time
        current_datetime = datetime.now().strftime("%m-%d-%Y %H:%M:%S")
    
        # Insert or update the "announcement_read" key with the current date and time
        payload['announcement_read'] = current_datetime


        i = 0
        for each in payload['announcement_uid']:
            # print("current uid: ", each)
            key = {'announcement_uid': each}
            # print("Annoucement Key: ", key)
            with connect() as db:
                response = db.update('announcements', key, payload)
                i = i + 1
        response["rows affected"] = i
        return response
    
class LeaseExpiringNotify(Resource):
    def get(self):
        with connect() as db:
            response = db.execute("""
            SELECT *
            FROM space.leases l
            LEFT JOIN space.t_details t ON t.lt_lease_id = l.lease_uid
            LEFT JOIN space.b_details b ON b.contract_property_id = l.lease_property_id
            LEFT JOIN space.properties p ON p.property_uid = l.lease_property_id
            WHERE l.lease_end = DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 2 MONTH), "%Y-%m-%d")
            AND l.lease_status='ACTIVE'
            AND b.contract_status='ACTIVE'; """)
            print(response)
            if len(response['result']) > 0:
                for i in range(len(response['result'])):
                    name = response['result'][i]['tenant_first_name'] + \
                           ' ' + response['result'][i]['tenant_last_name']
                    address = response['result'][i]["tenant_address"] + \
                              ' ' + response['result'][i]["tenant_unit"] + ", " + response['result'][i]["tenant_city"] + \
                              ', ' + response['result'][i]["tenant_state"] + \
                              ' ' + response['result'][i]["tenant_zip"]
                    start_date = response['result'][i]['lease_start']
                    end_date = response['result'][i]['lease_end']
                    business_name = response['result'][i]['business_name']
                    phone = response['result'][i]['business_phone_number']
                    email = response['result'][i]['business_email']
                    recipient = response['result'][i]['tenant_email']
                    subject = "Lease ending soon..."
                    body = (
                            "Hello " + str(name) + "," + "\n"
                             "\n"
                             "Property: " + str(address) + "\n"
                           "This is your 2 month reminder, that your lease is ending. \n"
                           "Here are your lease details: \n"
                           "Start Date: " +
                            str(start_date) + "\n"
                          "End Date: " +
                            str(end_date) + "\n"
                        "Please contact your Property Manager if you wish to renew or end your lease before the time of expiry. \n"
                        "\n"
                        "Name: " + str(business_name) + "\n"
                        "Phone: " + str(phone) + "\n"
                         "Email: " + str(email) + "\n"
                                 "\n"
                                 "Thank you - Team Property Management\n\n"
                    )
                    sendEmail(recipient, subject, body)
                    print('sending')

                return response

# Twilio's settings
# from twilio.rest import Client

# TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
# TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')


# STRIPE KEYS

stripe_public_test_key = os.getenv("stripe_public_test_key")
stripe_secret_test_key = os.getenv("stripe_secret_test_key")

stripe_public_live_key = os.getenv("stripe_public_live_key")
stripe_secret_live_key = os.getenv("stripe_secret_live_key")

stripe.api_key = stripe_secret_test_key


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

class stripe_key(Resource):
    def get(self, desc):
        print(desc)
        if desc == "PMTEST":
            return {"publicKey": stripe_public_test_key}
        else:
            return {"publicKey": stripe_public_live_key}


#  -- ACTUAL ENDPOINTS    -----------------------------------------

# New APIs, uses connect() and disconnect()
# Create new api template URL
# api.add_resource(TemplateApi, '/api/v2/templateapi')

# Run on below IP address and port
# Make sure port number is unused (i.e. don't use numbers 0-1023)

# GET requests


# Dashboard Queries

api.add_resource(Dashboard, '/dashboard/<string:user_id>')


# Owner Queries


# Payment Endpoints
api.add_resource(PaymentStatus, '/paymentStatus/<string:user_id>')
api.add_resource(PaymentOwner, '/paymentOwner/<string:user_id>/<string:owner_id>')
api.add_resource(NewPayments, '/makePayment')
# api.add_resource(Payments, '/makePayment')  # Original endpoint

api.add_resource(PaymentMethod, '/paymentMethod','/paymentMethod/<string:user_id>')
api.add_resource(stripe_key, "/stripe_key/<string:desc>")


# Maintenance Endpoints
api.add_resource(MaintenanceStatus, '/maintenanceStatus/<string:uid>')
api.add_resource(MaintenanceRequests, '/maintenanceReq/<string:uid>', '/maintenanceRequests')
api.add_resource(MaintenanceQuotes, '/maintenanceQuotes', '/maintenanceQuotes/<string:uid>')
api.add_resource(MaintenanceQuotesByUid, '/maintenanceQuotes/<string:maintenance_quote_uid>')




api.add_resource(Rents, '/rents/<string:uid>')
api.add_resource(RentDetails, '/rentDetails/<string:uid>')

api.add_resource(Properties, '/properties/<string:uid>', '/properties' )


# appliances
api.add_resource(Appliances, '/appliances', '/appliances/<string:uid>')
# api.add_resource(RemoveAppliance, "/RemoveAppliance")



api.add_resource(Bills, '/bills','/bills/<string:user_id>')
api.add_resource(Contracts, '/contracts', '/contracts/<string:user_id>')
api.add_resource(AddExpense, '/addExpense')
api.add_resource(AddRevenue, '/addRevenue')
api.add_resource(Cashflow, '/cashflow/<string:user_id>/<string:year>')
api.add_resource(CashflowSimplified, '/cashflowSimplified/<string:user_id>')
api.add_resource(CashflowSummary, '/cashflowSummary/<string:user_id>')

api.add_resource(AllTransactions, '/allTransactions/<string:uid>')


api.add_resource(Profile, '/profile/<string:user_id>', '/profile' )

# Can we delete these?
api.add_resource(OwnerProfileByOwnerUid, '/ownerProfile/<string:owner_id>')
api.add_resource(TenantProfileByTenantUid, '/tenantProfile/<string:tenant_id>')
api.add_resource(BusinessProfileByUid,
                 '/businessProfile/<string:business_uid>')
api.add_resource(OwnerProfile, '/ownerProfile')  # POST, PUT OwnerProfile
api.add_resource(TenantProfile, '/tenantProfile')
api.add_resource(BusinessProfile, '/businessProfile')
api.add_resource(BusinessProfileWeb, '/businessProfileWeb')



api.add_resource(Employee, '/employee','/employee/<string:user_id>')
api.add_resource(EmployeeVerification, '/employeeVerification')
api.add_resource(OwnerDocuments, '/ownerDocuments/<string:owner_id>')
api.add_resource(TenantDocuments, '/tenantDocuments/<string:tenant_id>')
api.add_resource(QuoteDocuments, '/quoteDocuments', '/quoteDocuments/<string:quote_id>')
api.add_resource(Documents, '/documents','/documents/<string:user_id>')
api.add_resource(LeaseDetails, '/leaseDetails/<string:filter_id>')
api.add_resource(LeaseApplication, '/leaseApplication', '/leaseApplication/<string:tenant_id>/<string:property_id>')


api.add_resource(Contacts, '/contacts/<string:uid>')
api.add_resource(Announcements, '/announcements/<string:user_id>', '/announcements')
api.add_resource(RequestPayment, '/requestPayment')

api.add_resource(List, '/lists')
api.add_resource(Listings, '/listings/<string:tenant_id>')
api.add_resource(Utilities, '/utilities')

api.add_resource(Account, '/account')
api.add_resource(HappinessMatrix,'/happinessMatrix/<string:user_id>')

api.add_resource(SearchManager, '/searchManager')

api.add_resource(Password, '/password')

#CRON JOBS
# api.add_resource(LeaseExpiringNotify, '/LeaseExpiringNotify')
api.add_resource(MonthlyRentPurchase_CLASS, '/MonthlyRent')
api.add_resource(RentPurchase, '/rentPurchase')
api.add_resource(LateFees_CLASS, '/LateFees')

# api.add_resource(ExtendLease, '/ExtendLease')
# api.add_resource(MonthlyRent_CLASS, '/MonthlyRent')

api.add_resource(SendEmailCRON_CLASS, "/sendEmailCRON_CLASS")
api.add_resource(BusinessProfileList, "/businessProfileList/<string:business_type>")
api.add_resource(SendEmail, "/sendEmail")

api.add_resource(UserInfo, "/userInfo/<string:user_id>")

# refresh
# api.add_resource(Refresh, '/refresh')

# socialLogin
# api.add_resource(UserSocialLogin, '/userSocialLogin/<string:email>')
# api.add_resource(UserSocialSignup, '/userSocialSignup')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000)
