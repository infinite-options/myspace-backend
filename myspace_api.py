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


from dashboard import Dashboard
from appliances import Appliances, RemoveAppliance, Appliances_SB
from rents import Rents, RentDetails
from payments import Payments, PaymentOwner, NewPayments, PaymentStatus, PaymentMethod, RequestPayment
from properties import Properties
from transactions import AllTransactions
from cashflow import CashflowByOwner
from cashflow import Cashflow, CashflowSimplified, HappinessMatrix, CashflowSummary, CashflowRevised
from employees import Employee, EmployeeVerification
from profiles import Profile, BusinessProfile, BusinessProfileList
from documents import OwnerDocuments, TenantDocuments, QuoteDocuments
from documents import Documents
from leases import LeaseDetails, LeaseApplication
from purchases import Bills, AddExpense, AddRevenue, RentPurchase
from maintenance import MaintenanceStatus, MaintenanceByProperty, MaintenanceRequests, MaintenanceQuotes, MaintenanceQuotesByUid
from cron import PeriodicPurchases_CLASS # , ExtendLease, MonthlyRentPurchase_CLASS, MonthlyRentPurchase_CRON, LateFees_CLASS, LateFees_CRON
from contacts import Contacts
from contracts import Contracts
from settings import Account
from lists import List
from listings import Listings
from managers import SearchManager
from status_update import StatusUpdate
from utilities import Utilities
from cron import MonthlyRent_CLASS
from users import UserInfo


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
from decimal import Decimal
from hashlib import sha512
import sys
import pymysql
import requests
import pytz

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



# BING API KEY
# Import Bing API key into bing_api_key.py

#  NEED TO SOLVE THIS
# from env_keys import BING_API_KEY, RDS_PW

# from dotenv import load_dotenv




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


# STRIPE KEYS

stripe_public_test_key = os.getenv("stripe_public_test_key")
stripe_secret_test_key = os.getenv("stripe_secret_test_key")

stripe_public_live_key = os.getenv("stripe_public_live_key")
stripe_secret_live_key = os.getenv("stripe_secret_live_key")

stripe.api_key = stripe_secret_test_key



# Twilio's settings
# from twilio.rest import Client

# TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
# TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')

TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')




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




# -- Send Email Endpoints start here -------------------------------------------------------------------------------

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


class SendEmail_CLASS(Resource):
    def get(self):
        print("In Send EMail CRON get")
        try:
            conn = connect()

            recipient = "pmarathay@gmail.com"
            subject = "MySpace CRON Jobs Completed"
            body = "The Following CRON Jobs Ran:"
            # mail.send(msg)
            sendEmail(recipient, subject, body)

            return "Email Sent", 200

        except:
            raise BadRequest("Request failed, please try again later.")
        finally:
            print("exit SendEmail")


def SendEmail_CRON(self):
        print("In Send EMail CRON get")
        try:
            conn = connect()

            recipient = "pmarathay@gmail.com"
            subject = "MySpace CRON Jobs Completed"
            body = "The Following CRON Jobs Ran:"
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
        


# -- SPACE CRON ENDPOINTS start here -------------------------------------------------------------------------------

class MonthlyRentPurchase_CLASS(Resource):
    def get(self):
        print("In Monthly Rent CRON JOB")

        numCronPurchases = 0

        # Establish current month and year
        dt = datetime.today()
       
        try:
            # Run query to find rents of ACTIVE leases
            with connect() as db:    
                response = db.execute("""
                        -- CALCULATE RECURRING FEES
                        SELECT leaseFees.*
                            , lease_tenant.*
                            , property_owner_id, po_owner_percent
                            , contract_uid, contract_business_id, contract_status, contract_fees
                            , lease_property_id, lease_start, lease_end, lease_status, lease_early_end_date, lease_renew_status
                            -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, lease_move_in_date, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_consent, lease_actual_rent, lease_end_notice_period, lease_end_reason
                        FROM space.leaseFees
                        LEFT JOIN space.leases ON fees_lease_id = lease_uid
                        LEFT JOIN space.lease_tenant ON fees_lease_id = lt_lease_id
                        LEFT JOIN space.property_owner ON lease_property_id = property_id
                        LEFT JOIN space.contracts ON lease_property_id = contract_property_id
                        WHERE frequency != 'One-time' AND !ISNULL(frequency) AND frequency != ""
                            AND lease_status IN ('ACTIVE', 'ACTIVE MTM', 'APPROVED') 
                            AND contract_status = 'ACTIVE'
                        ORDER BY leaseFees_uid -- frequency;
                        """)

                for i in range(len(response['result'])):
                    # print("\n",i, response['result'][i]['leaseFees_uid'], response['result'][i]['fees_lease_id'], response['result'][i]['lease_property_id'], response['result'][i]['contract_uid'], response['result'][i]['contract_business_id'])
                
                    # Check Frequecy of Rent Payment.  Currently query only returns MONTHLY leases
                    # rentFrequency = response['result'][i]['frequency']

                    # print(response['result'][i]['frequency'])
                    # if rentFrequency == "Weekly":
                    #     print("Weekly Rent Fee")
                    # elif rentFrequency == "Anually":
                    #     print("Annual Rent Fee") 
                    # elif rentFrequency == "Monthly" or rentFrequency is None:
                    #     print("Monthly Rent Fee")
                    # else: print("Investigate")


                    # Check if available_topay is NONE
                    if response['result'][i]['available_topay'] is None:
                        # print("available_topay Is NULL!!")
                        payable = 10
                    else:
                        payable = response['result'][i]['available_topay']
                    # print("available_topay: ", payable)


                    # Check if due_by is NONE
                    # print(response['result'][i]['due_by'])
                    if response['result'][i]['due_by'] is None or response['result'][i]['due_by'] == 0:
                        # print("due_by Is NULL!!")
                        due_by = 1
                    else:
                        due_by = response['result'][i]['due_by']
                    # print("due_by: ", due_by, type(due_by))  # Day rent is due ie 2
                    # print("dt.day: ", dt.day, type(dt.day))  # Todays Day ie 8/3/2024 would return 3


                    # Calculate Actual Rent due date
                    # due_by < dt.day means that due_by date has already pasted so look towards next month.
                    # example:  2 < 3 means rent was already due on the second.  Look to rent due next month on the 2nd
                    if due_by < dt.day:                 # Due date has already passed.  Look to next month
                        # print(due_by, " < ", dt.day)
                        due_date = datetime(dt.year, dt.month, due_by) + relativedelta(months=1)
                    else:
                        due_date = datetime(dt.year, dt.month, due_by)    # Current month Due date has NOT passed.  Calculate this months rent
                    # print("due date: ", due_date,  type(due_date))        # Prints Date of Next Rent Due:  2024-09-02
                    pm_due_date = due_date + relativedelta(days=10)
                    # print("PM due date: ", pm_due_date,  type(pm_due_date))  # Prints Date of Next PM to Owner Payment :  2024-09-12

                    
                    # Calculate number of days until rent is due
                    if response['result'][i]['frequency'] == 'Monthly':
                        # print(due_date, dt)
                        days_for_rent = (due_date.date() - dt.date()).days
                        # print("Rent due in : ", days_for_rent, " days", type(days_for_rent))
                        # print("Rent Posts in: ", days_for_rent - payable , " days", type(payable))
                    elif response['result'][i]['frequency'] == 'Weekly':
                        # print("Weekly")
                        days_for_rent = 500
                        
                    elif response['result'][i]['frequency'] == 'Bi-Weekly':
                        # print("Bi-Weekly")
                        days_for_rent = 500
                        
                    elif response['result'][i]['frequency'] == 'Annually':
                        # print("Annually ", response['result'][i]['due_by_date'], type(response['result'][i]['due_by_date']) )
                        days_for_rent = 500






                    # CHECK IF RENT IS AVAILABLE TO PAY  ==> IF IT IS, ADD PURCHASES FOR TENANT TO PM AND PM TO OWNER
                    print(i, response['result'][i]['leaseFees_uid'], response['result'][i]['fees_lease_id'], response['result'][i]['lease_property_id'], "Rent Due: ", due_date.date(), "Rent Posts: ", due_date.date() - timedelta(days=payable), "    days_for_rent: ", days_for_rent, "   payable: ", payable, "    Rent posts in: ",  days_for_rent - payable)
                    if days_for_rent == payable + (0):  # Remove/Change number to get query to run and return data

                    # IF Changing the dates manually
                    # if days_for_rent >= 0:  # Remove/Change number to get query to run and return data
                    #     due_date = datetime(2024, 1, due_by)            # Comment this out since due_date is set above
                    #     pm_due_date = due_date + relativedelta(days=10) # Comment this out since due_date is set above


                        print("Rent posted.  Please Pay")
                        numCronPurchases = numCronPurchases + 1
                        # print(i, response['result'][i])           


                        # Perform Remainder Checks to ensure no blank fields
                        # Check if late_fee is NONE
                        # print(response['result'][i]['fee_name'], response['result'][i]['late_fee'], type(response['result'][i]['late_fee']))
                        if response['result'][i]['late_fee'] is None or response['result'][i]['late_fee'] == 0 or response['result'][i]['late_fee'] == "":
                            # print("Is NULL!!")
                            late_fee = 0
                        else:
                            late_fee = response['result'][i]['late_fee']
                        # print("late_fee: ", late_fee, type(late_fee))
                            
                        # Check if perDay_late_fee is NONE
                        # print(response['result'][i]['perDay_late_fee'])
                        if response['result'][i]['perDay_late_fee'] is None or response['result'][i]['perDay_late_fee'] == 0:
                            # print("Is NULL!!")
                            perDay_late_fee = 0
                        else:
                            perDay_late_fee = response['result'][i]['perDay_late_fee']
                        # print("perDay_late_fee: ", perDay_late_fee, type(perDay_late_fee))

                        # Check if late_by is NONE
                        # print(response['result'][i]['late_by'])
                        if response['result'][i]['late_by'] is None or response['result'][i]['late_by'] == 0:
                            # print("Is NULL!!")
                            late_by = 1
                        else:
                            late_by = response['result'][i]['late_by']
                        # print("late_by: ", late_by, type(late_by))

                        

                        # Check if tenant responsiblity is NONE
                        # print("What is in the db: ", response['result'][i]['lt_responsibility'])
                        if response['result'][i]['lt_responsibility'] is None:
                            # print("Is NULL!!")
                            responsible_percent = 1.0
                        else:
                            responsible_percent = response['result'][i]['lt_responsibility']
                        # print("What we set programmatically: ", responsible_percent, type(responsible_percent))
                        charge = response['result'][i]['charge']
                        # print("Charge: ", charge, type(charge))
                        amt_due = float(charge)  * responsible_percent
                        # print("Amount due: ", amt_due)


                        # Establish payer, initiator and receiver
                        contract_uid = response['result'][i]['contract_uid']
                        property = response['result'][i]['lease_property_id']
                        tenant = response['result'][i]['lt_tenant_id']
                        owner = response['result'][i]['property_owner_id']
                        manager = response['result'][i]['contract_business_id']
                        fee_name = response['result'][i]['fee_name']
                        # print("Purchase Parameters: ", i, contract_uid, tenant, owner, manager)


                        # Common JSON Object Attributes
                        newRequest = {}
                        
                        newRequest['pur_timestamp'] = datetime.today().date().strftime("%m-%d-%Y")
                        newRequest['pur_property_id'] = property
                        newRequest['purchase_type'] = "Rent"
                        newRequest['pur_cf_type'] = "revenue"
                        newRequest['pur_amount_due'] = amt_due
                        newRequest['purchase_status'] = "UNPAID"
                        newRequest['pur_status_value'] = "0"
                        newRequest['pur_notes'] = fee_name

                        newRequest['pur_due_by'] = due_by
                        newRequest['pur_late_by'] = late_by
                        newRequest['pur_late_fee'] = late_fee
                        newRequest['pur_perDay_late_fee'] = perDay_late_fee

                        newRequest['purchase_date'] = datetime.today().date().strftime("%m-%d-%Y")
                        newRequest['pur_description'] = f"Rent for {due_date.strftime('%B')} {due_date.year} CRON"

                        
                        # Create JSON Object for Rent Purchase for Tenant-PM Payment
                        newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                        grouping = newRequestID
                        newRequest['purchase_uid'] = newRequestID
                        newRequest['pur_group'] = grouping
                        # print(newRequestID)
                        newRequest['pur_receiver'] = manager
                        newRequest['pur_payer'] = tenant
                        newRequest['pur_initiator'] = manager
                        newRequest['pur_due_date'] = due_date.date().strftime("%m-%d-%Y")
                        
                        
                        # print(newRequest)
                        # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
                        db.insert('purchases', newRequest)



                        # Create JSON Object for Rent Purchase for PM-Owner Payment
                        newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                        newRequest['purchase_uid'] = newRequestID
                        # print(newRequestID)
                        newRequest['pur_receiver'] = owner
                        newRequest['pur_payer'] = manager
                        newRequest['pur_initiator'] = manager
                        newRequest['pur_due_date'] = pm_due_date.date().strftime("%m-%d-%Y")
                        newRequest['pur_group'] = grouping
                    
                        # print(newRequest)
                        # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
                        db.insert('purchases', newRequest)




                        # For each entry posted to the purchases table, post any contract fees based on Rent
                        # Find contract fees based rent
                        manager_fees = db.execute("""
                                        SELECT -- *
                                            contract_uid, contract_property_id, contract_business_id
                                            -- , contract_start_date, contract_end_date
                                            , contract_fees
                                            -- , contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                            , jt.*
                                        FROM 
                                            space.contracts,
                                            JSON_TABLE(
                                                contract_fees,
                                                "$[*]" COLUMNS (
                                                    of_column VARCHAR(50) PATH "$.of",
                                                    charge_column VARCHAR(50) PATH "$.charge",
                                                    fee_name_column VARCHAR(50) PATH "$.fee_name",
                                                    fee_type_column VARCHAR(10) PATH "$.fee_type",
                                                    frequency_column VARCHAR(20) PATH "$.frequency"
                                                )
                                            ) AS jt
                                        -- WHERE contract_uid = '010-000003' AND of_column LIKE '%rent%';
                                        WHERE contract_uid = \'""" + contract_uid + """\' AND of_column LIKE '%rent%';
                                    """)
                        # print(manager_fees)
                        

                        for j in range(len(manager_fees['result'])):

                            # Check if fees is monthly 
                            if manager_fees['result'][j]['frequency_column'] == 'Monthly' or manager_fees['result'][j]['frequency_column'] == 'monthly':

                                # Check if charge is a % or Fixed $ Amount
                                if manager_fees['result'][j]['fee_type_column'] == '%' or manager_fees['result'][j]['fee_type_column'] == 'PERCENT':
                                    charge_amt = Decimal(manager_fees['result'][j]['charge_column']) * Decimal(amt_due) / 100
                                else:
                                    charge_amt = Decimal(manager_fees['result'][j]['charge_column'])
                                # print("Charge Amount: ", charge_amt, property, contract_uid, manager_fees['result'][j]['charge_column'], response['result'][i]['charge'] )

                                # Create JSON Object for Fee Purchase
                                newPMRequest = {}
                                newPMRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                                # print(newPMRequestID)
                                newPMRequest['purchase_uid'] = newPMRequestID
                                newPMRequest['pur_timestamp'] = datetime.today().date().strftime("%m-%d-%Y")
                                newPMRequest['pur_property_id'] = property
                                newPMRequest['purchase_type'] = "Management"
                                newPMRequest['pur_cf_type'] = "expense"
                                newPMRequest['pur_amount_due'] = charge_amt
                                newPMRequest['purchase_status'] = "UNPAID"
                                newPMRequest['pur_status_value'] = "0"
                                newPMRequest['pur_notes'] = manager_fees['result'][j]['fee_name_column']
                                newPMRequest['pur_description'] =  f"{manager_fees['result'][j]['fee_name_column']} for {due_date.strftime('%B')} {due_date.year} "
                                # newPMRequest['pur_description'] =  newRequestID # Original Rent Purchase ID  
                                # newPMRequest['pur_description'] = f"Fees for MARCH {nextMonth.year} CRON"
                                newPMRequest['pur_receiver'] = manager
                                newPMRequest['pur_payer'] = owner
                                newPMRequest['pur_initiator'] = manager
                                newPMRequest['purchase_date'] = datetime.today().date().strftime("%m-%d-%Y")
                                newPMRequest['pur_group'] = grouping

                                # *********
                                newPMRequest['pur_due_date'] = due_date.date().strftime("%m-%d-%Y")
                                # newPMRequest['pur_due_date'] = datetime(nextMonth.year, nextMonth.month, due_by).date().strftime("%m-%d-%Y")
                                # newPMRequest['pur_due_date'] = datetime(nextMonth.year, 1, due_by).date().strftime("%m-%d-%Y")
                                
                                # print(newPMRequest)
                                print("Number of CRON Purchases: ", numCronPurchases, dt)
                                db.insert('purchases', newPMRequest)

                                # For each fee, post to purchases table

            response["cron_job"] = {'message': f'Successfully completed CRON Job for {dt}' ,
                        'rows affected': f'{numCronPurchases}',
                    'code': 200}
            
            try:
                recipient = "pmarathay@gmail.com"
                subject = "MySpace Monthly Rent CRON JOB Completed "
                body = "Monthly Rent CRON JOB"
                # mail.send(msg)
                sendEmail(recipient, subject, body)

                response["email"] = {'message': f'CRON Job Email for {dt} sent!' ,
                    'code': 500}

            except:
                response["email fail"] = {'message': f'CRON Job Email for {dt} could not be sent' ,
                    'code': 500}
                
        except:
            response["cron fail"] = {'message': f'CRON Job failed for {dt}' ,
                    'code': 500}
            try:
                recipient = "pmarathay@gmail.com"
                subject = "MySpace Monthly Rent CRON JOB Failed!"
                body = "Monthly Rent CRON JOB Failed"
                # mail.send(msg)
                sendEmail(recipient, subject, body)

                response["email"] = {'message': f'CRON Job Fail Email for {dt} sent!' ,
                    'code': 500}

            except:
                response["email fail"] = {'message': f'CRON Job Fail Email for {dt} could not be sent' ,
                    'code': 500}

        return response

def MonthlyRentPurchase_CRON(self):
        print("In Monthly Rent CRON JOB")

        numCronPurchases = 0

        # Establish current month and year
        dt = datetime.today()
       
        try:
            # Run query to find rents of ACTIVE leases
            with connect() as db:    
                response = db.execute("""
                        -- CALCULATE RECURRING FEES
                        SELECT leaseFees.*
                            , lease_tenant.*
                            , property_owner_id, po_owner_percent
                            , contract_uid, contract_business_id, contract_status, contract_fees
                            , lease_property_id, lease_start, lease_end, lease_status, lease_early_end_date, lease_renew_status
                            -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, lease_move_in_date, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_consent, lease_actual_rent, lease_end_notice_period, lease_end_reason
                        FROM space.leaseFees
                        LEFT JOIN space.leases ON fees_lease_id = lease_uid
                        LEFT JOIN space.lease_tenant ON fees_lease_id = lt_lease_id
                        LEFT JOIN space.property_owner ON lease_property_id = property_id
                        LEFT JOIN space.contracts ON lease_property_id = contract_property_id
                        WHERE frequency != 'One-time' AND !ISNULL(frequency) AND frequency != ""
                            AND lease_status IN ('ACTIVE', 'ACTIVE MTM', 'APPROVED') 
                            AND contract_status = 'ACTIVE'
                        ORDER BY leaseFees_uid -- frequency;
                        """)

                for i in range(len(response['result'])):
                    # print("\n",i, response['result'][i]['leaseFees_uid'], response['result'][i]['fees_lease_id'], response['result'][i]['lease_property_id'], response['result'][i]['contract_uid'], response['result'][i]['contract_business_id'])
                
                    # Check Frequecy of Rent Payment.  Currently query only returns MONTHLY leases
                    # rentFrequency = response['result'][i]['frequency']

                    # print(response['result'][i]['frequency'])
                    # if rentFrequency == "Weekly":
                    #     print("Weekly Rent Fee")
                    # elif rentFrequency == "Anually":
                    #     print("Annual Rent Fee") 
                    # elif rentFrequency == "Monthly" or rentFrequency is None:
                    #     print("Monthly Rent Fee")
                    # else: print("Investigate")


                    # Check if available_topay is NONE
                    if response['result'][i]['available_topay'] is None:
                        # print("available_topay Is NULL!!")
                        payable = 10
                    else:
                        payable = response['result'][i]['available_topay']
                    # print("available_topay: ", payable)


                    # Check if due_by is NONE
                    # print(response['result'][i]['due_by'])
                    if response['result'][i]['due_by'] is None or response['result'][i]['due_by'] == 0:
                        # print("due_by Is NULL!!")
                        due_by = 1
                    else:
                        due_by = response['result'][i]['due_by']
                    # print("due_by: ", due_by, type(due_by))  # Day rent is due ie 2
                    # print("dt.day: ", dt.day, type(dt.day))  # Todays Day ie 8/3/2024 would return 3


                    # Calculate Actual Rent due date
                    # due_by < dt.day means that due_by date has already pasted so look towards next month.
                    # example:  2 < 3 means rent was already due on the second.  Look to rent due next month on the 2nd
                    if due_by < dt.day:                 # Due date has already passed.  Look to next month
                        # print(due_by, " < ", dt.day)
                        due_date = datetime(dt.year, dt.month, due_by) + relativedelta(months=1)
                    else:
                        due_date = datetime(dt.year, dt.month, due_by)    # Current month Due date has NOT passed.  Calculate this months rent
                    # print("due date: ", due_date,  type(due_date))        # Prints Date of Next Rent Due:  2024-09-02
                    pm_due_date = due_date + relativedelta(days=10)
                    # print("PM due date: ", pm_due_date,  type(pm_due_date))  # Prints Date of Next PM to Owner Payment :  2024-09-12

                    
                    # Calculate number of days until rent is due
                    if response['result'][i]['frequency'] == 'Monthly':
                        # print(due_date, dt)
                        days_for_rent = (due_date.date() - dt.date()).days
                        # print("Rent due in : ", days_for_rent, " days", type(days_for_rent))
                        # print("Rent Posts in: ", days_for_rent - payable , " days", type(payable))
                    elif response['result'][i]['frequency'] == 'Weekly':
                        # print("Weekly")
                        days_for_rent = 500
                        
                    elif response['result'][i]['frequency'] == 'Bi-Weekly':
                        # print("Bi-Weekly")
                        days_for_rent = 500
                        
                    elif response['result'][i]['frequency'] == 'Annually':
                        # print("Annually ", response['result'][i]['due_by_date'], type(response['result'][i]['due_by_date']) )
                        days_for_rent = 500






                    # CHECK IF RENT IS AVAILABLE TO PAY  ==> IF IT IS, ADD PURCHASES FOR TENANT TO PM AND PM TO OWNER
                    print(i, response['result'][i]['leaseFees_uid'], response['result'][i]['fees_lease_id'], response['result'][i]['lease_property_id'], "Rent Due: ", due_date.date(), "Rent Posts: ", due_date.date() - timedelta(days=payable), "    days_for_rent: ", days_for_rent, "   payable: ", payable, "    Rent posts in: ",  days_for_rent - payable)
                    if days_for_rent == payable + (0):  # Remove/Change number to get query to run and return data

                    # IF Changing the dates manually
                    # if days_for_rent >= 0:  # Remove/Change number to get query to run and return data
                    #     due_date = datetime(2024, 1, due_by)            # Comment this out since due_date is set above
                    #     pm_due_date = due_date + relativedelta(days=10) # Comment this out since due_date is set above


                        print("Rent posted.  Please Pay")
                        numCronPurchases = numCronPurchases + 1
                        # print(i, response['result'][i])           


                        # Perform Remainder Checks to ensure no blank fields
                        # Check if late_fee is NONE
                        # print(response['result'][i]['fee_name'], response['result'][i]['late_fee'], type(response['result'][i]['late_fee']))
                        if response['result'][i]['late_fee'] is None or response['result'][i]['late_fee'] == 0 or response['result'][i]['late_fee'] == "":
                            # print("Is NULL!!")
                            late_fee = 0
                        else:
                            late_fee = response['result'][i]['late_fee']
                        # print("late_fee: ", late_fee, type(late_fee))
                            
                        # Check if perDay_late_fee is NONE
                        # print(response['result'][i]['perDay_late_fee'])
                        if response['result'][i]['perDay_late_fee'] is None or response['result'][i]['perDay_late_fee'] == 0:
                            # print("Is NULL!!")
                            perDay_late_fee = 0
                        else:
                            perDay_late_fee = response['result'][i]['perDay_late_fee']
                        # print("perDay_late_fee: ", perDay_late_fee, type(perDay_late_fee))

                        # Check if late_by is NONE
                        # print(response['result'][i]['late_by'])
                        if response['result'][i]['late_by'] is None or response['result'][i]['late_by'] == 0:
                            # print("Is NULL!!")
                            late_by = 1
                        else:
                            late_by = response['result'][i]['late_by']
                        # print("late_by: ", late_by, type(late_by))

                        

                        # Check if tenant responsiblity is NONE
                        # print("What is in the db: ", response['result'][i]['lt_responsibility'])
                        if response['result'][i]['lt_responsibility'] is None:
                            # print("Is NULL!!")
                            responsible_percent = 1.0
                        else:
                            responsible_percent = response['result'][i]['lt_responsibility']
                        # print("What we set programmatically: ", responsible_percent, type(responsible_percent))
                        charge = response['result'][i]['charge']
                        # print("Charge: ", charge, type(charge))
                        amt_due = float(charge)  * responsible_percent
                        # print("Amount due: ", amt_due)


                        # Establish payer, initiator and receiver
                        contract_uid = response['result'][i]['contract_uid']
                        property = response['result'][i]['lease_property_id']
                        tenant = response['result'][i]['lt_tenant_id']
                        owner = response['result'][i]['property_owner_id']
                        manager = response['result'][i]['contract_business_id']
                        fee_name = response['result'][i]['fee_name']
                        # print("Purchase Parameters: ", i, contract_uid, tenant, owner, manager)


                        # Common JSON Object Attributes
                        newRequest = {}
                        
                        newRequest['pur_timestamp'] = datetime.today().date().strftime("%m-%d-%Y")
                        newRequest['pur_property_id'] = property
                        newRequest['purchase_type'] = "Rent"
                        newRequest['pur_cf_type'] = "revenue"
                        newRequest['pur_amount_due'] = amt_due
                        newRequest['purchase_status'] = "UNPAID"
                        newRequest['pur_status_value'] = "0"
                        newRequest['pur_notes'] = fee_name

                        newRequest['pur_due_by'] = due_by
                        newRequest['pur_late_by'] = late_by
                        newRequest['pur_late_fee'] = late_fee
                        newRequest['pur_perDay_late_fee'] = perDay_late_fee

                        newRequest['purchase_date'] = datetime.today().date().strftime("%m-%d-%Y")
                        newRequest['pur_description'] = f"Rent for {due_date.strftime('%B')} {due_date.year} CRON"

                        
                        # Create JSON Object for Rent Purchase for Tenant-PM Payment
                        newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                        grouping = newRequestID
                        newRequest['purchase_uid'] = newRequestID
                        newRequest['pur_group'] = grouping
                        # print(newRequestID)
                        newRequest['pur_receiver'] = manager
                        newRequest['pur_payer'] = tenant
                        newRequest['pur_initiator'] = manager
                        newRequest['pur_due_date'] = due_date.date().strftime("%m-%d-%Y")
                        
                        
                        # print(newRequest)
                        # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
                        db.insert('purchases', newRequest)



                        # Create JSON Object for Rent Purchase for PM-Owner Payment
                        newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                        newRequest['purchase_uid'] = newRequestID
                        # print(newRequestID)
                        newRequest['pur_receiver'] = owner
                        newRequest['pur_payer'] = manager
                        newRequest['pur_initiator'] = manager
                        newRequest['pur_due_date'] = pm_due_date.date().strftime("%m-%d-%Y")
                        newRequest['pur_group'] = grouping
                    
                        # print(newRequest)
                        # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
                        db.insert('purchases', newRequest)




                        # For each entry posted to the purchases table, post any contract fees based on Rent
                        # Find contract fees based rent
                        manager_fees = db.execute("""
                                        SELECT -- *
                                            contract_uid, contract_property_id, contract_business_id
                                            -- , contract_start_date, contract_end_date
                                            , contract_fees
                                            -- , contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                            , jt.*
                                        FROM 
                                            space.contracts,
                                            JSON_TABLE(
                                                contract_fees,
                                                "$[*]" COLUMNS (
                                                    of_column VARCHAR(50) PATH "$.of",
                                                    charge_column VARCHAR(50) PATH "$.charge",
                                                    fee_name_column VARCHAR(50) PATH "$.fee_name",
                                                    fee_type_column VARCHAR(10) PATH "$.fee_type",
                                                    frequency_column VARCHAR(20) PATH "$.frequency"
                                                )
                                            ) AS jt
                                        -- WHERE contract_uid = '010-000003' AND of_column LIKE '%rent%';
                                        WHERE contract_uid = \'""" + contract_uid + """\' AND of_column LIKE '%rent%';
                                    """)
                        # print(manager_fees)
                        

                        for j in range(len(manager_fees['result'])):

                            # Check if fees is monthly 
                            if manager_fees['result'][j]['frequency_column'] == 'Monthly' or manager_fees['result'][j]['frequency_column'] == 'monthly':

                                # Check if charge is a % or Fixed $ Amount
                                if manager_fees['result'][j]['fee_type_column'] == '%' or manager_fees['result'][j]['fee_type_column'] == 'PERCENT':
                                    charge_amt = Decimal(manager_fees['result'][j]['charge_column']) * Decimal(amt_due) / 100
                                else:
                                    charge_amt = Decimal(manager_fees['result'][j]['charge_column'])
                                # print("Charge Amount: ", charge_amt, property, contract_uid, manager_fees['result'][j]['charge_column'], response['result'][i]['charge'] )

                                # Create JSON Object for Fee Purchase
                                newPMRequest = {}
                                newPMRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                                # print(newPMRequestID)
                                newPMRequest['purchase_uid'] = newPMRequestID
                                newPMRequest['pur_timestamp'] = datetime.today().date().strftime("%m-%d-%Y")
                                newPMRequest['pur_property_id'] = property
                                newPMRequest['purchase_type'] = "Management"
                                newPMRequest['pur_cf_type'] = "expense"
                                newPMRequest['pur_amount_due'] = charge_amt
                                newPMRequest['purchase_status'] = "UNPAID"
                                newPMRequest['pur_status_value'] = "0"
                                newPMRequest['pur_notes'] = manager_fees['result'][j]['fee_name_column']
                                newPMRequest['pur_description'] =  f"{manager_fees['result'][j]['fee_name_column']} for {due_date.strftime('%B')} {due_date.year} "
                                # newPMRequest['pur_description'] =  newRequestID # Original Rent Purchase ID  
                                # newPMRequest['pur_description'] = f"Fees for MARCH {nextMonth.year} CRON"
                                newPMRequest['pur_receiver'] = manager
                                newPMRequest['pur_payer'] = owner
                                newPMRequest['pur_initiator'] = manager
                                newPMRequest['purchase_date'] = datetime.today().date().strftime("%m-%d-%Y")
                                newPMRequest['pur_group'] = grouping

                                # *********
                                newPMRequest['pur_due_date'] = due_date.date().strftime("%m-%d-%Y")
                                # newPMRequest['pur_due_date'] = datetime(nextMonth.year, nextMonth.month, due_by).date().strftime("%m-%d-%Y")
                                # newPMRequest['pur_due_date'] = datetime(nextMonth.year, 1, due_by).date().strftime("%m-%d-%Y")
                                
                                # print(newPMRequest)
                                print("Number of CRON Purchases: ", numCronPurchases, dt)
                                db.insert('purchases', newPMRequest)

                                # For each fee, post to purchases table

            response["cron_job"] = {'message': f'Successfully completed CRON Job for {dt}' ,
                        'rows affected': f'{numCronPurchases}',
                    'code': 200}
            
            try:
                recipient = "pmarathay@gmail.com"
                subject = "MySpace Monthly Rent CRON JOB Completed "
                body = "Monthly Rent CRON JOB"
                # mail.send(msg)
                sendEmail(recipient, subject, body)

                response["email"] = {'message': f'CRON Job Email for {dt} sent!' ,
                    'code': 500}

            except:
                response["email fail"] = {'message': f'CRON Job Email for {dt} could not be sent' ,
                    'code': 500}
                
        except:
            response["cron fail"] = {'message': f'CRON Job failed for {dt}' ,
                    'code': 500}
            try:
                recipient = "pmarathay@gmail.com"
                subject = "MySpace Monthly Rent CRON JOB Failed!"
                body = "Monthly Rent CRON JOB Failed"
                # mail.send(msg)
                sendEmail(recipient, subject, body)

                response["email"] = {'message': f'CRON Job Fail Email for {dt} sent!' ,
                    'code': 500}

            except:
                response["email fail"] = {'message': f'CRON Job Fail Email for {dt} could not be sent' ,
                    'code': 500}

        return response

class LateFees_CLASS(Resource):
    def get(self):
        print("In Late Fees")

        numCronPurchases = 0
        numCronUpdates = 0

        # Establish current day, month and year
        dt = date.today()
        month = dt.month
        year = dt.year
        nextmonth = (dt.replace(day=28) + timedelta(days=4)).replace(day=1).strftime("%m-%d-%Y")
        print(dt, type(dt), month, type(month), year, type(year), nextmonth, type(nextmonth))

        try:

            # FIND ALL Rents that are UNPAID OR PARTIALLY PAID
            with connect() as db:
                response = db.execute("""
                    -- DETERMINE WHICH RENTS ARE PAID OR PARTIALLY PAID
                    SELECT *
                    FROM space.purchases
                    LEFT JOIN space.contracts ON contract_property_id = pur_property_id
                    LEFT JOIN space.property_owner ON property_id = pur_property_id
                    WHERE purchase_type = "RENT" AND
                        contract_status = "ACTIVE" AND
                        (purchase_status = "UNPAID" OR purchase_status = "PARTIALLY PAID") AND 
                        SUBSTRING(pur_payer, 1, 3) = '350';
                    """)

            # EXTRACT KEY DATES FOR EACH UNPAID RENT
                for i in range(len(response['result'])):
                    purchase_uid = response['result'][i]['purchase_uid']
                    property_id = response['result'][i]['pur_property_id']
                    description = response['result'][i]['pur_description']
                    print("\nNext Row: ", i, purchase_uid, property_id, description)

                    # PAYMENT DATES
                    # Set Due Date - If None set to due on 1st day of the Month
                    print("pur_due_date: ", response['result'][i]['pur_due_date'], type(response['result'][i]['pur_due_date']))
                    due_by_str = response['result'][i]['pur_due_date'] if response['result'][i]['pur_due_date'] else nextmonth
                    print("Due by STR: ", due_by_str, type(due_by_str))
                    due_by = datetime.strptime(due_by_str, "%m-%d-%Y").date()
                    print("Due by: ", due_by, type(due_by))

                    # Set Late By Date - If None set to late after 1 day
                    late_by = int(response['result'][i]['pur_late_by'] if response['result'][i]['pur_late_by'] else 1)
                    # print("Late by: ", late_by, type(late_by))
                    late_date = due_by + timedelta(days=late_by)
                    print("Late Date: ", late_date, type(late_date))

                    # Set Previous Day for Late Fee Calculations
                    yesterday = dt - timedelta(days=1) 
                    # print("previous_day: ", yesterday, type(yesterday) )

                    # Number of Days Late
                    numDays = (yesterday - late_date).days
                    # print("Number of Days Late: ", numDays, type(numDays))

                    # Set Date for PM to Pay Late Fees to Owner
                    pm_due_date = dt + timedelta(days=30)
                    # print("pm_due_date: ", pm_due_date, type(pm_due_date) )

                    # print("Due, Late, Yesterday, NumDays, PM_Due: ", due_by, late_date, yesterday, numDays, pm_due_date)


            # DETERMINE IF UNPAID RENT IS LATE
                    if late_date < dt:
                        print("Rent is late!", purchase_uid, property_id)


            # EXTRACT KEY PARAMETERS FOR EACH UNPAID RENT
                        # PAYMENT PARAMTERS
                        rent_due = response['result'][i]['pur_amount_due']
                        one_time_late_fee = response['result'][i]['pur_late_Fee']
                        per_day_late_fee = response['result'][i]['pur_perDay_late_fee']
                        purchase_notes = response['result'][i]['pur_notes']
                        purchase_description = response['result'][i]['pur_description']
                        fees = json.loads(response['result'][i]['contract_fees'])
                        # PAYMENT PARTIES
                        tenant = response['result'][i]['pur_payer']
                        owner = response['result'][i]['property_owner_id']
                        manager = response['result'][i]['contract_business_id']
                    
                        # print("\nPurchase UID: ", purchase_uid, type(purchase_uid))
                        # print("Property id: ", property_id, type(property_id) )
                        # print("Payment Description: ", description, type(description))
                        # print("Payment Amount Due: ", rent_due, type(rent_due))
                        # print("Lease Late Fees: ", one_time_late_fee, type(one_time_late_fee), per_day_late_fee, type(per_day_late_fee))
                        # print("Purchase Notes: ", purchase_notes, purchase_description)
                        # print("PM Contract Fees: ", fees, type(fees))
                        # print("Tenant, Owner, PM: ", tenant, owner, manager, type(manager))

            # CALCULATE THE LATE FEE AMOUNT
                        late_fee = round(float(one_time_late_fee) + float(per_day_late_fee) * numDays, 2)
                        # print("Late Fee: ", late_fee, type(late_fee))


            # FIND ALL ROWS THAT ALREADY EXIST FOR THIS LATE FEE (IE DESCRIPTION MATCHES PURCHASE_ID)
                        # Run Query to get all late fees
                        lateFees = db.execute("""
                                -- DETERMINE WHICH LATE FEES ALREADY EXIST
                                SELECT *
                                FROM space.purchases    
                                WHERE purchase_type = "LATE FEE" AND
                                    (purchase_status = "UNPAID" OR purchase_status = "PARTIALLY PAID")
                                """)
                        # print("\n",lateFees['result'][0:11], type(lateFees))


            # UPDATE APPRORIATE ROWS
                        putFlag = 0
                        if len(lateFees['result']) > 0 and late_fee > 0:
                            for j in range(len(lateFees['result'])):
                                # print(lateFees['result'][j]['pur_description'])
                                if  purchase_uid == lateFees['result'][j]['pur_description']:
                                    putFlag = putFlag + 1
                                    # print("\nFound Matching Entry ", putFlag, lateFees['result'][j]['pur_notes'])
                                    # print("Entire Row: ", lateFees['result'][j])
                                    payer = lateFees['result'][j]['pur_payer']
                                    receiver = lateFees['result'][j]['pur_receiver']
                                    key = {'purchase_uid': lateFees['result'][j]['purchase_uid']}
                                    if payer[0:3] == '350' or payer[0:3] == '600':
                                        payload = {'pur_amount_due': late_fee}
                                        # print(key, payload)

                                        response['purchase_table_update'] = db.update('space.purchases', key, payload)
                                        # print("updated ", key, payload)
                                        numCronUpdates = numCronUpdates + 1
                                        # print(response)
                                    elif payer[0:3] == '110':                                   
                                        # print("Figure out what the appropriate Fee split is", purchase_notes )
                                        for fee in fees:
                                            # Extract only the monthly fees
                                            if 'fee_type' in fee and (fee['frequency'] == 'Monthly' or fee['frequency'] == 'monthly') and fee['charge'] != "" and (fee['fee_type'] == "%" or fee['fee_type'] == "PERCENT") and fee['fee_name'] == lateFees['result'][j]['pur_notes']:
                                                charge = fee['charge']
                                                charge_type = fee['fee_type']
                                                # print("\nCharge: ", charge, charge_type)

                                                amount_due = float(late_fee) * float(charge) / 100
                                                payload = {'pur_amount_due': amount_due}
                                                # print(key, payload)

                                                response['purchase_table_update'] = db.update('space.purchases', key, payload)
                                                numCronUpdates = numCronUpdates + 1
                                                # print("Updated PM", key, payload)
                                    else:
                                        print("No Match Found: ", payer)
                            
                                continue
            # INSERT NEW ROWS IF THIS IS THE FIRST TIME LATE FEES ARE ASSESSED
                        
                        if putFlag == 0 and late_fee > 0:
                            print("PUT Flag: ", putFlag, "New Late Fee for: ", i, purchase_uid)

                            # Create JSON Object for Rent Purchase
                            newRequest = {}
                            newRequestID = db.call('space.new_purchase_uid')['result'][0]['new_id']
                            grouping = newRequestID
                            # print(newRequestID)

                            # Common JSON Object Attributes
                            newRequest['purchase_uid'] = newRequestID
                            newRequest['pur_group'] = grouping
                            # newRequest['pur_timestamp'] = datetime.today().date().strftime("%m-%d-%Y")
                            newRequest['pur_timestamp'] = dt.strftime("%m-%d-%Y")
                            newRequest['pur_property_id'] = property_id
                            newRequest['purchase_type'] = "Late Fee"
                            newRequest['pur_cf_type'] = "revenue"
                            newRequest['pur_amount_due'] = late_fee
                            newRequest['purchase_status'] = "UNPAID"
                            newRequest['pur_status_value'] = "0"
                            

                            newRequest['pur_due_by'] = 1
                            newRequest['pur_late_by'] = 90
                            newRequest['pur_late_fee'] = 0
                            newRequest['pur_perDay_late_fee'] = 0

                            # newRequest['purchase_date'] = datetime.today().date().strftime("%m-%d-%Y")
                            newRequest['purchase_date'] = dt.strftime("%m-%d-%Y")
                            newRequest['pur_description'] = purchase_uid
                            
                        
                            # Create JSON Object for Rent Purchase for Tenant-PM Payment
                            newRequest['pur_receiver'] = manager
                            newRequest['pur_payer'] = tenant
                            newRequest['pur_initiator'] = manager
                            # newRequest['pur_due_date'] = datetime.today().date().strftime("%m-%d-%Y")
                            newRequest['pur_due_date'] = dt.strftime("%m-%d-%Y")
                            

                            if numDays ==  0:
                                # print("\n", "Late Today")
                                newRequest['pur_notes'] = "One Time Late Fee Applied"
                            else:
                                newRequest['pur_notes'] = "One Time Late Fee and Per Day Late Fee Applied"
                                # newRequest['pur_notes'] = f"Late for { calendar.month_name[nextMonth.month]} {nextMonth.year} {response['result'][i]['purchase_uid']}"

                            # print("\nInsert Tenant to Property Manager Late Fee")
                            db.insert('space.purchases', newRequest)
                            numCronPurchases = numCronPurchases + 1
                            # print("Inserted into db: ", newRequest)


                            # Create JSON Object for Rent Purchase for PM-Owner Payment
                            newRequestID = db.call('space.new_purchase_uid')['result'][0]['new_id']
                            newRequest['purchase_uid'] = newRequestID
                            # print(newRequestID)
                            newRequest['pur_receiver'] = owner
                            newRequest['pur_payer'] = manager

                                
                            # print(newRequest)
                            # print("\nPurchase Parameters: ", i, newRequestID, grouping, tenant, owner, manager)
                            # print("\nInsert Property Manager to Owner Late Fee")
                            db.insert('space.purchases', newRequest)
                            numCronPurchases = numCronPurchases + 1

                            
                            # Create JSON Object for Rent Purchase for Owner-PM Payment
                            # Determine Split between PM and Owner
                            # print("\n  Contract Fees", response['result'][i]['contract_fees'])
                            fees = json.loads(response['result'][i]['contract_fees'])
                            # print("\nFees: ", fees, type(fees))

                            for fee in fees:
                                # print(fee)
                                # Extract only the monthly fees
                                if 'fee_type' in fee and (fee['frequency'] == 'Monthly' or fee['frequency'] == 'monthly') and fee['charge'] != "" and (fee['fee_type'] == "%" or fee['fee_type'] == "PERCENT"):
                                    charge = fee['charge']
                                    charge_type = fee['fee_type']
                                    # print("\nCharge: ", charge, charge_type)

                                    # Use this fee to create an Owner-PM late Fee PUT OR PST 
                                    # Create JSON Object for Rent Purchase for PM-Owner Payment
                                    newRequestID = db.call('space.new_purchase_uid')['result'][0]['new_id']
                                    newRequest['purchase_uid'] = newRequestID
                                    # print(newRequestID)
                                    newRequest['pur_receiver'] = manager
                                    newRequest['pur_payer'] = owner
                                    newRequest['pur_cf_type'] = "expense"
                                    newRequest['purchase_type'] = "Management - Late Fees"
                                    newRequest['pur_notes'] = fee['fee_name']
                                    newRequest['pur_amount_due'] = float(late_fee) * float(charge) / 100
                                    
                                    # print(newRequest)
                                    # print("Purchase Parameters: ", i, newRequestID, grouping, tenant, owner, manager)
                                    # print("\nInsert Owner to Property Manager Late Fee")
                                    db.insert('space.purchases', newRequest)  
                                    numCronPurchases = numCronPurchases + 1     


            print(f"Late Fee CRON job for {dt} completed. {numCronPurchases} rows added. {numCronUpdates} rows updated.")
            response["cron_job"] = {'message': f'Successfully completed LATE FEE CRON Job for {dt}' ,
                        'rows added': f'{numCronPurchases}', 'rows updated': f'{numCronUpdates}',
                    'code': 200}
            
            try:
                recipient = "pmarathay@gmail.com"
                subject = "MySpace LATE FEE CRON JOB Completed "
                body = "LATE FEE CRON JOB"
                # mail.send(msg)
                sendEmail(recipient, subject, body)

                response["email"] = {'message': f'LATE FEE CRON Job Email for {dt} sent!' ,
                    'code': 500}

            except:
                response["email fail"] = {'message': f'LATE FEE CRON Job Email for {dt} could not be sent' ,
                    'code': 500}
                
        except:
            response["cron fail"] = {'message': f'LATE FEE CRON Job failed for {dt}' ,
                    'code': 500}
            try:
                recipient = "pmarathay@gmail.com"
                subject = "MySpace LATE FEE CRON JOB Failed!"
                body = "LATE FEE CRON JOB Failed"
                # mail.send(msg)
                sendEmail(recipient, subject, body)

                response["email"] = {'message': f'LATE FEE CRON Job Fail Email for {dt} sent!' ,
                    'code': 500}

            except:
                response["email fail"] = {'message': f'LATE FEE CRON Job Fail Email for {dt} could not be sent' ,
                    'code': 500}


        return response

def LateFees_CRON(self):
        print("In Late Fees")

        numCronPurchases = 0
        numCronUpdates = 0

        # Establish current day, month and year
        dt = date.today()
        month = dt.month
        year = dt.year
        nextmonth = (dt.replace(day=28) + timedelta(days=4)).replace(day=1).strftime("%m-%d-%Y")
        print(dt, type(dt), month, type(month), year, type(year), nextmonth, type(nextmonth))

        try:

            # FIND ALL Rents that are UNPAID OR PARTIALLY PAID
            with connect() as db:
                response = db.execute("""
                    -- DETERMINE WHICH RENTS ARE PAID OR PARTIALLY PAID
                    SELECT *
                    FROM space.purchases
                    LEFT JOIN space.contracts ON contract_property_id = pur_property_id
                    LEFT JOIN space.property_owner ON property_id = pur_property_id
                    WHERE purchase_type = "RENT" AND
                        contract_status = "ACTIVE" AND
                        (purchase_status = "UNPAID" OR purchase_status = "PARTIALLY PAID") AND 
                        SUBSTRING(pur_payer, 1, 3) = '350';
                    """)

            # EXTRACT KEY DATES FOR EACH UNPAID RENT
                for i in range(len(response['result'])):
                    purchase_uid = response['result'][i]['purchase_uid']
                    property_id = response['result'][i]['pur_property_id']
                    description = response['result'][i]['pur_description']
                    # print("\nNext Row: ", i, purchase_uid, property_id, description)

                    # PAYMENT DATES
                    # Set Due Date - If None set to due on 1st day of the Month
                    due_by_str = response['result'][i]['pur_due_date'] if response['result'][i]['pur_due_date'] else nextmonth
                    due_by = datetime.strptime(due_by_str, "%m-%d-%Y").date()
                    # print("Due by: ", due_by, type(due_by))

                    # Set Late By Date - If None set to late after 1 day
                    late_by = int(response['result'][i]['pur_late_by'] if response['result'][i]['pur_late_by'] else 1)
                    # print("Late by: ", late_by, type(late_by))
                    late_date = due_by + timedelta(days=late_by)
                    # print("Late Date: ", late_date, type(late_date))

                    # Set Previous Day for Late Fee Calculations
                    yesterday = dt - timedelta(days=1) 
                    # print("previous_day: ", yesterday, type(yesterday) )

                    # Number of Days Late
                    numDays = (yesterday - late_date).days
                    # print("Number of Days Late: ", numDays, type(numDays))

                    # Set Date for PM to Pay Late Fees to Owner
                    pm_due_date = dt + timedelta(days=30)
                    # print("pm_due_date: ", pm_due_date, type(pm_due_date) )

                    # print("Due, Late, Yesterday, NumDays, PM_Due: ", due_by, late_date, yesterday, numDays, pm_due_date)


            # DETERMINE IF UNPAID RENT IS LATE
                    if late_date < dt:
                        # print("Rent is late!")


            # EXTRACT KEY PARAMETERS FOR EACH UNPAID RENT
                        # PAYMENT PARAMTERS
                        rent_due = response['result'][i]['pur_amount_due']
                        one_time_late_fee = response['result'][i]['pur_late_Fee']
                        per_day_late_fee = response['result'][i]['pur_perDay_late_fee']
                        purchase_notes = response['result'][i]['pur_notes']
                        purchase_description = response['result'][i]['pur_description']
                        fees = json.loads(response['result'][i]['contract_fees'])
                        # PAYMENT PARTIES
                        tenant = response['result'][i]['pur_payer']
                        owner = response['result'][i]['property_owner_id']
                        manager = response['result'][i]['contract_business_id']
                    
                        # print("\nPurchase UID: ", purchase_uid, type(purchase_uid))
                        # print("Property id: ", property_id, type(property_id) )
                        # print("Payment Description: ", description, type(description))
                        # print("Payment Amount Due: ", rent_due, type(rent_due))
                        # print("Lease Late Fees: ", one_time_late_fee, type(one_time_late_fee), per_day_late_fee, type(per_day_late_fee))
                        # print("Purchase Notes: ", purchase_notes, purchase_description)
                        # print("PM Contract Fees: ", fees, type(fees))
                        # print("Tenant, Owner, PM: ", tenant, owner, manager, type(manager))

            # CALCULATE THE LATE FEE AMOUNT
                        late_fee = round(float(one_time_late_fee) + float(per_day_late_fee) * numDays, 2)
                        # print("Late Fee: ", late_fee, type(late_fee))


            # FIND ALL ROWS THAT ALREADY EXIST FOR THIS LATE FEE (IE DESCRIPTION MATCHES PURCHASE_ID)
                        # Run Query to get all late fees
                        lateFees = db.execute("""
                                -- DETERMINE WHICH LATE FEES ALREADY EXIST
                                SELECT *
                                FROM space.purchases    
                                WHERE purchase_type = "LATE FEE" AND
                                    (purchase_status = "UNPAID" OR purchase_status = "PARTIALLY PAID")
                                """)
                        # print("\n",lateFees['result'][0:11], type(lateFees))


            # UPDATE APPRORIATE ROWS
                        putFlag = 0
                        if len(lateFees['result']) > 0 and late_fee > 0:
                            for j in range(len(lateFees['result'])):
                                # print(lateFees['result'][j]['pur_description'])
                                if  purchase_uid == lateFees['result'][j]['pur_description']:
                                    putFlag = putFlag + 1
                                    # print("\nFound Matching Entry ", putFlag, lateFees['result'][j]['pur_notes'])
                                    # print("Entire Row: ", lateFees['result'][j])
                                    payer = lateFees['result'][j]['pur_payer']
                                    receiver = lateFees['result'][j]['pur_receiver']
                                    key = {'purchase_uid': lateFees['result'][j]['purchase_uid']}
                                    if payer[0:3] == '350' or payer[0:3] == '600':
                                        payload = {'pur_amount_due': late_fee}
                                        # print(key, payload)

                                        response['purchase_table_update'] = db.update('space.purchases', key, payload)
                                        # print("updated ", key, payload)
                                        numCronUpdates = numCronUpdates + 1
                                        # print(response)
                                    elif payer[0:3] == '110':                                   
                                        # print("Figure out what the appropriate Fee split is", purchase_notes )
                                        for fee in fees:
                                            # Extract only the monthly fees
                                            if 'fee_type' in fee and (fee['frequency'] == 'Monthly' or fee['frequency'] == 'monthly') and fee['charge'] != "" and (fee['fee_type'] == "%" or fee['fee_type'] == "PERCENT") and fee['fee_name'] == lateFees['result'][j]['pur_notes']:
                                                charge = fee['charge']
                                                charge_type = fee['fee_type']
                                                # print("\nCharge: ", charge, charge_type)

                                                amount_due = float(late_fee) * float(charge) / 100
                                                payload = {'pur_amount_due': amount_due}
                                                # print(key, payload)

                                                response['purchase_table_update'] = db.update('space.purchases', key, payload)
                                                numCronUpdates = numCronUpdates + 1
                                                # print("Updated PM", key, payload)
                                    else:
                                        print("No Match Found: ", payer)
                            
                                continue
            # INSERT NEW ROWS IF THIS IS THE FIRST TIME LATE FEES ARE ASSESSED
                        
                        if putFlag == 0 and late_fee > 0:
                            print("PUT Flag: ", putFlag, "New Late Fee for: ", i, purchase_uid)

                            # Create JSON Object for Rent Purchase
                            newRequest = {}
                            newRequestID = db.call('space.new_purchase_uid')['result'][0]['new_id']
                            grouping = newRequestID
                            # print(newRequestID)

                            # Common JSON Object Attributes
                            newRequest['purchase_uid'] = newRequestID
                            newRequest['pur_group'] = grouping
                            # newRequest['pur_timestamp'] = datetime.today().date().strftime("%m-%d-%Y")
                            newRequest['pur_timestamp'] = dt.strftime("%m-%d-%Y")
                            newRequest['pur_property_id'] = property_id
                            newRequest['purchase_type'] = "Late Fee"
                            newRequest['pur_cf_type'] = "revenue"
                            newRequest['pur_amount_due'] = late_fee
                            newRequest['purchase_status'] = "UNPAID"
                            newRequest['pur_status_value'] = "0"
                            

                            newRequest['pur_due_by'] = 1
                            newRequest['pur_late_by'] = 90
                            newRequest['pur_late_fee'] = 0
                            newRequest['pur_perDay_late_fee'] = 0

                            # newRequest['purchase_date'] = datetime.today().date().strftime("%m-%d-%Y")
                            newRequest['purchase_date'] = dt.strftime("%m-%d-%Y")
                            newRequest['pur_description'] = purchase_uid
                            
                        
                            # Create JSON Object for Rent Purchase for Tenant-PM Payment
                            newRequest['pur_receiver'] = manager
                            newRequest['pur_payer'] = tenant
                            newRequest['pur_initiator'] = manager
                            # newRequest['pur_due_date'] = datetime.today().date().strftime("%m-%d-%Y")
                            newRequest['pur_due_date'] = dt.strftime("%m-%d-%Y")
                            

                            if numDays ==  0:
                                # print("\n", "Late Today")
                                newRequest['pur_notes'] = "One Time Late Fee Applied"
                            else:
                                newRequest['pur_notes'] = "One Time Late Fee and Per Day Late Fee Applied"
                                # newRequest['pur_notes'] = f"Late for { calendar.month_name[nextMonth.month]} {nextMonth.year} {response['result'][i]['purchase_uid']}"

                            # print("\nInsert Tenant to Property Manager Late Fee")
                            db.insert('space.purchases', newRequest)
                            numCronPurchases = numCronPurchases + 1
                            # print("Inserted into db: ", newRequest)


                            # Create JSON Object for Rent Purchase for PM-Owner Payment
                            newRequestID = db.call('space.new_purchase_uid')['result'][0]['new_id']
                            newRequest['purchase_uid'] = newRequestID
                            # print(newRequestID)
                            newRequest['pur_receiver'] = owner
                            newRequest['pur_payer'] = manager

                                
                            # print(newRequest)
                            # print("\nPurchase Parameters: ", i, newRequestID, grouping, tenant, owner, manager)
                            # print("\nInsert Property Manager to Owner Late Fee")
                            db.insert('space.purchases', newRequest)
                            numCronPurchases = numCronPurchases + 1

                            
                            # Create JSON Object for Rent Purchase for Owner-PM Payment
                            # Determine Split between PM and Owner
                            # print("\n  Contract Fees", response['result'][i]['contract_fees'])
                            fees = json.loads(response['result'][i]['contract_fees'])
                            # print("\nFees: ", fees, type(fees))

                            for fee in fees:
                                # print(fee)
                                # Extract only the monthly fees
                                if 'fee_type' in fee and (fee['frequency'] == 'Monthly' or fee['frequency'] == 'monthly') and fee['charge'] != "" and (fee['fee_type'] == "%" or fee['fee_type'] == "PERCENT"):
                                    charge = fee['charge']
                                    charge_type = fee['fee_type']
                                    # print("\nCharge: ", charge, charge_type)

                                    # Use this fee to create an Owner-PM late Fee PUT OR PST 
                                    # Create JSON Object for Rent Purchase for PM-Owner Payment
                                    newRequestID = db.call('space.new_purchase_uid')['result'][0]['new_id']
                                    newRequest['purchase_uid'] = newRequestID
                                    # print(newRequestID)
                                    newRequest['pur_receiver'] = manager
                                    newRequest['pur_payer'] = owner
                                    newRequest['pur_cf_type'] = "expense"
                                    newRequest['purchase_type'] = "Management - Late Fees"
                                    newRequest['pur_notes'] = fee['fee_name']
                                    newRequest['pur_amount_due'] = float(late_fee) * float(charge) / 100
                                    
                                    # print(newRequest)
                                    # print("Purchase Parameters: ", i, newRequestID, grouping, tenant, owner, manager)
                                    # print("\nInsert Owner to Property Manager Late Fee")
                                    db.insert('space.purchases', newRequest)  
                                    numCronPurchases = numCronPurchases + 1     


            print(f"Late Fee CRON job for {dt} completed. {numCronPurchases} rows added. {numCronUpdates} rows updated.")
            response["cron_job"] = {'message': f'Successfully completed LATE FEE CRON Job for {dt}' ,
                        'rows added': f'{numCronPurchases}', 'rows updated': f'{numCronUpdates}',
                    'code': 200}
            
            try:
                recipient = "pmarathay@gmail.com"
                subject = "MySpace LATE FEE CRON JOB Completed "
                body = "LATE FEE CRON JOB"
                # mail.send(msg)
                sendEmail(recipient, subject, body)

                response["email"] = {'message': f'LATE FEE CRON Job Email for {dt} sent!' ,
                    'code': 500}

            except:
                response["email fail"] = {'message': f'LATE FEE CRON Job Email for {dt} could not be sent' ,
                    'code': 500}
                
        except:
            response["cron fail"] = {'message': f'LATE FEE CRON Job failed for {dt}' ,
                    'code': 500}
            try:
                recipient = "pmarathay@gmail.com"
                subject = "MySpace LATE FEE CRON JOB Failed!"
                body = "LATE FEE CRON JOB Failed"
                # mail.send(msg)
                sendEmail(recipient, subject, body)

                response["email"] = {'message': f'LATE FEE CRON Job Fail Email for {dt} sent!' ,
                    'code': 500}

            except:
                response["email fail"] = {'message': f'LATE FEE CRON Job Fail Email for {dt} could not be sent' ,
                    'code': 500}


        return response

  

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
api.add_resource(Appliances_SB, '/appliancesSB')
# api.add_resource(RemoveAppliance, "/RemoveAppliance")



api.add_resource(Bills, '/bills','/bills/<string:user_id>')
api.add_resource(Contracts, '/contracts', '/contracts/<string:user_id>')
api.add_resource(AddExpense, '/addExpense')
api.add_resource(AddRevenue, '/addRevenue')
api.add_resource(Cashflow, '/cashflow/<string:user_id>/<string:year>')
api.add_resource(CashflowSimplified, '/cashflowSimplified/<string:user_id>')
api.add_resource(CashflowSummary, '/cashflowSummary/<string:user_id>')
# api.add_resource(CashflowRevised, '/cashflowRevised/<string:user_id>/<string:type>')
# api.add_resource(CashflowRevised, '/cashflowRevised/<string:user_id>/<string:month>/<string:year>')
api.add_resource(CashflowRevised, '/cashflowRevised/<string:user_id>')


api.add_resource(AllTransactions, '/allTransactions/<string:uid>')


api.add_resource(Profile, '/profile/<string:user_id>', '/profile' )

# Can we delete these?
# api.add_resource(OwnerProfileByOwnerUid, '/ownerProfile/<string:owner_id>')
# api.add_resource(TenantProfileByTenantUid, '/tenantProfile/<string:tenant_id>')
# api.add_resource(BusinessProfileByUid, '/businessProfile/<string:business_uid>')
# api.add_resource(OwnerProfile, '/ownerProfile')  # POST, PUT OwnerProfile
# api.add_resource(TenantProfile, '/tenantProfile')
api.add_resource(BusinessProfile, '/businessProfile')
# api.add_resource(BusinessProfileWeb, '/businessProfileWeb')



api.add_resource(Employee, '/employee','/employee/<string:user_id>')
api.add_resource(EmployeeVerification, '/employeeVerification')
api.add_resource(OwnerDocuments, '/ownerDocuments/<string:owner_id>')
api.add_resource(TenantDocuments, '/tenantDocuments/<string:tenant_id>')
api.add_resource(QuoteDocuments, '/quoteDocuments', '/quoteDocuments/<string:quote_id>')
api.add_resource(Documents, '/documents','/documents/<string:user_id>')
api.add_resource(LeaseDetails, '/leaseDetails/<string:filter_id>', '/leaseDetails')
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
api.add_resource(PeriodicPurchases_CLASS, '/periodicPurchase')
api.add_resource(RentPurchase, '/rentPurchase')
api.add_resource(LateFees_CLASS, '/LateFees')

# api.add_resource(ExtendLease, '/ExtendLease')
# api.add_resource(MonthlyRent_CLASS, '/MonthlyRent')

api.add_resource(SendEmail_CLASS, "/sendEmail_CLASS")
api.add_resource(BusinessProfileList, "/businessProfileList/<string:business_type>")
api.add_resource(SendEmail, "/sendEmail")

api.add_resource(UserInfo, "/userInfo/<string:user_id>", "/userInfo")

# refresh
# api.add_resource(Refresh, '/refresh')

# socialLogin
# api.add_resource(UserSocialLogin, '/userSocialLogin/<string:email>')
# api.add_resource(UserSocialSignup, '/userSocialSignup')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000)
