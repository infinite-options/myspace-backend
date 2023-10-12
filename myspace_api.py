# MANIFEST MY SPACE (PROPERTY MANAGEMENT) BACKEND PYTHON FILE
# https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/<enter_endpoint_details>
from announcements import Announcements, AnnouncementsByUserId
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

from dashboard import ownerDashboard, managerDashboard, tenantDashboard, maintenanceDashboard

from rents import Rents, RentDetails
from payments import Payments, PaymentStatus, PaymentMethod, RequestPayment
from properties import Properties, PropertiesByOwner, PropertyDashboardByOwner
from transactions import AllTransactions, TransactionsByOwner, TransactionsByOwnerByProperty
from cashflow import CashflowByOwner
from employees import Employee
from profiles import OwnerProfile, OwnerProfileByOwnerUid, TenantProfile, TenantProfileByTenantUid, BusinessProfile, \
    BusinessProfileByUid
from documents import OwnerDocuments, TenantDocuments
from leases import LeaseDetails
from purchases import Bills, AddExpense, AddRevenue, RentPurchase
from maintenance import MaintenanceStatus, MaintenanceStatusByProperty, MaintenanceByProperty, \
    MaintenanceRequests, MaintenanceReq, MaintenanceSummaryByOwner, \
    MaintenanceSummaryAndStatusByOwner, MaintenanceQuotes, MaintenanceQuotesByUid, MaintenanceDashboardPost
from purchases import Bills, AddExpense, AddRevenue
# from cron import RentPurchaseTest
# from maintenance import MaintenanceStatusByProperty, MaintenanceByProperty,  \
#     MaintenanceRequestsByOwner, MaintenanceRequests, MaintenanceSummaryByOwner, \
#     MaintenanceSummaryAndStatusByOwner, MaintenanceQuotes, MaintenanceQuotesByUid, MaintenanceDashboardPOST
from contacts import ContactsMaintenance, ContactsOwnerContactsDetails, ContactsBusinessContacts, ContactsBusinessContactsOwnerDetails, ContactsBusinessContactsTenantDetails, ContactsBusinessContactsMaintenanceDetails, ContactsOwnerManagerDetails, ContactsMaintenanceManagerDetails, ContactsMaintenanceTenantDetails
from contracts import Contracts, ContractsByBusiness
from settings import Account
from lists import List
from managers import SearchManager
from status_update import StatusUpdate
from quotes import QuotesByBusiness, QuotesStatusByBusiness, QuotesByRequest, Quotes

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
# from dotenv import load_dotenv

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

# class TenantDashboard(Resource):
#     def get(self, tenant_id):
#         response = {}
#         with connect() as db:
#             property = db.execute("""
#                     SELECT SUM(pur_amount_due) AS balance, 
#                         CAST(MIN(STR_TO_DATE(pur_due_date, '%Y-%m-%d')) AS CHAR) as earliest_due_date,
#                         p.property_uid, p.property_address, p.property_unit
#                     FROM space.properties p
#                         LEFT JOIN space.leases l ON l.lease_property_id = p.property_uid
#                         LEFT JOIN space.lease_tenant lt ON lt.lt_lease_id = l.lease_uid
#                         LEFT JOIN space.purchases pur ON p.property_uid = pur.pur_property_id
#                     WHERE pur.purchase_status = 'UNPAID' AND lt.lt_tenant_id = \'""" + tenant_id + """\'
#                     GROUP BY property_uid;
#                     """)
#             response["property"] = property
#             maintenance = db.execute("""
#                     SELECT mr.maintenance_images, mr.maintenance_title,
#                         mr.maintenance_request_status, mr.maintenance_priority,
#                         mr.maintenance_scheduled_date, mr.maintenance_scheduled_time
#                     FROM space.maintenanceRequests mr
#                         INNER JOIN space.properties p ON p.property_uid = mr.maintenance_property_id
#                     WHERE p.property_uid = \'""" + property['result'][0]['property_uid'] + """\';
#                     """)
#             response["maintenanceRequests"] = maintenance
#             announcements = db.execute("""
#                 SELECT * FROM announcements
#                 WHERE announcement_receiver LIKE '%""" + tenant_id + """%'
#                 AND (announcement_mode = 'Tenants' OR announcement_mode = 'Properties')
#                 AND announcement_properties LIKE  '%""" + property['result'][0]['property_uid'] + """%' """)
#             response["announcements"] = announcements
#             return response


# class managerDashboard(Resource):
#     def get(self,manager_id):
#         print('in Manager Dashboard')
#         response = {}

#         # print("Owner UID: ", owner_id)

#         with connect() as db:
#             print("in Manager dashboard")
#             maintenanceQuery = db.execute(""" 
#                     -- MAINTENANCE STATUS BY Manager
#                     SELECT -- * 
#                         contract_business_id
#                         , maintenance_request_status
#                         , COUNT(maintenance_request_status) AS num
#                     FROM space.maintenanceRequests
#                     LEFT JOIN space.b_details ON maintenance_property_id = contract_property_id
#                     WHERE contract_business_id = \'""" + manager_id + """\'
#                     GROUP BY maintenance_request_status;
#                     """)

#             # print("Query: ", maintenanceQuery)
#             response["MaintenanceStatus"] = maintenanceQuery

#             leaseQuery = db.execute(""" 
#                     -- LEASE STATUS BY USER
#                     SELECT b_details.contract_business_id
#                         , leases.lease_end
#                         , COUNT(lease_end) AS num
#                     FROM space.leases
#                     LEFT JOIN space.properties ON property_uid = lease_property_id
#                     LEFT JOIN space.leaseFees ON lease_uid = fees_lease_id
#                     LEFT JOIN space.leaseDocuments ON lease_uid = ld_lease_id
#                     LEFT JOIN space.t_details ON lease_uid = lt_lease_id
#                     LEFT JOIN space.b_details ON contract_property_id = lease_property_id
#                     WHERE lease_status = "ACTIVE"
#                         AND contract_status = "ACTIVE"
#                         AND fee_name = "RENT"
#                         AND ld_type = "LEASE"
#                         AND contract_business_id = \'""" + manager_id + """\'
#                     GROUP BY MONTH(lease_end),
#                             YEAR(lease_end); 
#                     """)

#             # print("lease Query: ", leaseQuery)
#             response["LeaseStatus"] = leaseQuery

#             rentQuery = db.execute(""" 
#                     -- RENT STATUS BY PROPERTY FOR OWNER DASHBOARD
#                     SELECT -- *,
#                         contract_business_id
#                         , rent_status
#                         , COUNT(rent_status) AS num
#                     FROM (
#                         SELECT b.contract_property_id, contract_business_id
#                             , pp_status.*
#                             , IF (ISNULL(payment_status), "VACANT", payment_status) AS rent_status
#                         FROM space.b_details AS b
#                         LEFT JOIN space.properties ON property_uid = b.contract_property_id
#                         LEFT JOIN space.pp_status ON pur_property_id = b.contract_property_id
#                         WHERE contract_business_id = \'""" + manager_id + """\'
#                             AND (purchase_type = "RENT" OR ISNULL(purchase_type))
#                             AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
#                             AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
#                         GROUP BY b.contract_property_id
#                         ) AS rs
#                     GROUP BY rent_status
#                                         """)

#             # print("rent Query: ", rentQuery)
#             response["RentStatus"] = rentQuery
#             # print(response)
#             return response


# class maintenanceDashboard(Resource):
#     def get(self, business_id):
#         print('in Maintenance Dashboard')
#         response = {}

#         # print("Owner UID: ", owner_id)

#         with connect() as db:
#             print("in owner dashboard")
#             currentActivity = db.execute(""" 
#                     -- CURRENT ACTIVITY
#                     SELECT *,
#                         COUNT(maintenance_status) AS num
#                         ,SUM(quote_total_estimate) AS total_estimate
#                     FROM (
#                         SELECT quote_business_id, quote_status, maintenance_request_status, quote_total_estimate
#                             , CASE
#                                     WHEN quote_status = "SENT" OR quote_status = "WITHDRAWN" OR quote_status = "REFUSED" OR quote_status = "REJECTED"  THEN "SUBMITTED"
#                                     WHEN quote_status = "ACCEPTED" OR quote_status = "SCHEDULE"   THEN "ACCEPTED"
#                                     WHEN quote_status = "SCHEDULED" OR quote_status = "RESCHEDULE"   THEN "SCHEDULED"
#                                     WHEN quote_status = "COMPLETED"   THEN "PAID"
#                                     ELSE quote_status
#                                 END AS maintenance_status
#                         FROM space.m_details
#                         WHERE quote_business_id = \'""" + business_id + """\'
#                         ) AS ms
#                     GROUP BY maintenance_status;
#                     """)

#             # print("Query: ", maintenanceQuery)
#             response["CurrentActivities"] = currentActivity

#             workOrders = db.execute(""" 
#                     -- WORK ORDERS
#                     SELECT *
#                     FROM (
#                         SELECT * -- , quote_business_id, quote_status, maintenance_request_status, quote_total_estimate
#                             , CASE
#                                     WHEN quote_status = "SENT" OR quote_status = "WITHDRAWN" OR quote_status = "REFUSED" OR quote_status = "REJECTED"  THEN "SUBMITTED"
#                                     WHEN quote_status = "ACCEPTED" OR quote_status = "SCHEDULE"   THEN "ACCEPTED"
#                                     WHEN quote_status = "SCHEDULED" OR quote_status = "RESCHEDULE"   THEN "SCHEDULED"
#                                     WHEN quote_status = "COMPLETED"   THEN "PAID"
#                                     ELSE quote_status
#                                 END AS maintenance_status
#                         FROM space.m_details
#                         WHERE quote_business_id = \'""" + business_id + """\'
#                             ) AS ms
#                     ORDER BY maintenance_status;
#                     """)

#             # print("Query: ", leaseQuery)
#             response["WorkOrders"] = workOrders

#             return response

#  -- ACTUAL ENDPOINTS    -----------------------------------------

# New APIs, uses connect() and disconnect()
# Create new api template URL
# api.add_resource(TemplateApi, '/api/v2/templateapi')

# Run on below IP address and port
# Make sure port number is unused (i.e. don't use numbers 0-1023)

# GET requests


# Dashboard Queries 
# Owner Dashboard: Maintenance,Lease, Rent, Vacancy, Cashflow.  Still need to Need to add Cashflow
api.add_resource(ownerDashboard, '/ownerDashboard/<string:owner_id>')
# Manager Dashboard: Maintenance,Lease, Rent, Vacancy, Cashflow.  Still need to Need to add Cashflow
api.add_resource(managerDashboard, '/managerDashboard/<string:manager_id>')
# Tenant Dashboard: Property Maintenance, Announcements
api.add_resource(tenantDashboard, '/tenantDashboard/<string:tenant_id>')
# Maintenance Dashboard: Current Activities, Work Orders
api.add_resource(maintenanceDashboard, '/maintenanceDashboard/<string:business_id>')


# Owner Queries


# Payment Queries
api.add_resource(PaymentStatus, '/paymentStatus/<string:user_id>')
api.add_resource(Payments, '/makePayment')


# Maintenance Queries
# Maintenance Status for Businesses (Property Manager and Maintenance Company) 
api.add_resource(MaintenanceStatus, '/maintenanceStatus/<string:uid>')
# Mainentance Requests is POST and PUT for new and modified maintenance requests
api.add_resource(MaintenanceRequests, '/maintenanceReq/<string:uid>', '/maintenanceRequests')
# Maintenance Req is Maintenance Status for Owner and Tenant
# api.add_resource(MaintenanceReq, '/maintenanceReq/<string:uid>')
# 



# api.add_resource(ownerDashboardProperties,
#                  '/ownerDashboardProperties/<string:owner_id>')



api.add_resource(Rents, '/rents/<string:owner_id>')
api.add_resource(RentDetails, '/rentDetails/<string:owner_id>')

api.add_resource(Properties, '/properties/<string:uid>', '/properties/' )
api.add_resource(PropertiesByOwner, '/propertiesByOwner/<string:owner_id>')
api.add_resource(PropertyDashboardByOwner,
                 '/propertyDashboardByOwner/<string:owner_id>')





# api.add_resource(MaintenanceRequestCount, '/maintenanceReqCount/<string:uid>')


# api.add_resource(MaintenanceRequestsByOwner,
#                  '/maintenanceRequestsByOwner/<string:owner_id>')
api.add_resource(MaintenanceByProperty,
                 '/maintenanceByProperty/<string:property_id>')
# api.add_resource(MaintenanceStatusByProperty,
#                  '/maintenanceStatusByProperty/<string:property_id>')
# api.add_resource(MaintenanceSummaryByOwner,
#                  '/maintenanceSummaryByOwner/<string:owner_id>')
# api.add_resource(MaintenanceSummaryAndStatusByOwner,
#                  '/maintenanceSummaryAndStatusByOwner/<string:owner_id>')

api.add_resource(MaintenanceQuotes, '/maintenanceQuotes')
api.add_resource(MaintenanceQuotesByUid,
                 '/maintenanceQuotes/<string:maintenance_quote_uid>')
api.add_resource(Quotes, '/quotes')
# api.add_resource(QuotesByBusiness, '/quotesByBusiness')
# api.add_resource(QuotesStatusByBusiness, '/quotesStatusByBusiness')
# api.add_resource(StatusUpdate, '/statusUpdate')
# api.add_resource(QuotesByRequest, '/quotesByRequest')

api.add_resource(Bills, '/bills')
# api.add_resource(ContractsByBusiness, '/contracts/<string:business_id>')
api.add_resource(Contracts, '/contracts')
api.add_resource(AddExpense, '/addExpense')
api.add_resource(AddRevenue, '/addRevenue')

api.add_resource(
    CashflowByOwner, '/cashflowByOwner/<string:owner_id>/<string:year>')

# api.add_resource(TransactionsByOwner, '/transactionsByOwner/<string:owner_id>')
# api.add_resource(TransactionsByOwnerByProperty,
#                  '/transactionsByOwnerByProperty/<string:owner_id>/<string:property_id>')
api.add_resource(AllTransactions, '/allTransactions')




api.add_resource(OwnerProfileByOwnerUid, '/ownerProfile/<string:owner_id>')
api.add_resource(TenantProfileByTenantUid, '/tenantProfile/<string:tenant_id>')
api.add_resource(BusinessProfileByUid,
                 '/businessProfile/<string:business_uid>')

api.add_resource(OwnerProfile, '/ownerProfile')  # POST, PUT OwnerProfile
api.add_resource(TenantProfile, '/tenantProfile')
api.add_resource(BusinessProfile, '/businessProfile')
api.add_resource(Employee, '/employee')

api.add_resource(OwnerDocuments, '/ownerDocuments/<string:owner_id>')
api.add_resource(TenantDocuments, '/tenantDocuments/<string:tenant_id>')

api.add_resource(LeaseDetails, '/leaseDetails/<string:filter_id>')

api.add_resource(ContactsMaintenance, '/contactsMaintenance')
# api.add_resource(ContactsOwnerContactsDetails,
#                  '/contactsOwnerContactsDetails/<string:owner_uid>')
api.add_resource(ContactsBusinessContacts,
                 '/contactsBusinessContacts/<string:business_uid>')
api.add_resource(ContactsBusinessContactsOwnerDetails,
                 '/contactsBusinessContactsOwnerDetails/<string:business_uid>')
api.add_resource(ContactsBusinessContactsTenantDetails,
                 '/contactsBusinessContactsTenantDetails/<string:business_uid>')
api.add_resource(ContactsBusinessContactsMaintenanceDetails,
                 '/contactsBusinessContactsMaintenanceDetails/<string:business_uid>')
# api.add_resource(ContactsOwnerManagerDetails,
#                  '/contactsOwnerManagerDetails/<string:owner_uid>')
# api.add_resource(ContactsMaintenanceManagerDetails,
#                  '/contactsMaintenanceManagerDetails/<string:business_uid>')
# api.add_resource(ContactsMaintenanceTenantDetails,
#                  '/contactsMaintenanceTenantDetails/<string:business_uid>')

api.add_resource(Announcements, '/announcements')
api.add_resource(AnnouncementsByUserId, '/announcements/<string:user_id>')
# api.add_resource(RolesByUserid, '/rolesByUserId/<string:user_id>')
api.add_resource(RequestPayment, '/requestPayment')
# api.add_resource(RentPurchaseTest, '/RentPurchase')

api.add_resource(List, '/lists')

api.add_resource(Account, '/account')


api.add_resource(SearchManager, '/searchManager')

api.add_resource(Password, '/password')

api.add_resource(PaymentMethod, '/paymentMethod')
api.add_resource(stripe_key, "/stripe_key/<string:desc>")

# refresh
# api.add_resource(Refresh, '/refresh')

# socialLogin
# api.add_resource(UserSocialLogin, '/userSocialLogin/<string:email>')
# api.add_resource(UserSocialSignup, '/userSocialSignup')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000)
