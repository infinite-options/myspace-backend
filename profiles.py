
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest


# OVERVIEW
#           TENANT      OWNER     PROPERTY MANAGER     
# BY MONTH    X           X               X
# BY YEAR     X           X               X

class OwnerProfile(Resource):
    def post(self):
        print('in Owner Profile')
        response = {}
        payload = request.get_json()
        with connect() as db:
            response = db.insert('ownerProfileInfo', payload)
        return response

    def put(self):
        print('in Owner Profile')
        response = {}
        payload = request.get_json()
        if payload.get('owner_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'owner_uid': payload.pop('owner_uid')}
        with connect() as db:
            response = db.update('ownerProfileInfo', key, payload)
        return response

class OwnerProfileByOwnerUid(Resource):
    # decorators = [jwt_required()]

    def get(self, owner_id):
        print('in Owner Profile')
        response = {}

        with connect() as db:
            print("in connect loop")
            profileQuery = db.execute(""" 
                    -- OWNER PROFILE
                    SELECT * FROM space.ownerProfileInfo
                    WHERE owner_uid = \'""" + owner_id + """\';
                    """)
            

            # print("Query: ", profileQuery)
            # items = execute(profileQuery, "get", conn)
            # print(items)
            response["Profile"] = profileQuery 


            return response

class TenantProfile(Resource): 
    def post(self):
        response = {}
        print('in TenantProfile')
        payload = request.get_json()

        with connect() as db:
            response = db.insert('tenantProfileInfo', payload)
        return response
    
    def put(self):
        response = {}
        print('in TenantProfile')
        payload = request.get_json()
        if payload.get('tenant_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'tenant_uid': payload.pop('tenant_uid')}
        with connect() as db:
            response = db.update('tenantProfileInfo', key, payload)
        return response

class TenantProfileByTenantUid(Resource):
    # decorators = [jwt_required()]

    def get(self, tenant_id):
        print('in Tenant Profile')
        response = {}

        with connect() as db:
            print("in connect loop")
            profileQuery = db.execute(""" 
                    -- TENANT PROFILE
                    SELECT * FROM space.tenantProfileInfo
                    WHERE tenant_uid = \'""" + tenant_id + """\';
                    """)
            

            # print("Query: ", profileQuery)
            # items = execute(profileQuery, "get", conn)
            # print(items)
            response["Profile"] = profileQuery
            return response


class BusinessProfile(Resource):
    def post(self):
        print('in BusinessProfile')
        payload = request.get_json()
        with connect() as db:
            response = db.insert('businessProfileInfo', payload)
        return response

    def put(self):
        print('in BusinessProfile')
        payload = request.get_json()
        if payload.get('business_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'business_uid': payload.pop('business_uid')}
        with connect() as db:
            response = db.update('businessProfileInfo', key, payload)
        return response


class BusinessProfileByUid(Resource):
    def get(self, business_uid):
        print('in BusinessProfileByUid')
        with connect() as db:
            response = db.select('businessProfileInfo', {"business_uid": business_uid})
        return response

