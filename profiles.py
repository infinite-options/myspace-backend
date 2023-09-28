
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
        response = {}
        payload = request.form
        with connect() as db:
            owner_profile = {}
            owner_profile["owner_uid"] = db.call('space.new_owner_uid')['result'][0]['new_id']
            owner_profile["owner_user_id"] = payload.get("owner_user_id")
            owner_profile["owner_first_name"] = payload.get("owner_first_name")
            owner_profile["owner_last_name"] = payload.get("owner_last_name")
            owner_profile["owner_phone_number"] = payload.get("owner_phone_number")
            owner_profile["owner_email"] = payload.get("owner_email")
            owner_profile["owner_ein_number"] = payload.get("owner_ein_number")
            owner_profile["owner_ssn"] = payload.get("owner_ssn")
            owner_profile["owner_address"] = payload.get("owner_address")
            owner_profile["owner_unit"] = payload.get("owner_unit")
            owner_profile["owner_city"] = payload.get("owner_city")
            owner_profile["owner_state"] = payload.get("owner_state")
            owner_profile["owner_zip"] = payload.get("owner_zip")
            owner_photo_url = ""
            file = request.files.get("owner_photo")
            if file:
                key = f'ownerProfileInfo/{owner_profile["owner_uid"]}/owner_photo'
                owner_photo_url = uploadImage(file, key, '')
            owner_profile["owner_photo_url"] = owner_photo_url
            response = db.insert('ownerProfileInfo', owner_profile)
            response["owner_uid"] = owner_profile["owner_uid"]
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
        payload = request.form
        with connect() as db:
            tenant_profile = {}
            tenant_profile["tenant_uid"] = db.call('space.new_tenant_uid')['result'][0]['new_id']
            tenant_profile["tenant_user_id"] = payload.get("tenant_user_id")
            tenant_profile["tenant_first_name"] = payload.get("tenant_first_name")
            tenant_profile["tenant_last_name"] = payload.get("tenant_last_name")
            tenant_profile["tenant_phone_number"] = payload.get("tenant_phone_number")
            tenant_profile["tenant_email"] = payload.get("tenant_email")
            tenant_profile["tenant_ssn"] = payload.get("tenant_ssn")
            tenant_profile["tenant_address"] = payload.get("tenant_address")
            tenant_profile["tenant_unit"] = payload.get("tenant_unit")
            tenant_profile["tenant_city"] = payload.get("tenant_city")
            tenant_profile["tenant_state"] = payload.get("tenant_state")
            tenant_profile["tenant_zip"] = payload.get("tenant_zip")
            tenant_photo_url = ""
            file = request.files.get("tenant_photo")
            if file:
                key = f'tenantProfileInfo/{tenant_profile["tenant_uid"]}/tenant_photo'
                tenant_photo_url = uploadImage(file, key, '')
            tenant_profile["tenant_photo_url"] = tenant_photo_url
            response = db.insert('tenantProfileInfo', tenant_profile)
            response["tenant_uid"] = tenant_profile["tenant_uid"]
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


class RolesByUserid(Resource):
    def get(self, user_id):
        print('in RolesByUserid')
        with connect() as db:
            response = db.select('user_profiles', {"user_id": user_id})
        return response


class BusinessProfile(Resource):
    def post(self):
        response = {}
        payload = request.form
        with connect() as db:
            business_profile = {}
            business_profile["business_uid"] = db.call('space.new_business_uid')['result'][0]['new_id']
            business_profile["business_user_id"] = payload.get("business_user_id")
            business_profile["business_type"] = payload.get("business_type")
            business_profile["business_name"] = payload.get("business_name")
            business_profile["business_phone_number"] = payload.get("business_phone_number")
            business_profile["business_services_fees"] = payload.get("business_services_fees")
            business_profile["business_address"] = payload.get("business_address")
            business_profile["business_unit"] = payload.get("business_unit")
            business_profile["business_city"] = payload.get("business_city")
            business_profile["business_state"] = payload.get("business_state")
            business_profile["business_zip"] = payload.get("business_zip")
            file = request.files.get("business_photo")
            if file:
                key = f'businessProfileInfo/{business_profile["business_uid"]}/business_photo'
                business_photo_url = uploadImage(file, key, '')
            business_profile["business_photo_url"] = business_photo_url
            response = db.insert('businessProfileInfo', business_profile)
            response["business_uid"] = business_profile["business_uid"]
        return response

    def put(self):
        response = {}
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

