
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

def clean_json_data(data):
    for field, value in data.items():
        if value == '':
            value = None
        elif isinstance(value, list) and all(isinstance(item, dict) for item in value):
            data[field] = json.dumps(value)
    
    # data = {key: value for key, value in data.items() if "-DNU" not in key}

    print("Cleaned data")
    print(data)
    return data

class OwnerProfile(Resource):
    def post(self):
        response = {}
        owner_profile = request.form.to_dict()
        with connect() as db:
            owner_profile["owner_uid"] = db.call('space.new_owner_uid')['result'][0]['new_id']
            file = request.files.get("owner_photo")
            if file:
                key = f'ownerProfileInfo/{owner_profile["owner_uid"]}/owner_photo'
                owner_profile["owner_photo_url"] = uploadImage(file, key, '')
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
        tenant_profile = request.form.to_dict()
        with connect() as db:
            tenant_profile["tenant_uid"] = db.call('space.new_tenant_uid')['result'][0]['new_id']
            file = request.files.get("tenant_photo")
            if file:
                key = f'tenantProfileInfo/{tenant_profile["tenant_uid"]}/tenant_photo'
                tenant_profile["tenant_photo_url"] = uploadImage(file, key, '')
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
            response = db.update('tenantProfileInfo', key, clean_json_data(payload))
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


# class RolesByUserid(Resource):
#     def get(self, user_id):
#         print('in RolesByUserid')
#         with connect() as db:
#             response = db.select('user_profiles', {"user_id": user_id})
#         return response


class BusinessProfile(Resource):
    def post(self):
        response = {}
        business_profile = request.form.to_dict()
        with connect() as db:
            business_profile["business_uid"] = db.call('space.new_business_uid')['result'][0]['new_id']
            file = request.files.get("business_photo")
            if file:
                key = f'businessProfileInfo/{business_profile["business_uid"]}/business_photo'
                business_profile["business_photo_url"] = uploadImage(file, key, '')
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
    
    def get(self):
        response = {}
        where = request.args.to_dict()
        with connect() as db:
            response = db.select('businessProfileInfo', where)
        return response


class BusinessProfileByUid(Resource):
    def get(self, business_uid):
        print('in BusinessProfileByUid')
        with connect() as db:
            response = db.select('businessProfileInfo', {"business_uid": business_uid})
        return response


class Profile(Resource):
    def get(self, user_id):
        with connect() as db:
            if user_id.startswith("110"):
                    response = db.select('ownerProfileInfo', {"owner_uid": user_id})

            elif user_id.startswith("600"):
                    response = db.select('businessProfileInfo', {"business_uid": user_id})

            elif user_id.startswith("350"):
                    response = db.select('tenantProfileInfo', {"tenant_uid": user_id})
            else:
                raise BadRequest("Request failed, no UID in payload.")

            return response

    def post(self):
        response = {}
        profile_info = request.form.to_dict()
        owner = [key for key in profile_info.keys() if "owner" in key]
        tenant = [key for key in profile_info.keys() if "tenant" in key]
        prop_man = [key for key in profile_info.keys() if "business" in key]

        if owner:
            print("owner")
            with connect() as db:
                profile_info["owner_uid"] = db.call('space.new_owner_uid')['result'][0]['new_id']
                file = request.files.get("owner_photo")
                if file:
                    key = f'ownerProfileInfo/{profile_info["owner_uid"]}/owner_photo'
                    profile_info["owner_photo_url"] = uploadImage(file, key, '')
                response = db.insert('ownerProfileInfo', profile_info)
                response["owner_uid"] = profile_info["owner_uid"]
        elif tenant:
            print("tenant")
            with connect() as db:
                profile_info["tenant_uid"] = db.call('space.new_tenant_uid')['result'][0]['new_id']
                file = request.files.get("tenant_photo")
                if file:
                    key = f'tenantProfileInfo/{profile_info["tenant_uid"]}/tenant_photo'
                    profile_info["tenant_photo_url"] = uploadImage(file, key, '')
                response = db.insert('tenantProfileInfo', profile_info)
                response["tenant_uid"] = profile_info["tenant_uid"]
        elif prop_man:
            print("manager")
            with connect() as db:
                profile_info["business_uid"] = db.call('space.new_business_uid')['result'][0]['new_id']
                file = request.files.get("business_photo")
                if file:
                    key = f'businessProfileInfo/{profile_info["business_uid"]}/business_photo'
                    profile_info["business_photo_url"] = uploadImage(file, key, '')
                response = db.insert('businessProfileInfo', profile_info)
                response["business_uid"] = profile_info["business_uid"]
        else:
            raise BadRequest("Request failed, check payload.")

        return response

    def put(self):
        payload = request.get_json()
        if payload.get('business_uid'):
            key = {'business_uid': payload.pop('business_uid')}
            with connect() as db:
                response = db.update('businessProfileInfo', key, payload)
        elif payload.get('tenant_uid'):
            key = {'tenant_uid': payload.pop('tenant_uid')}
            with connect() as db:
                response = db.update('tenantProfileInfo', key, clean_json_data(payload))
        elif payload.get('owner_uid'):
            key = {'owner_uid': payload.pop('owner_uid')}
            with connect() as db:
                response = db.update('ownerProfileInfo', key, payload)
        else:
            raise BadRequest("Request failed, no UID in payload.")
        return response
