
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar


# OVERVIEW
#           TENANT      OWNER     PROPERTY MANAGER     
# BY MONTH    X           X               X
# BY YEAR     X           X               X


class OwnerProfile(Resource):
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
