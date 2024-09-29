
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity


from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar

from queries import testQuery, RentStatusQuery, RentDetailsQuery



class RentTest(Resource):
    def get(self, user_id):
        print("in Get Rent TEST Status")
        response = {}

        response["testStatus"] = testQuery(user_id)
        return response
 

class Rents(Resource):
    def get(self, user_id):
        print("in Get Rent Status")
        response = {}

        response["RentStatus"] = RentStatusQuery(user_id)
        return response


class RentDetails(Resource):
    def get(self, user_id):
        print("in Get Rent Details")
        response = {}

        response["RentStatus"] = RentDetailsQuery(user_id)
        return response
