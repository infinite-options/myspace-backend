
from flask import request
from flask_restful import Resource
from data_pm import connect


class BankAccount(Resource):
    def put(self):
        print("In BankAccount")
        response = {}
        with connect() as db:
            payload = request.get_json()
            key = {'business_uid': payload.pop('business_uid')}
            response = db.update('businessProfileInfo', key, payload)
            response["status"] = "Success"
        return response