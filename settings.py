
from flask import request
from flask_restful import Resource
from data_pm import connect


class BankAccount(Resource):
    def put(self):
        response = {}
        with connect() as db:
            payload = request.get_json()
            key = {'business_uid': payload.pop('business_uid')}
            response = db.update('businessProfileInfo', key, payload)
        return response
    
    def get(self, business_id):
        response = {}
        with connect() as db:
            where = {'business_uid': business_id}
            response = db.select('businessProfileInfo', where)
        return response