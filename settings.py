
from flask import request
from flask_restful import Resource
from data_pm import connect


class Account(Resource):
    def post(self):
        response = {}
        payload = request.get_json()
        with connect() as db:
            response = db.insert('space_prod.accounts', payload)
        return response
    
    def get(self):
        response = {}
        where = request.args.to_dict()
        with connect() as db:
            response = db.select('accounts', where)
        return response