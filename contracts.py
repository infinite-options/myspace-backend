from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from data_pm import connect


class Contracts(Resource):
    def post(self):
        print('in Contracts')
        payload = request.get_json()
        with connect() as db:
            response = db.insert('contracts', payload)
        return response

    def put(self):
        print('in Contracts')
        payload = request.get_json()
        if payload.get('contract_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'contract_uid': payload.pop('contract_uid')}
        with connect() as db:
            response = db.update('contracts', key, payload)
        return response

    def get(self, user_id):
        if user_id.startswith("600"):
            print('in ContractsByBusiness')
            with connect() as db:
                response = db.select('contracts', {"contract_business_id": user_id})
            return response

        elif user_id.startswith("110"):
            print('in ContractsByOwner')
            with connect() as db:
                response = db.execute("""
                SELECT c.*, po.property_owner_id
                FROM space.contracts c
                LEFT JOIN space.property_owner  po ON c.contract_property_id = po.property_id
                WHERE po.property_owner_id = \'""" + user_id + """\';
                """)
                return response
        else:
            return "No records for this Uid"



class ContractsByBusiness(Resource):
    def get(self, business_id):
        print('in ContractsByBusiness')
        with connect() as db:
            response = db.select('contracts', {"contract_business_id": business_id})
        return response
