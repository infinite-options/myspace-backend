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


class ContractsByUid(Resource):
    def get(self, contract_uid):
        print('in ContractsByUid')
        with connect() as db:
            response = db.select('contracts', {"contract_uid": contract_uid})
        return response
