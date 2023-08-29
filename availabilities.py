
from flask import request
from flask_restful import Resource
from data_pm import connect


class Availabilities(Resource):
    def get(self):
        response = {}
        with connect() as db:
            response = db.select('availability')
        return response
    def put(self):
        response = []
        with connect() as db:
            payload = request.get_json()
            for idx, availability in enumerate(payload):
                key = {'availability_uid': availability.pop('id')}
                response[idx] = db.update('availability', key, availability)
        return response
    

class Unavailabilities(Resource):
    def get(self):
        response = {}
        with connect() as db:
            response = db.select('unavailability')
        return response
    def post(self):
        response = {}
        with connect() as db:
            payload = request.get_json()
            new_id = db.call('unavailability_uid')['result'][0]['new_id']
            db.insert('unavailability', {'unavailability_uid': new_id, **payload})
        return response
    def put(self):
        response = {}
        with connect() as db:
            payload = request.get_json()
            key = {'unavailability_uid': payload.pop('id')}
            response = db.update('unavailability', key, payload)
        return response
    def delete(self):
        response = {}
        with connect() as db:
            payload = request.get_json()
            query = (""" 
                    DELETE FROM space.unavailability
                    WHERE unavailability_uid = \'""" + payload.pop('id') + """\';         
                    """)
            response = db.delete(query)
        return response