from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from data_pm import connect


class Announcements(Resource):
    def post(self):
        print('in Announcements')
        payload = request.get_json()
        with connect() as db:
            response = db.insert('announcements', payload)
        return response