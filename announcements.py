from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from data_pm import connect


class Announcements(Resource):
    def post(self):
        payload = request.get_json()
        with connect() as db:
            response = db.insert('announcements', payload)
        return response


class AnnouncementsByUid(Resource):
    def get(self, announcement_uid):
        with connect() as db:
            response = db.select('announcements', {"announcement_uid": announcement_uid})
        return response