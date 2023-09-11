from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from data_pm import connect
from util import createHash, createSalt


class Password(Resource):
    def put(self):
        print('in Password')
        payload = request.get_json()
        user_uid = payload.get('user_uid')
        if not user_uid:
            raise BadRequest("Request failed, no UID in payload.")
        with connect() as db:
            response = db.select('users', {"user_uid": user_uid})
        try:
            user = response.get('result')[0]
        except IndexError as e:
            print(e)
            raise BadRequest("Request failed, no such user in the database.")

        current_password = payload.get('current_password')
        new_password = payload.get('new_password')

        current_password_encrypted = createHash(current_password, user.get('password_salt'))
        if current_password_encrypted != user.get('password_hash'):
            raise BadRequest("Request failed, current password is incorrect.")

        new_salt = createSalt()
        new_password_hash = createHash(new_password, new_salt)

        with connect() as db:
            response = db.update('users', {"user_uid": user_uid},
                                 {
                                     "password_salt": new_salt,
                                     "password_hash": new_password_hash
                                 })
        return response
