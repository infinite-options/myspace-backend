from flask import request, abort 
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from werkzeug.utils import secure_filename


from data_pm import connect, uploadImage, s3
import json
import os
import ast



class UserInfo(Resource):

    def __call__(self):
        print("In User Info")

    def get(self, user_id):
        print("In UserInfo GET")
        print(user_id)

        with connect() as db:
            # print("in get lease applications")
            userQuery = db.execute("""                     
                    SELECT *
                    FROM space_prod.users 
                    -- WHERE user_uid = 'anureetksandhu7@gmail.com' OR email = 'anureetksandhu7@gmail.com'   ;                   
                    WHERE user_uid = \'""" + user_id + """\' OR email = \'""" + user_id + """\'  ;
                    """)
            # print(userQuery)                                    

            if userQuery['code'] == 200 and int(len(userQuery['result']) > 0):                
                print(userQuery['result'][0]['user_uid'])
                return userQuery
            else:                
                abort(404, description="User not found")

    def put(self):
        print("In update User")
        with connect() as db:
            payload = request.get_json()
            print(payload)

            if payload["user_uid"] is None:
                raise BadRequest("Request failed, no UID in payload.")
            
            key = {'user_uid': payload.pop('user_uid')}
            print(key)
            # print(payload)
            
            with connect() as db:
                response = db.update('space_prod.users', key, payload)
            return response


            

        