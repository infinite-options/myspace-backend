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
                    FROM space.users
                    
                    WHERE user_uid = \'""" + user_id + """\';
                    """)
            print(userQuery)                                    

            if userQuery['code'] == 200 and int(len(userQuery['result']) > 0):                
                print(userQuery['result'][0]['user_uid'])
                return userQuery
            else:                
                abort(404, description="User not found")
        