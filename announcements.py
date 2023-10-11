from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from data_pm import connect


class Announcements(Resource):
    def post(self, user_id):
        payload = request.get_json()
        manager_id = user_id

        with connect() as db:

            for i in range(len(payload["announcement_receiver"])):

                newRequest = {}
                newRequest['announcement_title'] = payload["announcement_title"]
                newRequest['announcement_msg'] = payload["announcement_msg"]
                newRequest['announcement_sender'] = manager_id
                newRequest['announcement_mode'] = 'Tenants'
                newRequest['announcement_receiver'] = payload["announcement_receiver"][i]
                # db.insert('announcements', newRequest)

                user_query = db.execute(""" 
                                    -- Find all the properties associated with the manager
                                    SELECT * 
                                    FROM space.user_profiles AS b
                                    WHERE b.profile_uid = \'""" + payload["announcement_receiver"][i] + """\';
                                    """)
                user_email = user_query['result'][0]['user_email']
                print(user_email)

        return 200

    def get(self, user_id):
        with connect() as db:
            response = db.select('announcements', {"announcement_sender": user_id})
        return response
