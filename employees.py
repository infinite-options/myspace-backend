from flask import request
from flask_restful import Resource
from data_pm import connect, uploadImage
from werkzeug.exceptions import BadRequest

class Employee(Resource):
    # print("Hello Employee")
    def get(self, user_id):
        response = {}
        if user_id[:3] == '120':
            with connect() as db:
                empQuery = db.execute("""
                                        SELECT * FROM space_dev.employees WHERE 
                                        employee_uid = \'""" + user_id + """\'
                """)

                response["employee"] = empQuery

        elif user_id[:3] == '600':
            with connect() as db:
                empQuery = db.execute("""
                                        SELECT * FROM space_dev.employees WHERE 
                                        employee_business_id = \'""" + user_id + """\'
                """)

                response["employee"] = empQuery

        return response

    def post(self):
        # print("In Employee")
        response = {}
        employee = request.form.to_dict()
        # print("Form data received: ", employee)
        with connect() as db:
            employee["employee_uid"] = db.call('space_dev.new_employee_uid')['result'][0]['new_id']
            file = request.files.get("employee_photo")
            if file:
                key = f'employees/{employee["employee_uid"]}/employee_photo'
                employee["employee_photo_url"] = uploadImage(file, key, '')
            response = db.insert('space_dev.employees', employee)
            response["employee_uid"] = employee["employee_uid"]
        # print(response)
        return response



class EmployeeVerification(Resource):
    def put(self):
        response = {}
        # print("In Employee Verification")
        payload = request.get_json(force=True)
        # print("Receive Payload: ", payload)
        for i in range(len(payload)):
            # print("Employee ID: ", payload[i].get('employee_uid'))
            if payload[i].get('employee_uid') is None:
                raise BadRequest("Request failed, no UID in payload.")
            key = {'employee_uid': payload[i].pop('employee_uid')}
            with connect() as db:
                response["employee_update"] = db.update('space_dev.employees',key,payload[i])

        return response