from flask import request
from flask_restful import Resource
from data_pm import connect, uploadImage

class Employee(Resource):
    def post(self):
        response = {}
        employee = request.form.to_dict()
        with connect() as db:
            employee["employee_uid"] = db.call('space.new_employee_uid')['result'][0]['new_id']
            file = request.files.get("employee_photo")
            if file:
                key = f'employees/{employee["employee_uid"]}/employee_photo'
                employee["employee_photo_url"] = uploadImage(file, key, '')
            response = db.insert('employees', employee)
            response["employee_uid"] = employee["employee_uid"]
        return response

    def get(self, user_id):
        response = {}
        if user_id[:3] == '120':
            with connect() as db:
                empQuery = db.execute("""
                                        SELECT * FROM space.employees WHERE 
                                        employee_uid = \'""" + user_id + """\'
                """)

                response["employee"] = empQuery

        elif user_id[:3] == '600':
            with connect() as db:
                empQuery = db.execute("""
                                        SELECT * FROM space.employees WHERE 
                                        employee_business_id = \'""" + user_id + """\'
                """)

                response["employee"] = empQuery

        return response

class EmployeeVerification(Resource):
    def post(self):
        response = {}

        payload = request.get_json()
        if payload.get('employee_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'employee_uid': payload.pop('employee_uid')}
        with connect() as db:
            response["employee_update"] = db.update('employees',key,payload)

        return response