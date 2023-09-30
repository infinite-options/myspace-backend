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