
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity


from data_pm import connect, deleteImage, uploadImage, s3, processImage, processDocument
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest
import ast



# def clean_json_data(data):
#     # print(data)
#     for field, value in data.items():
#         if value == '':
#             value = None
#         elif isinstance(value, list) and all(isinstance(item, dict) for item in value):
#             data[field] = json.dumps(value)
    
#     # data = {key: value for key, value in data.items() if "-DNU" not in key}

#     # print("Cleaned data: ", data)
#     return data

# ALLOWED_EXTENSIONS = {'txt', 'pdf', 'doc', 'docx'}

# def allowed_file(filename):
#     return '.' in filename and \
#            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


class BusinessProfile(Resource):   
    def get(self):
        response = {}
        where = request.args.to_dict()
        print("Where: ", where)
        with connect() as db:
            response = db.select('businessProfileInfo', where)
        return response

# class BusinessProfileList(Resource):
#     def get(self, business_type):
#         response = {}
#         with connect() as db:
#             business = db.execute("""
#                             SELECT business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, 
#                              business_locations, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
#                             FROM space.businessProfileInfo 
#                             WHERE business_type = \'""" + business_type + """\' 
#             """)
#         response["Businesses"] = business
#         return response

# class BusinessProfileByUid(Resource):
    # def get(self, business_uid):
    #     print('in BusinessProfileByUid')
    #     with connect() as db:
    #         response = db.select('businessProfileInfo', {"business_uid": business_uid})
    #     return response


class Profile(Resource):
    def get(self, user_id):
        print("In Profile Endpoint", user_id)
        with connect() as db:
            if user_id.startswith("110"):
                ownerQuery = db.execute("""
                            SELECT * FROM space.ownerProfileInfo 
                            LEFT JOIN (
                                SELECT paymentMethod_profile_id, JSON_ARRAYAGG(JSON_OBJECT
                                    ('paymentMethod_uid', paymentMethod_uid,
                                    'paymentMethod_type', paymentMethod_type,
                                    'paymentMethod_name', paymentMethod_name,
                                    'paymentMethod_acct', paymentMethod_acct,
                                    'paymentMethod_routing_number', paymentMethod_routing_number,
                                    'paymentMethod_micro_deposits', paymentMethod_micro_deposits,
                                    'paymentMethod_exp_date', paymentMethod_exp_date,
                                    'paymentMethod_cvv', paymentMethod_cvv,
                                    'paymentMethod_billingzip', paymentMethod_billingzip,
                                    'paymentMethod_status', paymentMethod_status
                                    )) AS paymentMethods
                                    FROM space.paymentMethods
                                    GROUP BY paymentMethod_profile_id) as p ON owner_uid = paymentMethod_profile_id
                            WHERE owner_uid = \'""" + user_id + """\'
                        """)
                response = {}
                response["profile"] = ownerQuery

            elif user_id.startswith("600"):
                businessQuery = db.execute("""
                            SELECT * FROM space.businessProfileInfo 
                            LEFT JOIN (
                                SELECT paymentMethod_profile_id, JSON_ARRAYAGG(JSON_OBJECT
                                    ('paymentMethod_uid', paymentMethod_uid,
                                    'paymentMethod_type', paymentMethod_type,
                                    'paymentMethod_name', paymentMethod_name,
                                    'paymentMethod_acct', paymentMethod_acct,
                                    'paymentMethod_routing_number', paymentMethod_routing_number,
                                    'paymentMethod_micro_deposits', paymentMethod_micro_deposits,
                                    'paymentMethod_exp_date', paymentMethod_exp_date,
                                    'paymentMethod_cvv', paymentMethod_cvv,
                                    'paymentMethod_billingzip', paymentMethod_billingzip,
                                    'paymentMethod_status', paymentMethod_status
                                    )) AS paymentMethods
                                FROM space.paymentMethods
                                GROUP BY paymentMethod_profile_id) as p ON business_uid = paymentMethod_profile_id
                            -- WHERE business_uid = '600-000003'
                            WHERE business_uid = \'""" + user_id + """\'
                            """)
                response = {}
                response["profile"] = businessQuery

            elif user_id.startswith("350"):
                tenantQuery = db.execute("""
                            SELECT * FROM space.tenantProfileInfo 
                            LEFT JOIN (
                                SELECT paymentMethod_profile_id, JSON_ARRAYAGG(JSON_OBJECT
                                    ('paymentMethod_uid', paymentMethod_uid,
                                    'paymentMethod_type', paymentMethod_type,
                                    'paymentMethod_name', paymentMethod_name,
                                    'paymentMethod_acct', paymentMethod_acct,
                                    'paymentMethod_routing_number', paymentMethod_routing_number,
                                    'paymentMethod_micro_deposits', paymentMethod_micro_deposits,
                                    'paymentMethod_exp_date', paymentMethod_exp_date,
                                    'paymentMethod_cvv', paymentMethod_cvv,
                                    'paymentMethod_billingzip', paymentMethod_billingzip,
                                    'paymentMethod_status', paymentMethod_status
                                    )) AS paymentMethods
                                    FROM space.paymentMethods
                                    GROUP BY paymentMethod_profile_id) as p ON tenant_uid = paymentMethod_profile_id
                            WHERE tenant_uid = \'""" + user_id + """\'
                            """)
                response = {}
                response["profile"] = tenantQuery

            elif user_id.startswith("120"):
                    # response["profile"] = db.select('employees', {"employee_uid": user_id})
                print ('    -in Get Employee Profile')
                employeeQuery = db.execute(""" 
                            -- EMPLOYEE CONTACTS
                            SELECT *
                            FROM space.employees
                            LEFT JOIN space.businessProfileInfo ON employee_business_id = business_uid
                            LEFT JOIN (
                                SELECT paymentMethod_profile_id AS pm_employee_id, JSON_ARRAYAGG(JSON_OBJECT
                                    ('paymentMethod_uid', paymentMethod_uid,
                                    'paymentMethod_type', paymentMethod_type,
                                    'paymentMethod_name', paymentMethod_name,
                                    'paymentMethod_acct', paymentMethod_acct,
                                    'paymentMethod_routing_number', paymentMethod_routing_number,
                                    'paymentMethod_micro_deposits', paymentMethod_micro_deposits,
                                    'paymentMethod_exp_date', paymentMethod_exp_date,
                                    'paymentMethod_cvv', paymentMethod_cvv,
                                    'paymentMethod_billingzip', paymentMethod_billingzip,
                                    'paymentMethod_status', paymentMethod_status
                                    )) AS employeePaymentMethods
                                FROM space.paymentMethods
                                GROUP BY paymentMethod_profile_id) as e ON employee_uid = e.pm_employee_id
                            LEFT JOIN (
                                SELECT paymentMethod_profile_id AS pm_business_id, JSON_ARRAYAGG(JSON_OBJECT
                                    ('paymentMethod_uid', paymentMethod_uid,
                                    'paymentMethod_type', paymentMethod_type,
                                    'paymentMethod_name', paymentMethod_name,
                                    'paymentMethod_acct', paymentMethod_acct,
                                    'paymentMethod_routing_number', paymentMethod_routing_number,
                                    'paymentMethod_micro_deposits', paymentMethod_micro_deposits,
                                    'paymentMethod_exp_date', paymentMethod_exp_date,
                                    'paymentMethod_cvv', paymentMethod_cvv,
                                    'paymentMethod_billingzip', paymentMethod_billingzip,
                                    'paymentMethod_status', paymentMethod_status
                                    )) AS paymentMethods
                                FROM space.paymentMethods
                                GROUP BY paymentMethod_profile_id) as p ON business_uid = p.pm_business_id
                            -- WHERE employee_uid = '120-000441';
                            WHERE employee_uid = \'""" + user_id + """\'
                            """)
                
                print(employeeQuery)
                    
                if len(employeeQuery["result"]) > 0:
                    response = {}
                    response["profile"] = employeeQuery
                    
            else:
                where = request.args.to_dict()
                with connect() as db:
                    response = db.select('businessProfileInfo', where)
                return response
                # raise BadRequest("Request failed, no UID in payload.")

            return response

    def post(self):
        # print("In Profile POST")
        response = {}
        employee_info  = {}
        profile_info = request.form.to_dict()
        print("Incoming Profile: ", profile_info)
        owner = [key for key in profile_info.keys() if "owner" in key]
        tenant = [key for key in profile_info.keys() if "tenant" in key]
        business = [key for key in profile_info.keys() if "business" in key]
        employee = [key for key in profile_info.keys() if "employee" in key]

        with connect() as db:
            if owner:
                print("owner")
                profile_info["owner_uid"] = db.call('space.new_owner_uid')['result'][0]['new_id']
                file = request.files.get("owner_photo")
                if file:
                    key = f'ownerProfileInfo/{profile_info["owner_uid"]}/owner_photo'
                    profile_info["owner_photo_url"] = uploadImage(file, key, '')
                profile_info['owner_documents'] = '[]' if profile_info.get('owner_documents') in {None, '', 'null'} else profile_info.get('owner_documents', '[]')
                response = db.insert('ownerProfileInfo', profile_info)
                response["owner_uid"] = profile_info["owner_uid"]
            elif tenant:
                print("tenant")

                # Check and add the keys using ternary expressions
                profile_info['tenant_documents'] = profile_info['tenant_documents'] if 'tenant_documents' in profile_info else '[]'
                profile_info['tenant_adult_occupants'] = profile_info['tenant_adult_occupants'] if 'tenant_adult_occupants' in profile_info else '[]'
                profile_info['tenant_children_occupants'] = profile_info['tenant_children_occupants'] if 'tenant_children_occupants' in profile_info else '[]'
                profile_info['tenant_vehicle_info'] = profile_info['tenant_vehicle_info'] if 'tenant_vehicle_info' in profile_info else '[]'
                profile_info['tenant_references'] = profile_info['tenant_references'] if 'tenant_references' in profile_info else '[]'
                profile_info['tenant_pet_occupants'] = profile_info['tenant_pet_occupants'] if 'tenant_pet_occupants' in profile_info else '[]'
                profile_info['tenant_employment'] = profile_info['tenant_employment'] if 'tenant_employment' in profile_info else '[]'
                # print("Updated Tenant Profile: ", tenant_profile)

                profile_info["tenant_uid"] = db.call('space.new_tenant_uid')['result'][0]['new_id']
                file = request.files.get("tenant_photo")
                if file:
                    key = f'tenantProfileInfo/{profile_info["tenant_uid"]}/tenant_photo'
                    profile_info["tenant_photo_url"] = uploadImage(file, key, '')
                response = db.insert('tenantProfileInfo', profile_info)
                response["tenant_uid"] = profile_info["tenant_uid"]
            elif business:
                print("manager")
                profile_info["business_uid"] = db.call('space.new_business_uid')['result'][0]['new_id']
                file = request.files.get("business_photo")
                # print(profile_info, file)
                if file:
                    key = f'businessProfileInfo/{profile_info["business_uid"]}/business_photo'
                    profile_info["business_photo_url"] = uploadImage(file, key, '')
                profile_info['business_documents'] = '[]' if profile_info.get('business_documents') in {None, '', 'null'} else profile_info.get('business_documents', '[]')
                response = db.insert('businessProfileInfo', profile_info)
                # response["business_uid"] = profile_info["business_uid"]

                print("employee")
                employee_info["employee_uid"] = db.call('space.new_employee_uid')['result'][0]['new_id']
                employee_info["employee_user_id"] = profile_info["business_user_id"]
                employee_info["employee_business_id"] = profile_info["business_uid"]
                employee_info["employee_role"] = "OWNER"
                file = request.files.get("employee_photo_url")
                # print(employee_info, file)
                if file:
                    key = f'employee/{employee_info["employee_uid"]}/employee_photo'
                    employee_info["employee_photo_url"] = uploadImage(file, key, '')
                response = db.insert('employees', employee_info)
                response["business_uid"] = profile_info["business_uid"]
                response["employee_uid"] = employee_info["employee_uid"]
            elif employee:
                print("employee")
                employee_info["employee_uid"] = db.call('space.new_employee_uid')['result'][0]['new_id']
                employee_info["employee_user_id"] = profile_info["employee_user_id"]
                employee_info["employee_business_id"] = profile_info["business_uid"]
                file = request.files.get("employee_photo_url")
                # print(employee_info, file)
                if file:
                    key = f'employee/{employee_info["employee_uid"]}/employee_photo'
                    employee_info["employee_photo_url"] = uploadImage(file, key, '')
                response = db.insert('employees', employee_info)
                response["employee_uid"] = employee_info["employee_uid"]
            else:
                raise BadRequest("Request failed, check payload.")

        return response

    def put(self):
        print("\nIn Profile PUT")
        response = {}
        flag = 'false'
        payload = request.form.to_dict()
        print("Profile Update Payload: ", payload) 
   
        if payload.get('tenant_uid'):
            filtered_payload = request.form.to_dict()
            key = {'tenant_uid': filtered_payload.pop('tenant_uid')}
        elif payload.get('owner_uid'):
            filtered_payload = request.form.to_dict()
            key = {'owner_uid': filtered_payload.pop('owner_uid')}
        elif payload.get('business_uid'):
            valid_columns = {"business_uid", "business_user_id", "business_type", "business_name", "business_phone_number", "business_email", "business_ein_number", "business_services_fees", "business_locations", "business_documents", 'business_address', "business_unit", "business_city", "business_state", "business_zip", "business_photo_url", 'business_documents_details', 'delete_documents'}
            filtered_payload = {key: value for key, value in payload.items() if key in valid_columns}
            key = {'business_uid': filtered_payload.pop('business_uid')}
        else:
            print("No tenant, owner or buisness uid passed")
            flag = 'true'

        if flag == 'false':
            print("Key Received: ", key)
            print("Filtered Payload: ", filtered_payload)


            # --------------- PROCESS IMAGES ------------------

            processImage(key, filtered_payload)
            print("Payload after function: ", filtered_payload)
            
            # --------------- PROCESS IMAGES ------------------


            # --------------- PROCESS DOCUMENTS ------------------

            processDocument(key, filtered_payload)
            print("Payload after function: ", filtered_payload)
            
            # --------------- PROCESS DOCUMENTS ------------------


        if payload.get('employee_uid'):
            print("In employee")
            valid_columns = {"employee_uid", "employee_user_id", "employee_business_id", "employee_first_name", "employee_last_name", "employee_phone_number", "employee_email", "employee_role", "employee_photo_url", "employee_ssn", "employee_address", "employee_unit", "employee_city", "employee_state", "employee_zip", "employee_verification"}
            employee_payload = {key: value for key, value in payload.items() if key in valid_columns}
            employee_key = {'employee_uid': employee_payload.pop('employee_uid')}
            print("Key Received: ", employee_key)
            print("Filtered Payload: ", employee_payload)

            # --------------- PROCESS IMAGES ------------------

            processImage(employee_key, employee_payload)
            print("Payload after function: ", employee_payload)
            
            # --------------- PROCESS IMAGES ------------------


            # --------------- PROCESS DOCUMENTS ------------------

            # processDocument(employee_key, employee_payload)
            # print("Payload after function: ", employee_payload)
            
            # --------------- PROCESS DOCUMENTS ------------------

        else:
            if flag == 'true':
                print("No uid passed")
                return
            

        with connect() as db:
                # print("Checking Inputs: ", key)
                if payload.get('tenant_uid'):
                    # response['tenant_docs'] = db.update('tenantProfileInfo', key, filtered_payload)
                    response = db.update('tenantProfileInfo', key, filtered_payload)
                if payload.get('owner_uid'):
                    response = db.update('ownerProfileInfo', key, filtered_payload)
                if payload.get('business_uid'):
                    response = db.update('businessProfileInfo', key, filtered_payload)
                if payload.get('employee_uid'):
                    response['employee'] = db.update('employees', employee_key, employee_payload)

        return response
   
   
    # def put(self):
    #     print("\nIn Profile PUT")
    #     response = {}
    #     payload = request.form.to_dict()
    #     print("Profile Update Payload: ", payload)
        
    #     # Profile Picture is Unique to Profile 
    #     if payload.get('business_uid'):
    #         print("In Business")  # Need valid-columns since business can be updated with employee
    #         valid_columns = {"business_uid", "business_user_id", "business_type", "business_name", "business_phone_number", "business_email", "business_ein_number", "business_services_fees", "business_locations", "business_documents", 'business_address', "business_unit", "business_city", "business_state", "business_zip", "business_photo_url", 'business_documents_details', 'delete_documents'}
    #         filtered_payload = {key: value for key, value in payload.items() if key in valid_columns}
    #         print("Filtered Payload: ", filtered_payload)
    #         key = {'business_uid': filtered_payload.pop('business_uid')}
    #         print("Business Key: ", key)

    #         file = request.files.get("business_photo")
    #         if file:
    #             key1 = f'businessProfileInfo/{key["business_uid"]}/business_photo'

    #             try:
    #                 deleteImage(key1)
    #                 print(f"Deleted existing file {key1}")
    #             except s3.exceptions.ClientError as e:
    #                 if e.response['Error']['Code'] == '404':
    #                     print(f"File {key1} does not exist")
    #                 else:
    #                     print(f"Error deleting file {key1}: {e}")

    #             filtered_payload["business_photo_url"] = uploadImage(file, key1, '')
    #             # print("business photo: ", filtered_payload["business_photo_url"] )

    #         # --------------- PROCESS DOCUMENTS ------------------

    #         processDocument(key, filtered_payload)
    #         print("Payload after function: ", filtered_payload)
            
    #         # --------------- PROCESS DOCUMENTS ------------------


    #         with connect() as db:
    #             # print("Checking Inputs: ", key, filtered_payload)
    #             response = db.update('businessProfileInfo', key, filtered_payload)
        
        
    #     if payload.get('tenant_uid'):
    #         print("In Tenant")
    #         # tenant_uid = payload.get('tenant_uid')
    #         key = {'tenant_uid': payload.pop('tenant_uid')}
    #         print("Tenant Key: ", key)

    #         file = request.files.get("tenant_photo_url")
    #         if file:
    #             key1 = f'tenantProfileInfo/{key["tenant_uid"]}/tenant_photo'
                
    #             try:    
    #                 deleteImage(key1)
    #                 print(f"Deleted existing file {key1}")
    #             except s3.exceptions.ClientError as e:
    #                 if e.response['Error']['Code'] == '404':
    #                     print(f"File {key1} does not exist")
    #                 else:
    #                     print(f"Error deleting file {key1}: {e}")

    #             payload["tenant_photo_url"] = uploadImage(file, key1, '')


    #         # --------------- PROCESS DOCUMENTS ------------------

    #         processDocument(key, payload)
    #         print("Payload after function: ", payload)
            
    #         # --------------- PROCESS DOCUMENTS ------------------


    #         # Update File List in Database        
    #         # print("tenant")
    #         # print("key: ", key )
    #         # print("payload: ", payload)

    #         with connect() as db:
    #             # print("Checking Inputs: ", key, payload)
    #             response['tenant_docs'] = db.update('tenantProfileInfo', key, payload)
    #             # print("Response:" , response)


    #     if payload.get('owner_uid'):
    #         # print("In Owner")
    #         key = {'owner_uid': payload.pop('owner_uid')}
    #         # print("Owner Key: ", key)

    #         file = request.files.get("owner_photo_url")
    #         if file:
    #             key1 = f'ownerProfileInfo/{key["owner_uid"]}/owner_photo'

    #             try:
    #                 deleteImage(key1)
    #                 print(f"Deleted existing file {key1}")
    #             except s3.exceptions.ClientError as e:
    #                 if e.response['Error']['Code'] == '404':
    #                     print(f"File {key1} does not exist")
    #                 else:
    #                     print(f"Error deleting file {key1}: {e}")

    #             payload["owner_photo_url"] = uploadImage(file, key1, '')
    #             # print("Owner Payload: ", payload)

    #         with connect() as db:
    #             # print("Checking Inputs: ", key, filtered_payload)
    #             response = db.update('ownerProfileInfo', key, payload)
    #             # print(response)

        
    #     if payload.get('employee_uid'):
    #         # print("In Employee")
    #         valid_columns = {"employee_uid", "employee_user_id", "employee_business_id", "employee_first_name", "employee_last_name", "employee_phone_number", "employee_email", "employee_role", "employee_photo_url", "employee_ssn", "employee_address", "employee_unit", "employee_city", "employee_state", "employee_zip", "employee_verification"}
    #         filtered_payload = {key: value for key, value in payload.items() if key in valid_columns}
    #         print("Filtered Payload: ", filtered_payload)
    #         key = {'employee_uid': filtered_payload.pop('employee_uid')}
    #         # print("Employee Key: ", key)

    #         file = request.files.get("employee_photo_url")
    #         if file:
    #             key1 = f'employees/{key["employee_uid"]}/employee_photo'

    #             try:
    #                 deleteImage(key1)
    #                 print(f"Deleted existing file {key1}")
    #             except s3.exceptions.ClientError as e:
    #                 if e.response['Error']['Code'] == '404':
    #                     print(f"File {key1} does not exist")
    #                 else:
    #                     print(f"Error deleting file {key1}: {e}")

    #             filtered_payload["employee_photo_url"] = uploadImage(file, key1, '')
    #             # print("employee photo: ", filtered_payload["employee_photo_url"] )
            
            
    #         with connect() as db:
    #             # print("Checking Inputs: ", key, filtered_payload)
    #             response = db.update('employees', key, filtered_payload)

    #     # else:
    #     #     raise BadRequest("Request failed, no UID in payload.")
        
    #     return response
