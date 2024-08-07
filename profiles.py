
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, deleteImage, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest
import ast



# OVERVIEW
#           TENANT      OWNER     PROPERTY MANAGER     
# BY MONTH    X           X               X
# BY YEAR     X           X               X

def clean_json_data(data):
    # print(data)
    for field, value in data.items():
        if value == '':
            value = None
        elif isinstance(value, list) and all(isinstance(item, dict) for item in value):
            data[field] = json.dumps(value)
    
    # data = {key: value for key, value in data.items() if "-DNU" not in key}

    # print("Cleaned data: ", data)
    return data

ALLOWED_EXTENSIONS = {'txt', 'pdf', 'doc', 'docx'}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# class OwnerProfile(Resource):
    def post(self):
        response = {}
        owner_profile = request.form.to_dict()
        with connect() as db:
            owner_profile["owner_uid"] = db.call('space.new_owner_uid')['result'][0]['new_id']
            file = request.files.get("owner_photo")
            if file:
                key = f'ownerProfileInfo/{owner_profile["owner_uid"]}/owner_photo'
                owner_profile["owner_photo_url"] = uploadImage(file, key, '')
            response = db.insert('ownerProfileInfo', owner_profile)
            response["owner_uid"] = owner_profile["owner_uid"]
        return response

    def put(self):
        print('in Owner Profile')
        response = {}
        payload = request.get_json()
        if payload.get('owner_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'owner_uid': payload.pop('owner_uid')}
        with connect() as db:
            response = db.update('ownerProfileInfo', key, payload)
        return response

# class OwnerProfileByOwnerUid(Resource):
#     # decorators = [jwt_required()]

#     def get(self, owner_id):
#         print('in Owner Profile')
#         response = {}

#         with connect() as db:
#             print("in connect loop")
#             profileQuery = db.execute(""" 
#                     -- OWNER PROFILE
#                     SELECT * FROM space.ownerProfileInfo
#                     WHERE owner_uid = \'""" + owner_id + """\';
#                     """)
            

#             # print("Query: ", profileQuery)
#             # items = execute(profileQuery, "get", conn)
#             # print(items)
#             response["Profile"] = profileQuery 


#             return response

# class TenantProfile(Resource): 
#     def post(self):
#         response = {}
#         tenant_profile = request.form.to_dict()
#         # print("Tenant Profile: ", tenant_profile)

#         # Check and add the keys using ternary expressions
#         tenant_profile['tenant_documents'] = tenant_profile['tenant_documents'] if 'tenant_documents' in tenant_profile else '[]'
#         tenant_profile['tenant_adult_occupants'] = tenant_profile['tenant_adult_occupants'] if 'tenant_adult_occupants' in tenant_profile else '[]'
#         tenant_profile['tenant_children_occupants'] = tenant_profile['tenant_children_occupants'] if 'tenant_children_occupants' in tenant_profile else '[]'
#         tenant_profile['tenant_vehicle_info'] = tenant_profile['tenant_vehicle_info'] if 'tenant_vehicle_info' in tenant_profile else '[]'
#         tenant_profile['tenant_references'] = tenant_profile['tenant_references'] if 'tenant_references' in tenant_profile else '[]'
#         tenant_profile['tenant_pet_occupants'] = tenant_profile['tenant_pet_occupants'] if 'tenant_pet_occupants' in tenant_profile else '[]'
#         # print("Updated Tenant Profile: ", tenant_profile)

#         with connect() as db:
#             tenant_profile["tenant_uid"] = db.call('space.new_tenant_uid')['result'][0]['new_id']
#             file = request.files.get("tenant_photo")
#             if file:
#                 key = f'tenantProfileInfo/{tenant_profile["tenant_uid"]}/tenant_photo'
#                 tenant_profile["tenant_photo_url"] = uploadImage(file, key, '')
#             response = db.insert('tenantProfileInfo', tenant_profile)
#             response["tenant_uid"] = tenant_profile["tenant_uid"]
#         return response
    
#     def put(self):
#         response = {}
#         print('in TenantProfile')
#         payload = request.get_json()
#         if payload.get('tenant_uid') is None:
#             raise BadRequest("Request failed, no UID in payload.")
#         key = {'tenant_uid': payload.pop('tenant_uid')}
#         with connect() as db:
#             response = db.update('tenantProfileInfo', key, clean_json_data(payload))
#         return response

# class TenantProfileByTenantUid(Resource):
#     # decorators = [jwt_required()]

#     def get(self, tenant_id):
#         print('in Tenant Profile')
#         response = {}

#         with connect() as db:
#             print("in connect loop")
#             profileQuery = db.execute(""" 
#                     -- TENANT PROFILE
#                     SELECT * FROM space.tenantProfileInfo
#                     WHERE tenant_uid = \'""" + tenant_id + """\';
#                     """)
            

#             # print("Query: ", profileQuery)
#             # items = execute(profileQuery, "get", conn)
#             # print(items)
#             response["Profile"] = profileQuery
#             return response


# class RolesByUserid(Resource):
#     def get(self, user_id):
#         print('in RolesByUserid')
#         with connect() as db:
#             response = db.select('user_profiles', {"user_id": user_id})
#         return response



# class BusinessProfileWeb(Resource):
#     def post(self):
#         response = {}
#         business_profile = request.form.to_dict()
#         print("Business Profile Info: ", business_profile)

#         with connect() as db:
#             business_profile["business_uid"] = db.call('space.new_business_uid')['result'][0]['new_id']
#             business_profile["employee_uid"] = db.call('space.new_employee_uid')['result'][0]['new_id']

#             # Photos
#             # file_business = request.files.get("business_photo")
#             # file_employee = request.files.get("employee_photo")

#             # if file_business:
#             #     key = f'businessProfileInfo/{business_profile["business_uid"]}/business_photo'
#             #     business_profile["business_photo_url"] = uploadImage(file_business, key, '')
#             # if file_employee:
#             #     key = f'employees/{business_profile["employee_uid"]}/employee_photo'
#             #     business_profile["employee_photo_url"] = uploadImage(file_employee, key, '')

#             # Insert into databases
#             response = db.insert('businessProfileInfo', business_profile)
#             response["business_uid"] = business_profile["business_uid"]

#             response = db.insert('employees', business_profile)
#             response["employee_uid"] = business_profile["employee_uid"]

#             # print("Payment Methods: ", business_profile["paymentpayload"])


#         return response

#     # def post(self):
#     #     response = {    }
#     #     payload = request.get_json()
#     #     print(payload)
#     #     with connect() as db:
#     #         query_response = db.insert('paymentMethods', payload)
#     #         print(query_response)
#     #         response = query_response
#     #     return response

#     # def post(self):
#     #     response = {}
#     #     employee = request.form.to_dict()
#     #     with connect() as db:
#     #         employee["employee_uid"] = db.call('space.new_employee_uid')['result'][0]['new_id']
#     #         file = request.files.get("employee_photo")
#     #         if file:
#     #             key = f'employees/{employee["employee_uid"]}/employee_photo'
#     #             employee["employee_photo_url"] = uploadImage(file, key, '')
#     #         response = db.insert('employees', employee)
#     #         response["employee_uid"] = employee["employee_uid"]
#     #     return response

class BusinessProfile(Resource):
    # def post(self):
    #     response = {}
    #     business_profile = request.form.to_dict()
    #     with connect() as db:
    #         business_profile["business_uid"] = db.call('space.new_business_uid')['result'][0]['new_id']
    #         file = request.files.get("business_photo")
    #         if file:
    #             key = f'businessProfileInfo/{business_profile["business_uid"]}/business_photo'
    #             business_profile["business_photo_url"] = uploadImage(file, key, '')
    #         response = db.insert('businessProfileInfo', business_profile)
    #         response["business_uid"] = business_profile["business_uid"]
    #     return response

    # def put(self):
    #     response = {}
    #     payload = request.get_json()
    #     if payload.get('business_uid') is None:
    #         raise BadRequest("Request failed, no UID in payload.")
    #     key = {'business_uid': payload.pop('business_uid')}
    #     print(payload)
    #     with connect() as db:
    #         response = db.update('businessProfileInfo', key, payload)
    #     return response
    
    def get(self):
        response = {}
        where = request.args.to_dict()
        with connect() as db:
            response = db.select('businessProfileInfo', where)
        return response

class BusinessProfileList(Resource):
    def get(self, business_type):
        response = {}
        with connect() as db:
            business = db.execute("""
                            SELECT business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, 
                             business_locations, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                            FROM space.businessProfileInfo 
                            WHERE business_type = \'""" + business_type + """\' 
            """)
        response["Businesses"] = business
        return response

# class BusinessProfileByUid(Resource):
    def get(self, business_uid):
        print('in BusinessProfileByUid')
        with connect() as db:
            response = db.select('businessProfileInfo', {"business_uid": business_uid})
        return response


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
        response = {}
        payload = request.form.to_dict()
        print("Profile Payload: ", payload)
        

        if payload.get('business_uid'):
            valid_columns = {"business_uid", "business_user_id", "business_type", "business_name", "business_phone_number", "business_email", "business_ein_number", "business_services_fees", "business_locations", "business_documents", 'business_address', "business_unit", "business_city", "business_state", "business_zip", "business_photo_url"}
            filtered_payload = {key: value for key, value in payload.items() if key in valid_columns}
            key = {'business_uid': filtered_payload.pop('business_uid')}
            file = request.files.get("business_photo")
            if file:
                key1 = f'businessProfileInfo/{key["business_uid"]}/business_photo'

                try:
                    deleteImage(key1)
                    print(f"Deleted existing file {key1}")
                except s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        print(f"File {key1} does not exist")
                    else:
                        print(f"Error deleting file {key1}: {e}")

                filtered_payload["business_photo_url"] = uploadImage(file, key1, '')
            print("business")
            with connect() as db:
                response = db.update('businessProfileInfo', key, filtered_payload)
        
        
        # Profile Picture is Unique to Profile 
        if payload.get('tenant_uid'):
            tenant_uid = payload.get('tenant_uid')
            print("In Tenant")
            key = {'tenant_uid': payload.pop('tenant_uid')}
            print("Tenant Key: ", key)

            file = request.files.get("tenant_photo_url")
            if file:
                key1 = f'tenantProfileInfo/{key["tenant_uid"]}/tenant_photo'
                
                try:
                    deleteImage(key1)
                    print(f"Deleted existing file {key1}")
                except s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        print(f"File {key1} does not exist")
                    else:
                        print(f"Error deleting file {key1}: {e}")

                payload["tenant_photo_url"] = uploadImage(file, key1, '')


            # Check if documents are being added OR deleted
            current_docs = payload.get('tenant_documents')
            add_docs = payload.get('tenant_documents_details') 
            del_docs = payload.get('deleted_documents')
            print("Current Documents: ", current_docs, type(current_docs))
            print("Documents to Add: ", add_docs, type(add_docs))
            print("Documents to Del: ", del_docs, type(del_docs))


            # Code requires that FrontEnd always passes in tenant_documents whenever adding or deleting
            # if add_docs is not None or del_docs is not None:    
            if current_docs is not None:    
                # Store Existing Documents
                tenant_docs = json.loads(payload.get('tenant_documents', '[]'))
                print("Tenant Docs: ", tenant_docs)


                # Check if documents are being added
                if add_docs is not None:
                        
                        json_add_docs = json.loads(add_docs)     
                        print("Document Details: ", json_add_docs)           
                        del payload['tenant_documents_details']

                        files = request.files

                        if files:
                            print("In tenant files: ", files)
                            detailsIndex = 0
                            for fileKey in files:
                                file = files[fileKey]
                            # for file in files:
                                file_info = json_add_docs[detailsIndex]

                                if file and allowed_file(file.filename):
                                    s3key = f'tenants/{tenant_uid}/{file.filename}'
                                    print("S3 Key: ", s3key)
                                    s3_link = uploadImage(file, s3key, '')
                                    # s3_link = 'doc_link' # to test locally
                                    docObject = {}
                                    docObject["link"] = s3_link
                                    docObject["filename"] = file.filename
                                    docObject["type"] = file_info["fileType"]
                                    tenant_docs.append(docObject)
                                detailsIndex += 1

                            payload['tenant_documents'] = json.dumps(tenant_docs)


                

                # Check if documents are being deleted
                if del_docs is not None:
                
                    # delete documents from s3
                    print("In Delete")              
                    del payload['deleted_documents']
                    deleted_docs = []
                    
                    if del_docs is not None and isinstance(del_docs, str):
                        try:                
                            deleted_docs = ast.literal_eval(del_docs)                                
                        except (ValueError, SyntaxError) as e:
                            print(f"Error parsing the deleted_docs string: {e}")
                            
                    
                    s3Client = boto3.client('s3')

                    response = {'s3_delete_responses': []}
                    if(deleted_docs):
                        try:                
                            objects_to_delete = []
                            for doc in deleted_docs:                    
                                docKey = "tenants/" + doc.split("tenants/")[-1]
                                objects_to_delete.append(docKey)               

                            for obj_key in objects_to_delete:                    
                                delete_response = s3Client.delete_object(Bucket='io-pm', Key=f'{obj_key}')
                                response['s3_delete_responses'].append({obj_key: delete_response})

                        except Exception as e:
                            print(f"Deletion from s3 failed: {str(e)}")
                            response['s3_delete_error'] = f"Deletion from s3 failed: {str(e)}"
                    



            # Update File List in Database        
            print("tenant")
            print("key: ", key )
            print("payload: ", payload)

            with connect() as db:
                response['tenant_docs'] = db.update('tenantProfileInfo', key, payload)
            print("Response:" , response)






        if payload.get('owner_uid'):
            # print("In Owner")
            key = {'owner_uid': payload.pop('owner_uid')}
            # print("Owner Key: ", key)
            file = request.files.get("owner_photo_url")
            if file:
                key1 = f'ownerProfileInfo/{key["owner_uid"]}/owner_photo'

                try:
                    deleteImage(key1)
                    print(f"Deleted existing file {key1}")
                except s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        print(f"File {key1} does not exist")
                    else:
                        print(f"Error deleting file {key1}: {e}")

                payload["owner_photo_url"] = uploadImage(file, key1, '')
            # print("owner")
            # print("Owner Payload: ", payload)
            with connect() as db:
                response = db.update('ownerProfileInfo', key, payload)
            # print(response)

        
        if payload.get('employee_uid'):
            valid_columns = {"employee_uid", "employee_user_id", "employee_business_id", "employee_first_name", "employee_last_name", "employee_phone_number", "employee_email", "employee_role", "employee_photo_url", "employee_ssn", "employee_address", "employee_unit", "employee_city", "employee_state", "employee_zip", "employee_verification"}
            filtered_payload = {key: value for key, value in payload.items() if key in valid_columns}
            key = {'employee_uid': filtered_payload.pop('employee_uid')}
            file = request.files.get("employee_photo_url")
            if file:
                key1 = f'employees/{key["employee_uid"]}/employee_photo'

                try:
                    deleteImage(key1)
                    print(f"Deleted existing file {key1}")
                except s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        print(f"File {key1} does not exist")
                    else:
                        print(f"Error deleting file {key1}: {e}")

                filtered_payload["employee_photo_url"] = uploadImage(file, key1, '')
            print("employee")
            with connect() as db:
                response = db.update('employees', key, filtered_payload)

        # else:
        #     raise BadRequest("Request failed, no UID in payload.")
        
        return response
