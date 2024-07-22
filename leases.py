
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, deleteImage, uploadImage, s3
import boto3
import json
import datetime
# from datetime import date, datetime, timedelta
# from dateutil.relativedelta import relativedelta
# import calendar
from werkzeug.exceptions import BadRequest
import ast

ALLOWED_EXTENSIONS = {'txt', 'pdf', 'doc', 'docx'}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


class LeaseDetails(Resource):
    # decorators = [jwt_required()]

    def get(self, filter_id):
        print('in Lease Details', filter_id)
        response = {}

        with connect() as db:
            if filter_id[:3] == "110":
                print('in Owner Lease Details')

                leaseQuery = db.execute(""" 
                        -- OWNER, PROPERTY MANAGER, TENANT LEASES
                        SELECT * 
                        FROM (
                            -- FIND ALL ACTIVE/ENDED LEASES WITH OR WITHOUT A MOVE OUT DATE
                            SELECT *,
                            DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) AS lease_days_remaining,
                            CASE
                                    WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) > DATEDIFF(LAST_DAY(DATE_ADD(NOW(), INTERVAL 11 MONTH)), NOW()) THEN 'FUTURE' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'FUTURE'
                                    WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) < 0 THEN 'MTM' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'MTM'
                                    ELSE MONTHNAME(STR_TO_DATE(LEFT(lease_end, 2), '%m'))
                            END AS lease_end_month
                            FROM space.leases 
                            WHERE (lease_status = "ACTIVE" OR lease_status = "ENDED")
                            ) AS l
                        LEFT JOIN (
                            SELECT fees_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                                ('leaseFees_uid', leaseFees_uid,
                                'fee_name', fee_name,
                                'fee_type', fee_type,
                                'charge', charge,
                                'due_by', due_by,
                                'late_by', late_by,
                                'late_fee', late_fee,
                                'perDay_late_fee', perDay_late_fee,
                                'frequency', frequency,
                                'available_topay', available_topay,
                                'due_by_date', due_by_date
                                )) AS lease_fees
                                FROM space.leaseFees
                                GROUP BY fees_lease_id) as f ON lease_uid = fees_lease_id
                        LEFT JOIN space.properties ON property_uid = lease_property_id
                        LEFT JOIN space.o_details ON property_id = lease_property_id
                        LEFT JOIN (
                            SELECT lt_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                                ('tenant_uid', tenant_uid,
                                'lt_responsibility', if(lt_responsibility IS NOT NULL, lt_responsibility, "1"),
                                'tenant_first_name', tenant_first_name,
                                'tenant_last_name', tenant_last_name,
                                'tenant_phone_number', tenant_phone_number,
                                'tenant_email', tenant_email,
                                'tenant_drivers_license_number', tenant_drivers_license_number,
                                'tenant_drivers_license_state', tenant_drivers_license_state,
                                'tenant_ssn', tenant_ssn
                                )) AS tenants
                                FROM space.t_details 
                                GROUP BY lt_lease_id) as t ON lease_uid = lt_lease_id
                        LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
                        LEFT JOIN space.u_details ON utility_property_id = lease_property_id
                        -- WHERE owner_uid = "110-000003"
                        -- WHERE contract_business_id = "600-000003"
                        -- WHERE tenants LIKE "%350-000040%"
                        WHERE owner_uid = \'""" + filter_id + """\'
                        -- WHERE contract_business_id = \'""" + filter_id + """\'
                        -- WHERE tenants LIKE '%""" + filter_id + """%'
                        ;
                        """)
                
            elif filter_id[:3] == "600":
                print('in PM Lease Details')

                leaseQuery = db.execute(""" 
                        -- OWNER, PROPERTY MANAGER, TENANT LEASES
                        SELECT * 
                        FROM (
                            -- FIND ALL ACTIVE/ENDED LEASES WITH OR WITHOUT A MOVE OUT DATE
                            SELECT *,
                            DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) AS lease_days_remaining,
                            CASE
                                    WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) > DATEDIFF(LAST_DAY(DATE_ADD(NOW(), INTERVAL 11 MONTH)), NOW()) THEN 'FUTURE' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'FUTURE'
                                    WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) < 0 THEN 'MTM' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'MTM'
                                    ELSE MONTHNAME(STR_TO_DATE(LEFT(lease_end, 2), '%m'))
                            END AS lease_end_month
                            FROM space.leases 
                            WHERE (lease_status = "ACTIVE" OR lease_status = "ENDED")
                            ) AS l
                        LEFT JOIN (
                            SELECT fees_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                                ('leaseFees_uid', leaseFees_uid,
                                'fee_name', fee_name,
                                'fee_type', fee_type,
                                'charge', charge,
                                'due_by', due_by,
                                'late_by', late_by,
                                'late_fee', late_fee,
                                'perDay_late_fee', perDay_late_fee,
                                'frequency', frequency,
                                'available_topay', available_topay,
                                'due_by_date', due_by_date
                                )) AS lease_fees
                                FROM space.leaseFees
                                GROUP BY fees_lease_id) as f ON lease_uid = fees_lease_id
                        LEFT JOIN space.properties ON property_uid = lease_property_id
                        LEFT JOIN space.o_details ON property_id = lease_property_id
                        LEFT JOIN (
                            SELECT lt_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                                ('tenant_uid', tenant_uid,
                                'lt_responsibility', if(lt_responsibility IS NOT NULL, lt_responsibility, "1"),
                                'tenant_first_name', tenant_first_name,
                                'tenant_last_name', tenant_last_name,
                                'tenant_phone_number', tenant_phone_number,
                                'tenant_email', tenant_email,
                                'tenant_drivers_license_number', tenant_drivers_license_number,
                                'tenant_drivers_license_state', tenant_drivers_license_state,
                                'tenant_ssn', tenant_ssn
                                )) AS tenants
                                FROM space.t_details 
                                GROUP BY lt_lease_id) as t ON lease_uid = lt_lease_id
                        LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
                        LEFT JOIN space.u_details ON utility_property_id = lease_property_id
                        -- WHERE owner_uid = "110-000003"
                        -- WHERE contract_business_id = "600-000003"
                        -- WHERE tenants LIKE "%350-000040%"
                        -- WHERE owner_uid = \'""" + filter_id + """\'
                        WHERE contract_business_id = \'""" + filter_id + """\'
                        -- WHERE tenants LIKE '%""" + filter_id + """%'
                        ;
                        """)
                
                # print(leaseQuery)

            elif filter_id[:3] == "350":
                print('in Tenant Lease Details')

                leaseQuery = db.execute(""" 
                        -- OWNER, PROPERTY MANAGER, TENANT LEASES
                        SELECT * 
                        FROM (
                            -- FIND ALL ACTIVE/ENDED LEASES WITH OR WITHOUT A MOVE OUT DATE
                            SELECT *,
                            DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) AS lease_days_remaining,
                            CASE
                                    WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) > DATEDIFF(LAST_DAY(DATE_ADD(NOW(), INTERVAL 11 MONTH)), NOW()) THEN 'FUTURE' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'FUTURE'
                                    WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) < 0 THEN 'MTM' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'MTM'
                                    ELSE MONTHNAME(STR_TO_DATE(LEFT(lease_end, 2), '%m'))
                            END AS lease_end_month
                            FROM space.leases 
                            WHERE (lease_status = "ACTIVE" OR lease_status = "ENDED")
                            ) AS l
                        LEFT JOIN (
                            SELECT fees_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                                ('leaseFees_uid', leaseFees_uid,
                                'fee_name', fee_name,
                                'fee_type', fee_type,
                                'charge', charge,
                                'due_by', due_by,
                                'late_by', late_by,
                                'late_fee', late_fee,
                                'perDay_late_fee', perDay_late_fee,
                                'frequency', frequency,
                                'available_topay', available_topay,
                                'due_by_date', due_by_date
                                )) AS lease_fees
                                FROM space.leaseFees
                                GROUP BY fees_lease_id) as f ON lease_uid = fees_lease_id
                        LEFT JOIN space.properties ON property_uid = lease_property_id
                        LEFT JOIN space.o_details ON property_id = lease_property_id
                        LEFT JOIN (
                            SELECT lt_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                                ('tenant_uid', tenant_uid,
                                'lt_responsibility', if(lt_responsibility IS NOT NULL, lt_responsibility, "1"),
                                'tenant_first_name', tenant_first_name,
                                'tenant_last_name', tenant_last_name,
                                'tenant_phone_number', tenant_phone_number,
                                'tenant_email', tenant_email,
                                'tenant_drivers_license_number', tenant_drivers_license_number,
                                'tenant_drivers_license_state', tenant_drivers_license_state,
                                'tenant_ssn', tenant_ssn
                                )) AS tenants
                                FROM space.t_details 
                                GROUP BY lt_lease_id) as t ON lease_uid = lt_lease_id
                        LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
                        LEFT JOIN space.u_details ON utility_property_id = lease_property_id
                        -- WHERE owner_uid = "110-000003"
                        -- WHERE contract_business_id = "600-000003"
                        -- WHERE tenants LIKE "%350-000040%"
                        -- WHERE owner_uid = \'""" + filter_id + """\'
                        -- WHERE contract_business_id = \'""" + filter_id + """\'
                        WHERE tenants LIKE '%""" + filter_id + """%'
                        ;
                        """)
            else:
                leaseQuery = "UID Not Found"

            # print("Lease Query: ", leaseQuery)
            # items = execute(leaseQuery, "get", conn)
            # print(items)

            response["Lease_Details"] = leaseQuery

            return response


    def put(self):
        print("in Lease Details PUT!  THIS IS FOR TEST ONLY.  NOT AN ACTUAL ENDPOINT")
        data = request.form.to_dict()
        print("Data Reeived: ", data)
        key1 = data['documentID']
        print("Key: ", key1, type(key1))

        try:
            response = deleteImage(key1)
            print("Response: ", response)
            print(f"Deleted existing file {key1}")
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"File {key1} does not exist")
            else:
                print(f"Error deleting file {key1}: {e}")

        return


class LeaseApplication(Resource):

    def __call__(self):
        print("In Lease Application")

# CHECK IF TENANT HAS APPLIED TO THIS PROPERTY BEFORE
    def get(self, tenant_id, property_id):
        print("In Lease Application GET")
        print(tenant_id, property_id)
        response = {}

        with connect() as db:
            # print("in get lease applications")
            leaseQuery = db.execute(""" 
                    -- FIND LEASE-TENANT APPLICATION
                    SELECT *
                    FROM space.lease_tenant
                    LEFT JOIN space.leases ON lt_lease_id = lease_uid
                    WHERE lt_tenant_id = \'""" + tenant_id + """\' AND lease_property_id = \'""" + property_id + """\';
                    """)
            print(leaseQuery)
            print(leaseQuery['code'])
            print(len(leaseQuery['result']))
            

            if leaseQuery['code'] == 200 and int(len(leaseQuery['result']) > 0):
                # return("Tenant has already applied")
                print(leaseQuery['result'][0]['lt_lease_id'])
                return(leaseQuery['result'][0]['lt_lease_id'])

        return ("New Application")


    # decorators = [jwt_required()]

    # Trigger to generate UIDs created using Alter Table in MySQL

    def post(self):
        print("In Lease Application POST")
        response = {}

        # data = request.form
        # print("data as received",data)
        data = request.form.to_dict()
        # print("data for to dict",data)

        # Store tenant and property uids as separate variables - Need to modify this for multiple tenants
        if 'tenant_uid' in data and data['tenant_uid']!="":
            print("TENANT_ID",data['tenant_uid'])
            tenant_uid = data.get('tenant_uid')
            property_uid = data.get('lease_property_id')
            print(tenant_uid)
            # del data['tenant_uid']
            
            with connect() as db:

                # Verify that there is not already an active lease application
                response = db.execute("""
                            SELECT * From space.leases
                            LEFT JOIN space.lease_tenant ON lease_uid = lt_lease_id
                            WHERE lt_tenant_id = \'""" + tenant_uid + """\' 
                            AND lease_property_id = \'""" + property_uid + """\'
                            AND lease_status in ('NEW','PROCESSING');
                            """)
                if len(response['result']) != 0:
                    return ("Tenant Request for this Property is already Active, Please wait for the PM to respond.")
                else:

                    # New Application
                    ApplicationStatus = data.get('lease_status')

                    response = {}
                    fields = ["lease_property_id", "lease_start", "lease_end", "lease_end_notice_period", "lease_status", "lease_assigned_contacts",
                              "lease_documents", "lease_early_end_date", "lease_renew_status", "move_out_date","lease_move_in_date",
                              "lease_effective_date", "lease_application_date", "lease_docuSign", "lease_actual_rent"]
                    fields_with_lists = ["lease_adults", "lease_children", "lease_pets", "lease_vehicles", "lease_referred", "lease_assigned_contacts"
                                         , "lease_documents"]
                    fields_leaseFees = ["charge", "due_by", "due_by_date", "late_by", "fee_name", "fee_type", "frequency", "available_topay",
                                        "perDay_late_fee", "late_fee"]
                   
                    print("Data before if statement: ",data)

                    # Insert data into leases table
                    newLease = {}
                        
                    # Get New Lease UID    
                    lease_uid = db.call('new_lease_uid')['result'][0]['new_id']
                    newLease['lease_uid'] = lease_uid
                    response['lease_uid'] = lease_uid
                    print("\nNew Lease UID: ", lease_uid)   



                    # Put Incoming Data in Correct Fields
                    for field in fields:
                        print("field", field)
                        if field in data:
                            newLease[field] = data[field]
                            print("new_lease field", newLease[field])
                    for field in fields_with_lists:
                        print("field list", field)
                        if data.get(field) is None:
                            print(field,"Is None")
                            newLease[field] = '[]'
                        else: 
                            newLease[field] = data[field]
                            print("new_lease field list", newLease[field])
                    print("new_lease", newLease)
                    





                    # Initializes lease_docs for POST
                    lease_docs = []  

                    
                    if data.get("lease_documents") and request.files:
                        print("Documents attached")
                        docInfo = json.loads(data["lease_documents"])
                        print("Currently in lease_documents: ", type(docInfo), docInfo)
                        print("Type: ", docInfo[0]["type"])
                    
                    
                    
                        # Insert documents into Correct Fields
                        dateKey = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                        print(dateKey)
                        
                        files = request.files
                        print("files", files)
                        # files_details = json.loads(data.get('lease_documents_details'))
                        if files:
                            detailsIndex = 0
                            for key in files:
                                # print("key", key)
                                file = files[key]
                                # print("file", file)
                    
                                if file and allowed_file(file.filename):
                                    key = f'leases/{lease_uid}/{dateKey}/{file.filename}'
                                    # print("key", key)
                                    s3_link = uploadImage(file, key, '')
                                    docObject = {}
                                    docObject["link"] = s3_link
                                    docObject["filename"] = file.filename
                                    docObject["type"] = docInfo[detailsIndex]["type"]
                                    # docObject["type"] = file_info["fileType"]
                                    lease_docs.append(docObject)
                                detailsIndex += 1

                            newLease['lease_documents'] = json.dumps(lease_docs)
                            print("\nLease Docs: ", newLease['lease_documents'])


                    # Actual Insert Statement
                    print("About to insert: ", newLease)
                    response["lease"] = db.insert('leases', newLease)
                    # print("Data inserted into space.leases", response)

                    

                    # Insert data into leaseFees table
                    if "lease_fees" in data:
                        # print("lease_fees in data")
                        json_object = json.loads(data["lease_fees"])
                        # print("lease fees json_object", json_object)
                        for fees in json_object:
                            # print("fees",fees)
                            new_leaseFees = {}
                            new_leaseFees["fees_lease_id"] = lease_uid
                            for item in fields_leaseFees:
                                if item in fees:
                                    new_leaseFees[item] = fees[item]
                            response["lease_fees"] = db.insert('leaseFees', new_leaseFees)

                    
                    # Insert data into lease-Tenants table
                    tenant_responsibiity = str(1)      # Change this when we get multiple tenants

                    # with connect() as db:
                    print("Add record in lease_tenant table", lease_uid, tenant_uid, tenant_responsibiity)
                    # print("Made it to here")
                    ltQuery = (""" 
                            INSERT INTO space.lease_tenant
                            SET lt_lease_id = \'""" + lease_uid + """\'
                                , lt_tenant_id = \'""" + tenant_uid + """\'
                                , lt_responsibility = \'""" + tenant_responsibiity + """\';
                            """)
                    response["lease_tenant"] = db.execute(ltQuery, [], 'post')
                    response["tenant_id"] = tenant_uid
                    response["property_id"] = property_uid

                    # print(ltQuery)
                    # print("Data inserted into space.lease_tenant")

                # key = {'lease_uid': data.pop('lease_uid')}
                # print(key)

                response["lease_status"] = ApplicationStatus



        else:
            response['error'] = "Please Enter a correct tenant_id. Database was not updated"
        return response
	
    def put(self):
        print("\nIn Lease Application PUT")
        response = {}
        payload = request.form.to_dict()
        print("Lease Application Payload: ", payload)

        # Verify lease_uid has been included in the data
        if payload.get('lease_uid') is None:
            print("No lease_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        lease_uid = payload.get('lease_uid')
        print("In Lease Application PUT")
        key = {'lease_uid': payload.pop('lease_uid')}
        print("Lease Key: ", key)


        # Check if documents are being added OR deleted
        current_docs = payload.get('lease_documents')
        add_docs = payload.get('lease_documents_details') 
        del_docs = payload.get('deleted_documents')
        print("Current Documents: ", current_docs, type(current_docs))
        print("Documents to Add: ", add_docs, type(add_docs))
        print("Documents to Del: ", del_docs, type(del_docs))


        # Code requires that FrontEnd always passes in lease_documents whenever adding or deleting
        # if add_docs is not None or del_docs is not None:    
        if current_docs is not None:    
            # Store Existing Documents
            lease_docs = json.loads(payload.get('lease_documents', '[]'))
            print("Lease Application Docs: ", lease_docs)


            # Check if documents are being added
            if add_docs is not None:
            
                    json_add_docs = json.loads(add_docs)     
                    print("Document Details: ", json_add_docs)           
                    del payload['lease_documents_details']

                    files = request.files

                    if files:
                        print("In Lease Application files: ", files)
                        detailsIndex = 0
                        for fileKey in files:
                            file = files[fileKey]
                        # for file in files:
                            file_info = json_add_docs[detailsIndex]

                            if file and allowed_file(file.filename):
                                s3key = f'leases/{lease_uid}/{file.filename}'
                                print("S3 Key: ", s3key)
                                s3_link = uploadImage(file, s3key, '')
                                # s3_link = 'doc_link' # to test locally
                                docObject = {}
                                docObject["link"] = s3_link
                                docObject["filename"] = file.filename
                                docObject["type"] = file_info["fileType"]
                                lease_docs.append(docObject)
                            detailsIndex += 1

                        payload['lease_documents'] = json.dumps(lease_docs)


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
                            docKey = "leases/" + doc.split("leases/")[-1]
                            objects_to_delete.append(docKey)               

                        for obj_key in objects_to_delete:                    
                            delete_response = s3Client.delete_object(Bucket='io-pm', Key=f'{obj_key}')
                            response['s3_delete_responses'].append({obj_key: delete_response})

                    except Exception as e:
                        print(f"Deletion from s3 failed: {str(e)}")
                        response['s3_delete_error'] = f"Deletion from s3 failed: {str(e)}"
                



        # Update File List in Database        
        print("leases")
        print("key: ", key )
        print("payload: ", payload)


        with connect() as db:
            response['lease_docs'] = db.update('leases', key, payload)
        print("Response:" , response)
       








        # with connect() as db:
        
        
            
        #     response = {}
        #     fields = ["lease_property_id", "lease_start", "lease_end", "lease_end_notice_period", "lease_status", "lease_assigned_contacts",
        #                         "lease_documents", "lease_early_end_date", "lease_renew_status", "move_out_date","lease_move_in_date",
        #                         "lease_effective_date", "lease_application_date", "lease_docuSign", "lease_actual_rent", "lease_end_reason"]
        #     fields_with_lists = ["lease_adults", "lease_children", "lease_pets", "lease_vehicles", "lease_referred", "lease_assigned_contacts"
        #                         , "lease_documents"]
        #     fields_leaseFees = ["charge", "due_by", "due_by_date", "late_by", "fee_name", "fee_type", "frequency", "available_topay",
        #                         "perDay_late_fee", "late_fee"]

        #     # print("Data before if statement: ",data)
            
        #     # Insert data into leases table
        #     payload = {}
            
        #     lease_id = data['lease_uid']
        #     print('Lease id: ', lease_id)
            


            # Put Incoming Data in Correct Fields
            # for field in data:
                # # print("field: ", field)
                # if field in fields:
                #     payload[field] = data[field]
                #     # print("payload field", payload[field])
                # if field in fields_with_lists:
                #     payload[field] = data[field]
                    # print("payload field", payload[field])
            # for field in fields_with_lists:
            #             # print("field list", field)
            #             if data.get(field) is None:
            #                 # print(field,"Is None")
            #                 payload[field] = '[]'
            #             else: 
            #                 payload[field] = data[field]
            #                 # print("payload field list", payload[field])
            # print("Payload: ", payload)




            # # Get Existing Docs and store in lease_docs
            # lease_uid = {'lease_uid': lease_id}
            # query = db.select('leases', lease_uid)
            # print(f"QUERY: {query} {type(query)}")
            # try:
            #     lease_from_db = query.get('result')[0]
            #     # print("RESULT: ", lease_from_db)
            #     lease_vehs = lease_from_db.get("lease_vehicles")
            #     lease_docs = lease_from_db.get("lease_documents")
            #     print("\nvehicles: ", lease_vehs, type(lease_vehs))
            #     print("\nDOCS: ", lease_docs, type(lease_docs))
            #     # lease_docs = ast.literal_eval(lease_docs) if lease_docs else []  # Original statement that tried to convert to list of documents
            #     lease_docs = json.loads(lease_docs) if lease_docs and lease_docs.strip() else '[]'  # convert to list of documents
            #     print("Lease Docs: ", lease_docs, type(lease_docs))
            #     payload[ "lease_documents"] = lease_docs
            #     print("payload[ lease_documents]: ", payload[ "lease_documents"])

            #     # print('TYPE: ', lease_docs, type(lease_docs))
            #     # print(f'previously saved documents: {lease_docs}')
            # except IndexError as e:
            #     print(e)
            #     raise BadRequest("Request failed, no such CONTRACT in the database.")

            



        #     if data.get("lease_documents") and request.files:
        #         print("\nDocuments attached")
        #         docInfo = json.loads(data["lease_documents"])
        #         print("Currently in lease_documents: ", type(docInfo), docInfo)
        #         print("Type: ", docInfo[0]["type"])
            
            
            
        #         # Insert documents into Correct Fields
        #         dateKey = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        #         print(dateKey)
                
        #         files = request.files
        #         print("files", files)
        #         # files_details = json.loads(data.get('lease_documents_details'))
        #         if files:
        #             detailsIndex = 0
        #             for key in files:
        #                 # print("key", key)
        #                 file = files[key]
        #                 # print("file", file)
                        
        #                 if file and allowed_file(file.filename):
        #                     s3key = f'leases/{lease_id}/{dateKey}/{file.filename}'
        #                     print("S3 key", s3key)
        #                     s3_link = uploadImage(file, s3key, '')
        #                     docObject = {}
        #                     docObject["link"] = s3_link
        #                     docObject["filename"] = file.filename
        #                     docObject["type"] = docInfo[detailsIndex]["type"]
        #                     # docObject["type"] = file_info["fileType"]
        #                     lease_docs.append(docObject)
        #                 detailsIndex += 1

        #             payload['lease_documents'] = json.dumps(lease_docs)
        #             # print("\nLease Docs: ", payload['lease_documents'])


        #     # Actual Update Statement
        #     key = {'lease_uid': lease_id}
        #     print("About to update: ", payload)
        #     response["lease_update"] = db.update('leases', key, payload)

            
            
        #     # Insert data into leaseFees table
        #     if "lease_fees" in data:
        #         print("lease_fees in data")
        #         json_object = json.loads(data["lease_fees"])
        #         print("json_object",json_object)
        #         for fees in json_object:
        #             print("fees",fees)
        #             if "leaseFees_uid" in fees:
        #                 leaseFees_id = fees['leaseFees_uid']
        #                 # del fees['leaseFees_uid']
        #                 payload = {}
        #                 for field in fees:
        #                     if field in fields_leaseFees:                                                        
        #                         payload[field] = fees[field]
        #                 key = {'leaseFees_uid': leaseFees_id}
        #                 print("Fees Key: ", key)
        #                 print("Fees Payload: ", payload)
        #                 response["leaseFees_update"] = db.update('leaseFees', key, payload)
        #                 print(response["leaseFees_update"])
        #             else:
        #                 print("In else")
        #                 payload = {}
        #                 payload["fees_lease_id"] = data['lease_uid']
        #                 for field in fees:
        #                     if field in fields_leaseFees:                                                        
        #                         payload[field] = fees[field]
        #                 print("Fees Payload: ", payload)
        #                 response["leaseFees_update"] = db.insert('leaseFees', payload)
        #                 print(response["leaseFees_update"])

        return response
