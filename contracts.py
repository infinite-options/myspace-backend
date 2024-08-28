from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity


# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, uploadImage, deleteImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest
import ast


ALLOWED_EXTENSIONS = {'txt', 'pdf', 'doc', 'docx'}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


class Contracts(Resource):
    # def post(self):
    #     print('in Contracts')
    #     payload = request.get_json()
    #     with connect() as db:
    #         response = db.insert('contracts', payload)
    #     return response

    def get(self, user_id):
        if user_id.startswith("600"):
            print('in ContractsByBusiness')
            with connect() as db:
                response = db.execute("""
                SELECT -- *,
                    property_id, property_unit, property_address, property_city, property_zip, property_owner_id, po_owner_percent
                    , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                    -- , owner_address, owner_unit, owner_city, owner_state, owner_zip
                    , owner_photo_url
                    , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                    , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email
                    -- , business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                FROM space.o_details o
                LEFT JOIN space.properties p ON o.property_id =p.property_uid 
                LEFT JOIN space.b_details b ON o.property_id = b.contract_property_id
                -- WHERE o.owner_uid = \'""" + user_id + """\';
                WHERE b.business_uid = \'""" + user_id + """\';
                """)
                return response

        elif user_id.startswith("110"):
            print('in ContractsByOwner')
            with connect() as db:
                response = db.execute("""
                SELECT -- *,
                    property_id, property_owner_id, po_owner_percent
                    , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                    -- , owner_address, owner_unit, owner_city, owner_state, owner_zip
                    , owner_photo_url
                    , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                    , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email
                    -- , business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                FROM space.o_details o
                LEFT JOIN space.b_details b ON o.property_id = b.contract_property_id
                WHERE o.owner_uid = \'""" + user_id + """\';
                -- WHERE b.business_uid = \'""" + user_id + """\';
                """)
                return response
        else:
            return "No records for this Uid"

    def post(self):
        print("In Contracts Start")
        with connect() as db:
            data = request.form
            fields = [
                'contract_business_id'
                , "contract_start_date"
                , 'contract_end_date'
                , "contract_fees"
                , "contract_assigned_contacts"
                , "contract_documents"
                , "contract_name"
                , "contract_status"
                , "contract_early_end_date"
            ]
            properties_l = data.get("contract_property_ids")
            print(properties_l)

            # properties_l is a list since you may create contracts for multiple properties at once
            # If contracts is called as part of AddProperty.jsx then it is a list of one property
            properties = json.loads(properties_l)
            print(properties)
            business_id = data.get('contract_business_id')
            print(business_id)

            # Check if there are active contracts for any properties in the given list
            index_to_remove = []
            properties_removed = []
            for i in range(len(properties)):
                print('in Contracts Loop', len(properties), i, properties[i])
                with connect() as db:
                    response = db.execute("""
                                SELECT * From space.contracts
                                WHERE contract_property_id = \'""" + properties[i] + """\' AND contract_business_id = \'""" + business_id + """\'
                                AND contract_status in ('NEW','ACTIVE','SENT');
                                """)
                    if len(response['result']) != 0:
                        index_to_remove.append(i)
                        properties_removed.append(properties[i])
            print("Response: ", response)
            print("Properties ", properties)
            print("Index to Remove: ", index_to_remove)
            # response = {}
            print("Properties to be removed: ", properties_removed)

            # removes any properties from the List that already have contracts
            for i in sorted(index_to_remove, reverse = True):
                del properties[i]
            print("Actual list of properties to be added:" , properties)
            # response['properties with contracts'] = properties_removed

            # Start actual query to add new contracts
            
            with connect() as db:
                for i in range(len(properties)):
                    print("loop", i)
                    newContract = {}
                    print(db.call('new_contract_uid'))
                    newRequestID = db.call('new_contract_uid')['result'][0]['new_id']
                    newContract['contract_uid'] = newRequestID
                    print("In /contracts - POST. new contract UID = ", newRequestID)
                    newContract["contract_property_id"] = properties[i]
                    for field in fields:
                        if field in data:
                            newContract[field] = data.get(field)
                            # print(newContract[field])

                    response = db.insert('contracts', newContract)
                    response['contract_UID'] = newRequestID
            response['properties with contracts 2'] = properties_removed
        return response

    def put(self):
        print("\nIn Contracts PUT")
        response = {}
        payload = request.form.to_dict()
        print("Contract Update Payload: ", payload)

        # Verify uid has been included in the data
        if payload.get('contract_uid') is None:
            print("No contract_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        contract_uid = payload.get('contract_uid')
        key = {'contract_uid': payload.pop('contract_uid')}
        print("Contract Key: ", key)
   
        # Check if files already exist
        # Put current db files into current_documents
        current_documents = []
        if payload.get('contract_documents') is not None:
            current_documents =ast.literal_eval(payload.get('contract_documents'))
            print("Current images: ", current_documents, type(current_documents))

        if payload.get('contract_documents_details') is not None:
            documents_details = json.loads(payload.pop('contract_documents_details'))
            print("documents_details: ", documents_details, type(documents_details))

        # Check if images are being added OR deleted
        documents = []
        i = 0
        documentFiles = {}
        
        while True:
            filename = f'file_{i}'
            print("\nPut file into Filename: ", filename) 
            

            file = request.files.get(filename)
            print("File:" , file)    
            # print("Filename:", file.filename)
            # print("File Type:", file.content_type) 

        
            s3Link = payload.get(filename)
            print("S3Link: ", s3Link)


            if file:
                print("In File if Statement")
                documentFiles[filename] = file
                unique_filename = filename + "_" + datetime.utcnow().strftime('%Y%m%d%H%M%SZ')
                image_key = f'contracts/{contract_uid}/{unique_filename}'
                # This calls the uploadImage function that generates the S3 link
                document = uploadImage(file, image_key, '')  # This returns the document http link
                print("Document after upload: ", document)

                docObject = {}
                docObject["link"] = document
                docObject["filename"] = file.filename
                docObject["type"] = file.content_type
                docObject["fileType"] = next((doc['fileType'] for doc in documents_details if doc['fileIndex'] == i), None)
                print("Doc Object: ", docObject)

                documents.append(docObject)

                


            elif s3Link:
                documentFiles[filename] = s3Link
                documents.append(s3Link)

                

            else:
                break
            i += 1
        
        print("Documents after loop: ", documents)
        if documents != []:
            current_documents.extend(documents)
            payload['contract_documents'] = json.dumps(current_documents) 

        # Delete Images
        if payload.get('delete_documents'):
            delete_documents = ast.literal_eval(payload.get('delete_documents'))
            del payload['delete_documents']
            print(delete_documents, type(delete_documents), len(delete_documents))
            for document in delete_documents:
                print("Document to Delete: ", document, type(document))
                # Delete from db list assuming it is in db list
                try:
                    current_documents.remove(document)
                except:
                    print("Document not in list")

                #  Delete from S3 Bucket
                try:
                    delete_key = document.split('io-pm/', 1)[1]
                    print("Delete key", delete_key)
                    deleteImage(delete_key)
                except: 
                    print("could not delete from S3")
            
            print("Updated List of Images: ", current_documents)

            payload['contract_documents'] = json.dumps(current_documents) 

        # Write to Database
        with connect() as db:
            print("Checking Inputs: ", key, payload)
            response['contract_info'] = db.update('contracts', key, payload)
            # print("Response:" , response)
        return response


    # def put(self):
    #     print("In contracts PUT")
    #     response = {}
    #     data = request.form
    #     print("Form Data: ", data)
    #     contract_id = data.get("contract_uid")
    #     print(f"Updating contract with ID {contract_id}")
    #     with connect() as db:


    #         fields = [
    #             "contract_property_id",
    #             "contract_business_id",
    #             "contract_start_date",
    #             "contract_end_date",
    #             "contract_fees",
    #             "contract_assigned_contacts",
    #             "contract_documents",
    #             "contract_name",
    #             "contract_status",
    #             "contract_early_end_date"
    #         ]

    #         updated_contract = {}
    #         for field in fields:
    #             if field in data:
    #                 updated_contract[field] = data.get(field)
    #         print("Updated Contract: ", updated_contract)

            
    #         contract_docs = data.get('contract_documents')
    #         contract_docs = ast.literal_eval(contract_docs) if contract_docs else []  # convert to list of documents
    #         print("contract_docs: ", contract_docs)

    #         files = request.files
    #         print("Files: ", files)


    #         if(data.get('contract_documents_details')):
    #             files_details = json.loads(data.get('contract_documents_details'))
    #             print("FILES DETAILS LIST", files_details)


    #         if files:
    #             print("In for loop")
    #             detailsIndex = 0
    #             for key in files:
    #                 print("Key: ", key)
    #                 file = files[key]
    #                 print("File for FOR loop:  ", file)
    #                 file_info = files_details[detailsIndex]
    #                 # print("FILE DETAILS")
    #                 # print(file_info)
    #                 if file and allowed_file(file.filename):
    #                     key = f'contracts/{contract_id}/{file.filename}'
    #                     s3_link = uploadImage(file, key, '')
    #                     # s3_link = 'doc_link' # to test locally
    #                     docObject = {}
    #                     docObject["link"] = s3_link
    #                     docObject["filename"] = file.filename
    #                     docObject["type"] = file_info["fileType"]
    #                     contract_docs.append(docObject)
    #                 detailsIndex += 1

    #             updated_contract['contract_documents'] = json.dumps(contract_docs)
    #             # print("------updated_contract['contract_documents']------")
    #             # print(updated_contract['contract_documents'])

    #         # Check if there are fields to update
    #         if updated_contract:
    #             print("in Updated Contract")
    #             # Update the contract in the database based on the contract_id
    #             key = {'contract_uid': contract_id}
    #             print("Key: ", key)
    #             print("Data to update: ", updated_contract )
    #             result = db.update('contracts', key, updated_contract)
    #             print("Result: ", result)
    #             if result:
    #                 response["message"] = f"Contract with ID {contract_id} has been updated."
    #             else:
    #                 response["error"] = f"Contract with ID {contract_id} not found."
    #         else:

    #             response["error"] = "No fields to update."

    #     return response

