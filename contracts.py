from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from werkzeug.utils import secure_filename


from data_pm import connect, uploadImage, s3
import json
import os
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
        response = {}
        data = request.form
        contract_id = data.get("contract_uid")
        print(f"Updating contract with ID {contract_id}")
        with connect() as db:


            fields = [
                "contract_property_id",
                "contract_business_id",
                "contract_start_date",
                "contract_end_date",
                "contract_fees",
                "contract_assigned_contacts",
                "contract_documents",
                "contract_name",
                "contract_status",
                "contract_early_end_date"
            ]

            updated_contract = {}
            for field in fields:
                if field in data:
                    updated_contract[field] = data.get(field)


            # # get previously uploaded documents from DB
            # contract_uid = {'contract_uid': contract_id}
            # query = db.select('contracts', contract_uid)
            # print(f"QUERY: {query}")
            # try:
            #     contract_from_db = query.get('result')[0]
            #     contract_docs = contract_from_db.get("contract_documents")
            #     contract_docs = ast.literal_eval(contract_docs) if contract_docs else []  # convert to list of documents
            #     print('type: ', type(contract_docs))
            #     print(f'previously saved documents: {contract_docs}')
            # except IndexError as e:
            #     print(e)
            #     raise BadRequest("Request failed, no such CONTRACT in the database.")
            
            contract_docs = data.get('contract_documents')
            contract_docs = ast.literal_eval(contract_docs) if contract_docs else []  # convert to list of documents
            # print("contract_docs")
            # print(contract_docs)

            files = request.files
            
            if(data.get('contract_documents_details')):
                files_details = json.loads(data.get('contract_documents_details'))
                print("FILES DETAILS LIST")
                print(files_details)

            # contract_docs = []

            if files:
                detailsIndex = 0
                for key in files:
                    file = files[key]
                    file_info = files_details[detailsIndex]
                    # print("FILE DETAILS")
                    # print(file_info)
                    # file_path = os.path.join(os.getcwd(), file.filename)
                    # file.save(file_path)
                    if file and allowed_file(file.filename):
                        key = f'contracts/{contract_id}/{file.filename}'
                        s3_link = uploadImage(file, key, '')
                        # s3_link = 'doc_link' # to test locally
                        docObject = {}
                        docObject["link"] = s3_link
                        docObject["filename"] = file.filename
                        docObject["type"] = file_info["fileType"]
                        contract_docs.append(docObject)
                    detailsIndex += 1

                updated_contract['contract_documents'] = json.dumps(contract_docs)
                # print("------updated_contract['contract_documents']------")
                # print(updated_contract['contract_documents'])

            # Check if there are fields to update
            if updated_contract:
                # Update the contract in the database based on the contract_id
                key = {'contract_uid': contract_id}
                result = db.update('contracts', key, updated_contract)

                if result:
                    response["message"] = f"Contract with ID {contract_id} has been updated."
                else:
                    response["error"] = f"Contract with ID {contract_id} not found."
            else:
                response["error"] = "No fields to update."

        return response

    # def put(self):
    #     print('in Contracts')
    #     payload = request.get_json()
    #     if payload.get('contract_uid') is None:
    #         raise BadRequest("Request failed, no UID in payload.")
    #     key = {'contract_uid': payload.pop('contract_uid')}
    #     with connect() as db:
    #         response = db.update('contracts', key, payload)
    #     return response

    def get(self, user_id):
        if user_id.startswith("600"):
            print('in ContractsByBusiness')
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



class ContractsByBusiness(Resource):
    def get(self, business_id):
        print('in ContractsByBusiness')
        with connect() as db:
            response = db.select('contracts', {"contract_business_id": business_id})
        return response
