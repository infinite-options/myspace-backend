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
        print("In Contracts")
        with connect() as db:
            data = request.form
            fields = [
                "contract_uid"
                ,"contract_property_id"
                , 'contract_business_id'
                , "contract_start_date"
                , 'contract_end_date'
                , "contract_fees"
                , "contract_assigned_contacts"
                , "contract_documents"
                , "contract_name"
                , "contract_status"
                , "contract_early_end_date"
            ]
            newContract = {}
            for field in fields:
                newContract[field] = data.get(field)
                print(newContract[field])

            response = db.insert('contracts', newContract)
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


            # get previously uploaded documents
            contract_uid = {'contract_uid': contract_id}
            query = db.select('contracts', contract_uid)
            print(f"QUERY: {query}")
            try:
                contract_from_db = query.get('result')[0]
                contract_docs = contract_from_db.get("contract_documents")
                contract_docs = ast.literal_eval(contract_docs) if contract_docs else []  # convert to list of documents
                print('type: ', type(contract_docs))
                print(f'previously saved documents: {contract_docs}')
            except IndexError as e:
                print(e)
                raise BadRequest("Request failed, no such CONTRACT in the database.")

            files = request.files
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
                        docObject = {}
                        docObject["link"] = s3_link
                        docObject["filename"] = file.filename
                        docObject["type"] = file_info["fileType"]
                        contract_docs.append(docObject)
                    detailsIndex += 1

                updated_contract['contract_documents'] = json.dumps(contract_docs)
                print(updated_contract['contract_documents'])

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
                response = db.select('contracts', {"contract_business_id": user_id})
            return response

        elif user_id.startswith("110"):
            print('in ContractsByOwner')
            with connect() as db:
                response = db.execute("""
                SELECT c.*, po.property_owner_id
                FROM space.contracts c
                LEFT JOIN space.property_owner  po ON c.contract_property_id = po.property_id
                WHERE po.property_owner_id = \'""" + user_id + """\';
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
