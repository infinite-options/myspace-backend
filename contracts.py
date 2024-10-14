from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity



from data_pm import connect, uploadImage, deleteImage, s3, processDocument
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest
import ast

from queries import ContractDetails


ALLOWED_EXTENSIONS = {'txt', 'pdf', 'doc', 'docx'}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


class Contracts(Resource):

    def get(self, user_id):
        print("in Get Contracts")
        response = {}

        response = ContractDetails(user_id)
        return response

        # if user_id.startswith("600"):
        #     print('in ContractsByBusiness')
        #     with connect() as db:
        #         response = db.execute("""
        #         SELECT -- *,
        #             -- property_id, property_unit, property_address, property_city, property_state, property_zip, property_owner_id, po_owner_percent
        #             p.*
        #             , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
        #             -- , owner_address, owner_unit, owner_city, owner_state, owner_zip
        #             , owner_photo_url
        #             , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date, contract_end_notice_period, contract_m2m
        #             , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email
        #             -- , business_address, business_unit, business_city, business_state, business_zip, business_photo_url
        #         FROM space.o_details o
        #         LEFT JOIN space.properties p ON o.property_id =p.property_uid 
        #         LEFT JOIN space.b_details b ON o.property_id = b.contract_property_id
        #         -- WHERE b.business_uid = '600-000011'
        #         -- WHERE o.owner_uid = \'""" + user_id + """\';
        #         WHERE b.business_uid = \'""" + user_id + """\';
        #         """)
        #         return response

        # elif user_id.startswith("110"):
        #     print('in ContractsByOwner')
        #     with connect() as db:
        #         # response = db.execute("""
        #         # SELECT -- *,
        #         #     property_id, property_owner_id, po_owner_percent
        #         #     , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
        #         #     -- , owner_address, owner_unit, owner_city, owner_state, owner_zip
        #         #     , owner_photo_url
        #         #     , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date, contract_end_notice_period, contract_m2m
        #         #     , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email
        #         #     -- , business_address, business_unit, business_city, business_state, business_zip, business_photo_url
        #         #     , business_services_fees, business_locations
        #         # FROM space.o_details o
        #         # LEFT JOIN space.b_details b ON o.property_id = b.contract_property_id
        #         # WHERE o.owner_uid = \'""" + user_id + """\';
        #         # -- WHERE b.business_uid = \'""" + user_id + """\';
        #         # """)
        #         # return response

        #         response = db.execute("""
        #         SELECT -- *,
        #             -- property_id, property_unit, property_address, property_city, property_state, property_zip, property_owner_id, po_owner_percent
        #             p.*
        #             , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
        #             -- , owner_address, owner_unit, owner_city, owner_state, owner_zip
        #             , owner_photo_url
        #             , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date, contract_end_notice_period, contract_m2m
        #             , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_services_fees
        #             -- , business_address, business_unit, business_city, business_state, business_zip, business_photo_url
        #         FROM space.o_details o
        #         LEFT JOIN space.properties p ON o.property_id =p.property_uid 
        #         LEFT JOIN space.b_details b ON o.property_id = b.contract_property_id
        #         -- WHERE b.business_uid = '600-000011'
        #         -- WHERE o.owner_uid = '110-000003'
        #         WHERE o.owner_uid = \'""" + user_id + """\';
        #         -- WHERE b.business_uid = \'""" + user_id + """\';
        #         """)
        #         return response
        # else:
        #     return "No records for this Uid"

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
                , "contract_end_notice_period"
                , "contract_m2m"
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
                                AND contract_status in ('NEW','SENT');
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
        if payload.get('contract_uid') in {None, '', 'null'}:
            print("No contract_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        # contract_uid = payload.get('contract_uid')
        key = {'contract_uid': payload.pop('contract_uid')}
        print("Contract Key: ", key)


        # --------------- PROCESS DOCUMENTS ------------------
        
        processDocument(key, payload)
        print("Payload after function: ", payload)
        
        # --------------- PROCESS DOCUMENTS ------------------
   

        # Write to Database
        with connect() as db:
            print("Checking Inputs: ", key, payload)
            response['contract_info'] = db.update('contracts', key, payload)
            # print("Response:" , response)

        # Get all contracts between Owner - PM
        # If no other contracts then end
        # If new contract has a start date <= today then make it Active
        # If new contract becomes active, make other contracts Inactive
        
        return response



    