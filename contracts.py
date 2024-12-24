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

    
    def post(self):
        print("In Contracts POST ")
        with connect() as db:
            response = {}

            payload = request.form.to_dict()
            print("Contract Add Payload: ", payload)

            # Verify uid has NOT been included in the data
            if payload.get('contract_uid'):
                print("contract_uid found.  Please call PUT endpoint")
                raise BadRequest("Request failed, UID found in payload.")
            
            if payload.get('contract_property_ids') in {None, '', 'null', '[]'}:
                print("No property_uid")
                raise BadRequest("Request failed, no UID in payload.")
            

            # properties_l is a list since you may create contracts for multiple properties at once
            # If contracts is called as part of AddProperty.jsx then it is a list of one property
            properties_l = payload.pop("contract_property_ids")
            # print("Properties affected: ", properties_l)
            properties = json.loads(properties_l)
            print("Properties affected: ", properties)


             # Check if the property already has any active renewals for the given business
            for property in properties:
                print("In loop processing: ", property)


                contract_uid = db.call('new_contract_uid')['result'][0]['new_id']
                key = {'contract_uid': contract_uid}
                response['contract_uid'] = contract_uid 
                print("Contract Key: ", key)
            
                
                # --------------- PROCESS DOCUMENTS ------------------

                processDocument(key, payload)
                print("Payload after function: ", payload)
                
                # --------------- PROCESS DOCUMENTS ------------------


                # Actual Insert Statement
                print("About to insert: ", payload)
                payload["contract_property_id"] = property
                response["contract"] = db.insert('space_prod.contracts', payload)
                print("Data inserted into space_prod.contracts", response)

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
   

        # Check if Ownership % is 100%
        # ie  80% Owner accepts ==> record acceptance but don't change status ==> Waiting for others  80% Accepted?


        # Write to Database
        with connect() as db:
            print("Checking Inputs: ", key, payload)
            response['contract_info'] = db.update('space_prod.contracts', key, payload)
            # print("Response:" , response)
        
        return response



    