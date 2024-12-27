
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

import boto3
from data_pm import connect, uploadImage, deleteImage, deleteFolder, s3, processImage, processDocument
from datetime import date, timedelta, datetime
from calendar import monthrange
import json
import ast
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest


class Appliances(Resource):
    def get(self, uid):
        print("In GET Appliances", uid)
        response = {}

        with connect() as db:
            if uid[:3] == "200":
                response = db.execute("""
                    SELECT *
                    FROM space_dev.appliances 
                    LEFT JOIN space_dev.lists ON appliance_type = list_uid
                    -- WHERE appliance_property_id = '200-000001'
                    WHERE appliance_property_id= \'""" + uid + """\'
                    """)
                print(response)
            elif uid[:3] == "ALL":
                response = db.execute("""
                    SELECT * FROM space_dev.appliances
                    """)
                print(response)

        return response
    
    
    def post(self):
        print("In Appliances POST ")
        response = {}
        newAppliance = {}
        payload = request.form.to_dict()
        print("Appliance Add Payload: ", payload)

        # Verify uid has NOT been included in the data
        if payload.get('appliance_uid'):
            print("appliance_uid found.  Please call PUT endpoint")
            raise BadRequest("Request failed, UID found in payload.")
        
        with connect() as db:
            newApplianceUID = db.call('space_dev.new_appliance_uid')['result'][0]['new_id']
            key = {'appliance_uid': newApplianceUID}
            print("Appliance Key: ", key)
           
           # --------------- PROCESS IMAGES ------------------

            processImage(key, payload)
            print("Payload after function: ", payload)
            
            # --------------- PROCESS IMAGES ------------------

            
            # --------------- PROCESS DOCUMENTS ------------------

            processDocument(key, payload)
            print("Payload after function: ", payload)
            
            # --------------- PROCESS DOCUMENTS ------------------


            # Add Appliance Info
            payload['appliance_images'] = '[]' if payload.get('appliance_images') in {None, '', 'null'} else payload.get('appliance_images', '[]')
            payload['appliance_documents'] = '[]' if payload.get('appliance_documents') in {None, '', 'null'} else payload.get('appliance_documents', '[]')
            print("Add Appliance Payload: ", payload)  

            payload["appliance_uid"] = newApplianceUID  
            response['Add Appliance'] = db.insert('space_dev.appliances', payload)
            response['appliance_UID'] = newApplianceUID 
            response['Appliance Images Added'] = payload.get('appliance_images', "None")
            response['Appliance Documents Added'] = payload.get('appliance_documents', "None")
            print("\nNew Appliance Added")

        return response


    def put(self):
        print("\nIn Appliance PUT")
        response = {}

        payload = request.form.to_dict()
        print("Appliance Update Payload: ", payload)
        
         # Verify uid has been included in the data
        if payload.get('appliance_uid') in {None, '', 'null'}:
            print("No appliance_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        # appliance_uid = payload.get('appliance_uid')
        key = {'appliance_uid': payload.pop('appliance_uid')}
        print("Appliance Key: ", key) 


        # --------------- PROCESS IMAGES ------------------

        processImage(key, payload)
        print("Payload after function: ", payload)
        
        # --------------- PROCESS IMAGES ------------------
        

        # --------------- PROCESS DOCUMENTS ------------------

        processDocument(key, payload)
        print("Payload after function: ", payload)
        
        # --------------- PROCESS DOCUMENTS ------------------



        # Write to Database
        with connect() as db:
            print("Checking Inputs: ", key, payload)
            response['appliance_info'] = db.update('space_dev.appliances', key, payload)
            print("Response:" , response)
            
        return response

 
    def delete(self, uid):
        print("In DELETE Appliances", uid)
        response = {}
        with connect() as db:
            applianceQuery = ("""
                DELETE 
                FROM space_dev.appliances
                -- WHERE appliance_uid = '060-000005'
                WHERE appliance_uid = \'""" + uid + """\';
                """)
            response["delete"] = db.delete(applianceQuery)
            print(response)
        
        #  Delete from S3 Bucket
        try:
            folder = 'appliances'
            deleteFolder(folder, uid)
            response["S3"] = "Folder deleted successfully"

        except:
            response["S3"] = "Folder delete FAILED"

        return response



# class RemoveAppliance(Resource):
#     def put(self):
#         response = {}
#         with connect() as db:
#             data = request.form
#             # print(data)
#             property_uid = data.get('property_uid')
#             appliance = (data.get('appliance'))

#             getAppliance = db.execute(
#                 """SELECT appliances FROM pm.properties WHERE property_uid= \'""" + property_uid + """\'""")
#             getappLen = len(json.loads(
#                 getAppliance['result'][0]['appliances']).keys())
#             existingApp = json.loads(
#                 getAppliance['result'][0]['appliances'])
#             # print(existingApp)
#             if appliance in existingApp:

#                 del (existingApp[appliance])
#                 # print(existingApp)
#                 primaryKey = {
#                     'property_uid': property_uid
#                 }
#                 updatedProperty = {
#                     'appliances': json.dumps(existingApp)
#                 }

#                 response = db.update('space_dev.properties', primaryKey, updatedProperty)
#             else:
#                 response['message'] = 'No appliance'
#                 response['code'] = 200

#         return response
