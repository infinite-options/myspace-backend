
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

import boto3
from data_pm import connect, uploadImage, deleteImage, s3, processImage
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
                    FROM space.appliances 
                    LEFT JOIN space.lists ON appliance_type = list_uid
                    -- WHERE appliance_property_id = '200-000001'
                    WHERE appliance_property_id= \'""" + uid + """\'
                    """)
                print(response)
            elif uid[:3] == "ALL":
                response = db.execute("""
                    SELECT * FROM space.appliances
                    """)
                print(response)

        return response
    
    
    def post(self):
        print("In Appliances POST ")
        response = {}
        newAppliance = {}
        payload = request.form.to_dict()
        print("Appliance Add Payload: ", payload)


        if payload.get('appliance_uid'):
            print("appliance_uid found.  Please call PUT endpoint")
            raise BadRequest("Request failed, UID found in payload.")
        
        with connect() as db:
            newApplianceUID = db.call('new_appliance_uid')['result'][0]['new_id']
            key = {'appliance_uid': newApplianceUID}
            print("Appliance Key: ", key)
           
           # --------------- PROCESS IMAGES ------------------

            processImage(key, payload)
            print("Payload after function: ", payload)
            
            # --------------- PROCESS IMAGES ------------------


            # Add Property Info
            print("Add Appliance Payload: ", payload)  


            payload["appliance_uid"] = newApplianceUID  
            response['Add Appliance'] = db.insert('appliances', payload)
            response['appliance_UID'] = newApplianceUID 
            response['Appliance Images Added'] = payload.get('appliance_images', "None")
            print("\nNew Appliance Added")

        return response


    def put(self):
        print("\nIn Appliance PUT")
        response = {}

        payload = request.form.to_dict()
        print("Appliance Update Payload: ", payload)
        
         # Verify uid has been included in the data
        if payload.get('appliance_uid') is None:
            print("No appliance_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        # appliance_uid = payload.get('appliance_uid')
        key = {'appliance_uid': payload.pop('appliance_uid')}
        print("Appliance Key: ", key) 


        # --------------- PROCESS IMAGES ------------------

        processImage(key, payload)
        print("Payload after function: ", payload)
        
        # --------------- PROCESS IMAGES ------------------



        # Write to Database
        with connect() as db:
            print("Checking Inputs: ", key, payload)
            response['appliance_info'] = db.update('appliances', key, payload)
            print("Response:" , response)
            
        return response

 
    def delete(self, uid):
        print("In DELETE Appliances", uid)
        response = {}
        with connect() as db:
            applianceQuery = ("""
                DELETE 
                FROM space.appliances
                -- WHERE appliance_uid = '060-000005'
                WHERE appliance_uid = \'""" + uid + """\';
                """)
            response = db.delete(applianceQuery)
            print(response)
        return response



class Appliances_SB(Resource):
    def put(self):
        print("\nIn Appliance PUT")
        response = {}
        payload = request.form.to_dict()
        print("Appliance Update Payload: ", payload)
        
         # Verify uid has been included in the data
        if payload.get('appliance_uid') is None:
            print("No appliance_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        appliance_uid = payload.get('appliance_uid')
        key = {'appliance_uid': payload.pop('appliance_uid')}
        print("Appliance Key: ", key) 


        # Check if images already exist
        # Put current db images into current images
        current_images = []
        if payload.get('appliance_images') is not None:
            current_images =ast.literal_eval(payload.get('appliance_images'))
            print("Current images: ", current_images, type(current_images))

        # Check if images are being added OR deleted
        images = []
        # i = -1
        i = 0
        imageFiles = {}
        favorite_image = payload.get("appliance_favorite_image")
        while True:
            filename = f'img_{i}'
            print("Put image file into Filename: ", filename) 
            # if i == -1:
            #     filename = 'img_cover'
            file = request.files.get(filename)
            print("File:" , file)            
            s3Link = payload.get(filename)
            print("S3Link: ", s3Link)
            if file:
                imageFiles[filename] = file
                unique_filename = filename + "_" + datetime.utcnow().strftime('%Y%m%d%H%M%SZ')
                image_key = f'appliances/{appliance_uid}/{unique_filename}'
                # This calls the uploadImage function that generates the S3 link
                image = uploadImage(file, image_key, '')
                images.append(image)

                if filename == favorite_image:
                    payload["appliance_favorite_image"] = image

            elif s3Link:
                imageFiles[filename] = s3Link
                images.append(s3Link)

                if filename == favorite_image:
                    payload["appliance_favorite_image"] = s3Link
            else:
                break
            i += 1
        
        print("Images after loop: ", images)
        if images != []:
            current_images.extend(images)
            payload['appliance_images'] = json.dumps(current_images) 

        # Delete Images
        if payload.get('delete_images'):
            delete_images = ast.literal_eval(payload.get('delete_images'))
            del payload['delete_images']
            print(delete_images, type(delete_images), len(delete_images))
            for image in delete_images:
                print("Image to Delete: ", image, type(image))
                # Delete from db list assuming it is in db list
                try:
                    current_images.remove(image)
                except:
                    print("Image not in lsit")

                #  Delete from S3 Bucket
                try:
                    delete_key = image.split('io-pm/', 1)[1]
                    print("Delete key", delete_key)
                    deleteImage(delete_key)
                except: 
                    print("could not delete from S3")
            
            print("Updated List of Images: ", current_images)


            print("Current Images: ", current_images)
            payload['appliance_images'] = json.dumps(current_images) 

        # Write to Database
        with connect() as db:
            print("Checking Inputs: ", key, payload)
            response['appliance'] = db.update('appliances', key, payload)
            # print("Response:" , response)
        return response


class RemoveAppliance(Resource):
    def put(self):
        response = {}
        with connect() as db:
            data = request.form
            # print(data)
            property_uid = data.get('property_uid')
            appliance = (data.get('appliance'))

            getAppliance = db.execute(
                """SELECT appliances FROM pm.properties WHERE property_uid= \'""" + property_uid + """\'""")
            getappLen = len(json.loads(
                getAppliance['result'][0]['appliances']).keys())
            existingApp = json.loads(
                getAppliance['result'][0]['appliances'])
            # print(existingApp)
            if appliance in existingApp:

                del (existingApp[appliance])
                # print(existingApp)
                primaryKey = {
                    'property_uid': property_uid
                }
                updatedProperty = {
                    'appliances': json.dumps(existingApp)
                }

                response = db.update('properties', primaryKey, updatedProperty)
            else:
                response['message'] = 'No appliance'
                response['code'] = 200

        return response
