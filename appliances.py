
from flask import request
from flask_restful import Resource

import boto3
from data_pm import connect, uploadImage, s3
from datetime import date, timedelta, datetime
from calendar import monthrange
import json
import ast
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest


def updateImagesAppliances(imageFiles, property_uid, appliance):
    content = []
    for filename in imageFiles:
        if type(imageFiles[filename]) == str:
            bucket = 'io-pm'
            key = imageFiles[filename].split('/io-pm/')[1]
            data = s3.get_object(
                Bucket=bucket,
                Key=key
            )
            imageFiles[filename] = data['Body']
            content.append(data['ContentType'])
        else:
            content.append('')

    s3Resource = boto3.resource('s3')
    bucket = s3Resource.Bucket('io-pm')
    # bucket.objects.filter(Prefix=f'appliances/{property_uid}/').delete()
    images = []
    for i in range(len(imageFiles.keys())):
        filename = f'img_{appliance}_{i-1}'
        if i == 0:
            filename = f'img_{appliance}_cover'
        key = f'appliances/{property_uid}/{filename}'
        image = uploadImage(
            imageFiles[filename], key, content[i])
        images.append(image)
    return images


def updateDocumentsAppliances(documents, property_uid, appliance):
    content = []
    for i, doc in enumerate(documents):
        # print('i, doc', i, doc)
        if 'link' in doc:
            print('in if link in doc')
            bucket = 'io-pm'
            key = doc['link'].split('/io-pm/')[1]
            data = s3.get_object(
                Bucket=bucket,
                Key=key
            )
            doc['file'] = data['Body']
            content.append(data['ContentType'])
        else:
            content.append('')

    s3Resource = boto3.resource('s3')
    bucket = s3Resource.Bucket('io-pm')
    # bucket.objects.filter(Prefix=f'appliances/{property_uid}/').delete()
    docs = []
    for i, doc in enumerate(documents):

        filename = f'doc_{appliance}_{i}'
        key = f'appliances/{property_uid}/{filename}'
        # print(type(doc['file']))
        link = uploadImage(doc['file'], key, content[i])
        # print('link', link)
        doc['link'] = link
        del doc['file']
        docs.append(doc)
    return docs


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
    
    # def get(self):
        print("In GET Appliances")
        response = {}
        with connect() as db:
            filters = ['property_uid']
            print("Input Filters: ", filters)
            where = {}
            for filter in filters:
                filterValue = request.args.get(filter)
                if filterValue is not None:
                    where[filter] = filterValue
            print("Applied Filters: ", where['property_uid'])
            response = db.execute("""
                SELECT *
                FROM space.appliances
                -- WHERE appliance_property_id = '200-000001'
                WHERE appliance_property_id = \'""" + where['property_uid'] + """\'
                """)
            print(response)

        return response
    

    def post(self):
        print("In POST Appliances")
        response = {}

        with connect() as db:
            data = request.form
            print("Received Data: ", data)
            fields = [
                'appliance_property_id',
                'appliance_type',
                'appliance_desc',
                'appliance_url',
                'appliance_images',
                'appliance_available',
                'appliance_installed',
                'appliance_model_num',
                'appliance_purchased',
                'appliance_serial_num',
                'appliance_manufacturer',
                'appliance_warranty_info',
                'appliance_warranty_till',
                'appliance_purchased_from',
                'appliance_purchase_order'
                    ]

            newAppliance = {}

            # print("Property ID: ", data.get("appliance_property_id"))
            # print("Appliance Type: ", request.form.get('appliance_type'))

            for field in fields:
                # print("Field: ", field)
                # print("Form Data: ", data.get(field))
                newAppliance[field] = data.get(field)
                # print("New Appliance Field: ", newAppliance[field])
            # print("Current newAppliance", newAppliance, type(newAppliance))


            # GET NEW UID
            print("Get New Appliance UID")
            newRequestID = db.call('new_appliance_uid')['result'][0]['new_id']
            newAppliance['appliance_uid'] = newRequestID
            print(newAppliance['appliance_uid'])

            # Image Upload 
            print("\nIn images")
            images = []
            i = 0
            imageFiles = {}
            favorite_image = data.get("img_favorite")
            print("Favorite Image: ", favorite_image)
            while True:
                filename = f'img_{i}'
                print("Filename: ", filename)             
                file = request.files.get(filename)
                print("File:" , file)
                s3Link = data.get(filename)
                print("S3Link: ", s3Link)
                if file:
                    imageFiles[filename] = file
                    unique_filename = filename + "_" + datetime.utcnow().strftime('%Y%m%d%H%M%SZ')
                    print("unique_filename: ", unique_filename)
                    key = f'appliance/{newRequestID}/{unique_filename}'
                    image = uploadImage(file, key, '')
                    print("Image: ", image)
                    images.append(image)

                    if filename == favorite_image:
                        print("favorite image!")
                        newAppliance["appliance_images"] = image

                elif s3Link:
                    imageFiles[filename] = s3Link
                    images.append(s3Link)

                    if filename == favorite_image:
                        newAppliance["appliance_images"] = s3Link
                else:
                    break
                i += 1
            
            newAppliance["appliance_images"] = json.dumps(images)
            print("Appliance Images: ", newAppliance["appliance_images"])        


            print("New Appliance Object: ", newAppliance)
            response = db.insert('appliances', newAppliance)
            response['Appliance'] = "Added"
            response['appliance_uid'] = newRequestID
            response['images'] = newAppliance["appliance_images"]

        return response
    
    def put(self):
        print("In PUT Appliances")
        response = {}

        with connect() as db:
            data = request.form
            print("Received Data: ", data)
            fields = [
                'appliance_uid',
                'appliance_property_id',
                'appliance_type',
                'appliance_desc',
                'appliance_url',
                'appliance_images',
                'appliance_available',
                'appliance_installed',
                'appliance_model_num',
                'appliance_purchased',
                'appliance_serial_num',
                'appliance_manufacturer',
                'appliance_warranty_info',
                'appliance_warranty_till',
                'appliance_purchased_from',
                'appliance_purchase_order'
                    ]

            newAppliance = {}

            print("Property ID: ", data.get("appliance_property_id"))
            print("Appliance UID: ", data.get("appliance_uid"))
            print("Appliance Type: ", request.form.get('appliance_type'))


            # Check if appliance_uid is present and valid
            appliance_uid = data.get('appliance_uid')
    
            if appliance_uid is None or appliance_uid == 0 or appliance_uid == '':
                response = "appliance_uid is required and must be non-zero"
                return response

            for field in fields:
                print("Field: ", field)
                print("Form Data: ", data.get(field))
                newAppliance[field] = data.get(field)
                print("New Appliance Field: ", newAppliance[field])
            print("Current newAppliance", newAppliance, type(newAppliance))


            # Image Upload 
            images = []
            i = 0
            imageFiles = {}
            favorite_image = data.get("img_favorite")
            while True:
                filename = f'img_{i}'                
                file = request.files.get(filename)
                s3Link = data.get(filename)
                if file:
                    imageFiles[filename] = file
                    unique_filename = filename + "_" + datetime.utcnow().strftime('%Y%m%d%H%M%SZ')
                    key = f'appliance/{appliance_uid}/{unique_filename}'
                    image = uploadImage(file, key, '')
                    images.append(image)

                    if filename == favorite_image:
                        newAppliance["appliance_images"] = image

                elif s3Link:
                    imageFiles[filename] = s3Link
                    images.append(s3Link)

                    if filename == favorite_image:
                        newAppliance["appliance_images"] = s3Link
                else:
                    break
                i += 1
            
            newAppliance["appliance_images"] = json.dumps(images)
            print("Appliance Images: ", newAppliance["appliance_images"])        


            print("New Appliance Object: ", newAppliance)
            key = {'appliance_uid': appliance_uid}
            response = db.update('appliances', key, newAppliance)
            response['Appliance'] = "Added"
            response['appliance_uid'] = appliance_uid
            response['images'] = newAppliance["appliance_images"]

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
        response = {}
        payload = request.form.to_dict()
        print("Appliance Payload: ", payload)
        
        
        # Profile Picture is Unique to Profile 
        if payload.get('appliance_uid'):
            appliance_uid = payload.get('appliance_uid')
            print("In Appliances")
            key = {'appliance_uid': payload.pop('appliance_uid')}
            print("Appliance Key: ", key)                   

            # Update File List in Database        
            print("Appliance")
            print("key: ", key )
            print("payload: ", payload)

            with connect() as db:
                response['appliance'] = db.update('appliances', key, payload)
            print("Response:" , response)

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
