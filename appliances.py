
from flask import request
from flask_restful import Resource

import boto3
from data_pm import connect, uploadImage, s3
from datetime import date, timedelta, datetime
from calendar import monthrange
import json
import ast
from dateutil.relativedelta import relativedelta


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
    def get(self):
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

    def put(self):
        response = {}
        with connect() as db:
            data = request.form
            # print('data', data)
            property_uid = data.get('property_uid')
            # print('property_uid', property_uid)
            appliances = eval(data.get('appliances'))
            # print('appliances', appliances)
            documents = json.loads(data.get('documents'))
            images = []
            i = -1
            imageFiles = {}
            getAppliance = db.execute(
                """SELECT appliances FROM space.appliances WHERE property_uid= \'""" + property_uid + """\'""")
            getappLen = len(json.loads(
                getAppliance['result'][0]['appliances']).keys())
            existingApp = json.loads(
                getAppliance['result'][0]['appliances'])
            # print(getappLen)
            if getappLen == 0:
                # print('in if len 0')
                for key in appliances:
                    appliances[key]['images'] = []
                    # print(key, '->', (appliances[key]['available']))
                    if 'available' in appliances[key].keys():
                        # print('here')
                        appliances[key]['available'] = appliances[key]['available'] == 'True'
                        # print(appliances[key]['available'])
                    # print('images')
                    for i, doc in enumerate(documents):
                        filename = f'doc_{key}_{i}'
                        file = request.files.get(filename)
                        s3Link = doc.get('link')
                        if file:
                            doc['file'] = file
                        elif s3Link:
                            doc['link'] = s3Link
                        else:
                            break
                    documents = updateDocumentsAppliances(
                        documents, property_uid, key)
                    appliances[key]['documents'] = list(documents)
                    i = -1
                    while True:
                        filename = f'img_{key}_{i}'
                        if i == -1:
                            filename = f'img_{key}_cover'

                        file = request.files.get(filename)
                        s3Link = data.get(filename)
                        if file:
                            imageFiles[filename] = file
                        elif s3Link:
                            imageFiles[filename] = s3Link
                        else:
                            break
                        i += 1
                    images = updateImagesAppliances(
                        imageFiles, property_uid, key)
                    appliances[key]['images'] = list((images))
                    ## Image uploading stuff ends here###

                    primaryKey = {
                        'property_uid': property_uid
                    }
                    updatedProperty = {
                        'appliances': json.dumps(appliances)
                    }
                    print(appliances)
                    response = db.update(
                        'properties', primaryKey, updatedProperty)
            else:
                print('in else not 0')
                for key in appliances:
                    if key in existingApp.keys():
                        print('in if key in exisiitngapp')
                        print(key, '->', existingApp.keys())
                        # print('images')
                        if 'available' in appliances[key].keys():
                            print('if available')
                            print('available', '->', appliances[key].keys())
                            appliances[key]['available'] = appliances[key]['available'] == 'True'
                            appliances[key]['available']
                        for i, doc in enumerate(documents):
                            print('in add docs')
                            filename = f'doc_{key}_{i}'
                            file = request.files.get(filename)
                            s3Link = doc.get('link')
                            if file:
                                doc['file'] = file
                            elif s3Link:
                                doc['link'] = s3Link
                            else:
                                break
                        documents = updateDocumentsAppliances(
                            documents, property_uid, key)
                        appliances[key]['documents'] = list(documents)
                        i = -1
                        while True:
                            # print('in add images')
                            filename = f'img_{key}_{i}'
                            # print('filename', filename)
                            if i == -1:
                                filename = f'img_{key}_cover'
                            # print('filename after if', filename)
                            file = request.files.get(filename)
                            # print('file', file)
                            s3Link = data.get(filename)
                            # print('s3Link', s3Link)
                            if file:
                                # print('in if file')
                                imageFiles[filename] = file
                                # print(appliances[key])
                            elif s3Link:
                                # print('in if s3link')
                                imageFiles[filename] = s3Link
                            else:
                                break
                            i += 1
                        images = updateImagesAppliances(
                            imageFiles, property_uid, key)
                        appliances[key]['images'] = list((images))

                        existingApp[key] = appliances[key]
                    else:
                        # print('in else not in existingapp')
                        # print('images')
                        if 'available' in appliances[key].keys():
                            # print('here')
                            appliances[key]['available'] = appliances[key]['available'] == 'True'
                            appliances[key]['available']
                        for i, doc in enumerate(documents):
                            filename = f'doc_{key}_{i}'
                            file = request.files.get(filename)
                            s3Link = doc.get('link')
                            if file:
                                doc['file'] = file
                            elif s3Link:
                                doc['link'] = s3Link
                            else:
                                break
                        documents = updateDocumentsAppliances(
                            documents, property_uid, key)
                        appliances[key]['documents'] = list(documents)
                        i = -1
                        while True:
                            # print('in add images')
                            filename = f'img_{key}_{i}'
                            # print('filename', filename)
                            if i == -1:
                                filename = f'img_{key}_cover'
                            # print('filename after if', filename)
                            file = request.files.get(filename)
                            # print('file', file)
                            s3Link = data.get(filename)
                            # print('s3Link', s3Link)
                            if file:
                                # print('in if file')
                                imageFiles[filename] = file
                                # print(appliances[key])
                            elif s3Link:
                                # print('in if s3link')
                                imageFiles[filename] = s3Link
                            else:
                                break
                            i += 1
                        images = updateImagesAppliances(
                            imageFiles, property_uid, key)
                        appliances[key]['images'] = list((images))
                        # print(appliances[key])
                        existingApp[key] = appliances[key]
                        # print(existingApp[key])
                        ## Image uploading stuff ends here###

                primaryKey = {
                    'property_uid': property_uid
                }
                updatedProperty = {
                    'appliances': json.dumps(existingApp)
                }
                # print(existingApp)
                response = db.update('properties', primaryKey, updatedProperty)
        return response

    # def post(self):
    #     print("In Maintenace Requests")
    #     response = {}
    #     with connect() as db:
    #         data = request.form
    #         fields = [
    #             'maintenance_property_id'
    #             , 'maintenance_title'
    #             , 'maintenance_desc'
    #             , 'maintenance_request_type'
    #             , 'maintenance_request_created_by'
    #             , 'maintenance_priority'
    #             , 'maintenance_can_reschedule'
    #             , 'maintenance_assigned_business'
    #             , 'maintenance_assigned_worker'
    #             , 'maintenance_scheduled_date'
    #             , 'maintenance_scheduled_time'
    #             , 'maintenance_frequency'
    #             , 'maintenance_notes'
    #             , 'maintenance_request_created_date'
    #             , 'maintenance_request_closed_date'
    #             , 'maintenance_request_adjustment_date'
    #             , 'maintenance_callback_number'
    #             , 'maintenance_estimated_cost'
    #         ]

    #         newRequest = {}
    #         for field in fields:
    #             newRequest[field] = data.get(field, None)
    #             # print(field, " = ", newRequest[field])


    #         # # GET NEW UID
    #         print("Get New Request UID")
    #         newRequestID = db.call('new_request_uid')['result'][0]['new_id']
    #         newRequest['maintenance_request_uid'] = newRequestID
    #         print(newRequestID)

    #         images = []
    #         i = -1
    #         # WHILE WHAT IS TRUE?
    #         while True:
    #             print("In while loop")
    #             filename = f'img_{i}'
    #             # print("Filename: ", filename)
    #             if i == -1:
    #                 filename = 'img_cover'
    #             file = request.files.get(filename)
    #             # print("File: ", file)
    #             if file:
    #                 key = f'maintenanceRequests/{newRequestID}/{filename}'
    #                 image = uploadImage(file, key, '')
    #                 images.append(image)
    #             else:
    #                 break
    #             i += 1
    #         newRequest['maintenance_images'] = json.dumps(images)
    #         # print("Images to be uploaded: ", newRequest['maintenance_images'])

    #         newRequest['maintenance_request_status'] = 'NEW'
    #         # newRequest['frequency'] = 'One time'
    #         # newRequest['can_reschedule'] = False

    #         # print(newRequest)

    #         response = db.insert('maintenanceRequests', newRequest)
    #         response['Maintenance_UID'] = newRequestID
    #         response['images'] = newRequest['maintenance_images']

    #     return response


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
