from flask import request

import os
import pymysql
import datetime
import json
import boto3
from botocore.response import StreamingBody
import calendar
from decimal import Decimal
# from datetime import date, datetime, timedelta
from werkzeug.datastructures import FileStorage
import mimetypes
import ast

# s3 = boto3.client('s3')
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('S3_KEY'),
    aws_secret_access_key=os.getenv('S3_SECRET'),
    region_name=os.getenv('S3_REGION')
)



def pmDueDate(due_date):
    print("\nIn pmDueDate: ", due_date, type(due_date))
    # Calculate PM Due Date
    try:
        # # Parse the date and time, handle cases where time may not be included
        # if ' ' in due_date:
        #     dd = datetime.strptime(due_date, '%m-%d-%Y %H:%M')
        # else:
        #     dd = datetime.strptime(due_date, '%m-%d-%Y')  # No time included, defaults to 00:00
        # print("Due Date: ", dd, type(dd))
        dd = due_date
    
        # Find the Last Day of the Due Date Month
        last_day_of_month = calendar.monthrange(dd.year, dd.month)[1]
        print("last_day_of_month: ", last_day_of_month, type(last_day_of_month))
        # Calcuate the last day of the Due Date Month
        pm_due_date = dd.replace(day=last_day_of_month)
        print("PM Due Date: ", pm_due_date, type(pm_due_date))
        return pm_due_date
    except ValueError as e:
        print("Error:", e)


def deleteFolder(folder, uid):
    #  Delete from S3 Bucket
    print("In Delete S3 Folder")

    # bucket_name = 'io-pm'
    bucket_name = os.getenv('BUCKET_NAME')
    folder_prefix = f'{folder}/{uid}/'

    # List all objects with the given prefix
    s3Objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
    print(s3Objects)

    if 'Contents' in s3Objects:
        print("In Contents")
        for obj in s3Objects['Contents']:
            print(f"Deleting {obj['Key']}")
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
        print(f"Folder '{folder_prefix}' deleted successfully")
    else:
        print(f"No files found to delete in '{folder_prefix}'")


def deleteImage(key):
    # bucket = 'io-pm'
    bucket = os.getenv('BUCKET_NAME')
    try:
        print("Before Delete: ", bucket, key)
        delete_file = s3.delete_object(
            Bucket=bucket,
            Key=key
        )
        print("After Delete: ", delete_file)
        print("After Delete Status Code: ", delete_file['ResponseMetadata']['HTTPStatusCode'])
        print(f"Deleted existing file {key} from bucket {bucket}")
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"File {key} does not exist in bucket {bucket}")
        else:
            print(f"Error deleting file {key} from bucket {bucket}: {e}")
        return False


def upload_multipart(file_content, bucket, key, content_type):
    print("\nIn Upload Multipart:")
    try:
        # Step 1: Initiate the multipart upload
        multipart_upload = s3.create_multipart_upload(
            Bucket=bucket,
            Key=key,
            ACL='public-read',
            ContentType=content_type
        )
        upload_id = multipart_upload['UploadId']
        print("Multipart upload initiated with UploadId:", upload_id)
        # Step 2: Upload parts
        parts = []
        part_number = 1
        chunk_size = 5 * 1024 * 1024  # Set chunk size (5 MB)
        # Split file content into chunks
        for i in range(0, len(file_content), chunk_size):
            chunk = file_content[i:i + chunk_size]
            response = s3.upload_part(
                Bucket=bucket,
                Key=key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=chunk
            )
            parts.append({'PartNumber': part_number, 'ETag': response['ETag']})
            print(f"Uploaded part {part_number}")
            part_number += 1
        # Step 3: Complete the multipart upload
        response = s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        print("Multipart upload completed successfully:", response)
        # Return the S3 file URL
        filename = f'https://{bucket}.s3.amazonaws.com/{key}'
        print("Derived Filename:", filename)
        return filename
    except Exception as e:
        # Abort the multipart upload in case of an error
        if 'upload_id' in locals():
            s3.abort_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id
            )
            print("Multipart upload aborted.")
        print("Error during multipart upload:", str(e))
        return None


def uploadImage(file, key, content):
    print("\nIn Upload Function: ")
    # print("File: ", file)
    # print("Key: ", key)
    # print("Content: ", content)
    # bucket = 'io-pm'
    bucket = os.getenv('BUCKET_NAME')

    if isinstance(file, FileStorage): 
        print("In Upload Function isInstance File Storage: ", FileStorage)
        file.stream.seek(0)
        file_content = file.stream.read()
        content_type, _ = mimetypes.guess_type(file.filename)
        contentType = content_type if content_type else 'application/octet-stream'  # Fallback if MIME type is not detected
        print("In Upload Function contentType: ", contentType) # This returns jpeg, png, ect

    elif isinstance(file, StreamingBody):
        print("In Upload Function isInstance Streaming Body")
        file_content = file.read()
        contentType = content
        print("In Upload Function contentType: ", contentType)
        # Set content type based on your logic or metadata
        # Example: contentType = 'image/jpeg' or other appropriate content type


    if file_content:
        # print("file_content: ", file_content )   # Unnecessary print statement.  Return hexedemical file info
        filename = f'https://s3-us-west-1.amazonaws.com/{bucket}/{key}'
        # print("Before Upload: ", bucket, key, filename, contentType)

        # This Statement Actually uploads the file into S3
        # upload_file = s3.put_object(
        #     Bucket=bucket,
        #     Body=file_content,
        #     Key=key,
        #     ACL='public-read',
        #     ContentType=contentType
        # )

        # Call the multipart upload function
        upload_file = upload_multipart(file_content, bucket, key, content_type)

        print("After Upload: ", upload_file)
        # print("After Upload Status Code: ", upload_file['ResponseMetadata']['HTTPStatusCode'])
        print("Derived Filename: ", filename)
        return filename
    
    return None

# --------------- PROCESS IMAGES ------------------

def processImage(key, payload):
    # print("\nIn Process Image: ", payload)
    # print("Key Passed into processImage: ", key)
    response = {}
    bucket = os.getenv('BUCKET_NAME')
    with connect() as db:

        if 'appliance_uid' in key:
            print("Appliance Key passed")
            key_type = 'appliances'
            key_uid = key['appliance_uid']
            payload_delete_images = payload.pop('delete_images', None)      # Images to Delete
            if 'img_0' in request.files or payload_delete_images != None:   #  New appliance images are passed in as img_0, img_1.  No Image attributes are passed in
                payload_query = db.execute(""" SELECT appliance_images FROM space_dev.appliances WHERE appliance_uid = \'""" + key_uid + """\'; """)     # Current Images
                print("1: ", payload_query)
                print("2: ", payload_query['result'], type(payload_query['result']))
                if len(payload_query['result']) > 0:
                    print("4: ", payload_query.get('result', [{}])[0].get('appliance_images', None))
                payload_images = payload_query['result'][0]['appliance_images'] if payload_query['result'] else None  # Current Images from database
                payload_fav_images = payload.get("appliance_favorite_image") or payload.pop("img_favorite", None)   # (PUT & POST)
                print("5: ", payload_fav_images)
            else:
                return payload
            
        elif 'bill_uid' in key:
            print("Bill Key passed")
            key_type = 'bills'
            key_uid = key['bill_uid']
            payload_delete_images = payload.pop('delete_images', None)      # Images to Delete
            if 'img_0' in request.files or payload_delete_images != None:   #  New bill images are passed in as img_0, img_1.  No Image attributes are passed in
                payload_query = db.execute(""" SELECT bill_images FROM space_dev.bills WHERE bill_uid = \'""" + key_uid + """\'; """)     # Current Images
                print("1: ", payload_query)
                print("2: ", payload_query['result'], type(payload_query['result']))
                if len(payload_query['result']) > 0:
                    print("4: ", payload_query.get('result', [{}])[0].get('bill_images', None))
                payload_images = payload_query['result'][0]['bill_images'] if payload_query['result'] else None  # Current Images
                # payload_fav_images = payload.pop("img_favorite") if payload.get("img_favorite") else None  # (PUT & POST)
                payload_fav_images = payload.get("bill_favorite_image") or payload.pop("img_favorite", None)   # (PUT & POST)
            else:
                return payload

        elif 'maintenance_request_uid' in key:
            print("Maintenance Request Key passed")
            key_type = 'maintenance request'
            key_uid = key['maintenance_request_uid']
            payload_delete_images = payload.pop('delete_images', None)      # Images to Delete
            if 'img_0' in request.files or payload_delete_images != None:   #  New maintenance request images are passed in as img_0, img_1.  No Image attributes are passed in
                payload_query = db.execute(""" SELECT maintenance_images FROM space_dev.maintenanceRequests WHERE maintenance_request_uid = \'""" + key_uid + """\'; """)     # Current Images
                print("1: ", payload_query)
                print("2: ", payload_query['result'], type(payload_query['result']))
                if len(payload_query['result']) > 0:
                    print("4: ", payload_query.get('result', [{}])[0].get('maintenance_images', None))
                payload_images = payload_query['result'][0]['maintenance_images'] if payload_query['result'] else None  # Current Images
                payload_fav_images = payload.get("maintenance_favorite_image") or payload.pop("img_favorite", None)   # (PUT & POST)
            else:
                return payload

        elif 'maintenance_quote_uid' in key:
            print("Maintenance Quote Key passed")
            key_type = 'maintenance quote'
            key_uid = key['maintenance_quote_uid']
            payload_delete_images = payload.pop('delete_images', None)      # Images to Delete
            if 'img_0' in request.files or payload_delete_images != None:   #  New maintenance quote images are passed in as img_0, img_1.  No Image attributes are passed in
                payload_query = db.execute(""" SELECT quote_maintenance_images FROM space_dev.maintenanceQuotes WHERE maintenance_quote_uid = \'""" + key_uid + """\'; """)     # Current Images
                print("1: ", payload_query)
                print("2: ", payload_query['result'], type(payload_query['result']))
                if len(payload_query['result']) > 0:
                    print("4: ", payload_query.get('result', [{}])[0].get('quote_maintenance_images', None))
                payload_images = payload_query['result'][0]['quote_maintenance_images'] if payload_query['result'] else None  # Current Images
                payload_fav_images = payload.get("quote_favorite_image") or payload.pop("img_favorite", None)   # (PUT & POST)
            else:
                return payload            
        
        elif 'property_uid' in key:
            print("Property Key passed")
            key_type = 'properties'
            key_uid = key['property_uid']
            payload_delete_images = payload.pop('delete_images', None)      # Images to Delete
            if 'img_0' in request.files or payload_delete_images != None:   #  New property images are passed in as img_0, img_1.  No Image attributes are passed in
                payload_query = db.execute(""" SELECT property_images FROM space_dev.properties WHERE property_uid = \'""" + key_uid + """\'; """)     # Current Images
                print("1: ", payload_query)
                print("2: ", payload_query['result'], type(payload_query['result']))
                if len(payload_query['result']) > 0:
                    print("4: ", payload_query.get('result', [{}])[0].get('property_images', None))
                payload_images = payload_query['result'][0]['property_images'] if payload_query['result'] else None  # Current Images
                payload_fav_images = payload.get("property_favorite_image") or payload.pop("img_favorite", None)   # (PUT & POST)
            else:
                return payload


        elif 'tenant_uid' in key:
            print("Tenant Profile Key passed")
            key_type = 'tenantProfile'
            key_uid = key['tenant_uid']
            # payload_delete_images = payload.pop('delete_images', None)      # Images to Delete
            if 'tenant_photo_url' in request.files: #  New images are passed in as photo_url
                payload_query = db.execute(""" SELECT tenant_photo_url FROM space_dev.tenantProfileInfo WHERE tenant_uid = \'""" + key_uid + """\'; """)     # Current Images
                print("1: ", payload_query)
                print("2: ", payload_query['result'], type(payload_query['result']))
                if len(payload_query['result']) > 0:
                    print("4: ", payload_query.get('result', [{}])[0].get('tenant_photo_url', None))
                    payload_delete_images = payload_query['result'][0]['tenant_photo_url'] if payload_query['result'] else None
                    # payload_delete_images = payload_delete_images.replace("https://s3-us-west-1.amazonaws.com/io-pm", "")
                    if payload_delete_images is not None: payload_delete_images = '["'+payload_delete_images+'"]'
                payload_images =  None  # Current Images
            else:
                return payload
            
        elif 'owner_uid' in key:
            print("Owner Profile Key passed")
            key_type = 'ownerProfile'
            key_uid = key['owner_uid']
            # payload_delete_images = payload.pop('delete_images', None)      # Images to Delete
            if 'owner_photo_url' in request.files: #  New images are passed in as photo_url
                payload_query = db.execute(""" SELECT owner_photo_url FROM space_dev.ownerProfileInfo WHERE owner_uid = \'""" + key_uid + """\'; """)     # Current Images
                print("1: ", payload_query)
                print("2: ", payload_query['result'], type(payload_query['result']))
                if len(payload_query['result']) > 0:
                    print("4: ", payload_query.get('result', [{}])[0].get('owner_photo_url', None))
                    payload_delete_images = payload_query['result'][0]['owner_photo_url'] if payload_query['result'] else None
                    # payload_delete_images = payload_delete_images.replace("https://s3-us-west-1.amazonaws.com/io-pm", "")
                    if payload_delete_images is not None: payload_delete_images = '["'+payload_delete_images+'"]'
                payload_images =  None  # Current Images
            else:
                return payload
        
        elif 'business_uid' in key:
            print("Business Profile Key passed")
            key_type = 'businessProfile'
            key_uid = key['business_uid']
            # payload_delete_images = payload.pop('delete_images', None)      # Images to Delete
            # if 'business_photo' in request.files: #  New images are passed in as photo_url
            if 'business_photo_url' in request.files: #  New images are passed in as photo_url
                payload_query = db.execute(""" SELECT business_photo_url FROM space_dev.businessProfileInfo WHERE business_uid = \'""" + key_uid + """\'; """)     # Current Images
                print("1: ", payload_query)
                print("2: ", payload_query['result'], type(payload_query['result']))
                if len(payload_query['result']) > 0:
                    print("4: ", payload_query.get('result', [{}])[0].get('business_photo_url', None))
                    payload_delete_images = payload_query['result'][0]['business_photo_url'] if payload_query['result'] else None
                    # payload_delete_images = payload_delete_images.replace("https://s3-us-west-1.amazonaws.com/io-pm", "")
                    if payload_delete_images is not None: payload_delete_images = '["'+payload_delete_images+'"]'
                payload_images =  None  # Current Images
            else:
                return payload
            
        elif 'employee_uid' in key:
            print("Employee Profile Key passed")
            key_type = 'employeeProfile'
            key_uid = key['employee_uid']
            # payload_delete_images = payload.pop('delete_images', None)      # Images to Delete
            if 'employee_photo_url' in request.files: #  New images are passed in as photo_url
                payload_query = db.execute(""" SELECT employee_photo_url FROM space_dev.employees WHERE employee_uid = \'""" + key_uid + """\'; """)     # Current Images
                print("1: ", payload_query)
                print("2: ", payload_query['result'], type(payload_query['result']))
                if len(payload_query['result']) > 0:
                    print("4: ", payload_query.get('result', [{}])[0].get('employee_photo_url', None))
                    payload_delete_images = payload_query['result'][0]['employee_photo_url'] if payload_query['result'] else None
                    # payload_delete_images = payload_delete_images.replace("https://s3-us-west-1.amazonaws.com/io-pm", "")
                    if payload_delete_images is not None: payload_delete_images = '["'+payload_delete_images+'"]'
                payload_images =  None  # Current Images
            else:
                return payload

        else:
            print("No UID found in key")
            return
        print("Verified Add or Delete Images in Payload")


        print("\nkey_type: ", key_type, type(key_type))
        print("key_uid: ", key_uid, type(key_uid))
        print("payload_images: ", payload_images, type(payload_images))
        print("payload_images delete: ", payload_delete_images, type(payload_delete_images))       # Documents to Delete
        if key_type in ['appliances', 'bills', 'maintenance request', 'properties']: print("payload_fav_images: ", payload_fav_images, type(payload_fav_images))

        
        # Check if images already exist
        # Put current db images into current_images
        print("\nAbout to process CURRENT imagess in database")
        current_images = []
        if payload_images not in {None, '', 'null'}:
            # print("Current Database Images: ", payload_images)
            current_images =ast.literal_eval(payload_images)
            print("Current images     : ", current_images, type(current_images))
        print("processed current images ", current_images)

        # Check if images are being ADDED OR DELETED
        images = []
        i = 0
        imageFiles = {}

        # ADD Images
        print("\nAbout to process ADDED Images")

        if 'profile' in key_type.lower():  # Use lower() for case-insensitivity
            print("Key type contains 'Profile'. Performing action for profile.")
            filename = 'profile'
            if key_type == 'tenantProfile': file = request.files.get("tenant_photo_url")
            if key_type == 'ownerProfile': file = request.files.get("owner_photo_url")
            # if key_type == 'businessProfile': file = request.files.get("business_photo")
            if key_type == 'businessProfile': file = request.files.get("business_photo_url")
            if key_type == 'employeeProfile': file = request.files.get("employee_photo_url")
            # file = request.files.get("tenant_photo_url")
            print("After Profile get filename", filename, file)
            unique_filename = filename + "_" + datetime.datetime.utcnow().strftime('%Y%m%d%H%M%SZ')
            image_key = f'{key_type}/{key_uid}/{unique_filename}'
            profileImage = uploadImage(file, image_key, '')
            print("Image after upload: ", profileImage)
            images.append(profileImage)
            print("Image after upload: ", images)
        else:
            # Do something else if it does not contain 'profile'
            print("Key type does not contain 'Profile'. Performing alternative action.")

            while True:
                filename = f'img_{i}'
                print("\nPut image file into Filename: ", filename) 
                file = request.files.get(filename)
                print("File:" , file)            
                s3Link = payload.get(filename)
                print("S3Link: ", s3Link)

                if file:
                    print("In File if Statement")
                    imageFiles[filename] = file
                    unique_filename = filename + "_" + datetime.datetime.utcnow().strftime('%Y%m%d%H%M%SZ')
                    image_key = f'{key_type}/{key_uid}/{unique_filename}'
                    # This calls the uploadImage function that generates the S3 link
                    image = uploadImage(file, image_key, '')
                    print("Image after upload: ", image)
                    

                    images.append(image)

                    if filename == payload_fav_images:
                        if key_type == 'properties': payload["property_favorite_image"] = image
                        if key_type == 'appliances': payload["appliance_favorite_image"] = image
                        if key_type == 'maintenance request': payload["maintenance_favorite_image"] = image



                elif s3Link:
                    # payload.pop(f'img_{i}')
                    imageFiles[filename] = s3Link
                    images.append(s3Link)

                    if filename == payload_fav_images:
                        if key_type == 'properties': payload["property_favorite_image"] = s3Link
                        if key_type == 'appliances': payload["appliance_favorite_image"] = s3Link
                        if key_type == 'maintenance request': payload["maintenance_favorite_image"] = s3Link
                
                
                else:
                    # print("Processing Favorite Images if no files were added")
                    # if key_type == 'properties': payload["property_favorite_image"] = payload_fav_images
                    # if key_type == 'appliances': payload["appliance_favorite_image"] = payload_fav_images
                    # if key_type == 'maintenance request': payload["maintenance_favorite_image"] = payload_fav_images
                    break
                i += 1
            
            print("Images after loop: ", images)
            if images != []:
                current_images.extend(images)
            
            print("processed ADDED documents")


        # DELETE Images
        print("\nAbout to process DELETED images in database;")
        if payload_delete_images:
            print("In image delete: ", payload_delete_images, type( payload_delete_images))
            delete_images = ast.literal_eval(payload_delete_images)
            print("After ast: ", delete_images, type(delete_images), len(delete_images))
            for image in delete_images:
                # print("Image to Delete: ", image, type(image))
                # print("Payload Image:", current_images, type(current_images))
                # print("Current images before deletion:", [doc['link'] for doc in current_images])

                # Delete from db list assuming it is in db list
                try:
                    current_images.remove(image)   # Works becuase this is an Exact match
                except:
                    print("Image not in list")

                #  Delete from S3 Bucket
                try:
                    # delete_key = image.split('io-pm/', 1)[1]
                    # delete_key = image.split('space-prod/', 1)[1]
                    delete_key = image.split(f'{bucket}/', 1)[1]
                    # print("Delete key", delete_key)
                    deleteImage(delete_key)
                except: 
                    print("could not delete from S3")
        print("processed DELETED Images")
            
        print("\nCurrent Images in Function: ", current_images, type(current_images))
        # print("Key Type: ", key_type)
        
        if key_type == 'appliances': payload['appliance_images'] = json.dumps(current_images) 
        if key_type == 'bills': payload['bill_images'] = json.dumps(current_images) 
        if key_type == 'maintenance request': payload['maintenance_images'] = json.dumps(current_images) 
        if key_type == 'maintenance quote': payload['quote_maintenance_images'] = json.dumps(current_images) 
        if key_type == 'properties': payload['property_images'] = json.dumps(current_images) 
        if key_type == 'tenantProfile': payload['tenant_photo_url'] = profileImage
        if key_type == 'ownerProfile': payload['owner_photo_url'] = profileImage
        if key_type == 'businessProfile': payload['business_photo_url'] = profileImage
        if key_type == 'employeeProfile': payload['employee_photo_url'] = profileImage

        print("Payload before return: ", payload)
        return payload



# --------------- PROCESS DOCUMENTS ------------------

def processDocument(key, payload):
    print("\nIn Process Documents: ", payload)
    print("Key Passed into processDocuments: ", key)
    response = {}
    bucket = os.getenv('BUCKET_NAME')
    with connect() as db:

        if 'contract_uid' in key:
            print("Contract Key passed")
            key_type = 'contracts'
            key_uid = key['contract_uid']
            payload_changed_documents = payload.pop('contract_documents', None)             # Current Documents     (if there is a change in a current document)
            payload_document_details = payload.pop('contract_documents_details', None)      # New Documents         (if there are New documents being added)
            payload_delete_documents = payload.pop('delete_documents', None)                # Documents to Delete   (if documents are being deleted)
            if payload_changed_documents != None or payload_document_details != None or payload_delete_documents != None:
                payload_query = db.execute(""" SELECT contract_documents FROM space_dev.contracts WHERE contract_uid = \'""" + key_uid + """\'; """)     # Get Current Documents from db
                # print("1: ", payload_query)
                # print("2: ", payload_query['result'], type(payload_query['result']))
                # if payload_query['result']: print("3: ", payload_query['result'][0] ) 
                # if payload_query['result']: print("4: ", payload_query['result'][0]['contract_documents'], type(payload_query['result'][0]['contract_documents']))
                payload_documents = payload_query['result'][0]['contract_documents'] if payload_query['result'] else None
            else:
                return payload
            

        elif 'lease_uid' in key:
            print("Lease Key passed")
            key_type = 'leases'
            key_uid = key['lease_uid']
            payload_changed_documents = payload.pop('lease_documents', None)                # Current Documents     (if there is a change in a current document)
            payload_document_details = payload.pop('lease_documents_details', None)         # New Documents
            payload_delete_documents = payload.pop('delete_documents', None)                # Documents to Delete
            if payload_changed_documents != None or payload_document_details != None or payload_delete_documents != None:
                payload_query = db.execute(""" SELECT lease_documents FROM space_dev.leases WHERE lease_uid = \'""" + key_uid + """\'; """)     # Current Documents
                print("1: ", payload_query)
                print("2: ", payload_query['result'], type(payload_query['result']))
                if payload_query['result']: print("3: ", payload_query['result'][0] ) 
                if payload_query['result']: print("4: ", payload_query['result'][0]['lease_documents'], type(payload_query['result'][0]['lease_documents']))
                payload_documents = payload_query['result'][0]['lease_documents'] if payload_query['result'] else None
            else:
                return payload
            
        elif 'bill_uid' in key:
            print("Bill Key passed")
            key_type = 'bills'
            key_uid = key['bill_uid']
            payload_changed_documents = payload.pop('bill_documents', None)                # Current Documents     (if there is a change in a current document)
            payload_document_details = payload.pop('bill_documents_details', None)         # New Documents
            payload_delete_documents = payload.pop('delete_documents', None)                # Documents to Delete
            if payload_changed_documents != None or payload_document_details != None or payload_delete_documents != None:
                payload_query = db.execute(""" SELECT bill_documents FROM space_dev.bills WHERE bill_uid = \'""" + key_uid + """\'; """)     # Current Documents
                # print("1: ", payload_query)
                # print("2: ", payload_query['result'], type(payload_query['result']))
                # if payload_query['result']: print("3: ", payload_query['result'][0] ) 
                # if payload_query['result']: print("4: ", payload_query['result'][0]['bill_documents'], type(payload_query['result'][0]['bill_documents']))
                payload_documents = payload_query['result'][0]['bill_documents'] if payload_query['result'] else None
            else:
                return payload
            

        elif 'appliance_uid' in key:
            print("Appliance Key passed")
            key_type = 'appliances'
            key_uid = key['appliance_uid']
            payload_changed_documents = payload.pop('appliance_documents', None)            # Current Documents     (if there is a change in a current document)
            payload_document_details = payload.pop('appliance_documents_details', None)     # New Documents
            payload_delete_documents = payload.pop('delete_documents', None)                # Documents to Delete
            if payload_changed_documents != None or payload_document_details != None or payload_delete_documents != None:
                payload_query = db.execute(""" SELECT appliance_documents FROM space_dev.appliances WHERE appliance_uid = \'""" + key_uid + """\'; """)     # Current Documents
                # print("1: ", payload_query)
                # print("2: ", payload_query['result'], type(payload_query['result']))
                # if payload_query['result']: print("3: ", payload_query['result'][0] ) 
                # if payload_query['result']: print("4: ", payload_query['result'][0]['appliance_documents'], type(payload_query['result'][0]['appliance_documents']))
                payload_documents = payload_query['result'][0]['appliance_documents'] if payload_query['result'] else None
            else:
                return payload

        elif 'maintenance_quote_uid' in key:
            print("Quote Key passed")
            key_type = 'quotes'
            key_uid = key['maintenance_quote_uid']
            payload_changed_documents = payload.pop('quote_documents', None)                # Current Documents     (if there is a change in a current document)
            payload_document_details = payload.pop('quote_documents_details', None)         # New Documents
            payload_delete_documents = payload.pop('delete_documents', None)                # Documents to Delete
            if payload_changed_documents != None or payload_document_details != None or payload_delete_documents != None:
                payload_query =  db.execute(""" SELECT quote_documents FROM space_dev.maintenanceQuotes WHERE maintenance_quote_uid = \'""" + key_uid + """\'; """)                # Current Documents
                payload_documents = payload_query['result'][0]['quote_documents'] if payload_query['result'] else None
            else:
                return payload

        elif 'tenant_uid' in key:
            print("Tenant Key passed")
            key_type = 'tenants'
            key_uid = key['tenant_uid']
            payload_changed_documents = payload.pop('tenant_documents', None)                # Current Documents     (if there is a change in a current document)
            payload_document_details = payload.pop('tenant_documents_details', None)         # New Documents
            payload_delete_documents = payload.pop('delete_documents', None)                     # Documents to Delete
            if payload_changed_documents != None or payload_document_details != None or payload_delete_documents != None:
                payload_query = db.execute(""" SELECT tenant_documents FROM space_dev.tenantProfileInfo WHERE tenant_uid = \'""" + key_uid + """\'; """)                 # Current Documents
                payload_documents = payload_query['result'][0]['tenant_documents'] if payload_query['result'] else None
            else:
                return payload

        elif 'business_uid' in key:
            print("Business Key passed")
            key_type = 'business'
            key_uid = key['business_uid']
            payload_changed_documents = payload.pop('business_documents', None)                # Current Documents     (if there is a change in a current document)
            payload_document_details = payload.pop('business_documents_details', None)         # New Documents
            payload_delete_documents = payload.pop('delete_documents', None)                   # Documents to Delete
            if payload_changed_documents != None or payload_document_details != None or payload_delete_documents != None:
                payload_query = db.execute(""" SELECT business_documents FROM space_dev.businessProfileInfo WHERE business_uid = \'""" + key_uid + """\'; """)                # Current Documents
                payload_documents = payload_query['result'][0]['business_documents'] if payload_query['result'] else None                                          
            else:
                return payload
            
        elif 'employee_uid' in key:
            print("Employee Key passed")
            # No employee documents are passed
            return payload

        else:
            print("No UID found in key")
            return

        print("\nkey_type: ", key_type, type(key_type))
        print("key_uid: ", key_uid, type(key_uid))
        print("payload_documents: ", payload_documents, type(payload_documents))                            # Current Documents.  For POST requests there are no current documents
        print("payload_changed_documents: ", payload_changed_documents, type(payload_changed_documents))    # Documents being Changed
        print("payload_document_details: ", payload_document_details, type(payload_document_details))       # New Documents  
        print("payload_documents delete: ", payload_delete_documents, type(payload_delete_documents))       # Documents to Delete
        
        print("Verified Add or Delete Documents in Payload")


        # Put current db files into current_documents
        current_documents = []
        print("\nAbout to process CURRENT documents in database;")
        if payload_documents not in {None, '', 'null'}:
            print("Payload Documents: ", payload_documents)
            current_documents =ast.literal_eval(payload_documents)
            print("Current documents: ", current_documents, type(current_documents))
        print("processed current documents")


        # Replace current_document details with changed_document details
        print("\nAbout to process CHANGED documents in database;")
        # [{"link":"https://s3-us-west-1.amazonaws.com/io-pm/contracts/010-000001/file_0_20240827021719Z","type":"application/pdf","filename":"Sample Document 5.pdf"}]
        # Get Changed documents in a list
        if payload_changed_documents not in {None, '', 'null'}:
            changed_documents = json.loads(payload_changed_documents)
            print("changed_documents: ", changed_documents, type(changed_documents))

            try:
                list2_dict = {doc['link']: doc for doc in changed_documents}
                print("List2: ", list2_dict)
                current_documents = [list2_dict.get(doc['link'], doc) for doc in changed_documents]
                print(current_documents)
            except:
                print("No Current Documents")
            

        print("processed changed documents")
        

        # Put New Document Details into document_details
        print("\nAbout to process NEW documents in database;")
        if payload_document_details not in {None, '', 'null'}:
            documents_details = json.loads(payload_document_details)
            print("documents_details: ", documents_details, type(documents_details))
        print("processed new documents")

        # Initialize documents
        documents = []
        i = 0
        j = 0
        documentFiles = {}

        print("About to process ADDED Documents")
        
        # ADD Documents
        while True:
            filename = f'file_{i}'
            print("\nPut file into Filename: ", filename) 
            file = request.files.get(filename)
            print("File:" , file)    
            s3Link = payload.get(filename)
            print("S3Link: ", s3Link, type(s3Link))

            if file:
                print("In File if Statement")
                documentFiles[filename] = file
                unique_filename = filename + "_" + datetime.datetime.utcnow().strftime('%Y%m%d%H%M%SZ')
                doc_key = f'{key_type}/{key_uid}/{unique_filename}'
                # This calls the uploadImage function that generates the S3 link
                document = uploadImage(file, doc_key, '')  # This returns the document http link
                print("Document after upload: ", document)

                print("docObject: ", file)
                print("docObject: ", file.mimetype)
                print("docObject: ", file.filename)
                print("docObject: ", documents_details[j]['contentType'])

                docObject = {}
                docObject["link"] = document
                docObject["filename"] = file.filename
                docObject["contentType"] = documents_details[j]['contentType']
                docObject["fileType"] = file.mimetype
                print("Doc Object: ", docObject)

                documents.append(docObject)

                j += 1

            elif s3Link:
                documents.append(json.loads(s3Link))
                # print("S3 Documents: ", documents )
                s3Link = payload.pop(filename)

            
            else:
                break
            i += 1
        
        print("Documents after loop: ", documents)
        if documents != []:
            current_documents.extend(documents)
            # if key_type == 'contracts': payload['contract_documents'] = json.dumps(current_documents) 
            # if key_type == 'leases': payload['lease_documents'] = json.dumps(current_documents) 
            # if key_type == 'quotes': payload['quote_documents'] = json.dumps(current_documents) 
            # if key_type == 'tenants': payload['tenant_documents'] = json.dumps(current_documents) 
            # if key_type == 'business': payload['business_documents'] = json.dumps(current_documents) 

        print("processed ADDED documents")


        # Delete Documents
        print("\nAbout to process DELETED documents in database;")
        if payload_delete_documents:
            print("In document delete: ", payload_delete_documents, type( payload_delete_documents))
            delete_documents = ast.literal_eval(payload_delete_documents)
            print("After ast: ", delete_documents, type(delete_documents), len(delete_documents))
            for document in delete_documents:
                # print("Document to Delete: ", document, type(document))
                # print("Payload Doc:", current_documents, type(current_documents))
                # print("Current documents before deletion:", [doc['link'] for doc in current_documents])

                # Delete from db list assuming it is in db list
                try:
                    current_documents = [doc for doc in current_documents if doc['link'] != document]
                except:
                    print("Document not in list")

                #  Delete from S3 Bucket
                try:
                    # delete_key = document.split('io-pm/', 1)[1]
                    # delete_key = document.split('space-prod/', 1)[1]
                    delete_key = document.split(f'{bucket}/', 1)[1]
                    # print("Delete key", delete_key)
                    deleteImage(delete_key)
                except: 
                    print("could not delete from S3")
        print("processed DELETED documents")
            
        print("\nCurrent Documents in Function: ", current_documents, type(current_documents))
        # print("Key Type: ", key_type)
        if key_type == 'contracts': payload['contract_documents'] = json.dumps(current_documents)
        if key_type == 'leases': payload['lease_documents'] = json.dumps(current_documents)
        if key_type == 'bills': payload['bill_documents'] = json.dumps(current_documents) 
        if key_type == 'appliances': payload['appliance_documents'] = json.dumps(current_documents) 
        if key_type == 'quotes': payload['quote_documents'] = json.dumps(current_documents)
        if key_type == 'tenants': payload['tenant_documents'] = json.dumps(current_documents)
        if key_type == 'business': payload['business_documents'] = json.dumps(current_documents) 
            
        
        print("Payload before return: ", payload)
        return payload



# --------------- DATABASE CONFIGUATION ------------------
# Connect to MySQL database (API v2)
def connect():
    # global RDS_PW
    # global RDS_HOST
    # global RDS_PORT
    # global RDS_USER
    # global RDS_DB

    print(f"Trying to connect to RDS ({os.getenv('RDS_DB')})...")
    # print("RDS_HOST: ", os.getenv('RDS_HOST'))
    # print("RDS_USER: ", os.getenv('RDS_USER'))
    # print("RDS_PORT: ", os.getenv('RDS_PORT'), type(os.getenv('RDS_PORT')))
    # print("RDS_PW: ", os.getenv('RDS_PW'))
    # print("RDS_DB: ", os.getenv('RDS_DB'))

    try:
        conn = pymysql.connect(
            host=os.getenv('RDS_HOST'),
            user=os.getenv('RDS_USER'),
            port=int(os.getenv('RDS_PORT')),
            passwd=os.getenv('RDS_PW'),
            db=os.getenv('RDS_DB'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
        )
        print("Successfully connected to RDS. (API v2)")
        return DatabaseConnection(conn)
    except:
        print(f"Could not connect to RDS. ({os.getenv('RDS_DB')})")
        raise Exception("RDS Connection failed. (API v2)")


# Disconnect from MySQL database (API v2)
def disconnect(conn):
    try:
        conn.close()
        print("Successfully disconnected from MySQL database. (API v2)")
    except:
        print("Could not properly disconnect from MySQL database. (API v2)")
        raise Exception("Failure disconnecting from MySQL database. (API v2)")


# Serialize JSON
def serializeJSON(unserialized):
    # print("In serialized JSON: ", unserialized, type(unserialized))
    if type(unserialized) == list:
        # print("in list")
        serialized = []
        for entry in unserialized:
            # print("entry: ", entry)
            serializedEntry = serializeJSON(entry)
            serialized.append(serializedEntry)
        # print("Serialized: ", serialized)
        return serialized
    elif type(unserialized) == dict:
        # print("in dict")
        serialized = {}
        for entry in unserialized:
            serializedEntry = serializeJSON(unserialized[entry])
            serialized[entry] = serializedEntry
        return serialized
    elif type(unserialized) == datetime.datetime:
        # print("in date")
        return str(unserialized)
    elif type(unserialized) == bytes:
        # print("in bytes")
        return str(unserialized)
    elif type(unserialized) == Decimal:
        # print("in Decimal")
        return str(unserialized)
    else:
        # print("in else")
        return unserialized

# Actual Database Commands
class DatabaseConnection:
    def __init__(self, conn):
        self.conn = conn

    def disconnect(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()

    def execute(self, sql, args=[], cmd='get'):
        # print("\nIn execute.  SQL: ", sql)
        # print("In execute.  args: ",args)
        # print("In execute.  cmd: ",cmd)
        response = {}
        try:
            with self.conn.cursor() as cur:
                # print('IN EXECUTE')
                if len(args) == 0:
                    # print('execute', sql)
                    cur.execute(sql)
                else:
                    cur.execute(sql, args)
                formatted_sql = f"{sql} (args: {args})"
                # print(formatted_sql)
                    

                if 'get' in cmd:
                    # print('IN GET')
                    result = cur.fetchall()
                    # print("After result: ", result, type(result))
                    result = serializeJSON(result)
                    # print("After serialization: ", result)
                    # print('RESULT GET')
                    response['message'] = 'Successfully executed SQL query'
                    response['code'] = 200
                    response['result'] = result
                    # print('RESPONSE GET')
                elif 'post' in cmd:
                    # print('IN POST')
                    self.conn.commit()
                    response['message'] = 'Successfully committed SQL query'
                    response['code'] = 200
                    response['change'] = str(cur.rowcount) + " rows affected"
                    # print('RESPONSE POST')
        except Exception as e:
            print('ERROR', e)
            response['message'] = 'Error occurred while executing SQL query'
            response['code'] = 500
            response['error'] = e
            print('RESPONSE ERROR', response)
        return response

    def select(self, tables, where={}, cols='*', exact_match = True, limit = None):
        response = {}
        try:
            sql = f'SELECT {cols} FROM {tables}'
            # print(sql)
            for i, key in enumerate(where.keys()):
                # print(i, key)
                if i == 0:
                    sql += ' WHERE '
                if exact_match:
                    sql += f'{key} = %({key})s'
                else:
                    sql += f"{key} LIKE CONCAT('%%', %({key})s ,'%%')"
                if i != len(where.keys()) - 1:
                    sql += ' AND '
            if limit:
                sql += f' LIMIT {limit}'
            # print(sql)
            response = self.execute(sql, where, 'get')
        except Exception as e:
            print(e)
        return response

    def insert(self, table, object):
        # print("\nInside insert: ", table, object)
        response = {}
        try:
            sql = f'INSERT INTO {table} SET '
            for i, key in enumerate(object.keys()):
                sql += f'{key} = %({key})s'
                if i != len(object.keys()) - 1:
                    sql += ', '
            # print(sql)
            # print(object)
            response = self.execute(sql, object, 'post')
            # print("Insert Response: ", response)
        except Exception as e:
            print(e)
        return response

    def update(self, table, primaryKey, object):
        # print("\nIn Update: ", table, primaryKey, object)
        response = {}
        try:
            sql = f'UPDATE {table} SET '
            # print("SQL :", sql)
            # print(object.keys())
            for i, key in enumerate(object.keys()):
                # print("update here 0 ", key)
                sql += f'{key} = %({key})s'
                # print("sql: ", sql)
                if i != len(object.keys()) - 1:
                    sql += ', '
            sql += f' WHERE '
            # print("Updated SQL: ", sql)
            # print("Primary Key: ", primaryKey, type(primaryKey))
            for i, key in enumerate(primaryKey.keys()):
                # print("update here 1")
                sql += f'{key} = %({key})s'
                object[key] = primaryKey[key]
                # print("update here 2", key, primaryKey[key])
                if i != len(primaryKey.keys()) - 1:
                    # print("update here 3")
                    sql += ' AND '
            # print("SQL Query: ", sql, object)
            response = self.execute(sql, object, 'post')
            # print("Response: ", response)
        except Exception as e:
            print(e)
        return response

    def delete(self, sql):
        response = {}
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)

                self.conn.commit()
                response['message'] = 'Successfully committed SQL query'
                response['code'] = 200
                # response = self.execute(sql, 'post')
        except Exception as e:
            print(e)
        return response

    def call(self, procedure, cmd='get'):
        response = {}
        try:
            sql = f'CALL {procedure}()'
            response = self.execute(sql, cmd=cmd)
        except Exception as e:
            print(e)
        return response

