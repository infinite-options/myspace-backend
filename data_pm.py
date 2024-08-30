from flask import request

import os
import pymysql
import datetime
import json
import boto3
from botocore.response import StreamingBody
from decimal import Decimal
# from datetime import date, datetime, timedelta
from werkzeug.datastructures import FileStorage
import mimetypes
import ast

s3 = boto3.client('s3')


def deleteImage(key):
    bucket = 'io-pm'
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


def uploadImage(file, key, content):
    print("\nIn Upload Image: ")
    # print("File: ", file)
    # print("Key: ", key)
    # print("Content: ", content)
    bucket = 'io-pm'

    if isinstance(file, FileStorage): 
        print("In Upload Image isInstance File Storage: ", FileStorage)
        file.stream.seek(0)
        file_content = file.stream.read()
        content_type, _ = mimetypes.guess_type(file.filename)
        contentType = content_type if content_type else 'application/octet-stream'  # Fallback if MIME type is not detected
        print("In Upload Image contentType: ", contentType) # This returns jpeg, png, ect

    elif isinstance(file, StreamingBody):
        print("In Upload Image isInstance Streaming Body")
        file_content = file.read()
        contentType = content
        print("In Upload Image contentType: ", contentType)
        # Set content type based on your logic or metadata
        # Example: contentType = 'image/jpeg' or other appropriate content type


    if file_content:
        # print("file_content: ", file_content )   # Unnecessary print statement.  Return hexedemical file info
        filename = f'https://s3-us-west-1.amazonaws.com/{bucket}/{key}'
        print("Before Upload: ", bucket, key, filename, contentType)
        # This Statement Actually uploads the file into S3
        upload_file = s3.put_object(
            Bucket=bucket,
            Body=file_content,
            Key=key,
            ACL='public-read',
            ContentType=contentType
        )
        print("After Upload: ", upload_file)
        print("After Upload Status Code: ", upload_file['ResponseMetadata']['HTTPStatusCode'])
        print("Derived Filename: ", filename)
        return filename
    
    return None

# --------------- PROCESS IMAGES ------------------

def processImage(key, payload):
    print("\nIn Process Image: ", payload)
    response = {}

    if 'property_uid' in key:
        print("Property Key passed")
        key_type = 'properties'
        key_uid = key['property_uid']
        payload_images = payload.get('property_images', None)
        payload_fav_images = payload.get("property_favorite_image") or payload.get("img_favorite")   # (PUT & POST)
        

    elif 'appliance_uid' in key:
        print("Appliance Key passed")
        key_type = 'appliances'
        key_uid = key['appliance_uid']
        payload_images = payload.get('appliance_images', None)
        payload_fav_images = payload.get("appliance_favorite_image") or payload.get("img_favorite")   # (PUT & POST)


    elif 'maintenance_request_uid' in key:
        print("Maintenance Request Key passed")
        key_type = 'maintenance request'
        key_uid = key['maintenance_request_uid']
        payload_images = payload.get('maintenance_images', None)  # Current Images
        payload_fav_images = payload.get("maintenance_favorite_image") or payload.get("img_favorite")   # (PUT & POST)

    
    elif 'maintenance_quote_uid' in key:
        print("Maintenance Quote Key passed")
        key_type = 'maintenance quote'
        key_uid = key['maintenance_quote_uid']
        payload_images = payload.get('quote_maintenance_images', None)
        payload_fav_images = payload.get("maintenance_favorite_image") or payload.get("img_favorite")   # (PUT & POST)


    else:
        print("No UID found in key")
        return
    

    # print("key_type: ", key_type)
    # print("key_uid: ", key_uid)
    # print("payload_images: ", payload_images)
    # print("payload_fav_images: ", payload_fav_images)

    payload.pop("img_favorite", None)
    payload_delete_images = payload.pop('delete_images', None)  # Images to Delete
    print("Past if statement")
   
    # Check if images already exist
    # Put current db images into current_images
    current_images = []
    print("here 0")
    if payload_images is not None and payload_images != '' and payload_images != 'null':
        print("Payload Images: ", payload_images)
        current_images =ast.literal_eval(payload_images)
        print("Current images: ", current_images, type(current_images))
    print("here 1")

    # Check if images are being added OR deleted
    images = []
    i = 0
    imageFiles = {}

    print("here 2")

    # ADD Images
    while True:
        filename = f'img_{i}'
        print("Put image file into Filename: ", filename) 
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
            break
        i += 1
    
    print("Images after loop: ", images)
    if images != []:
        current_images.extend(images)
        if key_type == 'properties': payload['property_images'] = json.dumps(current_images) 
        if key_type == 'appliances': payload['appliance_images'] = json.dumps(current_images) 
        if key_type == 'maintenance request': payload['maintenance_images'] = json.dumps(current_images) 
        if key_type == 'maintenance quote': payload['quote_maintenance_images'] = json.dumps(current_images) 

    # Delete Images
    if payload_delete_images:
        delete_images = ast.literal_eval(payload_delete_images)
        print(delete_images, type(delete_images), len(delete_images))
        for image in delete_images:
            print("Image to Delete: ", image, type(image))
            # Delete from db list assuming it is in db list
            try:
                current_images.remove(image)
            except:
                print("Image not in list")

            #  Delete from S3 Bucket
            try:
                delete_key = image.split('io-pm/', 1)[1]
                print("Delete key", delete_key)
                deleteImage(delete_key)
            except: 
                print("could not delete from S3")
        
    print("\nCurrent Images in Function: ", current_images)
    if key_type == 'properties': payload['property_images'] = json.dumps(current_images) 
    if key_type == 'appliances': payload['appliance_images'] = json.dumps(current_images) 
    if key_type == 'maintenance request': payload['maintenance_images'] = json.dumps(current_images) 
    if key_type == 'maintenance quote': payload['quote_maintenance_images'] = json.dumps(current_images) 


    print("Payload before return: ", payload)
    return payload



# --------------- PROCESS DOCUMENT ------------------

def processDocument(key, payload):
    print("\nIn Process Image: ", payload)
    response = {}

    if 'contract_uid' in key:
        print("Contract Key passed")
        key_type = 'contracts'
        key_uid = key['contract_uid']
        payload_documents = payload.get('contract_documents', None)
        payload_document_details = payload.pop('contract_documents_details', None)
        # payload_fav_images = payload.get("property_favorite_image") or payload.get("img_favorite")   # (PUT & POST)

    elif 'lease_uid' in key:
        print("Lease Key passed")
        key_type = 'leases'
        key_uid = key['lease_uid']
        payload_documents = payload.get('lease_documents', None)
        payload_document_details = payload.pop('lease_documents_details', None)
        # payload_fav_images = payload.get("property_favorite_image") or payload.get("img_favorite")   # (PUT & POST)

    elif 'maintenance_quote_uid' in key:
        print("Quote Key passed")
        key_type = 'quotes'
        key_uid = key['maintenance_quote_uid']
        payload_documents = payload.get('quote_documents', None)
        payload_document_details = payload.pop('quote_documents_details', None)
        # payload_fav_images = payload.get("property_favorite_image") or payload.get("img_favorite")   # (PUT & POST)

    elif 'tenant_uid' in key:
        print("Tenant Key passed")
        key_type = 'tenants'
        key_uid = key['tenant_uid']
        payload_documents = payload.get('tenant_documents', None)
        payload_document_details = payload.pop('tenant_documents_details', None)
        # payload_fav_images = payload.get("property_favorite_image") or payload.get("img_favorite")   # (PUT & POST)

    else:
        print("No UID found in key")
        return

    print("key_type: ", key_type)
    print("key_uid: ", key_uid)
    print("payload_documents: ", payload_documents)
    print("payload_document_details: ", payload_document_details)
    
    # payload.pop("img_favorite", None)
    payload_delete_documents = payload.pop('delete_documents', None)
    print("Past if statement")


    # Check if files already exist
    # Put current db files into current_documents
    current_documents = []
    print("here 0")
    if payload_documents is not None and payload_documents != '' and payload_documents != 'null':
        print("Payload Documents: ", payload_documents)
        current_documents =ast.literal_eval(payload_documents)
        print("Current documents: ", current_documents, type(current_documents))
    print("here 1")

    if payload_document_details is not None and payload_document_details != '' and payload_document_details != 'null':
        documents_details = json.loads(payload_document_details)
        print("documents_details: ", documents_details, type(documents_details))
    print("here 1a")

    # Check if documents are being added OR deleted
    documents = []
    i = 0
    documentFiles = {}

    print("here 2")
    
    while True:
        filename = f'file_{i}'
        print("\nPut file into Filename: ", filename) 
        file = request.files.get(filename)
        print("File:" , file)    
        s3Link = payload.get(filename)
        print("S3Link: ", s3Link)


        if file:
            print("In File if Statement")
            documentFiles[filename] = file
            unique_filename = filename + "_" + datetime.datetime.utcnow().strftime('%Y%m%d%H%M%SZ')
            doc_key = f'{key_type}/{key_uid}/{unique_filename}'
            # This calls the uploadImage function that generates the S3 link
            document = uploadImage(file, doc_key, '')  # This returns the document http link
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
        if key_type == 'contracts': payload['contract_documents'] = json.dumps(current_documents) 
        if key_type == 'leases': payload['lease_documents'] = json.dumps(current_documents) 
        if key_type == 'quotes': payload['quote_documents'] = json.dumps(current_documents) 
        if key_type == 'tenants': payload['tenant_documents'] = json.dumps(current_documents) 



    # Delete Documents
    if payload_delete_documents:
        print("In document delete")
        delete_documents = ast.literal_eval(payload_delete_documents)
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
        
    print("\nCurrent Images in Function: ", current_documents)
    if key_type == 'contracts': payload['contract_documents'] = json.dumps(current_documents)
    if key_type == 'leases': payload['lease_documents'] = json.dumps(current_documents)
    if key_type == 'quotes': payload['quote_documents'] = json.dumps(current_documents)
    if key_type == 'tenants': payload['tenant_documents'] = json.dumps(current_documents)
        
     
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

    print("Trying to connect to RDS (API v2)...")
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
        print("Could not connect to RDS. (API v2)")
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
        # print("In execute.  SQL: ", sql)
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
            for i, key in enumerate(where.keys()):
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
            response = self.execute(sql, where, 'get')
        except Exception as e:
            print(e)
        return response

    def insert(self, table, object):
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
        except Exception as e:
            print(e)
        return response

    def update(self, table, primaryKey, object):
        print("\nIn Update: ", table, primaryKey, object)
        response = {}
        try:
            sql = f'UPDATE {table} SET '
            print("SQL :", sql)
            print(object.keys())
            for i, key in enumerate(object.keys()):
                print("update here 0 ", key)
                sql += f'{key} = %({key})s'
                # print("sql: ", sql)
                if i != len(object.keys()) - 1:
                    sql += ', '
            sql += f' WHERE '
            print("Updated SQL: ", sql)
            print("Primary Key: ", primaryKey, type(primaryKey))
            for i, key in enumerate(primaryKey.keys()):
                print("update here 1")
                sql += f'{key} = %({key})s'
                object[key] = primaryKey[key]
                print("update here 2", key, primaryKey[key])
                if i != len(primaryKey.keys()) - 1:
                    print("update here 3")
                    sql += ' AND '
            print("SQL Query: ", sql, object)
            response = self.execute(sql, object, 'post')
            print("Response: ", response)
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
