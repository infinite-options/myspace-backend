from flask import request

import os
import pymysql
import datetime
import json
import boto3
from botocore.response import StreamingBody
from decimal import Decimal
from datetime import date, datetime, timedelta
from werkzeug.datastructures import FileStorage
import mimetypes
import ast

s3 = boto3.client('s3')



# def uploadImage(file, key, content):
#     bucket = 'io-pm'
#     contentType = ''

#     if type(file) == StreamingBody:
#         contentType = content

#     if file:
#         # print('if file', file, bucket, key)
#         filename = f'https://s3-us-west-1.amazonaws.com/{bucket}/{key}'
#         upload_file = s3.put_object(
#             Bucket=bucket,
#             Body=file.read(),
#             Key=key,
#             ACL='public-read',
#             ContentType=contentType
#         )

#         return filename
#     return None

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
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"File {key} does not exist in bucket {bucket}")
        else:
            print(f"Error deleting file {key} from bucket {bucket}: {e}")
        return False


def uploadImage(file, key, content):
    print("\nIn Upload Image: ", file, key, content)
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
    property_uid = key['property_uid']
   
    current_images = []
    if payload.get('property_images') is not None:
        current_images =ast.literal_eval(payload.get('property_images'))
        print("Current images: ", current_images, type(current_images))

    # Check if images are being added OR deleted
    images = []
    # i = -1
    i = 0
    imageFiles = {}
    favorite_image = payload.get("property_favorite_image")
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
            print("In File if Statement")
            imageFiles[filename] = file
            unique_filename = filename + "_" + datetime.utcnow().strftime('%Y%m%d%H%M%SZ')
            image_key = f'properties/{property_uid}/{unique_filename}'
            # This calls the uploadImage function that generates the S3 link
            image = uploadImage(file, image_key, '')
            images.append(image)

            if filename == favorite_image:
                payload["property_favorite_image"] = image

        elif s3Link:
            imageFiles[filename] = s3Link
            images.append(s3Link)

            if filename == favorite_image:
                payload["property_favorite_image"] = s3Link
        else:
            break
        i += 1
    
    print("Images after loop: ", images)
    if images != []:
        current_images.extend(images)
        payload['property_images'] = json.dumps(current_images) 

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
                print("Image not in list")

            #  Delete from S3 Bucket
            try:
                delete_key = image.split('io-pm/', 1)[1]
                print("Delete key", delete_key)
                deleteImage(delete_key)
            except: 
                print("could not delete from S3")
        
        print("Updated List of Images: ", current_images)


        print("Current Images: ", current_images)
        payload['property_images'] = json.dumps(current_images) 

    # Write to Database
    with connect() as db:
        print("Checking Inputs: ", key, payload)
        response['property_info'] = db.update('properties', key, payload)
        # print("Response:" , response)
    return response


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
    # print(unserialized, type(unserialized))
    if type(unserialized) == list:
        # print("in list")
        serialized = []
        for entry in unserialized:
            serializedEntry = serializeJSON(entry)
            serialized.append(serializedEntry)
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
                    result = serializeJSON(result)
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
