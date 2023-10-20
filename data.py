
import pymysql
from urllib.parse import urlparse
from decimal import Decimal
from datetime import datetime, date, timedelta
import requests
from botocore.response import StreamingBody

ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg'])



# SECTION 3: DATABASE FUNCTIONALITY
# RDS for AWS SQL 5.7
# RDS_HOST = 'pm-mysqldb.cxjnrciilyjq.us-west-1.rds.amazonaws.com'
# RDS for AWS SQL 8.0
RDS_HOST = 'io-mysqldb8.cxjnrciilyjq.us-west-1.rds.amazonaws.com'
RDS_PORT = 3306
RDS_USER = 'admin'
RDS_DB = 'space'
RDS_PW="prashant"   # Not sure if I need this
# RDS_PW = os.environ.get('RDS_PW')
S3_BUCKET = "manifest-image-db"
# S3_BUCKET = os.environ.get('S3_BUCKET')
# S3_KEY = os.environ.get('S3_KEY')
# S3_SECRET_ACCESS_KEY = os.environ.get('S3_SECRET_ACCESS_KEY')


# CONNECT AND DISCONNECT TO MYSQL DATABASE ON AWS RDS (API v2)
# Connect to MySQL database (API v2)
def connect():
    global RDS_PW
    global RDS_HOST
    global RDS_PORT
    global RDS_USER
    global RDS_DB

    # print("Trying to connect to RDS (API v2)...")
    try:
        conn = pymysql.connect(host=RDS_HOST,
                               user=RDS_USER,
                               port=RDS_PORT,
                               passwd=RDS_PW,
                               db=RDS_DB,
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)
        # print("Successfully connected to RDS. (API v2)")
        return conn
    except:
        print("Could not connect to RDS. (API v2)")
        raise Exception("RDS Connection failed. (API v2)")

# Disconnect from MySQL database (API v2)
def disconnect(conn):
    try:
        conn.close()
        # print("Successfully disconnected from MySQL database. (API v2)")
    except:
        print("Could not properly disconnect from MySQL database. (API v2)")
        raise Exception("Failure disconnecting from MySQL database. (API v2)")

# Execute an SQL command (API v2)
# Set cmd parameter to 'get' or 'post'
# Set conn parameter to connection object
# OPTIONAL: Set skipSerialization to True to skip default JSON response serialization
def execute(sql, cmd, conn, skipSerialization=False):
    response = {}
    # print("==> Execute Query: ", cmd,sql)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cmd == 'get':
                print("in get")
                result = cur.fetchall()
                response['message'] = 'Successfully executed SQL query.'
                # Return status code of 280 for successful GET request
                response['code'] = 280
                if not skipSerialization:
                    print("Serialized Response")
                    result = serializeResponse(result)
                    # print("GET QUERY RESULT: ", result)
                response['result'] = result
            elif cmd == 'post':
                print("in post")
                conn.commit()
                response['message'] = 'Successfully committed SQL command.'
                # Return status code of 281 for successful POST request
                response['code'] = 281
            elif cmd == 'del':
                print("in del")
                conn.commit()
                response['message'] = 'Successfully committed SQL command.'
                # Return status code of 281 for successful POST request
                response['code'] = 281
            else:
                response['message'] = 'Request failed. Unknown or ambiguous instruction given for MySQL command.'
                # Return status code of 480 for unknown HTTP method
                response['code'] = 480
    except:
        response['message'] = 'Request failed, could not execute MySQL command.'
        # Return status code of 490 for unsuccessful HTTP request
        response['code'] = 490
    finally:
        # response['sql'] = sql
        return response

# Serialize JSON
def serializeResponse(response):
    try:
        for row in response:
            for key in row:
                if type(row[key]) is Decimal:
                    row[key] = float(row[key])
                elif (type(row[key]) is date or type(row[key]) is datetime) and row[key] is not None:
                # Change this back when finished testing to get only date
                    row[key] = row[key].strftime("%Y-%m-%d")
                    # row[key] = row[key].strftime("%Y-%m-%d %H-%M-%S")
                # elif is_json(row[key]):
                #     row[key] = json.loads(row[key])
                elif isinstance(row[key], bytes):
                    row[key] = row[key].decode()
        return response
    except:
        raise Exception("Bad query JSON")


# RUN STORED PROCEDURES

        # MOVE STORED PROCEDURES HERE


# Function to upload image to s3
def allowed_file(filename):
    # Checks if the file is allowed to upload
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def helper_upload_img(file):
    bucket = S3_BUCKET
    # creating key for image name
    salt = os.urandom(8)
    dk = hashlib.pbkdf2_hmac('sha256',  (file.filename).encode(
        'utf-8'), salt, 100000, dklen=64)
    key = (salt + dk).hex()

    if file and allowed_file(file.filename):

        # image link
        filename = 'https://s3-us-west-1.amazonaws.com/' \
                   + str(bucket) + '/' + str(key)

        # uploading image to s3 bucket
        upload_file = s3.put_object(
            Bucket=bucket,
            Body=file,
            Key=key,
            ACL='public-read',
            ContentType='image/jpeg'
        )
        return filename
    return None

# Function to upload icons
def helper_icon_img(url):

    bucket = S3_BUCKET
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        raw_data = response.content
        url_parser = urlparse(url)
        file_name = os.path.basename(url_parser.path)
        key = 'image' + "/" + file_name

        try:

            with open(file_name, 'wb') as new_file:
                new_file.write(raw_data)

            # Open the server file as read mode and upload in AWS S3 Bucket.
            data = open(file_name, 'rb')
            upload_file = s3.put_object(
                Bucket=bucket,
                Body=data,
                Key=key,
                ACL='public-read',
                ContentType='image/jpeg')
            data.close()

            file_url = 'https://%s/%s/%s' % (
                's3-us-west-1.amazonaws.com', bucket, key)

        except Exception as e:
            print("Error in file upload %s." % (str(e)))

        finally:
            new_file.close()
            os.remove(file_name)
    else:
        print("Cannot parse url")

    return file_url

def uploadImage(file, key, content):
    bucket = 'io-pm'
    contentType = ''
    # # contentType = 'image/jpeg'
    # print('file in uploadimage', file, key)
    # print('img in key', 'img' in key)
    # print('doc in key', 'doc' in key)
    # print('typefile streamingbody', type(file) == StreamingBody)
    # trying to upload a new image file
    # if type(file) != StreamingBody and '.svg' in file.filename:
    #     contentType = 'image/svg+xml'
    #     print('file in svg', contentType)
    # # trying to upload a new pdf file
    # elif type(file) != StreamingBody and '.pdf' in file.filename:
    #     contentType = 'application/pdf'
    #     print('file in pdf', contentType)
    if type(file) == StreamingBody:
        contentType = content
        # print('file in streamingbody', contentType)
    # # # trying to upload a new image file
    # elif type(file) == StreamingBody and 'img' in key:
    #     contentType = 'image/svg+xml'
    #     print('file in streamingbody and img', contentType)
    # # trying to reload an exisiting pdf
    # elif type(file) == StreamingBody and 'doc' in key:
    #     contentType = 'application/pdf'
    #     print('file in streamingbody and doc', contentType)
    # print('contentType', contentType)
    if file:
        # print('if file', file, bucket, key)
        filename = f'https://s3-us-west-1.amazonaws.com/{bucket}/{key}'
        upload_file = s3.put_object(
            Bucket=bucket,
            Body=file.read(),
            Key=key,
            ACL='public-read',
            ContentType=contentType
        )

        return filename
    return None

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
        response = {}
        try:
            with self.conn.cursor() as cur:
                # print('IN EXECUTE')
                if len(args) == 0:
                    # print('execute', sql)
                    cur.execute(sql)
                else:
                    cur.execute(sql, args)

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
                    # print('RESPONSE POST')
        except Exception as e:
            print('ERROR', e)
            response['message'] = 'Error occurred while executing SQL query'
            response['code'] = 500
            response['error'] = e
            print('RESPONSE ERROR', response)
        return response

    def select(self, tables, where={}, cols='*'):
        response = {}
        try:
            sql = f'SELECT {cols} FROM {tables}'
            for i, key in enumerate(where.keys()):
                if i == 0:
                    sql += ' WHERE '
                sql += f'{key} = %({key})s'
                if i != len(where.keys()) - 1:
                    sql += ', '
            # print(sql)
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
            response = self.execute(sql, object, 'post')
        except Exception as e:
            print(e)
        return response

    def update(self, table, primaryKey, object):
        response = {}
        try:
            sql = f'UPDATE {table} SET '
            # print(sql)
            for i, key in enumerate(object.keys()):
                sql += f'{key} = %({key})s'
                if i != len(object.keys()) - 1:
                    sql += ', '
            sql += f' WHERE '
            # print(sql)
            for i, key in enumerate(primaryKey.keys()):
                sql += f'{key} = %({key})s'
                object[key] = primaryKey[key]
                if i != len(primaryKey.keys()) - 1:
                    sql += ' AND '
            # print(sql, object)
            response = self.execute(sql, object, 'post')
            # print(response)
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
