from flask_restful import Resource
import requests
from data_pm import connect
import os
import pymysql
from dotenv import load_dotenv
import collections
from data_pm import connect, uploadImage, s3, serializeJSON

load_dotenv()

class Extract_API(Resource):
    def get(self):
        print("\nIn extract API get")
        response = {}

        my_space_api_set = set()
        print("\nIn try")
        try:
            my_space_api_url = "https://raw.githubusercontent.com/infinite-options/myspace-backend/refs/heads/master/myspace_api.py"
            my_space_api_response = requests.get(my_space_api_url)
            if my_space_api_response.status_code != 200:
                raise Exception(f"Error in reading my {os.getenv('RDS_DB')} api file")

            my_space_api_content = my_space_api_response.text
            lines = my_space_api_content.splitlines()
            for line in lines:
                line = line.lstrip()
                if line.startswith('api.add_resource'):
                    parts = line[16:].strip('()\n').split(',')
                    url = parts[1].strip(" '\"").split('/')
                    my_space_api_set.add(url[1])
                    
        except:
            response['message'] = 'Error in runnning the my_space code'
            return response

        test_api_set = set()
        try:
            test_api_url = "https://raw.githubusercontent.com/infinite-options/myspace-backend/refs/heads/master/test_api.py"
            test_api_response = requests.get(test_api_url)
            if test_api_response.status_code != 200:
                raise Exception('Error in reading test api file')

            test_api_content = test_api_response.text
            lines = test_api_content.splitlines()
            for line in lines:
                line = line.lstrip()
                idx = line.find('requests.')
                if idx != -1 and not line.startswith('#'):
                    url = line[idx:].strip("') \n\"").lstrip("requests.getpostputdelete(ENDPOINT +f'\"")
                    api = url.split('/')[1].split(',')[0].strip("\"")
                    api_idx = api.find("?")
                    if api_idx == -1:
                        test_api_set.add(api)

        except:
            response['message'] = 'Error in runnning the test_api code'
            return response
        
        response['remaining_apis'] = []
        try:
            for api in my_space_api_set:
                if api not in test_api_set:
                    response['remaining_apis'].append(api)
            
        except:
            response['message'] = 'Error in finding remaining apis'
        
        return response


class CleanUpDatabase(Resource):
    def get(self):
        print("\nIn Clean Up Database")
        # Define the patterns to search for
        patterns = [r"200-000000", r"600-000000", r"110-000000", r"350-000000", r"100-000000", r"050-000000", r"800-000000", r"900-000000", r"400-000000", r"010-000000", r"370-000000", r"300-000000"]
        response = {}

        # try:
        #     # Connect to the MySQL database
        #     connection = pymysql.connect(
        #         host=os.getenv('RDS_HOST'),
        #         user=os.getenv('RDS_USER'),
        #         port=int(os.getenv('RDS_PORT')),
        #         passwd=os.getenv('RDS_PW'),
        #         db=os.getenv('RDS_DB'),
        #         charset='utf8mb4',
        #         cursorclass=pymysql.cursors.DictCursor
        #     )
        # except:
        #     response['message'] = 'Error in database connection'
        #     return response
        
        
        try:
            # cursor = connection.cursor()
            cursor = connect.cursor()
            # cursor.execute("""
            #     SELECT TABLE_NAME 
            #     FROM information_schema.TABLES 
            #     WHERE TABLE_SCHEMA = 'space_dev' AND TABLE_TYPE = 'BASE TABLE';
            # """)
            tables = cursor.fetchall()

            response['Data deleted from (tables)'] = collections.defaultdict(list)

            for table_name in tables:
                # print(f"\nProcessing table: {table_name}")
                table = table_name['TABLE_NAME']

                cursor.execute(f"DESCRIBE `{table}`")
                columns = cursor.fetchall()

                for column_info in columns:
                    column_name = column_info['Field']
                    # print(f"Checking column: {column_name} in table: {value}")

                    for pattern in patterns:
                        delete_query = f"""
                        DELETE FROM `{table}`
                        WHERE `{column_name}` REGEXP %s
                        """
                        cursor.execute(delete_query, (pattern,))
                        rows_deleted = cursor.rowcount 
                        # connection.commit()
                        connect.commit()
                        if rows_deleted > 0:
                            response['Data deleted from (tables)'][table].append(f"\nDeleted {rows_deleted} rows in table `{table}` where column `{column_name}` matched '{pattern}'\n")
                            # print(f"\nDeleted {rows_deleted} rows in table `{table}` where column `{column_name}` matched '{pattern}'\n")

            if len(response['Data deleted from (tables)']) == 0:
                response.pop('Data deleted from (tables)')
            
            response['message'] = 'Clean Up Completed'
            # print("Cleanup completed.")

        except:
            response['message'] = 'Error in cleaning the database'
        finally:
            cursor.close()
            # connection.close()
            connect.close()
        
        return response