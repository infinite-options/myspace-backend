import pymysql
from datetime import datetime
import json
from decimal import Decimal
import requests

from flask_restful import Resource

#python -m pytest -v -s Use this in cmd to run the pytest script


def connect():
    conn = pymysql.connect(
        host='io-mysqldb8.cxjnrciilyjq.us-west-1.rds.amazonaws.com',
        port=3306,
        user='admin',
        passwd='prashant',
        db='space',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    return DatabaseConnection(conn)


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
                    sql += ' AND '

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
        response = {}
        try:
            sql = f'UPDATE {table} SET '
            print(sql)
            for i, key in enumerate(object.keys()):
                sql += f'{key} = %({key})s'
                if i != len(object.keys()) - 1:
                    sql += ', '
            sql += f' WHERE '
            print(sql)
            for i, key in enumerate(primaryKey.keys()):
                sql += f'{key} = %({key})s'
                object[key] = primaryKey[key]
                if i != len(primaryKey.keys()) - 1:
                    sql += ' AND '
            print(sql, object)
            response = self.execute(sql, object, 'post')
            print(response)
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


ENDPOINT = "https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev"


# Tables affecting: {maintenanceRequests, maintenanceQuotes, Properties, Property_Owner, Contracts, leases, lease_tenant, addPurchases, paymentMethods, payments}
class endPointTest_CLASS(Resource):
    def get():
        dt = datetime.today()
        response = {}
        # Insert temporary data into the database
        try:
            print("\n\n*** Inserting temporary data into the database ***\n")
            insert_property_query = """
                                        INSERT INTO `space`.`properties` (`property_uid`, `property_available_to_rent`, `property_active_date`, `property_listed_date`, `property_address`, `property_city`, `property_state`, `property_zip`, `property_longitude`, `property_latitude`, `property_type`, `property_num_beds`, `property_num_baths`, `property_value`, `property_value_year`, `property_area`, `property_listed_rent`, `property_deposit`, `property_pets_allowed`, `property_deposit_for_rent`, `property_featured`) 
                                        VALUES ('200-000000', '1', '10-28-2024', '10-28-2024', '123 Test Apt', 'San Jose', 'CA', '95119', '-121.7936071000', '37.2346668000', 'Single Family', '4', '2', '1500000', '2013', '2100', '2500', '1200', '1', '0', 'False');
                                    """
            
            insert_property_owner_query =   """
                                                INSERT INTO `space`.`property_owner` (`property_id`, `property_owner_id`, `po_owner_percent`) 
                                                VALUES ('200-000000', '110-000000', '1');
                                            """
            
            insert_users_query = """
                                    INSERT INTO `space`.`users` (`user_uid`, `first_name`, `last_name`, `phone_number`, `email`, `role`, `notifications`, `dark_mode`, `cookies`) 
                                    VALUES ('100-000000', 'Test', 'Account', '(000) 000-0000', 'test@gmail.com', 'OWNER,MANAGER,TENANT,MAINTENANCE', 'true', 'false', 'true');
                                """
            
            insert_business_profile_query = """
                                                INSERT INTO `space`.`businessProfileInfo` (`business_uid`, `business_user_id`, `business_type`, `business_name`, `business_phone_number`, `business_email`) 
                                                VALUES ('600-000000', '100-000000', 'MANAGEMENT', 'Reserved For Test', '(000) 000-0000', 'test@gmail.com');
                                            """

            insert_owner_profile_query = """
                                            INSERT INTO `space`.`ownerProfileInfo` (`owner_uid`, `owner_user_id`, `owner_first_name`, `owner_last_name`, `owner_phone_number`, `owner_email`) 
                                            VALUES ('110-000000', '100-000000', 'Test', 'Account', '(000) 000-0000', 'test@gmail.com');
                                        """
            
            insert_tenant_profile_query = """
                                            INSERT INTO `space`.`tenantProfileInfo` (`tenant_uid`, `tenant_user_id`, `tenant_first_name`, `tenant_last_name`, `tenant_email`, `tenant_phone_number`, `tenant_documents`, `tenant_adult_occupants`, `tenant_children_occupants`, `tenant_vehicle_info`, `tenant_references`, `tenant_pet_occupants`, `tenant_employment`) 
                                            VALUES ('350-000000', '100-000000', 'Test', 'Account', 'test@gmail.com', '(000) 000-0000', '[]', '[]', '[]', '[]', '[]', '[]', '[]');
                                        """

            insert_purchases_query = """
                                        INSERT INTO `space`.`purchases` (`purchase_uid`, `pur_timestamp`, `pur_property_id`, `purchase_type`, `pur_description`, `pur_notes`, `pur_cf_type`, `purchase_date`, `pur_due_date`, `pur_amount_due`, `purchase_status`, `pur_status_value`, `pur_receiver`, `pur_initiator`, `pur_payer`, `pur_late_Fee`, `pur_group`) 
                                        VALUES ('400-000000', '11-14-2024 00:00', '200-000000', 'Deposit', 'Test Deposit', 'Test Deposit Note', 'revenue', '11-15-2024 00:00', '11-30-2024 00:00', '299.00', 'UNPAID', '0', '110-000000', '350-000000', '350-000000', '0', '400-000000');
                                    """
            
            insert_maintenance_requests_query = """
                                                    INSERT INTO `space`.`maintenanceRequests` (`maintenance_request_uid`, `maintenance_property_id`, `maintenance_request_status`, `maintenance_title`, `maintenance_request_type`, `maintenance_request_created_by`, `maintenance_priority`, `maintenance_can_reschedule`) 
                                                    VALUES ('800-000000', '200-000000', 'NEW', 'Test Maintenance Request', 'Plumbing', '600-000000', 'Medium', '1');
                                                """

            insert_maintenance_quotes_query = """
                                                INSERT INTO `space`.`maintenanceQuotes` (`maintenance_quote_uid`, `quote_maintenance_request_id`, `quote_status`, `quote_business_id`, `quote_requested_date`) 
                                                VALUES ('900-000000', '800-000000', 'SCHEDULED', '600-000000', '11-12-2024 15:26:30');
                                            """
            
            insert_contracts_query = """
                                        INSERT INTO `space`.`contracts` (`contract_uid`, `contract_property_id`, `contract_business_id`, `contract_name`, `contract_status`, `contract_m2m`) 
                                        VALUES ('010-000000', '200-000000', '600-000000', 'Test Contract Name', 'ACTIVE', '1');
                                    """

            with connect() as db:
                insert_property_query_response = db.execute(insert_property_query, cmd='post')
                insert_property_owner_query_response = db.execute(insert_property_owner_query, cmd='post')
                insert_users_query_response = db.execute(insert_users_query, cmd='post')
                insert_business_profile_query_response = db.execute(insert_business_profile_query, cmd='post')
                insert_owner_profile_query_response = db.execute(insert_owner_profile_query, cmd='post')
                insert_tenant_profile_query_response = db.execute(insert_tenant_profile_query, cmd='post')
                insert_purchases_query_response = db.execute(insert_purchases_query, cmd='post')
                insert_maintenance_requests_query_response = db.execute(insert_maintenance_requests_query, cmd='post')
                insert_maintenance_quotes_query_response = db.execute(insert_maintenance_quotes_query, cmd='post')
                insert_contracts_query_response = db.execute(insert_contracts_query, cmd='post')

            print("\n*** Completed ***\n")
            response['insert_temporary_data'] = 'Passed'
        except:
            response['insert_temporary_data'] = 'Failed'

        
        response['No of APIs tested'] = 0
        response['APIs running successfully'] = []
        response['APIs failing'] = []

        try:
            # ------------------------- MAINTENANCE ------------------------------

            # -------- test post maintenance request --------
            print("\nIn test POST Maintenance Requests")
            post_maintenance_request_payload = {
                    "maintenance_property_id":"200-000000",
                    "maintenance_title":"Vents Broken",
                    "maintenance_desc":"Vents",
                    "maintenance_request_type":"Appliance",
                    "maintenance_request_created_by":"600-000000",
                    "maintenance_priority":"High",
                    "maintenance_can_reschedule":1,
                    "maintenance_assigned_business":"null",
                    "maintenance_assigned_worker":"null",
                    "maintenance_scheduled_date":"null",
                    "maintenance_scheduled_time":"null",
                    "maintenance_frequency":"One Time",
                    "maintenance_notes":"null",
                    "maintenance_request_created_date":"2024-11-13",
                    "maintenance_request_closed_date":"null",
                    "maintenance_request_adjustment_date":"null"
                }
            post_maintenance_request_response = requests.post(ENDPOINT + "/maintenanceRequests", data = post_maintenance_request_payload)
            maintenance_request_uid = post_maintenance_request_response.json()['maintenance_request_uid']
            if post_maintenance_request_response.status_code == 200:
                response['APIs running successfully'].append('POST Maintenance Requests')
            else:
                response['APIs failing'].append('POST Maintenance Requests')
            response['No of APIs tested'] += 1

            # -------- test post get maintenance request --------
            print("\nIn test GET after POST Maintenance Requests")
            post_get_maintenance_request_response = requests.get(ENDPOINT + f"/maintenanceReq/200-000000")
            data = post_get_maintenance_request_response.json()['result']['NEW REQUEST']['maintenance_items'][0]
            for k, v in post_maintenance_request_payload.items():
                if data[k] == v:
                    continue
                else:
                    print(k, v, "not a match")
            if post_get_maintenance_request_response.status_code == 200:
                response['APIs running successfully'].append('GET after POST Maintenance Requests')
            else:
                response['APIs failing'].append('GET after POST Maintenance Requests')
            response['No of APIs tested'] += 1

            # -------- test put maintenance request --------
            print("\nIn test PUT Maintenance Requests")
            put_maintenance_request_payload = {
                "maintenance_request_uid":f"{maintenance_request_uid}","maintenance_request_status":"SCHEDULED","maintenance_scheduled_date":"11/30/2024","maintenance_scheduled_time":"10:00:00"
            }
            put_maintenance_request_response = requests.put(ENDPOINT + "/maintenanceRequests", data = put_maintenance_request_payload) 
            if put_maintenance_request_response.status_code == 200:
                response['APIs running successfully'].append('PUT Maintenance Requests')
            else:
                response['APIs failing'].append('PUT Maintenance Requests')
            response['No of APIs tested'] += 1

            # -------- test put get maintenance request --------
            print("\nIn test GET after PUT Maintenance Requests")
            put_get_maintenance_request_response = requests.get(ENDPOINT + f"/maintenanceReq/200-000000")
            data = put_get_maintenance_request_response.json()['result']['SCHEDULED']['maintenance_items'][0]
            for k, v in put_maintenance_request_payload.items():
                if data[k] == v:
                    continue
                else:
                    print(k, v, "not a match")
            if put_get_maintenance_request_response.status_code == 200:
                response['APIs running successfully'].append('GET after PUT Maintenance Requests')
            else:
                response['APIs failing'].append('GET after PUT Maintenance Requests')
            response['No of APIs tested'] += 1

            # -------- test post maintenance quotes --------
            print("\nIn test POST Maintenance Quotes")
            post_maintenance_quotes_payload = {
                    'quote_maintenance_request_id': f'{maintenance_request_uid}', 
                    'quote_pm_notes': 'Vents',
                    'quote_business_id': '600-000000'
                }
            post_maintenance_quotes_response = requests.post(ENDPOINT + "/maintenanceQuotes", data = post_maintenance_quotes_payload)
            maintenance_quote_uid = post_maintenance_quotes_response.json()['maintenance_quote_uid']
            if post_maintenance_quotes_response.status_code == 200:
                response['APIs running successfully'].append('POST Maintenance Quotes')
            else:
                response['APIs failing'].append('POST Maintenance Quotes')
            response['No of APIs tested'] += 1

            # -------- test post get maintenance quotes --------
            print("\nIn test GET after POST Maintenance Quotes")
            post_get_maintenance_quotes_response = requests.get(ENDPOINT + f"/maintenanceQuotes/600-000000")
            data = post_get_maintenance_quotes_response.json()['maintenanceQuotes']['result'][0]
            for k, v in post_maintenance_quotes_payload.items():
                if data[k] == v:
                    continue
                else:
                    print(k, v, "not a match")
            if post_get_maintenance_quotes_response.status_code == 200:
                response['APIs running successfully'].append('GET after POST Maintenance Quotes')
            else:
                response['APIs failing'].append('GET after POST Maintenance Quotes')
            response['No of APIs tested'] += 1

            # -------- test put maintenance quotes --------
            print("\nIn test PUT Maintenance Quotes")
            put_maintenance_quotes_payload = {
                    'maintenance_quote_uid': f'{maintenance_quote_uid}',
                    'quote_maintenance_request_id': f'{maintenance_request_uid}',
                    'quote_business_id': '600-000000',
                    'quote_services_expenses': '{"per Hour Charge":"10","event_type":5,"service_name":"Labor","parts":[{"part":"250","quantity":"1","cost":"250"}],"labor":[{"description":"","hours":5,"rate":"10"}],"total_estimate":50}',
                    'quote_notes': 'vents',
                    'quote_status': 'SENT',
                    'quote_event_type': '5 Hour Job',
                    'quote_total_estimate': '300',
                    'quote_created_date': '2000-04-23 00:00:00',
                    'quote_earliest_available_date': '12-12-2023',
                    'quote_earliest_available_date': '00:00:00'
                }
            put_maintenance_quotes_response = requests.put(ENDPOINT + "/maintenanceQuotes", data = put_maintenance_quotes_payload)
            if put_maintenance_quotes_response.status_code == 200:
                response['APIs running successfully'].append('PUT Maintenance Quotes')
            else:
                response['APIs failing'].append('PUT Maintenance Quotes')
            response['No of APIs tested'] += 1

            # -------- test post get maintenance quotes --------
            print("\nIn test GET after PUT Maintenance Quotes")
            put_get_maintenance_quotes_response = requests.get(ENDPOINT + f"/maintenanceQuotes/600-000000")
            data = put_get_maintenance_quotes_response.json()['maintenanceQuotes']['result'][0]
            for k, v in put_maintenance_quotes_payload.items():
                if k == 'quote_services_expenses':
                    continue
                if data[k] == v:
                    continue
                else:
                    print(k, v, "not a match")
            if put_get_maintenance_quotes_response.status_code == 200:
                response['APIs running successfully'].append('GET after PUT Maintenance Quotes')
            else:
                response['APIs failing'].append('GET after PUT Maintenance Quotes')
            response['No of APIs tested'] += 1

            # -------- delete data from Maintenance Requests and Maintenance Quotes --------
            print("\nIn delete data from Maintenance Requests and Maintenance Quotes")
            print(f"Deleting {maintenance_request_uid} from Maintenance Requests and {maintenance_quote_uid} from Maintenance Quotes")
            with connect() as db:
                delQuery_maintenance_req = ("""
                                DELETE space.maintenanceRequests
                                FROM space.maintenanceRequests
                                WHERE maintenance_request_uid = \'""" + maintenance_request_uid + """\';
                            """)
                maintenance_req_response = db.delete(delQuery_maintenance_req)

                delQuery_maintenance_quotes = ("""
                                DELETE space.maintenanceQuotes
                                FROM space.maintenanceQuotes
                                WHERE maintenance_quote_uid = \'""" + maintenance_quote_uid + """\';
                            """)
                maintenance_quotes_response = db.delete(delQuery_maintenance_quotes)

            
            # ------------------------- Properties ------------------------------

            # -------- test post properties --------
            print("\nIn test POST Properties")
            post_properties_payload = {"property_latitude":37.2367236,
                        "property_longitude":-121.8876474,
                        "property_owner_id":"110-000000",
                        "property_active_date":"08-10-2024",
                        "property_address":"123 Test APT",
                        "property_unit":"2",
                        "property_city":"San Jose",
                        "property_state":"CA",
                        "property_zip":"95120",
                        "property_type":"Single Family",
                        "property_num_beds":4,
                        "property_num_baths":3,
                        "property_value":0,
                        "property_area":1450,
                        "property_listed":'1',
                        "property_notes":"Dot Court",
                        "appliances":["050-000000"],
                    }
            post_properties_response = requests.post(ENDPOINT + "/properties", data=post_properties_payload)
            property_uid = post_properties_response.json()['property_UID']
            if post_properties_response.status_code == 200:
                response['APIs running successfully'].append('POST Properties')
            else:
                response['APIs failing'].append('POST Properties')
            response['No of APIs tested'] += 1

            # -------- test get after post properties --------
            print("\nIn test GET after POST Properties")
            post_get_properties_response = requests.get(ENDPOINT + f"/properties/{property_uid}")
            data = post_get_properties_response.json()['Property']['result'][0]
            for k, v in post_properties_payload.items():
                if k == "property_listed" or k == "appliances" or k == "property_latitude" or k == "property_longitude":
                    continue
                if data[k] != v:
                    print('\n\n', k, v, '\tNot Match')    

            if post_get_properties_response.status_code == 200:
                response['APIs running successfully'].append('GET after POST Properties')
            else:
                response['APIs failing'].append('GET after POST Properties')
            response['No of APIs tested'] += 1

            # -------- test put properties --------
            print("\nIn test PUT Properties")
            put_properties_payload = {
                "property_uid": f"{property_uid}",
                "property_address": "456 Test House",
                "property_value":1500000
            }
            put_properties_response = requests.put(ENDPOINT + "/properties", data=put_properties_payload)
            if put_properties_response.status_code == 200:
                response['APIs running successfully'].append('PUT Properties')
            else:
                response['APIs failing'].append('PUT Properties')
            response['No of APIs tested'] += 1

            # -------- test get after put properties --------
            print("\nIn GET after PUT Properties")
            put_get_properties_response = requests.get(ENDPOINT + f"/properties/{property_uid}")
            data = put_get_properties_response.json()['Property']['result'][0]
            for k, v in put_properties_payload.items():
                if data[k] != v:
                    print('\n\n', k, v, '\tNot Match')
            if put_get_properties_response.status_code == 200:
                response['APIs running successfully'].append('GET after PUT Properties')
            else:
                response['APIs failing'].append('GET after PUT Properties')
            response['No of APIs tested'] += 1

            # -------- test delete properties --------
            print("\nIn Delete Properties")
            print(f"Deleteing property with property_uid: {property_uid} and property_owner_id: 110-000000")
            delete_properties_payload = {
                "property_owner_id": "110-000000",
                "property_id": f"{property_uid}"
            }
            headers = {
                'Content-Type': 'application/json'
            }            
            delete_properties_response = requests.delete(ENDPOINT + "/properties", data=json.dumps(delete_properties_payload), headers=headers)
            if delete_properties_response.status_code == 200:
                response['APIs running successfully'].append('DELETE Properties')
            else:
                response['APIs failing'].append('DELETE Properties')
            response['No of APIs tested'] += 1


            # ------------------------- Contracts ------------------------------

            # -------- test post contracts --------
            print("\nIn POST Contract")
            post_contract_payload = {
                "contract_property_ids": '["200-000000"]',
                "contract_business_id": "600-000000",
                "contract_start_date": "11-01-2024",
                "contract_status": "NEW"
            }
            post_contract_response = requests.post(ENDPOINT + "/contracts", data=post_contract_payload)
            contract_uid = post_contract_response.json()['contract_UID']
            if post_contract_response.status_code == 200:
                response['APIs running successfully'].append('POST Contracts')
            else:
                response['APIs failing'].append('POST Contracts')
            response['No of APIs tested'] += 1

            # -------- test get after contracts --------
            print("\nIn GET after POST Contract")
            post_get_contract_response = requests.get(ENDPOINT + "/contracts/600-000000")
            data = post_get_contract_response.json()['result'][0]
            if data['contract_uid'] == contract_uid:
                print("Not a match")
            if post_get_contract_response.status_code == 200:
                response['APIs running successfully'].append('GET after POST Contracts')
            else:
                response['APIs failing'].append('GET after POST Contracts')
            response['No of APIs tested'] += 1

            # -------- test put contracts --------
            print("\nIn PUT Contract")
            put_contract_payload = {
                "contract_uid": f"{contract_uid}",
                "contract_status": "ACTIVE"
            }
            put_contract_response = requests.put(ENDPOINT + "/contracts", data=put_contract_payload)
            if put_contract_response.status_code == 200:
                response['APIs running successfully'].append('PUT Contracts')
            else:
                response['APIs failing'].append('PUT Contracts')
            response['No of APIs tested'] += 1

            # -------- test get after put contracts --------
            print("\nIn GET after PUT Contract")
            put_get_contract_response = requests.get(ENDPOINT + "/contracts/600-000000")
            data = put_get_contract_response.json()['result'][0]
            if data['contract_uid'] == contract_uid:
                print("Not a match")
            if put_get_contract_response.status_code == 200:
                response['APIs running successfully'].append('PUT Contracts')
            else:
                response['APIs failing'].append('PUT Contracts')
            response['No of APIs tested'] += 1

            # -------- test delete contracts --------
            print("\nIn DELETE Contract")
            print(f"Deleting {contract_uid} from Contract Table")
            with connect() as db:
                delQuery_contracts = ("""
                                DELETE FROM space.contracts
                                WHERE contract_uid = \'""" + contract_uid + """\';
                            """)
                contract_response = db.delete(delQuery_contracts)
            

            # ------------------------- Leases ------------------------------
            
            # -------- test post lease application --------
            print("\nIn test POST Lease Application")
            post_lease_application_payload = {
                "lease_property_id":"200-000000",
                "lease_start":"01-31-2024",
                "lease_end":"01-30-2025",
                "lease_application_date":"06-27-2024",
                "tenant_uid":"350-000000",
                "lease_status":"NEW"
            }
            post_lease_application_response = requests.post(ENDPOINT + "/leaseApplication", data=post_lease_application_payload)
            if (post_lease_application_response.status_code == 200):
                response['APIs running successfully'].append('POST Lease Application')
            else:
                response['APIs failing'].append('POST Lease Application')
            response['No of APIs tested'] += 1

            # -------- get lease uid --------
            print("\nIn get lease_uid")
            get_lease_uid_response = requests.get(ENDPOINT + "/leaseApplication/350-000000/200-000000")
            lease_uid = get_lease_uid_response.json()
            print("lease_uid", lease_uid)

            # -------- test get after post lease details --------
            print("\nIn test GET after POST Lease Application")
            post_get_lease_application_response = requests.get(ENDPOINT + "/leaseDetails/350-000000")
            # data = post_get_lease_application_response.json()['Lease_Details']['result'][0]
            # if data['lease_status'] != "NEW":
            #     print('Not Match')
            if (post_get_lease_application_response.status_code == 200):
                response['APIs running successfully'].append('GET after POST Lease Application')
            else:
                response['APIs failing'].append('GET after POST Lease Application')
            response['No of APIs tested'] += 1

            # -------- test put lease application --------
            print("\nIn test PUT Lease Application")
            put_lease_application_payload = {
                "lease_uid":f"{lease_uid}",
                "lease_status":"PROCESSING"
            }
            put_lease_application_response = requests.put(ENDPOINT + "/leaseApplication", data=put_lease_application_payload)
            if (put_lease_application_response.status_code == 200):
                response['APIs running successfully'].append('PUT Lease Application')
            else:
                response['APIs failing'].append('PUT Lease Application')
            response['No of APIs tested'] += 1

            # -------- test get after put lease details --------
            print("\nIn test GET after PUT Lease Application")
            put_get_lease_application_response = requests.get(ENDPOINT + "/leaseDetails/350-000000")
            # data = put_get_lease_application_response.json()['Lease_Details']['result'][0]
            # if data['lease_status'] != "PROCESSING":
            #     print('Not Match')
            if (put_get_lease_application_response.status_code == 200):
                response['APIs running successfully'].append('GET after PUT Lease Application')
            else:
                response['APIs failing'].append('GET after PUT Lease Application')
            response['No of APIs tested'] += 1

            # -------- test delete lease --------
            print("\nIn DELETE Lease")
            print(f"Deleting {lease_uid} from Lease Table & {lease_uid} from Lease_tenant")
            with connect() as db:
                delQuery_leases = ("""
                                DELETE FROM space.leases
                                WHERE lease_uid = \'""" + lease_uid + """\';
                            """)
                delQuery_lease_tenant = ("""
                                DELETE FROM space.lease_tenant
                                WHERE lt_lease_id = \'""" + lease_uid + """\';
                            """)
                leases_response = db.delete(delQuery_leases)
                lease_tenant_response = db.delete(delQuery_lease_tenant)


            # ------------------------- Payment Method ------------------------------

            # -------- test POST Payment Method --------
            print("\nIn POST Payment Method")
            post_payment_method_payload = {
                "paymentMethod_profile_id": "110-000000",
                "paymentMethod_type":"zelle",
                "paymentMethod_name":"test123",
                "paymentMethod_status":"Active"
            } 
            post_payment_method_response = requests.post(ENDPOINT + "/paymentMethod", data=json.dumps(post_payment_method_payload), headers=headers)
            if (post_payment_method_response.status_code == 200):
                response['APIs running successfully'].append('POST Payment Method')
            else:
                response['APIs failing'].append('POST Payment Method')
            response['No of APIs tested'] += 1

            # -------- test GET Payment Method UID --------
            print("\nIn GET Payment Method UID")
            get_payment_method_uid = requests.get(ENDPOINT + "/paymentMethod/110-000000")
            payment_method_uid = get_payment_method_uid.json()['result'][0]['paymentMethod_uid']
            if (get_payment_method_uid.status_code == 200):
                response['APIs running successfully'].append('GET Payment Method UID')
            else:
                response['APIs failing'].append('GET Payment Method UID')
            response['No of APIs tested'] += 1

            # -------- test PUT Payment Method --------
            global put_payment_method_payload
            put_payment_method_payload = {
                "paymentMethod_uid": f"{payment_method_uid}",
                "paymentMethod_status":"Inactive"
            } 
            put_payment_method_response = requests.put(ENDPOINT + "/paymentMethod", data=json.dumps(put_payment_method_payload), headers=headers)
            if (put_payment_method_response.status_code == 200):
                response['APIs running successfully'].append('PUT Payment Method')
            else:
                response['APIs failing'].append('PUT Payment Method')
            response['No of APIs tested'] += 1

            # -------- test DELETE Payment Method --------
            print("\nIn DELETE Payment Method")
            delete_payment_method_response = requests.delete(ENDPOINT + f"/paymentMethod/110-000000/{payment_method_uid}")
            if (delete_payment_method_response.status_code == 200):
                response['APIs running successfully'].append('DELETE Payment Method')
            else:
                response['APIs failing'].append('DELETE Payment Method')
            response['No of APIs tested'] += 1


            # ------------------------- Add Purchases ------------------------------

            # -------- test POST Add Purchases --------
            print("\nIn POST add purchase")
            post_add_purchase_payload = {
                "pur_property_id": "200-000000",
                "purchase_type": "Rent",
                "pur_description": "Test Rent",
                "purchase_date": "11-07-2024",
                "pur_due_date": "11-11-2024",
                "pur_amount_due": 10.00,
                "pur_late_fee": "0",
                "pur_perDay_late_fee": "0",
                "purchase_status": "UNPAID",
                "pur_receiver": "600-000000",
                "pur_initiator": "600-000000",
                "pur_payer": "350-000000"
            }
            post_add_purchase_response = requests.post(ENDPOINT + "/addPurchase", data=post_add_purchase_payload)
            purchase_uid = post_add_purchase_response.json()['purchase_UID']
            if (post_add_purchase_response.status_code == 200):
                response['APIs running successfully'].append('POST Add Purchases')
            else:
                response['APIs failing'].append('POST Add Purchases')
            response['No of APIs tested'] += 1

            # -------- test PUT Add Purchases --------
            print("\nIn PUT add purchase")
            put_add_purchase_payload = {
                "purchase_uid": f"{purchase_uid}",
                "pur_late_fee": "10"
            }
            put_add_purchase_response = requests.put(ENDPOINT + "/addPurchase", data=put_add_purchase_payload)
            if (put_add_purchase_response.status_code == 200):
                response['APIs running successfully'].append('PUT Add Purchases')
            else:
                response['APIs failing'].append('PUT Add Purchases')
            response['No of APIs tested'] += 1


            # ------------------------- Payments ------------------------------

            # -------- test POST New Payments --------
            print("\nIn POST New Payments")
            post_payment_payload = {
                "pay_purchase_id": [
                    {
                        "purchase_uid": f"{purchase_uid}",
                        "pur_amount_due": "10.00"
                    }
                ],
                "pay_fee": 0,
                "pay_total": 10,
                "payment_notes": "Test Payment",
                "pay_charge_id": "stripe transaction key",
                "payment_type": "zelle",
                "payment_verify": "Unverified",
                "paid_by": "350-000000",
                "payment_intent": "pi_1testaccountpayment",
                "payment_method": "pm_1testaccountpayment"
            }
            post_payment_response = requests.post(ENDPOINT + "/makePayment", data=json.dumps(post_payment_payload), headers=headers)
            if (post_payment_response.status_code == 200):
                response['APIs running successfully'].append('POST New Payments')
            else:
                response['APIs failing'].append('POST New Payments')
            response['No of APIs tested'] += 1

            # -------- test DELETE Add purchases and New Payments --------
            print("\nIn DELETE add purchase")
            print(f"\nDeleting purchase_uid: {purchase_uid} from Purchases Table and payments with same purchase_uid from Payments Table")
            with connect() as db:
                delQuery_add_purchase = ("""
                                DELETE FROM space.purchases
                                WHERE purchase_uid = \'""" + purchase_uid + """\';
                            """)
                del_add_purchase_response = db.delete(delQuery_add_purchase)

                delQuery_payment = ("""
                        DELETE FROM space.payments
                        WHERE pay_purchase_id = \'""" + purchase_uid + """\' AND paid_by = '350-000000'
                    """)

                del_payment_response = db.delete(delQuery_payment)
            

            # ------------------------- Dashboard ------------------------------

            # -------- test GET Dashboard --------
            print("\nIn GET Dashboard")
            business_response = requests.get(ENDPOINT + "/dashboard/600-000000")
            owner_response = requests.get(ENDPOINT + "/dashboard/110-000000")
            tenant_response = requests.get(ENDPOINT + "/dashboard/350-000000")
            if (business_response.status_code == 200 and owner_response.status_code == 200 and tenant_response.status_code == 200):
                response['APIs running successfully'].append('GET Dashboard')
            else:
                response['APIs failing'].append('GET Dashboard')
            response['No of APIs tested'] += 1


            # ------------------------- Profiles ------------------------------

            # -------- test POST Profile --------
            print("\nIn test POST Profile")
            post_owner_profile_payload = {
                "owner_user_id": "100-000000",
                "owner_first_name": "Test",
                "owner_last_name": "Owner Account",
                "owner_phone_number": "(000) 000-0000",
                "owner_email": "test@gmail.com"
            }
            post_owner_profile_response = requests.post(ENDPOINT + "/profile", data=post_owner_profile_payload)
            owner_uid = post_owner_profile_response.json()["owner_uid"]
            
            post_business_profile_payload = {
                "business_user_id": "100-000000",
                "business_type": "Management",
                "business_name": "Test Business Account",
                "business_email": "test@gmail.com",
            }
            post_business_profile_response = requests.post(ENDPOINT + "/profile", data=post_business_profile_payload)
            business_uid = post_business_profile_response.json()["business_uid"]
            employee_uid = post_business_profile_response.json()["employee_uid"]

            post_tenant_profile_payload = {
                "tenant_user_id": "100-000000",
                "tenant_first_name": "Test",
                "tenant_last_name": "Tenant Account",
                "tenant_email": "test@gmail.com",
                "tenant_phone_number": "(000) 000-0000"
            }
            post_tenant_profile_response = requests.post(ENDPOINT + "/profile", data=post_tenant_profile_payload)
            tenant_uid = post_tenant_profile_response.json()["tenant_uid"]

            if (post_owner_profile_response.status_code == 200 and post_business_profile_response.status_code == 200 and post_tenant_profile_response.status_code == 200):
                response['APIs running successfully'].append('POST Profile')
            else:
                response['APIs failing'].append('POST Profile')
            response['No of APIs tested'] += 1

            # -------- test GET after POST Profile --------
            print("\nIn GET after POST Profile")
            post_get_owner_profile_response = requests.get(ENDPOINT + f"/profile/{owner_uid}")
            data = post_get_owner_profile_response.json()['profile']['result'][0]
            if data["owner_first_name"] != "Test":
                print("Not Match")

            post_get_business_profile_response = requests.get(ENDPOINT + f"/profile/{business_uid}")
            data = post_get_business_profile_response.json()['profile']['result'][0]
            if data["business_type"] != "Management":
                print("Not Match")

            post_get_tenant_profile_response = requests.get(ENDPOINT + f"/profile/{tenant_uid}")
            data = post_get_tenant_profile_response.json()['profile']['result'][0]
            if data["tenant_first_name"] != "Test":
                print("Not Match")

            if (post_get_owner_profile_response.status_code == 200 and post_get_business_profile_response.status_code == 200 and post_get_tenant_profile_response.status_code == 200):
                response['APIs running successfully'].append('GET after POST Profile')
            else:
                response['APIs failing'].append('GET after POST Profile')
            response['No of APIs tested'] += 1

            # -------- test PUT Profile --------
            print("\nIn test PUT Profile")
            put_owner_profile_payload = {
                "owner_uid": f"{owner_uid}",
                "owner_first_name": "Test Owner",
            }
            put_owner_profile_response = requests.put(ENDPOINT + "/profile", data=put_owner_profile_payload)

            put_business_profile_payload = {
                "business_uid": f"{business_uid}",
                "business_type": "Maintenance",
            }
            put_business_profile_response = requests.put(ENDPOINT + "/profile", data=put_business_profile_payload)

            put_tenant_profile_payload = {
                "tenant_uid": f"{tenant_uid}",
                "tenant_first_name": "Test Tenant",
            }
            put_tenant_profile_response = requests.put(ENDPOINT + "/profile", data=put_tenant_profile_payload)
            
            if (put_owner_profile_response.status_code == 200 and put_business_profile_response.status_code == 200 and put_tenant_profile_response.status_code == 200):
                response['APIs running successfully'].append('PUT Profile')
            else:
                response['APIs failing'].append('PUT Profile')
            response['No of APIs tested'] += 1

            # -------- test GET after PUT Profile --------
            print("\nIn test GET after PUT Profile")
            put_get_owner_profile_response = requests.get(ENDPOINT + f"/profile/{owner_uid}")
            data = put_get_owner_profile_response.json()['profile']['result'][0]
            if data["owner_first_name"] != "Test Owner":
                print("Not Match")

            put_get_business_profile_response = requests.get(ENDPOINT + f"/profile/{business_uid}")
            data = put_get_business_profile_response.json()['profile']['result'][0]
            if data["business_type"] != "Maintenance":
                print("Not Match")

            put_get_tenant_profile_response = requests.get(ENDPOINT + f"/profile/{tenant_uid}")
            data = put_get_tenant_profile_response.json()['profile']['result'][0]
            if data["tenant_first_name"] != "Test Tenant":
                print("Not Match")

            if (put_get_owner_profile_response.status_code == 200 and put_get_business_profile_response.status_code == 200 and put_get_tenant_profile_response.status_code == 200):
                response['APIs running successfully'].append('GET after PUT Profile')
            else:
                response['APIs failing'].append('GET after PUT Profile')
            response['No of APIs tested'] += 1

            # -------- test DELETE Profile --------
            print("\nIn DELETE Profile")
            print(f"Deleting {owner_uid} from Owner Table, {business_uid} from Business Table, {employee_uid} from Employee Table & {tenant_uid} from Tenant Table")
            with connect() as db:
                delQuery_owner = ("""
                                DELETE FROM space.ownerProfileInfo
                                WHERE owner_uid = \'""" + owner_uid + """\';
                            """)
                delQuery_business = ("""
                                DELETE FROM space.businessProfileInfo
                                WHERE business_uid = \'""" + business_uid + """\';
                            """)
                delQuery_employee = ("""
                                DELETE FROM space.employees
                                WHERE employee_uid = \'""" + employee_uid + """\';
                            """)
                delQuery_tenant = ("""
                                DELETE FROM space.tenantProfileInfo
                                WHERE tenant_uid = \'""" + tenant_uid + """\';
                            """)
                del_owner_profile_response = db.delete(delQuery_owner)
                del_business_profile_response = db.delete(delQuery_business)
                del_employee_profile_response = db.delete(delQuery_employee)
                del_tenant_profile_response = db.delete(delQuery_tenant)
            

            # ------------------------- Add Expense / Add Revenue ------------------------------

            # -------- test POST add expense --------
            post_add_expense_payload = {
                "pur_property_id":"200-000000",
                "purchase_type":"Rent",
                "pur_cf_type":"expense",
                "purchase_date":"2024-11-11",
                "pur_due_date":"2024-12-10",
                "pur_amount_due":1999,
                "purchase_status":"UNPAID",
                "pur_notes":"This is just a test note",
                "pur_description":"Test Description",
                "pur_receiver":"600-000000",
                "pur_initiator":"600-000000",
                "pur_payer":"350-000000"
            }
            post_add_expense_response = requests.post(ENDPOINT + "/addExpense", data=json.dumps(post_add_expense_payload), headers=headers)
            expense_uid = post_add_expense_response.json()['Purchases_UID']
            if (post_add_expense_response.status_code == 200):
                response['APIs running successfully'].append('POST Add Expense')
            else:
                response['APIs failing'].append('POST Add Expense')
            response['No of APIs tested'] += 1

            # -------- test PUT add expense --------
            print("\nIn PUT add expense")
            put_add_purchase_payload = {
                "purchase_uid": f"{expense_uid}",
                "pur_amount_due": 999
            }
            put_add_purchase_response = requests.put(ENDPOINT + "/addExpense", data=put_add_purchase_payload)
            if (put_add_purchase_response.status_code == 200):
                response['APIs running successfully'].append('PUT Add Expense')
            else:
                response['APIs failing'].append('PUT Add Expense')
            response['No of APIs tested'] += 1

            # -------- test POST add revenue --------
            print("\nIn POST Add Revenue")
            post_add_revenue_payload = {
                "pur_property_id":"200-000000",
                "purchase_type":"Rent",
                "pur_cf_type":"revenue",
                "purchase_date":"2024-11-11",
                "pur_due_date":"2024-12-10",
                "pur_amount_due":1999,
                "purchase_status":"UNPAID",
                "pur_notes":"This is just a test note",
                "pur_description":"Test Description",
                "pur_receiver":"600-000000",
                "pur_initiator":"600-000000",
                "pur_payer":"350-000000"
            }
            post_add_revenue_response = requests.post(ENDPOINT + "/addRevenue", data=json.dumps(post_add_revenue_payload), headers=headers)
            revenue_uid = post_add_revenue_response.json()['Purchases_UID']
            if (post_add_revenue_response.status_code == 200):
                response['APIs running successfully'].append('POST Add Revenue')
            else:
                response['APIs failing'].append('POST Add Revenue')
            response['No of APIs tested'] += 1

            # -------- test PUT add revenue --------
            print("\nIn PUT add revenue")
            put_add_revenue_payload = {
                "purchase_uid": f"{revenue_uid}",
                "pur_amount_due": 999
            }
            put_add_revenue_response = requests.put(ENDPOINT + "/addRevenue", data=put_add_revenue_payload)
            if (put_add_revenue_response.status_code == 200):
                response['APIs running successfully'].append('PUT Add Revenue')
            else:
                response['APIs failing'].append('PUT Add Revenue')
            response['No of APIs tested'] += 1

            # -------- test DELETE add expense / add revenue --------
            print("\nIn DELETE add expense / add revenue")
            print(f"\nDeleting purchase_uid: {expense_uid} from Purchases Table (for expense) and purchase_uid: {revenue_uid} from Purchases Table (for revenue)")
            with connect() as db:
                delQuery_add_expense = ("""
                                DELETE FROM space.purchases
                                WHERE purchase_uid = \'""" + expense_uid + """\';
                            """)
                del_add_expense_response = db.delete(delQuery_add_expense)
                
                delQuery_add_revenue = ("""
                                DELETE FROM space.purchases
                                WHERE purchase_uid = \'""" + revenue_uid + """\';
                            """)
                del_add_revenue_response = db.delete(delQuery_add_revenue)
            

            # ------------------------- Cashflow Transaction ------------------------------

            # -------- test GET Cashflow Transaction --------
            print("\nIn GET Cashflow Transaction")
            get_cashflow_response = requests.get(ENDPOINT + "/cashflowTransactions/600-000000/all")
            if (get_cashflow_response.status_code == 200):
                response['APIs running successfully'].append('GET Cashflow Transaction')
            else:
                response['APIs failing'].append('GET Cashflow Transaction')
            response['No of APIs tested'] += 1


            # ------------------------- Payment Verification ------------------------------

            # -------- test GET Payment Verification --------
            print("\nIn GET Payment Verification")
            get_payment_verification_response = requests.get(ENDPOINT + "/paymentVerification/600-000000")
            if (get_payment_verification_response.status_code == 200):
                response['APIs running successfully'].append('GET Payment Verification')
            else:
                response['APIs failing'].append('GET Payment Verification')
            response['No of APIs tested'] += 1


            # ------------------------- Rents / Rent Deatils ------------------------------

            # -------- test GET Rents --------
            get_rents_response = requests.get(ENDPOINT + "/rents/110-000000")
            if (get_rents_response.status_code == 200):
                response['APIs running successfully'].append('GET Rents')
            else:
                response['APIs failing'].append('GET Rents')
            response['No of APIs tested'] += 1

            # -------- test GET Rent Details --------
            get_rent_details_response = requests.get(ENDPOINT + "/rentDetails/110-000000")
            if (get_rent_details_response.status_code == 200):
                response['APIs running successfully'].append('GET Rent Details')
            else:
                response['APIs failing'].append('GET Rent Details')
            response['No of APIs tested'] += 1


            # ------------------------- Appliances ------------------------------
            
            # -------- test POST Appliance --------
            print("\nIn POST Appliances")
            post_appliance_payload= {
                "appliance_property_id":"200-000000",
                "appliance_type":"050-000023",
                "appliance_desc":"Test Appliance Description"
            }
            post_appliance_response = requests.post(ENDPOINT + "/appliances", data=post_appliance_payload)
            appliance_uid = post_appliance_response.json()['appliance_UID']
            if (post_appliance_response.status_code == 200):
                response['APIs running successfully'].append('POST Appliances')
            else:
                response['APIs failing'].append('POST Appliances')
            response['No of APIs tested'] += 1

            # -------- test GET after POST Appliance --------
            print("\nIn GET after POST Appliances")          
            get_post_appliance_response = requests.get(ENDPOINT + "/appliances/200-000000")
            data = get_post_appliance_response.json()['result'][0]
            if data["appliance_desc"] !=  "Test Appliance Description":
                print("Not Match")
            if (get_post_appliance_response.status_code == 200):
                response['APIs running successfully'].append('GET after POST Appliances')
            else:
                response['APIs failing'].append('GET after POST Appliances')
            response['No of APIs tested'] += 1

            # -------- test PUT Appliance --------
            print("\nIn PUT Appliances")
            put_appliance_payload = {
                "appliance_uid":f"{appliance_uid}",
                "appliance_desc":"Test Appliance Description 1"
            }
            put_appliance_response = requests.put(ENDPOINT + "/appliances", data=put_appliance_payload)
            if (put_appliance_response.status_code == 200):
                response['APIs running successfully'].append('PUT Appliances')
            else:
                response['APIs failing'].append('PUT Appliances')
            response['No of APIs tested'] += 1

            # -------- test GET after PUT Appliance --------
            print("\nIn GET after PUT Appliances")            
            get_put_appliance_response = requests.get(ENDPOINT + "/appliances/200-000000")
            data = get_put_appliance_response.json()['result'][0]
            if data["appliance_desc"] !=  "Test Appliance Description 1":
                print("Not Match")
            if (get_put_appliance_response.status_code == 200):
                response['APIs running successfully'].append('GET after PUT Appliances')
            else:
                response['APIs failing'].append('GET after PUT Appliances')
            response['No of APIs tested'] += 1

            # -------- test DELETE Appliance --------
            print("\nIn DELETE Appliances")
            print(f"Deleting appliance_uid: {appliance_uid} form Appliances Table")
            delete_appliance_response = requests.delete(ENDPOINT + f"/appliances/{appliance_uid}")
            if (delete_appliance_response.status_code == 200):
                response['APIs running successfully'].append('DELETE Appliances')
            else:
                response['APIs failing'].append('DELETE Appliances')
            response['No of APIs tested'] += 1


            # ------------------------- Employee / Employee Verification ------------------------------
            
            # -------- test POST Employee --------
            print("\nIn POST Employee")
            post_employee_payload = {
                "employee_user_id":"100-000000",
                "employee_business_id":"600-000000",
                "employee_first_name":"Test",
                "employee_last_name":"Employee"
            }
            post_employee_response = requests.post(ENDPOINT + "/employee", data=post_employee_payload)
            employee_uid = post_employee_response.json()['employee_uid']
            if (post_employee_response.status_code == 200):
                response['APIs running successfully'].append('POST Employee')
            else:
                response['APIs failing'].append('POST Employee')
            response['No of APIs tested'] += 1

            # -------- test GET after POST Employee --------
            print("\nIn GET after POST Employee")
            get_post_employee_response = requests.get(ENDPOINT + f"/employee/{employee_uid}")
            data = get_post_employee_response.json()['employee']['result'][0]
            if data["employee_first_name"] != "Test":
                print("Not Match")
            if (get_post_employee_response.status_code == 200):
                response['APIs running successfully'].append('GET after POST Employee')
            else:
                response['APIs failing'].append('GET after POST Employee')
            response['No of APIs tested'] += 1

            # -------- test PUT Employee Verification --------
            print("\nIn PUT Employee Verification")
            put_employee_verification_payload = [{
                "employee_uid":f"{employee_uid}",
                "employee_first_name":"Test Account"
            }]   
            put_employee_verification_response = requests.put(ENDPOINT + "/employeeVerification", data=json.dumps(put_employee_verification_payload), headers=headers)
            if (put_employee_verification_response.status_code == 200):
                response['APIs running successfully'].append('PUT Employee Verification')
            else:
                response['APIs failing'].append('PUT Employee Verification')
            response['No of APIs tested'] += 1

            # -------- test GET after PUT Employee --------
            print("\nIn GET after PUT Employee")
            get_put_employee_response = requests.get(ENDPOINT + f"/employee/{employee_uid}")
            data = get_put_employee_response.json()['employee']['result'][0]
            if data["employee_first_name"] != "Test Account":
                print("Not Match")
            if (get_put_employee_response.status_code == 200):
                response['APIs running successfully'].append('GET after PUT Employee Verification')
            else:
                response['APIs failing'].append('GET after PUT Employee Verification')
            response['No of APIs tested'] += 1

            # -------- test DELETE Employee --------
            print("\nIn DELETE Employee")
            print(f"\nDeleting employee_uid: {employee_uid} from Employees Table")
            with connect() as db:
                delQuery_employee = ("""
                                DELETE FROM space.employees
                                WHERE employee_uid = \'""" + employee_uid + """\';
                            """)
                del_employee_response = db.delete(delQuery_employee)
            

            # ------------------------- Contacts ------------------------------
            
            # -------- test GET Contacts --------
            print("\nIn GET Contacts")
            get_contacts_business_response = requests.get(ENDPOINT + "/contacts/600-000000")
            get_contacts_owner_response = requests.get(ENDPOINT + "/contacts/110-000000")
            get_contacts_tenant_response = requests.get(ENDPOINT + "/contacts/350-000000")
            if (get_contacts_business_response.status_code == 200 and get_contacts_owner_response.status_code == 200 and get_contacts_tenant_response.status_code == 200):
                response['APIs running successfully'].append('GET Contacts')
            else:
                response['APIs failing'].append('GET Contacts')
            response['No of APIs tested'] += 1


            # ------------------------- SearchManager ------------------------------
            
            # -------- test GET SearchManager --------
            print("\nIn GET Search Manager")
            get_search_manager_response = requests.get(ENDPOINT + "/searchManager")
            if (get_search_manager_response.status_code == 200):
                response['APIs running successfully'].append('GET Search Manager')
            else:
                response['APIs failing'].append('GET Search Manager')
            response['No of APIs tested'] += 1


            # ------------------------- Utility ------------------------------
            
            # -------- test POST Utility --------
            print("\nIn POST Utility")
            post_utility_payload = {
                "property_uid": "200-000000",
                "property_utility":json.dumps({
                    "050-000000":"050-000000"
                })
            }
            post_utility_payload_response = requests.post(ENDPOINT + "/utilities", data=post_utility_payload)
            if (post_utility_payload_response.status_code == 200):
                response['APIs running successfully'].append('POST Utility')
            else:
                response['APIs failing'].append('POST Utility')
            response['No of APIs tested'] += 1

            # -------- test GET after POST Utility --------
            print("\nIn GET after POST Utility")
            post_get_utility_response = requests.get(ENDPOINT + "/utilities?utility_property_id=200-000000")
            data = post_get_utility_response.json()['result'][0]
            if data['utility_payer_id'] != "050-000000":
                print("Not Match")
            if (post_get_utility_response.status_code == 200):
                response['APIs running successfully'].append('GET after POST Utility')
            else:
                response['APIs failing'].append('GET after POST Utility')
            response['No of APIs tested'] += 1

            # -------- test PUT Utility --------
            print("\nIn PUT Utility")
            put_utility_payload = {
                "property_uid": "200-000000",
                "property_utility":json.dumps({
                    "050-000000":"050-100000"
                })
            }
            put_utility_payload_response = requests.put(ENDPOINT + "/utilities", data=put_utility_payload)
            if (put_utility_payload_response.status_code == 200):
                response['APIs running successfully'].append('PUT Utility')
            else:
                response['APIs failing'].append('PUT Utility')
            response['No of APIs tested'] += 1

            # -------- test GET after PUT Utility --------
            print("\nIn GET after PUT Utility")
            put_get_utility_response = requests.get(ENDPOINT + "/utilities?utility_property_id=200-000000")
            data = put_get_utility_response.json()['result'][0]
            if data['utility_payer_id'] != "050-100000":
                print("Not Match")
            if (put_get_utility_response.status_code == 200):
                response['APIs running successfully'].append('GET after PUT Utility')
            else:
                response['APIs failing'].append('GET after PUT Utility')
            response['No of APIs tested'] += 1

            # -------- test DELETE Utility --------
            print("\nIn DELETE Utility")
            print(f"\nDeleting utility_property_id: '200-000000', utility_type_id: '050-000000' & utility_payer_id: '050-100000' from Employees Table")
            with connect() as db:
                delQuery_property_utility = ("""
                                DELETE FROM space.property_utility
                                WHERE utility_property_id = "200-000000"
                                AND utility_type_id = "050-000000"
                                AND utility_payer_id = "050-100000";
                            """)
                del_property_utility_response = db.delete(delQuery_property_utility)


            # ------------------------- Bills ------------------------------
            
            # -------- test POST Bills --------
            post_bill_payload = {
                "bill_created_by":"600-000000",
                "bill_description":"Test Bill Description",
                "bill_amount":199.99,
                "bill_utility_type":"maintenance",
                "bill_split":"Uniform",
                "bill_property_id":'[{"property_uid":"200-000000"}]',
                "bill_maintenance_request_id":"800-000000",
                "bill_maintenance_quote_id":"900-000000",
                "bill_notes":""
            }
            post_bill_response = requests.post(ENDPOINT + "/bills", data=post_bill_payload)
            bill_uid = post_bill_response.json()['maibill_uidtenance_request_uid']
            purchase_uids = post_bill_response.json()['purchase_ids_add']
            if (post_bill_response.status_code == 200):
                response['APIs running successfully'].append('POST Bills')
            else:
                response['APIs failing'].append('POST Bills')
            response['No of APIs tested'] += 1

            # -------- test GET after POST Bills --------
            get_post_response = requests.get(ENDPOINT + f"/bills/{bill_uid}")
            data = get_post_response.json()['result'][0]
            if data['bill_description'] != "Test Bill Description":
                print("Not Match")
            if (get_post_response.status_code == 200):
                response['APIs running successfully'].append('GET after POST Bills')
            else:
                response['APIs failing'].append('GET after POST Bills')
            response['No of APIs tested'] += 1

            # -------- test PUT Bills --------
            put_bill_payload = {
                "bill_uid":f"{bill_uid}",
                "bill_description":"Test Bill Description 1"
            }
            put_bill_response = requests.put(ENDPOINT + "/bills", data=put_bill_payload)
            if (put_bill_response.status_code == 200):
                response['APIs running successfully'].append('PUT Bills')
            else:
                response['APIs failing'].append('PUT Bills')
            response['No of APIs tested'] += 1

            # -------- test GET after PUT Bills --------
            get_put_response = requests.get(ENDPOINT + f"/bills/{bill_uid}")
            data = get_put_response.json()['result'][0]
            if data['bill_description'] != "Test Bill Description 1":
                print("Not Match")
            if (get_put_response.status_code == 200):
                response['APIs running successfully'].append('GET after PUT Bills')
            else:
                response['APIs failing'].append('GET after PUT Bills')
            response['No of APIs tested'] += 1

            # -------- test DELETE Bills --------
            print("\nIn DELETE Bill")
            print(f"\nDeleting bill_uid: {bill_uid} from Bills Table and purchase_uids: {purchase_uids} from Purchases Table")
            with connect() as db:
                delQuery_bill = ("""
                                DELETE FROM space.bills
                                WHERE bill_uid = \'""" + bill_uid + """\';
                            """)
                del_bill_response = db.delete(delQuery_bill)
                for pur_id in purchase_uids:
                    delQuery_purchase = ("""
                                DELETE FROM space.purchases
                                WHERE purchase_uid = \'""" + pur_id + """\';
                            """)
                    del_purchase_response = db.delete(delQuery_purchase)


            # ------------------------- Listings ------------------------------

            # -------- test GET Listings --------
            print("\nIn GET Listings")
            get_listings_response = requests.get(ENDPOINT + "/listings/350-000000")
            if (get_listings_response.status_code == 200):
                response['APIs running successfully'].append('GET Listings')
            else:
                response['APIs failing'].append('GET Listings')
            response['No of APIs tested'] += 1


            # ------------------------- Announcements ------------------------------

            # -------- test POST Announcement --------
            print("\nIn POST Announcement")
            global post_announcement_payload
            post_announcement_payload = {
                    "announcement_title": "Test Announcement",
                    "announcement_msg": "Hi! This is a test announcement",
                    "announcement_properties": "{\"350-000000\":[\"200-000000\"]}",
                    "announcement_mode": "LEASE",
                    "announcement_receiver": "350-000000",
                    "announcement_type": ["Text","Email"]
                }
            post_announcement_response = requests.post(ENDPOINT + "/announcements/110-000000", data=json.dumps(post_announcement_payload), headers=headers)
            if (post_announcement_response.status_code == 200):
                response['APIs running successfully'].append('POST Announcement')
            else:
                response['APIs failing'].append('POST Announcement')
            response['No of APIs tested'] += 1

            # -------- test GET after POST Announcement --------
            print("\nIn GET after POST Announcement")
            post_get_announcement_response = requests.get(ENDPOINT + "/announcements/110-000000")
            data = post_get_announcement_response.json()['sent']['result'][0]
            announcement_uid = data['announcement_uid']
            if data['announcement_title'] != "Test Announcement":
                print("Not Match")
            if (post_get_announcement_response.status_code == 200):
                response['APIs running successfully'].append('GET after POST Announcement')
            else:
                response['APIs failing'].append('GET after POST Announcement')
            response['No of APIs tested'] += 1

            # -------- test PUT Announcement --------
            put_announcement_payload = {
                "announcement_uid": [f"{announcement_uid}"],
                "announcement_title": "Test Announcement 1"
            }
            put_announcement_response = requests.put(ENDPOINT + "/announcements", data=json.dumps(put_announcement_payload), headers=headers)
            if (put_announcement_response.status_code == 200):
                response['APIs running successfully'].append('PUT Announcement')
            else:
                response['APIs failing'].append('PUT Announcement')
            response['No of APIs tested'] += 1

            # -------- test GET after PUT Announcement --------
            print("\nIn GET after PUT Announcement")
            put_get_announcement_response = requests.get(ENDPOINT + "/announcements/110-000000")
            data = put_get_announcement_response.json()['sent']['result'][0]
            if data['announcement_title'] != "Test Announcement 1":
                print("Not Match")
            if (put_get_announcement_response.status_code == 200):
                response['APIs running successfully'].append('GET after PUT Announcement')
            else:
                response['APIs failing'].append('GET after PUT Announcement')
            response['No of APIs tested'] += 1

            # -------- test DELETE Announcement --------
            print("\nIn DELETE Announcement")
            print(f"\nDeleting announcement_uid: {announcement_uid} from Announcements Table")
            with connect() as db:
                delQuery_announcement = ("""
                                DELETE FROM space.announcements
                                WHERE announcement_uid = \'""" + announcement_uid + """\';
                            """)
                del_announcement_response = db.delete(delQuery_announcement)


        except:
            response["cron fail"] = {'message': f'MySpace Test API CRON Job failed for {dt}' ,'code': 500}

        try:
            print("\n\n*** Deleting temporary data from the database ***\n")
            delete_property_query = """
                                        DELETE FROM space.properties
                                        WHERE property_uid = "200-000000";
                                    """
            
            delete_property_owner_query = """
                                        DELETE FROM space.property_owner
                                        WHERE property_id = "200-000000" AND property_owner_id = "110-000000";
                                    """
            
            delete_user_query = """
                                        DELETE FROM space.users
                                        WHERE user_uid = "100-000000";
                                    """
            
            delete_business_profile_query = """
                                        DELETE FROM space.businessProfileInfo
                                        WHERE business_uid = "600-000000";
                                    """
            
            delete_owner_profile_query = """
                                        DELETE FROM space.ownerProfileInfo
                                        WHERE owner_uid = "110-000000";
                                    """
            
            delete_tenant_profile_query = """
                                        DELETE FROM space.tenantProfileInfo
                                        WHERE tenant_uid = "350-000000";
                                    """
            
            delete_purchases_query = """
                                        DELETE FROM space.purchases
                                        WHERE purchase_uid = "400-000000";
                                    """
            
            delete_maintenance_requests_query = """
                                        DELETE FROM space.maintenanceRequests
                                        WHERE maintenance_request_uid = "800-000000";
                                    """
            
            delete_maintenance_quotes_query = """
                                        DELETE FROM space.maintenanceQuotes
                                        WHERE maintenance_quote_uid = "900-000000";
                                    """
            
            delete_contracts_query = """
                                        DELETE FROM space.contracts
                                        WHERE contract_uid = "010-000000";
                                    """
            
            with connect() as db:
                delete_property_query_response = db.delete(delete_property_query)
                delete_property_owner_query_response = db.delete(delete_property_owner_query)
                delete_user_query_response = db.delete(delete_user_query)
                delete_business_profile_query_response = db.delete(delete_business_profile_query)
                delete_owner_profile_query_response = db.delete(delete_owner_profile_query)
                delete_tenant_profile_query_response = db.delete(delete_tenant_profile_query)
                delete_purchases_query_response = db.delete(delete_purchases_query)
                delete_maintenance_requests_query_response = db.delete(delete_maintenance_requests_query)
                delete_maintenance_quotes_query_response = db.delete(delete_maintenance_quotes_query)
                delete_contracts_query_response = db.delete(delete_contracts_query)

            print("\n*** Completed ***\n")
            response['delete_temporary_data'] = 'Passed'
        except:
            response['delete_temporary_data'] = 'Failed'

        return response

# ********* FUNCTION IMPLEMENTATION *********
# -------- Maintenance Requests ---------
# maintenance_request_uid = ""
# post_payload = {}
# global put_payload

# def test_post_maintenanceReq():
#     global post_payload
#     post_payload = {
#             "maintenance_property_id":"200-000000",
#             "maintenance_title":"Vents Broken",
#             "maintenance_desc":"Vents",
#             "maintenance_request_type":"Appliance",
#             "maintenance_request_created_by":"600-000000",
#             "maintenance_priority":"High",
#             "maintenance_can_reschedule":1,
#             "maintenance_assigned_business":"null",
#             "maintenance_assigned_worker":"null",
#             "maintenance_scheduled_date":"null",
#             "maintenance_scheduled_time":"null",
#             "maintenance_frequency":"One Time",
#             "maintenance_notes":"null",
#             "maintenance_request_created_date":"2024-11-13",
#             "maintenance_request_closed_date":"null",
#             "maintenance_request_adjustment_date":"null"
#         }
    
#     response = requests.post(ENDPOINT + "/maintenanceRequests", data = post_payload)
#     global maintenance_request_uid
#     maintenance_request_uid = response.json()['maintenance_request_uid']
#     assert response.status_code == 200

# def test_post_get_maintenanceReq():
#     global maintenance_request_uid
#     response = requests.get(ENDPOINT + f"/maintenanceReq/200-000000")
#     data = response.json()['result']['NEW REQUEST']['maintenance_items'][0]
#     global post_payload
#     for k, v in post_payload.items():
#         if data[k] == v:
#             continue
#         else:
#             print(k, v, "not a match")
    
#     # print(data)
#     assert response.status_code == 200

# def test_put_maintenanceReq():
#     global maintenance_request_uid
#     global put_payload
#     put_payload = {
#         "maintenance_request_uid":f"{maintenance_request_uid}","maintenance_request_status":"SCHEDULED","maintenance_scheduled_date":"11/30/2024","maintenance_scheduled_time":"10:00:00"
#     }

#     response = requests.put(ENDPOINT + "/maintenanceRequests", data = put_payload) 
#     assert response.status_code == 200

# def test_put_get_maintenanceReq():
#     global maintenance_request_uid
#     response = requests.get(ENDPOINT + f"/maintenanceReq/200-000000")

#     data = response.json()['result']['SCHEDULED']['maintenance_items'][0]
#     global put_payload
#     for k, v in put_payload.items():
#         if data[k] == v:
#             continue
#         else:
#             print(k, v, "not a match")

#     # print(data)
#     assert response.status_code == 200

# -------- Maintenance Status ---------
# def test_get_maintenanceStatus():
#     response = requests.get(ENDPOINT + "/maintenanceStatus/600-000038")
#     # print(response.json())
#     assert response.status_code == 200
# -------------------------------------



# maintenance_quote_uid = ""
# post_maintenance_quotes_payload = {}
# def test_post_maintenanceQuotes():
#     print("In Quotes")
#     global maintenance_request_uid
#     global post_maintenance_quotes_payload
#     post_maintenance_quotes_payload = {
#             'quote_maintenance_request_id': f'{maintenance_request_uid}', 
#             'quote_pm_notes': 'Vents',
#             'quote_business_id': '600-000000'
#         }

#     response = requests.post(ENDPOINT + "/maintenanceQuotes", data = post_maintenance_quotes_payload)
#     global maintenance_quote_uid
#     maintenance_quote_uid = response.json()['maintenance_quote_uid']
#     assert response.status_code == 200

# def test_post_get_maintenanceQuotes():
#     print("\n\n\nIn POST GET Maintenance\n\n\n")
#     global maintenance_quote_uid
#     response = requests.get(ENDPOINT + f"/maintenanceQuotes/600-000000")
#     # print(response.json())
#     global post_maintenance_quotes_payload
#     data = response.json()['maintenanceQuotes']['result'][0]
#     for k, v in post_maintenance_quotes_payload.items():
#         if data[k] == v:
#             continue
#         else:
#             print(k, v, "not a match")
#     assert response.status_code == 200

# put_maintenance_quotes_payload = {}
# def test_put_maintenancyQuotes():
#     global maintenance_quote_uid
#     global maintenance_request_uid
#     global put_maintenance_quotes_payload
#     put_maintenance_quotes_payload = {
#             'maintenance_quote_uid': f'{maintenance_quote_uid}',
#             'quote_maintenance_request_id': f'{maintenance_request_uid}',
#             'quote_business_id': '600-000000',
#             'quote_services_expenses': '{"per Hour Charge":"10","event_type":5,"service_name":"Labor","parts":[{"part":"250","quantity":"1","cost":"250"}],"labor":[{"description":"","hours":5,"rate":"10"}],"total_estimate":50}',
#             'quote_notes': 'vents',
#             'quote_status': 'SENT',
#             'quote_event_type': '5 Hour Job',
#             'quote_total_estimate': '300',
#             'quote_created_date': '2000-04-23 00:00:00',
#             'quote_earliest_available_date': '12-12-2023',
#             'quote_earliest_available_date': '00:00:00'
#         }

#     response = requests.put(ENDPOINT + "/maintenanceQuotes", data = put_maintenance_quotes_payload)

#     assert response.status_code == 200

# def test_put_get_maintenanceQuotes():
#     print("\n\n\nIn PUT GET Maintenance\n\n\n")
#     global maintenance_quote_uid
#     response = requests.get(ENDPOINT + f"/maintenanceQuotes/600-000000")
#     # print(response.json())
#     global put_maintenance_quotes_payload
#     data = response.json()['maintenanceQuotes']['result'][0]
#     for k, v in put_maintenance_quotes_payload.items():
#         if k == 'quote_services_expenses':
#             continue
#         if data[k] == v:
#             continue
#         else:
#             print(k, v, "not a match")
#             print("\n\n", type(data[k]))
#             print("\n\n", type(v))

#     assert response.status_code == 200

# def test_delete_maintenanceReq():
#     global maintenance_request_uid
#     global maintenance_quote_uid

#     print(f"Deleting {maintenance_request_uid} from Maintenance Requests and {maintenance_quote_uid} from Maintenance Quotes")
#     with connect() as db:
#         delQuery_maintenance_req = ("""
#                         DELETE space.maintenanceRequests
#                         FROM space.maintenanceRequests
#                         WHERE maintenance_request_uid = \'""" + maintenance_request_uid + """\';
#                     """)

#         response = db.delete(delQuery_maintenance_req)

#         delQuery_maintenance_quotes = ("""
#                         DELETE space.maintenanceQuotes
#                         FROM space.maintenanceQuotes
#                         WHERE maintenance_quote_uid = \'""" + maintenance_quote_uid + """\';
#                     """)

#         response = db.delete(delQuery_maintenance_quotes)




# -------- POST Methods --------
# property_uid = ""
# properties_payload = {}
# def test_post_properties():
#     print("\n\nIn POST Properties")
#     global properties_payload
#     properties_payload = {"property_latitude":37.2367236,
#                 "property_longitude":-121.8876474,
#                 "property_owner_id":"110-000000",
#                 "property_active_date":"08-10-2024",
#                 "property_address":"123 Test APT",
#                 "property_unit":"2",
#                 "property_city":"San Jose",
#                 "property_state":"CA",
#                 "property_zip":"95120",
#                 "property_type":"Single Family",
#                 "property_num_beds":4,
#                 "property_num_baths":3,
#                 "property_value":0,
#                 "property_area":1450,
#                 "property_listed":'1',
#                 "property_notes":"Dot Court",
#                 "appliances":["050-000000"],
#             }
    
#     response = requests.post(ENDPOINT + "/properties", data=properties_payload)
#     global property_uid
#     property_uid = response.json()['property_UID']
#     assert response.status_code == 200

# def test_get_post_properties():
#     print("\n\nIn GET after POST Properties")
#     global property_uid
#     global properties_payload

#     response = requests.get(ENDPOINT + f"/properties/{property_uid}")
#     data = response.json()['Property']['result'][0]
#     # print(data)

#     for k, v in properties_payload.items():
#         if k == "property_listed" or k == "appliances" or k == "property_latitude" or k == "property_longitude":
#             continue

#         if data[k] != v:
#             print('\n\n', k, v, '\tNot Match')    

#     assert response.status_code == 200

# put_property_payload = {}
# def test_put_properties():
#     print("\n\nIn PUT Properties")
#     global property_uid
#     global put_property_payload

#     put_property_payload = {
#         "property_uid": f"{property_uid}",
#         "property_address": "456 Test House",
#         "property_value":1500000
#     }

#     response = requests.put(ENDPOINT + "/properties", data=put_property_payload)

#     assert response.status_code == 200

# def test_get_put_properties():
#     print("\n\nIn GET after PUT Properties")
#     global property_uid
#     global put_property_payload

#     response = requests.get(ENDPOINT + f"/properties/{property_uid}")
#     data = response.json()['Property']['result'][0]

#     for k, v in put_property_payload.items():

#         if data[k] != v:
#             print('\n\n', k, v, '\tNot Match')

#     assert response.status_code == 200

# def test_delete_properties():
#     print("\n\nIn Delete Properties")
#     global property_uid
#     print(f"Deleteing property with property_uid: {property_uid} and property_owner_id: 110-000000")
    
#     payload = {
#         "property_owner_id": "110-000000",
#         "property_id": f"{property_uid}"
#     }
#     headers = {
#         'Content-Type': 'application/json'
#     }
    
#     response = requests.delete(ENDPOINT + "/properties", data=json.dumps(payload), headers=headers)
    
#     assert response.status_code == 200

# post_contract_payload = {}
# contract_uid = ""
# def test_post_contracts():
#     global post_contract_payload
#     post_contract_payload = {
#         "contract_property_ids": '["200-000000"]',
#         "contract_business_id": "600-000000",
#         "contract_start_date": "11-01-2024",
#         "contract_status": "NEW"
#     }

#     response = requests.post(ENDPOINT + "/contracts", data=post_contract_payload)
#     global contract_uid
#     contract_uid = response.json()['contract_UID']

#     assert response.status_code == 200

# def test_post_get_contracts():
#     global post_contract_payload
#     global contract_uid
#     response = requests.get(ENDPOINT + "/contracts/600-000000")

#     data = response.json()['result'][0]

#     if data['contract_uid'] == contract_uid:
#         print("Not a match")

#     assert response.status_code == 200

# put_contract_payload = {}
# def test_put_contracts():
#     global contract_uid
#     global put_contract_payload
#     put_contract_payload = {
#         "contract_uid": f"{contract_uid}",
#         "contract_status": "ACTIVE"
#     }

#     response = requests.put(ENDPOINT + "/contracts", data=put_contract_payload)

#     assert response.status_code == 200

# def test_put_get_contracts():
#     global post_contract_payload
#     response = requests.get(ENDPOINT + "/contracts/600-000000")

#     data = response.json()['result'][0]

#     if data['contract_uid'] == contract_uid:
#         print("Not a match")
    
#     assert response.status_code == 200

# def test_delete_contracts():
#     global contract_uid

#     print(f"Deleting {contract_uid} from Contract Table")
#     with connect() as db:
#         delQuery_contracts = ("""
#                         DELETE FROM space.contracts
#                         WHERE contract_uid = \'""" + contract_uid + """\';
#                     """)

#         response = db.delete(delQuery_contracts)


# post_lease_application_payload = {}
# lease_uid = ""
# def test_post_lease_application():
#     print("\nIn test POST Lease Application")
#     global post_lease_application_payload
#     post_lease_application_payload = {
#         "lease_property_id":"200-000000",
#         "lease_start":"01-31-2024",
#         "lease_end":"01-30-2025",
#         "lease_application_date":"06-27-2024",
#         "tenant_uid":"350-000000",
#         "lease_status":"NEW"
#     }

#     post_lease_application_response = requests.post(ENDPOINT + "/leaseApplication", data=post_lease_application_payload)
#     assert post_lease_application_response.status_code == 200

# def test_get_lease_uid():
#     get_lease_uid_response = requests.get(ENDPOINT + "/leaseApplication/350-000000/200-000000")
#     global lease_uid 
#     lease_uid = get_lease_uid_response.json()
#     print("lease_uid", lease_uid)

# def test_post_get_lease_details():
#     post_get_lease_application_response = requests.get(ENDPOINT + "/leaseDetails/350-000000")
#     # print(post_get_lease_application_response.json())
#     # data = post_get_lease_application_response.json()['Lease_Details']['result'][0]

#     # if data['lease_status'] != "NEW":
#     #     print('Not Match')
#     assert post_get_lease_application_response.status_code == 200

# put_lease_application_payload = {}
# def test_put_lease_application():
#     print("\nIn test PUT Lease Application")
#     global put_lease_application_payload
#     global lease_uid
#     put_lease_application_payload = {
#         "lease_uid":f"{lease_uid}",
#         "lease_status":"PROCESSING"
#     }
#     put_lease_application_response = requests.put(ENDPOINT + "/leaseApplication", data=put_lease_application_payload)
#     assert put_lease_application_response.status_code == 200

# def test_put_get_lease_details():
#     put_get_lease_application_response = requests.get(ENDPOINT + "/leaseDetails/350-000000")
#     # data = put_get_lease_application_response.json()['Lease_Details']['result'][0]

#     # if data['lease_status'] != "PROCESSING":
#     #     print('Not Match')
#     assert put_get_lease_application_response.status_code == 200

# def test_delete_lease():
#     global lease_uid

#     print(f"Deleting {lease_uid} from Lease Table & {lease_uid} from Lease_tenant")
#     with connect() as db:
#         delQuery_leases = ("""
#                         DELETE FROM space.leases
#                         WHERE lease_uid = \'""" + lease_uid + """\';
#                     """)
#         delQuery_lease_tenant = ("""
#                         DELETE FROM space.lease_tenant
#                         WHERE lt_lease_id = \'""" + lease_uid + """\';
#                     """)

#         response = db.delete(delQuery_leases)
#         response = db.delete(delQuery_lease_tenant)




# payment methods
# post_payment_method_payload = {}
# def test_post_payment_method():
#     print("\nIn POST Payment Method")
#     global post_payment_method_payload
#     post_payment_method_payload = {
#         "paymentMethod_profile_id": "110-000000",
#         "paymentMethod_type":"zelle",
#         "paymentMethod_name":"test123",
#         "paymentMethod_status":"Active"
#     }
#     headers = {
#                 'Content-Type': 'application/json'
#             }    
#     post_payment_method_response = requests.post(ENDPOINT + "/paymentMethod", data=json.dumps(post_payment_method_payload), headers=headers)
#     assert post_payment_method_response.status_code == 200

# payment_method_uid = ""
# def test_get_payment_method_uid():
#     print("\nIn GET Payment Method UID")
#     get_payment_method_uid = requests.get(ENDPOINT + "/paymentMethod/110-000000")
#     global payment_method_uid
#     payment_method_uid = get_payment_method_uid.json()['result'][0]['paymentMethod_uid']
#     assert get_payment_method_uid.status_code == 200

# put_payment_method_payload = {}
# def test_put_payment_method():
#     print("\nIn PUT Payment Method")
#     global payment_method_uid
#     global put_payment_method_payload
#     put_payment_method_payload = {
#         "paymentMethod_uid": f"{payment_method_uid}",
#         "paymentMethod_status":"Inactive"
#     }
#     headers = {
#                 'Content-Type': 'application/json'
#             }    
#     put_payment_method_response = requests.put(ENDPOINT + "/paymentMethod", data=json.dumps(put_payment_method_payload), headers=headers)

#     assert put_payment_method_response.status_code == 200

# def test_delete_payment_method():
#     print("\nIn DELETE Payment Method")
#     global payment_method_uid
#     delete_payment_method_response = requests.delete(ENDPOINT + f"/paymentMethod/110-000000/{payment_method_uid}")
#     assert delete_payment_method_response.status_code == 200


# post_add_purchase_payload = {}
# purchase_uid = ""
# def test_post_add_purchases():
#     print("\nIn POST add purchase")
#     global post_add_purchase_payload
#     post_add_purchase_payload = {
#         "pur_property_id": "200-000000",
#         "purchase_type": "Rent",
#         "pur_description": "Test Rent",
#         "purchase_date": "11-07-2024",
#         "pur_due_date": "11-11-2024",
#         "pur_amount_due": 10.00,
#         "pur_late_fee": "0",
#         "pur_perDay_late_fee": "0",
#         "purchase_status": "UNPAID",
#         "pur_receiver": "600-000000",
#         "pur_initiator": "600-000000",
#         "pur_payer": "350-000000"
#     }

#     post_add_purchase_response = requests.post(ENDPOINT + "/addPurchase", data=post_add_purchase_payload)

#     global purchase_uid
#     purchase_uid = post_add_purchase_response.json()['purchase_UID']
#     print(purchase_uid, 'purchase_UID')
#     assert post_add_purchase_response.status_code == 200

# put_add_purchase_payload = {}
# def test_put_add_purchase():
#     print("\nIn PUT add purchase")
#     global put_add_purchase_payload
#     global purchase_uid
#     put_add_purchase_payload = {
#         "purchase_uid": f"{purchase_uid}",
#         "pur_late_fee": "10"
#     }

#     put_add_purchase_response = requests.put(ENDPOINT + "/addPurchase", data=put_add_purchase_payload)

#     assert put_add_purchase_response.status_code == 200


# post_payment_payload = {}
# def test_make_payment():
#     global purchase_uid
#     global post_payment_payload
#     post_payment_payload = {
#         "pay_purchase_id": [
#             {
#                 "purchase_uid": f"{purchase_uid}",
#                 "pur_amount_due": "10.00"
#             }
#         ],
#         "pay_fee": 0,
#         "pay_total": 10,
#         "payment_notes": "Test Payment",
#         "pay_charge_id": "stripe transaction key",
#         "payment_type": "zelle",
#         "payment_verify": "Unverified",
#         "paid_by": "350-000000",
#         "payment_intent": "pi_1testaccountpayment",
#         "payment_method": "pm_1testaccountpayment"
#     }

#     headers = {
#                 'Content-Type': 'application/json'
#             } 

#     post_payment_response = requests.post(ENDPOINT + "/makePayment", data=json.dumps(post_payment_payload), headers=headers)
#     print("\n\n", post_payment_response.json(), "\n\n")
#     assert post_payment_response.status_code == 200


# def test_delete_add_purchase():
#     global purchase_uid
#     print("\nIn DELETE add purchase")
#     print(f"\nDeleting purchase_uid: {purchase_uid} from Purchases Table and payments with same purchase_uid from Payments Table")

#     with connect() as db:
#         delQuery_add_purchase = ("""
#                         DELETE FROM space.purchases
#                         WHERE purchase_uid = \'""" + purchase_uid + """\';
#                     """)

#         del_add_purchase_response = db.delete(delQuery_add_purchase)

#         delQuery_payment = ("""
#                         DELETE FROM space.payments
#                         WHERE pay_purchase_id = \'""" + purchase_uid + """\' AND paid_by = '350-000000'
#                     """)

#         del_payment_response = db.delete(delQuery_payment)


# def test_get_dashboard():
#     print("\nIn test GET Dashboard")
#     business_response = requests.get(ENDPOINT + "/dashboard/600-000000")
#     owner_response = requests.get(ENDPOINT + "/dashboard/110-000000")
#     tenant_response = requests.get(ENDPOINT + "/dashboard/350-000000")
#     print("\n\n", business_response.json())
#     print("\n\n", owner_response.json())
#     print("\n\n", tenant_response.json())

#     assert business_response.status_code == 200
#     assert owner_response.status_code == 200
#     assert tenant_response.status_code == 200


# post_owner_profile_payload = {}
# post_business_profile_payload = {}
# post_tenant_profile_payload = {}
# owner_uid = ""
# business_uid = ""
# employee_uid = ""
# tenant_uid = ""
# def test_post_profile():
#     print("\nIn test POST Profile")
#     global post_owner_profile_payload
#     post_owner_profile_payload = {
#         "owner_user_id": "100-000000",
#         "owner_first_name": "Test",
#         "owner_last_name": "Owner Account",
#         "owner_phone_number": "(000) 000-0000",
#         "owner_email": "test@gmail.com"
#     }
#     post_owner_profile_response = requests.post(ENDPOINT + "/profile", data=post_owner_profile_payload)
#     global owner_uid
#     owner_uid = post_owner_profile_response.json()["owner_uid"]
    
#     global post_business_profile_payload
#     post_business_profile_payload = {
#         "business_user_id": "100-000000",
#         "business_type": "Management",
#         "business_name": "Test Business Account",
#         "business_email": "test@gmail.com",
#     }
#     post_business_profile_response = requests.post(ENDPOINT + "/profile", data=post_business_profile_payload)
#     global business_uid
#     global employee_uid
#     business_uid = post_business_profile_response.json()["business_uid"]
#     employee_uid = post_business_profile_response.json()["employee_uid"]

#     global post_tenant_profile_payload
#     post_tenant_profile_payload = {
#         "tenant_user_id": "100-000000",
#         "tenant_first_name": "Test",
#         "tenant_last_name": "Tenant Account",
#         "tenant_email": "test@gmail.com",
#         "tenant_phone_number": "(000) 000-0000"
#     }
#     post_tenant_profile_response = requests.post(ENDPOINT + "/profile", data=post_tenant_profile_payload)
#     global tenant_uid
#     tenant_uid = post_tenant_profile_response.json()["tenant_uid"]

#     assert post_owner_profile_response.status_code == 200
#     assert post_business_profile_response.status_code == 200
#     assert post_tenant_profile_response.status_code == 200

# def test_post_get_profile():
#     print("\nIn test GET after POST Profile")
#     global owner_uid
#     post_get_owner_profile_response = requests.get(ENDPOINT + f"/profile/{owner_uid}")
#     data = post_get_owner_profile_response.json()['profile']['result'][0]
#     if data["owner_first_name"] != "Test":
#         print("Not Match")


#     global business_uid
#     post_get_business_profile_response = requests.get(ENDPOINT + f"/profile/{business_uid}")
#     data = post_get_business_profile_response.json()['profile']['result'][0]
#     if data["business_type"] != "Management":
#         print("Not Match")


#     global tenant_uid
#     post_get_tenant_profile_response = requests.get(ENDPOINT + f"/profile/{tenant_uid}")
#     data = post_get_tenant_profile_response.json()['profile']['result'][0]
#     if data["tenant_first_name"] != "Test":
#         print("Not Match")


#     assert post_get_owner_profile_response.status_code == 200
#     assert post_get_business_profile_response.status_code == 200
#     assert post_get_tenant_profile_response.status_code == 200


# put_owner_profile_payload = {}
# put_business_profile_payload = {}
# put_tenant_profile_payload = {}
# def test_put_profile():
#     print("\nIn test PUT Profile")
#     global put_owner_profile_payload
#     global owner_uid
#     put_owner_profile_payload = {
#         "owner_uid": f"{owner_uid}",
#         "owner_first_name": "Test Owner",
#     }
#     put_owner_profile_response = requests.put(ENDPOINT + "/profile", data=put_owner_profile_payload)
        
#     global put_business_profile_payload
#     global business_uid
#     put_business_profile_payload = {
#         "business_uid": f"{business_uid}",
#         "business_type": "Maintenance",
#     }
#     put_business_profile_response = requests.put(ENDPOINT + "/profile", data=put_business_profile_payload)

#     global put_tenant_profile_payload
#     global tenant_uid
#     put_tenant_profile_payload = {
#         "tenant_uid": f"{tenant_uid}",
#         "tenant_first_name": "Test Tenant",
#     }
#     put_tenant_profile_response = requests.put(ENDPOINT + "/profile", data=put_tenant_profile_payload)
    

#     assert put_owner_profile_response.status_code == 200
#     assert put_business_profile_response.status_code == 200
#     assert put_tenant_profile_response.status_code == 200

# def test_put_get_profile():
#     print("\nIn test GET after PUT Profile")
#     global owner_uid
#     put_get_owner_profile_response = requests.get(ENDPOINT + f"/profile/{owner_uid}")
#     data = put_get_owner_profile_response.json()['profile']['result'][0]
#     if data["owner_first_name"] != "Test Owner":
#         print("Not Match")


#     global business_uid
#     put_get_business_profile_response = requests.get(ENDPOINT + f"/profile/{business_uid}")
#     data = put_get_business_profile_response.json()['profile']['result'][0]
#     if data["business_type"] != "Maintenance":
#         print("Not Match")


#     global tenant_uid
#     put_get_tenant_profile_response = requests.get(ENDPOINT + f"/profile/{tenant_uid}")
#     data = put_get_tenant_profile_response.json()['profile']['result'][0]
#     if data["tenant_first_name"] != "Test Tenant":
#         print("Not Match")


#     assert put_get_owner_profile_response.status_code == 200
#     assert put_get_business_profile_response.status_code == 200
#     assert put_get_tenant_profile_response.status_code == 200

# def test_delete_profile():
#     print("\nIn DELETE Profile")
#     global owner_uid
#     global business_uid
#     global employee_uid
#     global tenant_uid
#     print(f"Deleting {owner_uid} from Owner Table, {business_uid} from Business Table, {employee_uid} from Employee Table & {tenant_uid} from Tenant Table")
#     with connect() as db:
#         delQuery_owner = ("""
#                         DELETE FROM space.ownerProfileInfo
#                         WHERE owner_uid = \'""" + owner_uid + """\';
#                     """)
#         delQuery_business = ("""
#                         DELETE FROM space.businessProfileInfo
#                         WHERE business_uid = \'""" + business_uid + """\';
#                     """)
#         delQuery_employee = ("""
#                         DELETE FROM space.employees
#                         WHERE employee_uid = \'""" + employee_uid + """\';
#                     """)
#         delQuery_tenant = ("""
#                         DELETE FROM space.tenantProfileInfo
#                         WHERE tenant_uid = \'""" + tenant_uid + """\';
#                     """)
        

#         response = db.delete(delQuery_owner)
#         response = db.delete(delQuery_business)
#         response = db.delete(delQuery_employee)
#         response = db.delete(delQuery_tenant)

# post_add_expense_payload = {}
# expense_uid = ""
# def test_post_add_expense():
#     print("\nIn POST Add Expense")
#     global post_add_expense_payload
#     global expense_uid
#     post_add_expense_payload = {
#         "pur_property_id":"200-000000",
#         "purchase_type":"Rent",
#         "pur_cf_type":"expense",
#         "purchase_date":"2024-11-11",
#         "pur_due_date":"2024-12-10",
#         "pur_amount_due":1999,
#         "purchase_status":"UNPAID",
#         "pur_notes":"This is just a test note",
#         "pur_description":"Test Description",
#         "pur_receiver":"600-000000",
#         "pur_initiator":"600-000000",
#         "pur_payer":"350-000000"
#     }
#     headers = {
#                 'Content-Type': 'application/json'
#             }  
#     post_add_expense_response = requests.post(ENDPOINT + "/addExpense", data=json.dumps(post_add_expense_payload), headers=headers)
#     expense_uid = post_add_expense_response.json()['Purchases_UID']
#     assert post_add_expense_response.status_code == 200

# put_add_expense_payload = {}
# def test_put_add_expense():
#     print("\nIn PUT add expense")
#     global put_add_expense_payload
#     global expense_uid
#     put_add_purchase_payload = {
#         "purchase_uid": f"{expense_uid}",
#         "pur_amount_due": 999
#     }
#     put_add_purchase_response = requests.put(ENDPOINT + "/addExpense", data=put_add_purchase_payload)
#     assert put_add_purchase_response.status_code == 200


# post_add_revenue_payload = {}
# revenue_uid = ""
# def test_post_add_revenue():
#     print("\nIn POST Add Revenue")
#     global post_add_revenue_payload
#     global revenue_uid
#     post_add_revenue_payload = {
#         "pur_property_id":"200-000000",
#         "purchase_type":"Rent",
#         "pur_cf_type":"revenue",
#         "purchase_date":"2024-11-11",
#         "pur_due_date":"2024-12-10",
#         "pur_amount_due":1999,
#         "purchase_status":"UNPAID",
#         "pur_notes":"This is just a test note",
#         "pur_description":"Test Description",
#         "pur_receiver":"600-000000",
#         "pur_initiator":"600-000000",
#         "pur_payer":"350-000000"
#     }
#     headers = {
#                 'Content-Type': 'application/json'
#             }  
#     post_add_revenue_response = requests.post(ENDPOINT + "/addRevenue", data=json.dumps(post_add_revenue_payload), headers=headers)
#     revenue_uid = post_add_revenue_response.json()['Purchases_UID']
#     assert post_add_revenue_response.status_code == 200

# put_add_revenue_payload = {}
# def test_put_add_revenue():
#     print("\nIn PUT add revenue")
#     global put_add_revenue_payload
#     global revenue_uid
#     put_add_revenue_payload = {
#         "purchase_uid": f"{revenue_uid}",
#         "pur_amount_due": 999
#     }
#     put_add_revenue_response = requests.put(ENDPOINT + "/addRevenue", data=put_add_revenue_payload)
#     assert put_add_revenue_response.status_code == 200

# def test_delete_add_expense_revenue():
#     global expense_uid
#     global revenue_uid
#     print("\nIn DELETE add expense")
#     print(f"\nDeleting purchase_uid: {expense_uid} from Purchases Table (for expense) and purchase_uid: {revenue_uid} from Purchases Table (for revenue)")

#     with connect() as db:
#         delQuery_add_expense = ("""
#                         DELETE FROM space.purchases
#                         WHERE purchase_uid = \'""" + expense_uid + """\';
#                     """)

#         del_add_expense_response = db.delete(delQuery_add_expense)
        
#         delQuery_add_revenue = ("""
#                         DELETE FROM space.purchases
#                         WHERE purchase_uid = \'""" + revenue_uid + """\';
#                     """)

#         del_add_revenue_response = db.delete(delQuery_add_revenue)

# def test_get_cashflow_transaction():
#     get_cashflow_response = requests.get(ENDPOINT + "/cashflowTransactions/600-000000/all")
#     # print(get_cashflow_response.json())
#     assert get_cashflow_response.status_code == 200

# def test_get_payment_verification():
#     get_payment_verification_response = requests.get(ENDPOINT + "/paymentVerification/600-000011")
#     print(get_payment_verification_response.json())
#     assert get_payment_verification_response.status_code == 200

# def test_get_rents():
#     get_rents_response = requests.get(ENDPOINT + "/rents/110-000000")
#     print(get_rents_response.json())
#     assert get_rents_response.status_code == 200

# def test_get_rent_details():
#     get_rent_details_response = requests.get(ENDPOINT + "/rentDetails/110-000000")
#     print(get_rent_details_response.json())
#     assert get_rent_details_response.status_code == 200


# -----Appliances----
# post_appliance_payload = {}
# appliance_uid = ""
# def test_post_appliance():
#     print("\nIn POST Appliances")
#     global post_appliance_payload
#     post_appliance_payload= {
#         "appliance_property_id":"200-000000",
#         "appliance_type":"050-000023",
#         "appliance_desc":"Test Appliance Description"
#     }

#     post_appliance_response = requests.post(ENDPOINT + "/appliances", data=post_appliance_payload)
#     global appliance_uid
#     appliance_uid = post_appliance_response.json()['appliance_UID']

#     assert post_appliance_response.status_code == 200

# def test_get_post_appliance():
#     print("\nIn GET after POST Appliances")
#     global post_appliance_payload
    
#     get_post_appliance_response = requests.get(ENDPOINT + "/appliances/200-000000")
#     data =get_post_appliance_response.json()['result'][0]
#     if data["appliance_desc"] !=  "Test Appliance Description":
#         print("Not Match")

#     assert get_post_appliance_response.status_code == 200

# put_appliance_payload = {}
# def test_put_appliance():
#     print("\nIn PUT Appliances")
#     global put_appliance_payload
#     global appliance_uid
#     put_appliance_payload = {
#         "appliance_uid":f"{appliance_uid}",
#         "appliance_desc":"Test Appliance Description 1"
#     }

#     put_appliance_response = requests.put(ENDPOINT + "/appliances", data=put_appliance_payload)
#     assert put_appliance_response.status_code == 200

# def test_get_put_appliance():
#     print("\nIn GET after PUT Appliances")
#     global put_appliance_payload
    
#     get_put_appliance_response = requests.get(ENDPOINT + "/appliances/200-000000")
#     data =get_put_appliance_response.json()['result'][0]
#     if data["appliance_desc"] !=  "Test Appliance Description 1":
#         print("Not Match")

#     assert get_put_appliance_response.status_code == 200

# def test_delete_appliance():
#     print("\nIn DELETE Appliances")
#     global appliance_uid
#     print(f"Deleting appliance_uid: {appliance_uid} form Appliances Table")
#     delete_appliance_response = requests.delete(ENDPOINT + f"/appliances/{appliance_uid}")

#     assert delete_appliance_response.status_code == 200


# post_employee_payload = {}
# employee_uid = ""
# def test_post_employee():
#     global post_employee_payload
#     post_employee_payload = {
#         "employee_user_id":"100-000000",
#         "employee_business_id":"600-000000",
#         "employee_first_name":"Test",
#         "employee_last_name":"Employee"
#     }

#     post_employee_response = requests.post(ENDPOINT + "/employee", data=post_employee_payload)
#     global employee_uid
#     employee_uid = post_employee_response.json()['employee_uid']

#     assert post_employee_response.status_code == 200


# def test_get_post_employee():
#     global employee_uid
#     get_post_employee_response = requests.get(ENDPOINT + f"/employee/{employee_uid}")
#     data = get_post_employee_response.json()['employee']['result'][0]
#     if data["employee_first_name"] != "Test":
#         print("Not Match")

#     assert get_post_employee_response.status_code == 200

# put_employee_verification_payload = {}
# def test_put_employee_verification():
#     global put_employee_verification_payload
#     global employee_uid
#     put_employee_verification_payload = [{
#         "employee_uid":f"{employee_uid}",
#         "employee_first_name":"Test Account"
#     }]
#     headers = {
#                 'Content-Type': 'application/json'
#             }    
#     put_employee_verification_response = requests.put(ENDPOINT + "/employeeVerification", data=json.dumps(put_employee_verification_payload), headers=headers)
#     print(put_employee_verification_response.json())

#     assert put_employee_verification_response.status_code == 200

# def test_get_put_employee():
#     global employee_uid
#     get_put_employee_response = requests.get(ENDPOINT + f"/employee/{employee_uid}")
#     data = get_put_employee_response.json()['employee']['result'][0]
#     if data["employee_first_name"] != "Test Account":
#         print("Not Match")

#     assert get_put_employee_response.status_code == 200

# def test_delete_employee():
#     global employee_uid
#     print("\nIn DELETE employee")
#     print(f"\nDeleting employee_uid: {employee_uid} from Employees Table")

#     with connect() as db:
#         delQuery_employee = ("""
#                         DELETE FROM space.employees
#                         WHERE employee_uid = \'""" + employee_uid + """\';
#                     """)

#         del_employee_response = db.delete(delQuery_employee)


# ------ Contacts ------
# def test_get_contacts():
#     print("\nIn GET Contacts")
#     get_contacts_business_response = requests.get(ENDPOINT + "/contacts/600-000000")
#     get_contacts_owner_response = requests.get(ENDPOINT + "/contacts/110-000000")
#     get_contacts_tenant_response = requests.get(ENDPOINT + "/contacts/350-000000")
#     assert get_contacts_business_response.status_code == 200
#     assert get_contacts_owner_response.status_code == 200
#     assert get_contacts_tenant_response.status_code == 200


# ------ SearchManager -------
# def test_get_search_manager():
#     print("\nIn GET Search Manager")
#     get_search_manager_response = requests.get(ENDPOINT + "/searchManager")
#     assert get_search_manager_response.status_code == 200


# ------ Utility -------
# post_utility_payload = {}
# def test_post_utility():
#     print("\nIn POST Utility")
#     global post_utility_payload
#     post_utility_payload = {
#         "property_uid": "200-000000",
#         "property_utility":json.dumps({
#             "050-000000":"050-000000"
#         })
#     }

#     post_utility_payload_response = requests.post(ENDPOINT + "/utilities", data=post_utility_payload)
#     # print("\n\n", post_utility_payload_response.json(), "\n\n")
#     assert post_utility_payload_response.status_code == 200

# def test_post_get_utility():
#     print("\nIn GET after POST Utility")
#     post_get_utility_response = requests.get(ENDPOINT + "/utilities?utility_property_id=200-000000")
#     data = post_get_utility_response.json()['result'][0]
#     if data['utility_payer_id'] != "050-000000":
#         print("Not Match")
#     # print("\n\n", post_get_utility_response.json())
#     assert post_get_utility_response.status_code == 200

# put_utility_payload = {}
# def test_put_utility():
#     print("\nIn PUT Utility")
#     global put_utility_payload
#     put_utility_payload = {
#         "property_uid": "200-000000",
#         "property_utility":json.dumps({
#             "050-000000":"050-100000"
#         })
#     }
#     put_utility_payload_response = requests.put(ENDPOINT + "/utilities", data=put_utility_payload)
#     # print("\n\n", put_utility_payload_response.json())
#     assert put_utility_payload_response.status_code == 200

# def test_put_get_utility():
#     print("\nIn GET after PUT Utility")
#     put_get_utility_response = requests.get(ENDPOINT + "/utilities?utility_property_id=200-000000")
#     data = put_get_utility_response.json()['result'][0]
#     if data['utility_payer_id'] != "050-100000":
#         print("Not Match")
#     # print("\n\n", put_get_utility_response.json()['result'][0])
#     assert put_get_utility_response.status_code == 200

# def test_delete_utility():
#     print("\nIn DELETE Utility")
#     print(f"\nDeleting utility_property_id: '200-000000', utility_type_id: '050-000000' & utility_payer_id: '050-100000' from Employees Table")

#     with connect() as db:
#         delQuery_property_utility = ("""
#                         DELETE FROM space.property_utility
#                         WHERE utility_property_id = "200-000000"
#                         AND utility_type_id = "050-000000"
#                         AND utility_payer_id = "050-100000";
#                     """)

#         del_property_utility_response = db.delete(delQuery_property_utility)



# ------- Bills -------
# post_bill_payload = {}
# bill_uid = ""
# purchase_uids = {}
# def test_post_bill():
#     global post_bill_payload
#     post_bill_payload = {
#         "bill_created_by":"600-000000",
#         "bill_description":"Test Bill Description",
#         "bill_amount":199.99,
#         "bill_utility_type":"maintenance",
#         "bill_split":"Uniform",
#         "bill_property_id":'[{"property_uid":"200-000000"}]',
#         "bill_maintenance_request_id":"800-000000",
#         "bill_maintenance_quote_id":"900-000000",
#         "bill_notes":""
#     }
#     post_bill_response = requests.post(ENDPOINT + "/bills", data=post_bill_payload)
#     global bill_uid
#     bill_uid = post_bill_response.json()['maibill_uidtenance_request_uid']
#     global purchase_uids
#     purchase_uids = post_bill_response.json()['purchase_ids_add']

#     assert post_bill_response.status_code == 200

# def test_get_post_bill():
#     global bill_uid
#     get_post_response = requests.get(ENDPOINT + f"/bills/{bill_uid}")
#     data = get_post_response.json()['result'][0]
#     if data['bill_description'] != "Test Bill Description":
#         print("Not Match")

#     assert get_post_response.status_code == 200

# put_bill_payload = {}
# def test_put_bill():
#     global put_bill_payload
#     put_bill_payload = {
#         "bill_uid":f"{bill_uid}",
#         "bill_description":"Test Bill Description 1"
#     }
#     put_bill_response = requests.put(ENDPOINT + "/bills", data=put_bill_payload)
#     assert put_bill_response.status_code == 200

# def test_get_put_bill():
#     global bill_uid

#     get_put_response = requests.get(ENDPOINT + f"/bills/{bill_uid}")
#     data = get_put_response.json()['result'][0]
#     if data['bill_description'] != "Test Bill Description 1":
#         print("Not Match")
#     assert get_put_response.status_code == 200

# def test_delete_bill():
#     print("\nIn DELETE Bill")
#     global bill_uid
#     global purchase_uids
#     print(f"\nDeleting bill_uid: {bill_uid} from Bills Table and purchase_uids: {purchase_uids} from Purchases Table")

#     with connect() as db:
#         delQuery_bill = ("""
#                         DELETE FROM space.bills
#                         WHERE bill_uid = \'""" + bill_uid + """\';
#                     """)

#         del_bill_response = db.delete(delQuery_bill)

#         for pur_id in purchase_uids:
#             delQuery_purchase = ("""
#                         DELETE FROM space.purchases
#                         WHERE purchase_uid = \'""" + pur_id + """\';
#                     """)

#             del_purchase_response = db.delete(delQuery_purchase)

# def test_get_listings():
#     get_listings_response = requests.get(ENDPOINT + "/listings/350-000000")
#     assert get_listings_response.status_code == 200

# ------ Announcements ------
# post_announcement_payload = {}
# def test_post_announcement():
#     print("\nIn POST Announcement")
#     global post_announcement_payload
#     post_announcement_payload = {
#             "announcement_title": "Test Announcement",
#             "announcement_msg": "Hi! This is a test announcement",
#             "announcement_properties": "{\"350-000000\":[\"200-000000\"]}",
#             "announcement_mode": "LEASE",
#             "announcement_receiver": "350-000000",
#             "announcement_type": ["Text","Email"]
#         }
#     headers = {
#             'Content-Type': 'application/json'
#         } 
#     post_announcement_response = requests.post(ENDPOINT + "/announcements/110-000000", data=json.dumps(post_announcement_payload), headers=headers)
#     assert post_announcement_response.status_code == 200

# announcement_uid = ""
# def test_post_get_announcement():
#     print("\nIn GET after POST Announcement")
#     post_get_announcement_response = requests.get(ENDPOINT + "/announcements/110-000000")
#     data = post_get_announcement_response.json()['sent']['result'][0]
#     global announcement_uid
#     announcement_uid = data['announcement_uid']
#     if data['announcement_title'] != "Test Announcement":
#         print("Not Match")
#     assert post_get_announcement_response.status_code == 200

# put_announcement_payload = {}
# def test_put_announcement():
#     global put_announcement_payload
#     global announcement_uid
#     put_announcement_payload = {
#         "announcement_uid": [f"{announcement_uid}"],
#         "announcement_title": "Test Announcement 1"
#     }
#     headers = {
#             'Content-Type': 'application/json'
#         }
#     put_announcement_response = requests.put(ENDPOINT + "/announcements", data=json.dumps(put_announcement_payload), headers=headers)
#     assert put_announcement_response.status_code == 200

# def test_put_get_announcement():
#     print("\nIn GET after PUT Announcement")
#     put_get_announcement_response = requests.get(ENDPOINT + "/announcements/110-000000")
#     data = put_get_announcement_response.json()['sent']['result'][0]
#     if data['announcement_title'] != "Test Announcement 1":
#         print("Not Match")
#     assert put_get_announcement_response.status_code == 200

# def test_delete_announcement():
#     print("\nIn DELETE Announcement")
#     global announcement_uid
#     print(f"\nDeleting announcement_uid: {announcement_uid} from Announcements Table")

#     with connect() as db:
#         delQuery_announcement = ("""
#                         DELETE FROM space.announcements
#                         WHERE announcement_uid = \'""" + announcement_uid + """\';
#                     """)
#         del_announcement_response = db.delete(delQuery_announcement)
