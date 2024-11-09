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

# -------- Dashboard -------- Completed
# def test_get_dashboard():
#     response_m = requests.get(ENDPOINT + "/dashboard/600-000038")
#     assert response_m.status_code == 200

#     # response_o = requests.get(ENDPOINT + "/dashboard/110-000099")
#     # response_t = requests.get(ENDPOINT + "/dashboard/350-000068")
#     # response_mt = requests.get(ENDPOINT + "/dashboard/600-000039")
#     # assert response_o.status_code == 200
#     # assert response_t.status_code == 200
#     # assert response_mt.status_code == 200

# -------- Cashflow Transactions -------- Completed
# def test_get_cashflow():

#     response = requests.get(ENDPOINT + "/cashflowTransactions/600-000011/all")
#     assert response.status_code == 200

# # -------- Property Endpoints --------
# def test_get_properties():
#     response = requests.get(ENDPOINT + '/properties/110-000099')
#     assert response.status_code == 200

# def test_get_listings():
#     response = requests.get(ENDPOINT + "/listings/350-000002")

#     assert response.status_code == 200

# def test_get_contracts():
#     response = requests.get(ENDPOINT + "/contracts/110-000099")

#     assert response.status_code == 200

# def test_get_rents():
#     response = requests.get(ENDPOINT + "/rents/110-000099")

#     assert response.status_code == 200

# def test_get_searchManager():
#     response = requests.get(ENDPOINT + "/searchManager?")

#     assert response.status_code == 200

# def test_get_leaseDetails():
#     response = requests.get(ENDPOINT + "/leaseDetails/600-000038")

#     assert response.status_code == 200

# def test_get_announcements():
#     response = requests.get(ENDPOINT + "/announcements/110-000099")

#     assert response.status_code == 200

# def test_get_contacts():
#     response = requests.get(ENDPOINT + "/contacts/600-000038")

#     assert response.status_code == 200

# def test_get_contactsMaintenance():
#     response = requests.get(ENDPOINT + "/contacts/600-000012")

#     assert response.status_code == 200

# def test_get_businessProfile():
#     response = requests.get(ENDPOINT + "/businessProfile")

#     assert response.status_code == 200

# Tables affecting: {maintenanceRequests, maintenanceQuotes, Properties, Property_Owner, Contracts, leases, lease_tenant, addPurchases, paymentMethods, payments}
class endPointTest_CLASS(Resource):
    def get():
        dt = datetime.today()
        response = {}
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
            data = post_get_lease_application_response.json()['Lease_Details']['result'][0]
            if data['lease_status'] != "NEW":
                print('Not Match')
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
            data = put_get_lease_application_response.json()['Lease_Details']['result'][0]
            if data['lease_status'] != "PROCESSING":
                print('Not Match')
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

        except:
            response["cron fail"] = {'message': f'MySpace Test API CRON Job failed for {dt}' ,'code': 500}

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
#     data = post_get_lease_application_response.json()['Lease_Details']['result'][0]

#     if data['lease_status'] != "NEW":
#         print('Not Match')
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
#     data = put_get_lease_application_response.json()['Lease_Details']['result'][0]

#     if data['lease_status'] != "PROCESSING":
#         print('Not Match')
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

# 400-001364
post_add_purchase_payload = {}
purchase_uid = ""
def test_post_add_purchases():
    print("\nIn POST add purchase")
    global post_add_purchase_payload
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

    global purchase_uid
    purchase_uid = post_add_purchase_response.json()['purchase_UID']
    print(purchase_uid, 'purchase_UID')
    assert post_add_purchase_response.status_code == 200

put_add_purchase_payload = {}
def test_put_add_purchase():
    print("\nIn PUT add purchase")
    global put_add_purchase_payload
    global purchase_uid
    put_add_purchase_payload = {
        "purchase_uid": f"{purchase_uid}",
        "pur_late_fee": "10"
    }

    put_add_purchase_response = requests.put(ENDPOINT + "/addPurchase", data=put_add_purchase_payload)

    assert put_add_purchase_response.status_code == 200

# running this function, add 2 more entries in purchase table

post_payment_payload = {}
def test_make_payment():
    global purchase_uid
    global post_payment_payload
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

    headers = {
                'Content-Type': 'application/json'
            } 

    post_payment_response = requests.post(ENDPOINT + "/makePayment", data=json.dumps(post_payment_payload), headers=headers)
    print("\n\n", post_payment_response.json(), "\n\n")
    assert post_payment_response.status_code == 200


def test_delete_add_purchase():
    global purchase_uid
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



# lease_uid:300-000020
# lease_status:PROCESSING
# lease_effective_date:09-22-2024
# lease_start:09-22-2024
# lease_end:09-21-2025
# lease_fees:[{"charge":1300,"due_by":1,"late_by":1,"fee_name":"Unit Rent","fee_type":"Rent","late_fee":"60","frequency":"Monthly","due_by_date":"","leaseFees_uid":"370-000054","available_topay":7,"perDay_late_fee":6},{"charge":1300,"due_by":0,"late_by":2,"fee_name":"Deposit","fee_type":"Deposit","late_fee":"20","frequency":"One Time","due_by_date":"09-22-2024","leaseFees_uid":"370-000055","available_topay":2,"perDay_late_fee":2},{"charge":200,"due_by":7,"late_by":7,"fee_name":"Extra Charge","fee_type":"Extra Charge","late_fee":"30","frequency":"Monthly","due_by_date":"","leaseFees_uid":"370-000057","available_topay":7,"perDay_late_fee":3},{"charge":25,"due_by":1,"late_by":5,"fee_name":"Replace Air Filter","fee_type":"Maintenance","late_fee":"50","frequency":"Quarterly","due_by_date":"","leaseFees_uid":"370-000058","available_topay":5,"perDay_late_fee":5}]
# lease_move_in_date:09-22-2024
# lease_end_notice_period:30
# delete_fees:["370-000057"]




# lease_property_id:200-000023
# lease_start:01-31-2024
# lease_end:01-30-2025
# lease_end_notice_period:null
# lease_application_date:6/27/2024
# tenant_uid:350-000000
# lease_move_in_date:11/04/2024
# frequency:Monthly
# lease_status:PROCESSING

# lease_fees:[{"charge":1250,"due_by":1,"late_by":1,"fee_name":"Deposit","fee_type":"$","late_fee":"95","frequency":"One-time","due_by_date":"01-01-2024","leaseFees_uid":"370-000000","available_topay":10,"perDay_late_fee":10},{"charge":1700,"due_by":1,"late_by":1,"fee_name":"Rent","fee_type":"$","late_fee":"95","frequency":"Monthly","due_by_date":null,"leaseFees_uid":"370-000014","available_topay":10,"perDay_late_fee":10}]


# lease_assigned_contacts:[{"email": "pmarathay@gmail.com", "last_name": "Marathay", "first_name": "Prashant", "company_role": "Property Manager", "phone_number": "(408) 476-0001"}]
# lease_adults:[{"dob":"1958-01-18","name":"Donald Dingwell","relationship":"Father"},{"dob":"1985-08-18","name":"Evan Dingwell","relationship":"self"}]
# lease_children:[{"dob":"2015-04-28","name":"Olivia","relationship":"Dingwell"}]
# lease_pets:[]
# lease_vehicles:[{"make":"Ford ","year":"2005","model":"f250","state":"az","license":"cbh2214"},{"make":"Ford ","year":"2010","model":"f150","state":"az","license":"cfv4636"}]
# lease_referred:[]
# property_listed_rent:1700
# lease_documents:[{"fileIndex":0,"fileName":"California-Standard-Lease-Agreement.pdf","fileType":"pdf","type":"Lease Agreement"},{"fileIndex":1,"fileName":"California-Standard-Lease-Agreement.pdf","fileType":"pdf","type":"Hi Divya"}]




# -------------------------------------------------------------------------------------------

# def test_get_payment_status():
#     response = requests.get(ENDPOINT + "/paymentStatus/600-000038")

#     assert response.status_code == 200

# def test_get_allTransactions():
#     response = requests.get(ENDPOINT + "/allTransactions/100-000003")

#     assert response.status_code == 200

# def test_get_contactsMaintenance():
#     response = requests.get(ENDPOINT + "/contacts/600-000012")

#     assert response.status_code == 200

# def test_get_ownerProfile():
#     response = requests.get(ENDPOINT + "/ownerProfile/110-000099")

#     assert response.status_code == 200

# def test_get_tenantProfile():
#     response = requests.get(ENDPOINT + "/tenantProfile/350-000068")

#     assert response.status_code == 200

# def test_get_ownerDocuments():
#     response = requests.get(ENDPOINT + "/ownerDocuments/110-000099")

#     assert response.status_code == 200


# def test_put_properties():
#     payload = {'property_uid': '200-000084', 'property_address': 'Bellevue Square 2302', 'property_unit': '2', 'property_city': 'Seattle', 'property_state': 'CA', 'property_zip': '97324', 'property_type': 'Single Family', 'property_num_beds': '0', 'property_num_baths': '2', 'property_area': '0', 'property_listed_rent': '0', 'property_deposit': '0', 'property_pets_allowed': '0', 'property_deposit_for_rent': '0', 'property_taxes': '0', 'property_mortgages': '0', 'property_insurance': '0', 'property_featured': '0', 'property_description': 'test', 'property_notes': '', 'property_available_to_rent': ''}
#     response = requests.put(ENDPOINT + "/properties", data=payload)

#     assert response.status_code == 200
#     data = response.json()
#     print(data)

# def test_post_makepayments():
#     data = {"pur_property_id":"200-000088","purchase_type":"Insurance","pur_cf_type":"expense","purchase_date":"04-04-2024","pur_due_date":"04-04-2024","pur_amount_due":323232,"purchase_status":"COMPLETED","pur_notes":"This is just a note","pur_description":"hi","pur_receiver":"110-000099","pur_initiator":"110-000099","pur_payer":"350-000068"}
#     response = requests.post(ENDPOINT + "/addExpense", json=data)
#     assert response.status_code == 200
#     with connect() as db:
#         purQuery = db.execute("""
#                         SELECT purchase_uid, pur_payer from space.purchases
#                         WHERE pur_notes = "This is just a note" and pur_description = "hi" and pur_property_id = "200-000088"
#                                         """)

#         # print("Query: ", propertiesQuery)
#         response = purQuery
#         purchase_uid = response["result"][0]["purchase_uid"]
#         pur_payer = response["result"][0]["pur_payer"]
#         print(purchase_uid)
#     payload = {
#     "pay_purchase_id": [
#         {
#             "purchase_uid":purchase_uid,
#             "pur_amount_due": "30.00"
#         }
#     ],
#     "pay_fee": 0,
#     "pay_total": 34,
#     "payment_notes": "PMTEST",
#     "pay_charge_id": "stripe transaction key",
#     "payment_type": "Credit Card",
#     "payment_verify": "Unverified",
#     "paid_by":pur_payer,
#     "payment_intent": "pi_3OtBBYAdqquNNobL0H1HlLHO",
#     "payment_method": "pm_1OtBBZAdqquNNobLecgTA3Se"
#     }

#     response = requests.post(ENDPOINT + "/makePayment", json = payload)
#     assert response.status_code == 200

#     with connect() as db:

#         delQuery = ("""
#                 DELETE space.purchases
#                 FROM space.purchases
#                 WHERE purchase_uid = \'""" + purchase_uid + """\';
#                 """)

#         response = db.delete(delQuery)


#         delQuery1 = ("""
#                 DELETE space.payments
#                 FROM space.payments
#                 WHERE pay_purchase_id = \'""" + purchase_uid + """\';
#                 """)

#         response = db.delete(delQuery1)



# def test_post_properties():
#     payload = {'property_uid': '200-000084', 'property_address': 'Bellevue Square 2302', 'property_unit': '2',
#                'property_city': 'Seattle', 'property_state': 'CA', 'property_zip': '97324',
#                'property_type': 'Single Family', 'property_num_beds': '0', 'property_num_baths': '2',
#                'property_area': '0', 'property_listed_rent': '0', 'property_deposit': '0', 'property_pets_allowed': '0',
#                'property_deposit_for_rent': '0', 'property_taxes': '0', 'property_mortgages': '0',
#                'property_insurance': '0', 'property_featured': '0', 'property_description': 'test',
#                'property_notes': '', 'property_available_to_rent': ''}

# # def test_post_addExpense():
# #     payload = {"pur_property_id":"200-000088","purchase_type":"Insurance","pur_cf_type":"expense","purchase_date":"10-23-2023","pur_due_date":"10-23-2023","pur_amount_due":323232,"purchase_status":"COMPLETED","pur_notes":"This is just a note","pur_description":"hi","pur_receiver":"110-000099","pur_initiator":"110-000099","pur_payer": ''}
# #     response = requests.post(ENDPOINT + "/addExpense", json = payload)
# #
# #     assert response.status_code == 200
# #     data = response.json()
# #
# #     purchaseQuery = ("""
# #             SELECT * from space.purchases
# #             WHERE pur_property_id = '200-000088';
# #             """)
# #
# #     propertyQuery = ("""
# #             DELETE space.properties
# #             FROM space.properties
# #             WHERE lt_lease_id = \'""" + lease_uid + """\';
# #             """)
# #
# #     response["delete_lease_tenant"] = db.delete(propertyQuery)
# #
# #     print(data)

# def test_PM_Tenant_Flow():
#     print("In test_PM_Tenant_flow", datetime.now())

#     tenant_id = '350-000068'
#     property_id = '200-000084'

#     payload = {'property_uid': '200-000084', 'property_available_to_rent': '1', 'property_active_date': '2023-11-06', 'property_address': 'Bellevue Square 2302', 'property_unit': '2', 'property_city': 'Seattle', 'property_state': 'CA', 'property_zip': '97324', 'property_type': 'Single Family', 'property_num_beds': '0', 'property_num_baths': '2', 'property_area': '0', 'property_listed_rent': '0', 'property_deposit': '0', 'property_pets_allowed': '0', 'property_deposit_for_rent': '0', 'property_taxes': '0', 'property_mortgages': '0', 'property_insurance': '0', 'property_featured': '0', 'property_description': '', 'property_notes': ''}
#     response = requests.put(ENDPOINT + "/properties", data=payload)

#     assert response.status_code == 200


#     response = requests.get(ENDPOINT + "/listings/350-000002")

#     assert response.status_code == 200

#     response = requests.get(ENDPOINT + "/leaseDetails/350-000068")

#     assert response.status_code == 200

#     payload = {"announcement_title":"New Tenant Application","announcement_msg":"You have a new tenant application for your property","announcement_sender":"350-000068","announcement_date":"Mon Nov 06 2023","announcement_properties":"{\"600-000038\":[\"200-000084\"]}","announcement_mode":"LEASE","announcement_receiver":["600-000038"],"announcement_type":["App"]}

#     response = requests.post(ENDPOINT + "/announcements/350-000068", json=payload)

#     assert response.status_code == 200

#     payload = {"lease_property_id":"200-000084","lease_status":"NEW","lease_assigned_contacts":"[\"350-000068\"]","lease_documents":"[]","lease_adults":"[]","lease_children":"[]","lease_pets":"[]","lease_vehicles":"[]","lease_referred":"[]","lease_rent":"[]","lease_application_date":"11/6/2023","tenant_uid":"350-000068"}

#     response = requests.post(ENDPOINT + "/leaseApplication", data = payload)

#     data = response.json()

#     print(data)

#     assert response.status_code == 200

#     response = requests.get(ENDPOINT + "/contracts/600-000038")

#     assert response.status_code == 200

#     response = {}
#     with connect() as db:
#         leaseQuery = db.execute("""
#                         SELECT lease_uid from space.leases
#                         LEFT JOIN space.lease_tenant ON lt_lease_id = lease_uid
#                         WHERE lt_tenant_id = \'""" + tenant_id + """\' AND lease_property_id = \'""" + property_id + """\';
#                                         """)

#         # print("Query: ", propertiesQuery)
#         response["lease"] = leaseQuery

#         lease_uid = response['lease']['result'][0]['lease_uid']

#     payload = {"lease_uid": lease_uid,"lease_status":"PROCESSING","lease_start":"11/6/2023","lease_end":"11/5/2024","lease_fees":[{"id":1,"fee_name":"rent","fee_type":"$","frequency":"Monthly","charge":"300","due_by":"1st","late_by":"2","late_fee":"200","perDay_late_fee":"20","available_topay":""}]}

#     response = requests.put(ENDPOINT + "/leaseApplication", json = payload)

#     assert response.status_code == 200

#     payload = {"announcement_title":"New Lease created","announcement_msg":"You have a new lease to be approved for your property","announcement_sender":"600-000038","announcement_date":"Mon Nov 06 2023","announcement_properties":"{\"350-000002\":[\"200-000084\"]}","announcement_mode":"LEASE","announcement_receiver":["350-000002"],"announcement_type":["App"]}

#     response = requests.post(ENDPOINT + "/announcements/600-000038", json=payload)

#     assert response.status_code == 200

#     payload = {"lease_uid":lease_uid,"lease_status":"ACTIVE"}

#     response = requests.put(ENDPOINT + "/leaseApplication", json=payload)

#     assert response.status_code == 200

#     response = {}
#     with connect() as db:

#         leaseQuery = ("""
#                 DELETE space.leases
#                 FROM space.leases
#                 LEFT JOIN space.lease_tenant ON lt_lease_id = lease_uid
#                 WHERE lease_uid = \'""" + lease_uid + """\';
#                 """)

#         response["delete_lease"] = db.delete(leaseQuery)


#         leaseQuery = ("""
#                 DELETE space.lease_tenant
#                 FROM space.lease_tenant
#                 WHERE lt_lease_id = \'""" + lease_uid + """\';
#                 """)

#         response["delete_lease_tenant"] = db.delete(leaseQuery)

#     print(response)

# def test_PM_Owner():
#     print("In test_PM_Owner ", datetime.now())

#     payload = {'property_owner_id': ['110-000099'], 'property_available_to_rent': ['0'], 'property_active_date': ['2023-11-07'], 'property_address': ['253rd Arizona'], 'property_unit': ['2'], 'property_city': ['Tucson'], 'property_state': ['AZ'], 'property_zip': ['80000'], 'property_type': ['Condo'], 'property_num_beds': ['2'], 'property_num_baths': ['3'], 'property_value': ['200000'], 'property_area': ['2323'], 'property_listed': ['0'], 'property_deposit': ['0'], 'property_pets_allowed': ['0'], 'property_deposit_for_rent': ['0'], 'property_taxes': ['0'], 'property_mortgages': ['0'], 'property_insurance': ['0'], 'property_featured': ['0'], 'property_description': [''], 'property_notes': ['test'], 'img_cover': ['']}

#     response = requests.post(ENDPOINT + "/properties", data = payload)

#     assert response.status_code == 200

#     response1 = {}
#     response2 = {}
#     with connect() as db:
#         propertyQuery = db.execute("""
#                                 SELECT property_uid from space.properties
#                                 LEFT JOIN space.property_owner ON property_uid = property_id
#                                 WHERE property_owner_id = '110-000099' AND property_address = '253rd Arizona';
#                                                 """)

#         # print("Query: ", propertiesQuery)
#         response1["property"] = propertyQuery

#         property_uid = response1['property']['result'][0]['property_uid']



#     payload = {"announcement_title":"black","announcement_msg":"hello","announcement_sender":"110-000099","announcement_date":"2023-11-07","announcement_properties":"{\"600-000038\":[\"200-000147\"]}","announcement_mode":"CONTRACT","announcement_receiver":["600-000038"],"announcement_type":["App"]}

#     response = requests.post(ENDPOINT + "/announcements/110-000099", json=payload)

#     assert response.status_code == 200

#     property_uid = f'["{property_uid}"]'

#     payload = {'contract_property_ids':property_uid,'contract_business_id':'600-000038','contract_start_date':'2023-11-07',
#     'contract_status': 'NEW'}

#     response = requests.post(ENDPOINT + "/contracts", data=payload)
#     property_uid = property_uid.strip('[]"')
#     assert response.status_code == 200

#     with connect() as db:
#         contractQuery = db.execute("""
#                                         SELECT contract_uid from space.contracts
#                                         WHERE contract_property_id = \'""" + property_uid + """\' AND contract_business_id = '600-000038';
#                 """)

#         response2["contract"] = contractQuery

#         contract_uid = response2['contract']['result'][0]['contract_uid']

#     payload = {'contract_uid': contract_uid, 'contract_name': 'Black ', 'contract_start_date': '2023-11-07', 'contract_end_date': '12-12-2023', 'contract_fees': '[{"fee_name":"Black","fee_type":"FLAT-RATE","frequency":"One Time","isPercentage":false,"isFlatRate":true,"charge":"1000"}]', 'contract_status': 'SENT', 'contract_assigned_contacts': '[]'}

#     response = requests.put(ENDPOINT + "/contracts", data=payload)

#     assert response.status_code == 200

#     payload = {'contract_uid': contract_uid, 'contract_status': 'ACTIVE'}

#     response = requests.put(ENDPOINT + "/contracts", data=payload)

#     assert response.status_code == 200

#     response = {}

#     with connect() as db:
#         propertyQuery = db.execute("""
#                         SELECT property_uid from space.properties
#                         LEFT JOIN space.property_owner ON property_uid = property_id
#                         WHERE property_owner_id = '110-000099' AND property_address = '253rd Arizona';
#                                         """)

#         # print("Query: ", propertiesQuery)
#         response["property"] = propertyQuery

#         for i in range(len(response['property']['result'])):
#             property_uid = response['property']['result'][i]['property_uid']
#             delQuery = ("""
#                         DELETE from space.properties WHERE property_uid = \'""" + property_uid + """\';
#             """)
#             delQuery2 = ("""
#                         DELETE from space.property_owner WHERE property_id = \'""" + property_uid + """\';
#             """)

#             response[i] = db.delete(delQuery)
#             response2[i] = db.delete(delQuery2)

#         contractQuery = ("""
#                         DELETE from space.contracts WHERE contract_uid = \'""" + contract_uid + """\';
#         """)

#         response3 = db.delete(contractQuery)

#     print(response, response2, response3)

# def test_Maintenance_flow():
# # Request URL:
# # https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/maintenanceRequests
# # Request Method:
# # POST

#     payload = {'maintenance_property_id': '200-000084', 'maintenance_title': 'Vents Broken', 'maintenance_desc': 'Vents ', 'maintenance_request_type': 'Appliance', 'maintenance_request_created_by': '600-000038', 'maintenance_priority': 'High', 'maintenance_can_reschedule': '1', 'maintenance_assigned_business': 'null', 'maintenance_assigned_worker': 'null', 'maintenance_scheduled_date': 'null', 'maintenance_scheduled_time': 'null', 'maintenance_frequency': 'One Time', 'maintenance_notes': 'null', 'maintenance_request_created_date': '2023-11-13', 'maintenance_request_closed_date': 'null', 'maintenance_request_adjustment_date': 'null'}

#     response = requests.post(ENDPOINT + "/maintenanceRequests", data = payload)

#     assert response.status_code == 200

#     response = {}
#     with connect() as db:
#         maintenanceQuery = db.execute("""
#                                         SELECT maintenance_request_uid from space.maintenanceRequests
#                                         WHERE maintenance_property_id = '200-000084' AND maintenance_title = 'Vents Broken';
#                 """)

#         response["maint"] = maintenanceQuery

#         maintenance_request_uid = response['maint']['result'][0]['maintenance_request_uid']

# # Request URL:
# # https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/quotes
# # Request Method:
# # POST
#     payload = {'quote_maintenance_request_id': maintenance_request_uid, 'quote_pm_notes': 'Vents', 'quote_maintenance_contacts': '600-000039'}

#     response = requests.post(ENDPOINT + "/maintenanceQuotes", data = payload)

#     assert response.status_code == 200
# # Request URL:
# # https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/maintenanceRequests
# # Request Method:
# # PUT
#     with connect() as db:
#         response = {}
#         quoteQuery = db.execute("""
#                                     SELECT maintenance_quote_uid from space.maintenanceQuotes
#                                     WHERE quote_maintenance_request_id = \'""" + maintenance_request_uid + """\';
#                 """)

#         response["quote"] = quoteQuery

#         maintenance_quote_uid = response['quote']['result'][0]['maintenance_quote_uid']

#     payload = {"maintenance_request_uid":maintenance_request_uid,"maintenance_request_status":"PROCESSING"}

#     response = requests.put(ENDPOINT + "/maintenanceRequests", data = payload)
#     assert response.status_code == 200
# # Request URL:
# # https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/maintenanceQuotes
# # Request Method:
# # PUT
#     payload = {'maintenance_quote_uid': maintenance_quote_uid, 'quote_maintenance_request_id': '800-000180', 'quote_business_id': '600-000039', 'quote_services_expenses': '{"per Hour Charge":"10","event_type":5,"service_name":"Labor","parts":[{"part":"250","quantity":"1","cost":"250"}],"labor":[{"description":"","hours":5,"rate":"10"}],"total_estimate":50}', 'quote_notes': 'vents', 'quote_status': 'SENT', 'quote_event_type': '5 Hour Job', 'quote_total_estimate': '300', 'quote_created_date': '2000-04-23 00:00:00', 'quote_earliest_available_date': '12-12-2023', 'quote_earliest_available_date': '00:00:00'}

#     response = requests.put(ENDPOINT + "/maintenanceQuotes", data = payload)

#     assert response.status_code == 200
# # Request URL:
# # https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/maintenanceQuotes
# # Request Method:
# # PUT
#     payload = {'maintenance_quote_uid': maintenance_quote_uid, 'quote_status': 'ACCEPTED'}

#     response = requests.put(ENDPOINT + "/maintenanceQuotes", data = payload)

#     assert response.status_code == 200
# # Request URL:
# # https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/maintenanceRequests
# # Request Method:
# # PUT
#     payload = {"maintenance_request_uid":maintenance_request_uid,"maintenance_request_status":"SCHEDULED","maintenance_scheduled_date":"10/30/2023","maintenance_scheduled_time":"10:00:00"}

#     response = requests.put(ENDPOINT + "/maintenanceRequests", data = payload)

#     assert response.status_code == 200
# # Request URL:
# # https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/maintenanceQuotes
# # Request Method:
# # PUT
#     payload = {'maintenance_quote_uid': maintenance_quote_uid, 'quote_status': 'SCHEDULED'}

#     response = requests.put(ENDPOINT + "/maintenanceQuotes", data = payload)

#     assert response.status_code == 200
# # Request URL:
# # https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/maintenanceRequests
# # Request Method:
# # PUT
#     payload = {"maintenance_request_uid":maintenance_request_uid,"maintenance_request_status":"COMPLETED"}

#     response = requests.put(ENDPOINT + "/maintenanceRequests", data=payload)

#     assert response.status_code == 200
# # Request URL:
# # Request URL:
# # https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/maintenanceQuotes
# # Request Method:
# # PUT
#     payload = {'maintenance_quote_uid': maintenance_quote_uid,'quote_status': 'FINISHED'}

#     response = requests.put(ENDPOINT + "/maintenanceQuotes", data=payload)

#     assert response.status_code == 200
# # Request URL:
# # https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/maintenanceRequests
# # Request Method:
# # PUT
#     payload = {"maintenance_request_uid":maintenance_request_uid,"maintenance_request_status":"PAID"}

#     response = requests.put(ENDPOINT + "/maintenanceRequests", data=payload)

#     assert response.status_code == 200
# # Request URL:
# # https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/maintenanceQuotes
# # Request Method:
# # PUT

#     payload = {'maintenance_quote_uid': maintenance_quote_uid,'quote_status': 'COMPLETED'}

#     response = requests.put(ENDPOINT + "/maintenanceQuotes", data=payload)

#     assert response.status_code == 200

#     response = {}
#     response1 = {}
#     with connect() as db:
#         delQuery = ("""
#                             DELETE space.maintenanceRequests
#                             FROM space.maintenanceRequests
#                             WHERE maintenance_request_uid = \'""" + maintenance_request_uid + """\';
#                             """)


#         response["delete_req"] = db.delete(delQuery)

#         delQuery = ("""
#                                     DELETE space.maintenanceQuotes
#                                     FROM space.maintenanceQuotes
#                                     WHERE maintenance_quote_uid = \'""" + maintenance_quote_uid + """\';
#                                     """)

#         response1["delete_quote"] = db.delete(delQuery)

#     print(response)
#     print(response1)
