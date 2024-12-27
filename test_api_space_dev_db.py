import pymysql
from datetime import datetime
import json
from decimal import Decimal
import requests
from dotenv import load_dotenv
from flask_restful import Resource
import os

#python -m pytest -v -s Use this in cmd to run the pytest script


def connect():
    conn = pymysql.connect(
        host=os.getenv('RDS_HOST'),
        user=os.getenv('RDS_USER'),
        port=int(os.getenv('RDS_PORT')),
        passwd=os.getenv('RDS_PW'),
        db='space_dev',
        # db=os.getenv('RDS_DB'),
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


# ENDPOINT = "https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev"
ENDPOINT = os.getenv('ENDPOINT')
POSTMAN_SECRET = os.getenv('POSTMAN_SECRET')

# Tables affecting: {maintenanceRequests, maintenanceQuotes, Properties, Property_Owner, Contracts, leases, lease_tenant, addPurchases, paymentMethods, payments}
class endPointTest_space_dev_db_CLASS(Resource):
    def get(self):
        dt = datetime.today()
        response = {}
        # Insert temporary data into the database
        try:
            print("\n\n*** Inserting temporary data into the database ***\n")
            insert_property_query = """
                                        INSERT INTO `space_dev`.`properties` (`property_uid`, `property_available_to_rent`, `property_active_date`, `property_listed_date`, `property_address`, `property_city`, `property_state`, `property_zip`, `property_longitude`, `property_latitude`, `property_type`, `property_num_beds`, `property_num_baths`, `property_value`, `property_value_year`, `property_area`, `property_listed_rent`, `property_deposit`, `property_pets_allowed`, `property_deposit_for_rent`, `property_featured`) 
                                        VALUES ('200-000000', '1', '10-28-2024', '10-28-2024', '123 Test Apt', 'San Jose', 'CA', '95119', '-121.7936071000', '37.2346668000', 'Single Family', '4', '2', '1500000', '2013', '2100', '2500', '1200', '1', '0', 'False');
                                    """
            
            insert_property_owner_query =   """
                                                INSERT INTO `space_dev`.`property_owner` (`property_id`, `property_owner_id`, `po_owner_percent`) 
                                                VALUES ('200-000000', '110-000000', '1');
                                            """
            
            insert_users_query = """
                                    INSERT INTO `space_dev`.`users` (`user_uid`, `first_name`, `last_name`, `phone_number`, `email`, `role`, `notifications`, `dark_mode`, `cookies`) 
                                    VALUES ('100-000000', 'Test', 'Account', '(000) 000-0000', 'test@gmail.com', 'OWNER,MANAGER,TENANT,MAINTENANCE', 'true', 'false', 'true');
                                """
            
            insert_business_profile_query = """
                                                INSERT INTO `space_dev`.`businessProfileInfo` (`business_uid`, `business_user_id`, `business_type`, `business_name`, `business_phone_number`, `business_email`) 
                                                VALUES ('600-000000', '100-000000', 'MANAGEMENT', 'Reserved For Test', '(000) 000-0000', 'test@gmail.com');
                                            """

            insert_owner_profile_query = """
                                            INSERT INTO `space_dev`.`ownerProfileInfo` (`owner_uid`, `owner_user_id`, `owner_first_name`, `owner_last_name`, `owner_phone_number`, `owner_email`) 
                                            VALUES ('110-000000', '100-000000', 'Test', 'Account', '(000) 000-0000', 'test@gmail.com');
                                        """
            
            insert_tenant_profile_query = """
                                            INSERT INTO `space_dev`.`tenantProfileInfo` (`tenant_uid`, `tenant_user_id`, `tenant_first_name`, `tenant_last_name`, `tenant_email`, `tenant_phone_number`, `tenant_documents`, `tenant_adult_occupants`, `tenant_children_occupants`, `tenant_vehicle_info`, `tenant_references`, `tenant_pet_occupants`, `tenant_employment`) 
                                            VALUES ('350-000000', '100-000000', 'Test', 'Account', 'test@gmail.com', '(000) 000-0000', '[]', '[]', '[]', '[]', '[]', '[]', '[]');
                                        """

            insert_purchases_query = """
                                        INSERT INTO `space_dev`.`purchases` (`purchase_uid`, `pur_timestamp`, `pur_property_id`, `purchase_type`, `pur_description`, `pur_notes`, `pur_cf_type`, `purchase_date`, `pur_due_date`, `pur_amount_due`, `purchase_status`, `pur_status_value`, `pur_receiver`, `pur_initiator`, `pur_payer`, `pur_late_Fee`, `pur_group`) 
                                        VALUES ('400-000000', '11-14-2024 00:00', '200-000000', 'Deposit', 'Test Deposit', 'Test Deposit Note', 'revenue', '11-15-2024 00:00', '11-30-2024 00:00', '299.00', 'UNPAID', '0', '110-000000', '350-000000', '350-000000', '0', '400-000000');
                                    """
            
            insert_maintenance_requests_query = """
                                                    INSERT INTO `space_dev`.`maintenanceRequests` (`maintenance_request_uid`, `maintenance_property_id`, `maintenance_request_status`, `maintenance_title`, `maintenance_request_type`, `maintenance_request_created_by`, `maintenance_priority`, `maintenance_can_reschedule`) 
                                                    VALUES ('800-000000', '200-000000', 'NEW', 'Test Maintenance Request', 'Plumbing', '600-000000', 'Medium', '1');
                                                """

            insert_maintenance_quotes_query = """
                                                INSERT INTO `space_dev`.`maintenanceQuotes` (`maintenance_quote_uid`, `quote_maintenance_request_id`, `quote_status`, `quote_business_id`, `quote_requested_date`) 
                                                VALUES ('900-000000', '800-000000', 'SCHEDULED', '600-000000', '11-12-2024 15:26:30');
                                            """
            
            insert_contracts_query = """
                                        INSERT INTO `space_dev`.`contracts` (`contract_uid`, `contract_property_id`, `contract_business_id`, `contract_name`, `contract_status`, `contract_m2m`) 
                                        VALUES ('010-000000', '200-000000', '600-000000', 'Test Contract Name', 'ACTIVE', '1');
                                    """
            update_contracts_query = """
                                        UPDATE `space_dev`.`contracts` 
                                        SET `contract_uid` = '010-000000' 
                                        WHERE (`contract_property_id` = '200-000000' AND `contract_business_id` = '600-000000' AND `contract_name` = 'Test Contract Name');
                                    """

            insert_leases_query = """
                                    INSERT INTO `space_dev`.`leases` (`lease_uid`, `lease_property_id`, `lease_application_date`, `lease_start`, `lease_end`, `lease_status`, `lease_assigned_contacts`, `lease_documents`, `lease_renew_status`, `lease_move_in_date`, `lease_adults`, `lease_children`, `lease_pets`, `lease_vehicles`, `lease_referred`, `lease_effective_date`, `lease_docuSign`, `lease_end_notice_period`, `lease_income`, `lease_m2m`, `lease_utilities`) 
                                    VALUES ('300-000000', '200-000000', '11-15-2024', '11-19-2024', '11-19-2025', 'NEW', '[\"350-000000\"]', '[]', 'TRUE', '11-19-2024', '[]', '[]', '[]', '[]', '[]', '11-19-2024', 'null', '30', '[]', '1', '[]');
                                """
            update_leases_query = """
                                    UPDATE `space_dev`.`leases` 
                                    SET `lease_uid` = '300-000000' 
                                    WHERE (`lease_property_id` = '200-000000' AND `lease_status` = 'NEW');
                                """

            insert_lease_tenant_query = """
                                            INSERT INTO `space_dev`.`lease_tenant` (`lt_lease_id`, `lt_tenant_id`, `lt_responsibility`) 
                                            VALUES ('300-000000', '350-000000', '1');
                                        """
            
            insert_lease_fees_query = """
                                        INSERT INTO `space_dev`.`leaseFees` (`leaseFees_uid`, `fees_lease_id`, `fee_name`, `fee_type`, `charge`, `frequency`, `available_topay`, `due_by`, `late_by`, `late_fee`, `perDay_late_fee`, `due_by_date`) 
                                        VALUES ('370-000000', '300-000000', 'Rent', 'Rent', '900.00', 'Monthly', '10', '5', '0', '0', '0.00', '12-19-2024');
                                    """
            update_lease_fees_query = """
                                    UPDATE `space_dev`.`leaseFees` 
                                    SET `leaseFees_uid` = '370-000000' 
                                    WHERE (`fees_lease_id` = '300-000000' AND `fee_name` = 'Rent');
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
                update_contracts_query_response = db.execute(update_contracts_query, cmd='post')
                # insert_leases_query_response = db.execute(insert_leases_query, cmd='post')
                # update_leases_query_response = db.execute(update_leases_query, cmd='post')
                # insert_lease_tenant_query_response = db.execute(insert_lease_tenant_query, cmd='post')
                # insert_lease_fees_query_response = db.execute(insert_lease_fees_query, cmd='post')
                # update_lease_fees_query_response = db.execute(update_lease_fees_query, cmd='post')

            print("\n*** Completed ***\n")
            response['insert_temporary_data'] = 'Passed'

        except:
            response['insert_temporary_data'] = 'Failed'

        if response['insert_temporary_data'] != 'Passed':
            return response['insert_temporary_data']
        
        response['No of APIs tested'] = 0
        response['APIs running successfully'] = []
        response['APIs failing'] = []
        response['Error in running APIs'] = []

        headers = {
                    'Postman-Secret': POSTMAN_SECRET
                }
        json_headers = {
                    'Content-Type': 'application/json',
                    'Postman-Secret': POSTMAN_SECRET
                } 

        try:
            # ------------------------- MAINTENANCE ------------------------------
            maintenance_request_uid = ""
            maintenance_quote_uid = ""
            try:
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
                post_maintenance_request_response = requests.post(ENDPOINT + "/maintenanceRequests", data = post_maintenance_request_payload, headers=headers)
                maintenance_request_uid = post_maintenance_request_response.json()['maintenance_request_uid']
                if post_maintenance_request_response.status_code == 200:
                    response['APIs running successfully'].append('POST Maintenance Requests')
                else:
                    response['APIs failing'].append('POST Maintenance Requests')
                response['No of APIs tested'] += 1

                # -------- test post get maintenance request --------
                print("\nIn test GET after POST Maintenance Requests")
                post_get_maintenance_request_response = requests.get(ENDPOINT + f"/maintenanceReq/200-000000", headers=headers)
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
                put_maintenance_request_response = requests.put(ENDPOINT + "/maintenanceRequests", data = put_maintenance_request_payload, headers=headers) 
                if put_maintenance_request_response.status_code == 200:
                    response['APIs running successfully'].append('PUT Maintenance Requests')
                else:
                    response['APIs failing'].append('PUT Maintenance Requests')
                response['No of APIs tested'] += 1

                # -------- test put get maintenance request --------
                print("\nIn test GET after PUT Maintenance Requests")
                put_get_maintenance_request_response = requests.get(ENDPOINT + f"/maintenanceReq/200-000000", headers=headers)
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
                post_maintenance_quotes_response = requests.post(ENDPOINT + "/maintenanceQuotes", data = post_maintenance_quotes_payload, headers=headers)
                maintenance_quote_uid = post_maintenance_quotes_response.json()['maintenance_quote_uid']
                if post_maintenance_quotes_response.status_code == 200:
                    response['APIs running successfully'].append('POST Maintenance Quotes')
                else:
                    response['APIs failing'].append('POST Maintenance Quotes')
                response['No of APIs tested'] += 1

                # -------- test post get maintenance quotes --------
                print("\nIn test GET after POST Maintenance Quotes")
                post_get_maintenance_quotes_response = requests.get(ENDPOINT + f"/maintenanceQuotes/600-000000", headers=headers)
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
                put_maintenance_quotes_response = requests.put(ENDPOINT + "/maintenanceQuotes", data = put_maintenance_quotes_payload, headers=headers)
                if put_maintenance_quotes_response.status_code == 200:
                    response['APIs running successfully'].append('PUT Maintenance Quotes')
                else:
                    response['APIs failing'].append('PUT Maintenance Quotes')
                response['No of APIs tested'] += 1

                # -------- test put get maintenance quotes --------
                print("\nIn test GET after PUT Maintenance Quotes")
                put_get_maintenance_quotes_response = requests.get(ENDPOINT + f"/maintenanceQuotes/600-000000", headers=headers)
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

                # -------- test get maintenance quotes by uid--------
                print("\nIn test GET Maintenance Quotes By UID")
                get_maintenance_quotes_by_uid_response = requests.get(ENDPOINT + f"/maintenanceQuotes/{maintenance_quote_uid}", headers=headers)
                if get_maintenance_quotes_by_uid_response.status_code == 200:
                    response['APIs running successfully'].append('GET Maintenance Quotes By UID')
                else:
                    response['APIs failing'].append('GET Maintenance Quotes By UID')
                response['No of APIs tested'] += 1

            except:
                response['Error in running APIs'].append('Maintenance API')
            
            finally:
                # -------- delete data from Maintenance Requests and Maintenance Quotes --------
                print("\nIn delete data from Maintenance Requests and Maintenance Quotes")
                print(f"Deleting {maintenance_request_uid} from Maintenance Requests and {maintenance_quote_uid} from Maintenance Quotes")
                with connect() as db:
                    if maintenance_request_uid != "":
                        delQuery_maintenance_req = ("""
                                        DELETE space_dev.maintenanceRequests
                                        FROM space_dev.maintenanceRequests
                                        WHERE maintenance_request_uid = \'""" + maintenance_request_uid + """\';
                                    """)
                        maintenance_req_response = db.delete(delQuery_maintenance_req)

                    if maintenance_quote_uid != "":
                        delQuery_maintenance_quotes = ("""
                                        DELETE space_dev.maintenanceQuotes
                                        FROM space_dev.maintenanceQuotes
                                        WHERE maintenance_quote_uid = \'""" + maintenance_quote_uid + """\';
                                    """)
                        maintenance_quotes_response = db.delete(delQuery_maintenance_quotes)
            

            # ------------------------- Properties ------------------------------
            try:
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
                post_properties_response = requests.post(ENDPOINT + "/properties", data=post_properties_payload, headers=headers)
                property_uid = post_properties_response.json()['property_UID']
                if post_properties_response.status_code == 200:
                    response['APIs running successfully'].append('POST Properties')
                else:
                    response['APIs failing'].append('POST Properties')
                response['No of APIs tested'] += 1

                # -------- test get after post properties --------
                print("\nIn test GET after POST Properties")
                post_get_properties_response = requests.get(ENDPOINT + f"/properties/{property_uid}", headers=headers)
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
                put_properties_response = requests.put(ENDPOINT + "/properties", data=put_properties_payload, headers=headers)
                if put_properties_response.status_code == 200:
                    response['APIs running successfully'].append('PUT Properties')
                else:
                    response['APIs failing'].append('PUT Properties')
                response['No of APIs tested'] += 1

                # -------- test get after put properties --------
                print("\nIn GET after PUT Properties")
                put_get_properties_response = requests.get(ENDPOINT + f"/properties/{property_uid}", headers=headers)
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
                delete_properties_response = requests.delete(ENDPOINT + "/properties", data=json.dumps(delete_properties_payload), headers=json_headers)
                if delete_properties_response.status_code == 200:
                    response['APIs running successfully'].append('DELETE Properties')
                else:
                    response['APIs failing'].append('DELETE Properties')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Properties API')


            # ------------------------- Contracts ------------------------------
            contract_uid = ""
            try:
                # -------- test post contracts --------
                print("\nIn POST Contract")
                post_contract_payload = {
                    "contract_property_ids": '["200-000000"]',
                    "contract_business_id": "600-000000",
                    "contract_start_date": "11-01-2024",
                    "contract_status": "NEW"
                }
                post_contract_response = requests.post(ENDPOINT + "/contracts", data=post_contract_payload, headers=headers)
                contract_uid = post_contract_response.json()['contract_uid']
                print('\nContract UID', contract_uid)
                if post_contract_response.status_code == 200:
                    response['APIs running successfully'].append('POST Contracts')
                else:
                    response['APIs failing'].append('POST Contracts')
                response['No of APIs tested'] += 1

                # -------- test get after contracts --------
                print("\nIn GET after POST Contract")
                post_get_contract_response = requests.get(ENDPOINT + "/contracts/600-000000", headers=headers)
                data = post_get_contract_response.json()['result'][0]
                if data.get('contract_uid', None) != contract_uid:
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
                put_contract_response = requests.put(ENDPOINT + "/contracts", data=put_contract_payload, headers=headers)
                if put_contract_response.status_code == 200:
                    response['APIs running successfully'].append('PUT Contracts')
                else:
                    response['APIs failing'].append('PUT Contracts')
                response['No of APIs tested'] += 1

                # -------- test get after put contracts --------
                print("\nIn GET after PUT Contract")
                put_get_contract_response = requests.get(ENDPOINT + "/contracts/600-000000", headers=headers)
                data = put_get_contract_response.json()['result'][0]
                if data.get('contract_uid', None) != contract_uid:
                    print("Not a match")
                if put_get_contract_response.status_code == 200:
                    response['APIs running successfully'].append('PUT Contracts')
                else:
                    response['APIs failing'].append('PUT Contracts')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Contracts API')
            
            finally:
                # -------- test delete contracts --------
                print("\nIn DELETE Contract")
                print(f"Deleting {contract_uid} from Contract Table")
                with connect() as db:
                    if contract_uid != "":
                        delQuery_contracts = ("""
                                        DELETE FROM space_dev.contracts
                                        WHERE contract_uid = \'""" + contract_uid + """\';
                                    """)
                        contract_response = db.delete(delQuery_contracts)


            # ------------------------- Leases ------------------------------
            curr_lease_api = ""
            lease_uid = ""
            try:
                # -------- test post lease application --------
                print("\nIn test POST Lease Application")
                curr_lease_api = "POST"
                post_lease_application_payload = {
                    "lease_property_id":"200-000000",
                    "lease_start":"01-31-2024",
                    "lease_end":"01-30-2025",
                    "lease_application_date":"06-27-2024",
                    "tenant_uid":"350-000000",
                    "lease_status":"NEW"
                }
                post_lease_application_response = requests.post(ENDPOINT + "/leaseApplication", data=post_lease_application_payload, headers=headers)
                if (post_lease_application_response.status_code == 200):
                    response['APIs running successfully'].append('POST Lease Application')
                else:
                    response['APIs failing'].append('POST Lease Application')
                response['No of APIs tested'] += 1

                # -------- get lease uid --------
                curr_lease_api = "GET uid"
                print("\nIn get lease_uid")
                get_lease_uid_response = requests.get(ENDPOINT + "/leaseApplication/350-000000/200-000000", headers=headers)
                lease_uid = get_lease_uid_response.json()
                print("lease_uid", lease_uid)

                # -------- test get after post lease details --------
                print("\nIn test GET after POST Lease Application")
                curr_lease_api = "GET POST"
                post_get_lease_application_response = requests.get(ENDPOINT + "/leaseDetails/350-000000", headers=headers)
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
                curr_lease_api = "PUT"
                put_lease_application_payload = {
                    "lease_uid":f"{lease_uid}",
                    "lease_status":"PROCESSING"
                }
                put_lease_application_response = requests.put(ENDPOINT + "/leaseApplication", data=put_lease_application_payload, headers=headers)
                if (put_lease_application_response.status_code == 200):
                    response['APIs running successfully'].append('PUT Lease Application')
                else:
                    response['APIs failing'].append('PUT Lease Application')
                response['No of APIs tested'] += 1

                # -------- test get after put lease details --------
                print("\nIn test GET after PUT Lease Application")
                curr_lease_api = "GET PUT"
                put_get_lease_application_response = requests.get(ENDPOINT + "/leaseDetails/350-000000", headers=headers)
                # data = put_get_lease_application_response.json()['Lease_Details']['result'][0]
                # if data['lease_status'] != "PROCESSING":
                #     print('Not Match')
                if (put_get_lease_application_response.status_code == 200):
                    response['APIs running successfully'].append('GET after PUT Lease Application')
                else:
                    response['APIs failing'].append('GET after PUT Lease Application')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Lease API')
            
            finally:
                # -------- test delete lease --------
                print("\nIn DELETE Lease")
                print(f"Deleting {lease_uid} from Lease Table & {lease_uid} from Lease_tenant")
                with connect() as db:
                    if curr_lease_api != "" and curr_lease_api != "POST" and lease_uid != "":
                        delQuery_leases = ("""
                                        DELETE FROM space_dev.leases
                                        WHERE lease_uid = \'""" + lease_uid + """\';
                                    """)
                        delQuery_lease_tenant = ("""
                                        DELETE FROM space_dev.lease_tenant
                                        WHERE lt_lease_id = \'""" + lease_uid + """\';
                                    """)
                        leases_response = db.delete(delQuery_leases)
                        lease_tenant_response = db.delete(delQuery_lease_tenant)


            # ------------------------- Payment Method ------------------------------
            try:
                # -------- test POST Payment Method --------
                print("\nIn POST Payment Method")
                post_payment_method_payload = {
                    "paymentMethod_profile_id": "110-000000",
                    "paymentMethod_type":"zelle",
                    "paymentMethod_name":"test123",
                    "paymentMethod_status":"Active"
                }
                post_payment_method_response = requests.post(ENDPOINT + "/paymentMethod", data=json.dumps(post_payment_method_payload), headers=json_headers)
                if (post_payment_method_response.status_code == 200):
                    response['APIs running successfully'].append('POST Payment Method')
                else:
                    response['APIs failing'].append('POST Payment Method')
                response['No of APIs tested'] += 1

                # -------- test GET Payment Method UID --------
                print("\nIn GET Payment Method UID")
                get_payment_method_uid = requests.get(ENDPOINT + "/paymentMethod/110-000000", headers=headers)
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
                put_payment_method_response = requests.put(ENDPOINT + "/paymentMethod", data=json.dumps(put_payment_method_payload), headers=json_headers)
                if (put_payment_method_response.status_code == 200):
                    response['APIs running successfully'].append('PUT Payment Method')
                else:
                    response['APIs failing'].append('PUT Payment Method')
                response['No of APIs tested'] += 1

                # -------- test DELETE Payment Method --------
                print("\nIn DELETE Payment Method")
                delete_payment_method_response = requests.delete(ENDPOINT + f"/paymentMethod/110-000000/{payment_method_uid}", headers=headers)
                if (delete_payment_method_response.status_code == 200):
                    response['APIs running successfully'].append('DELETE Payment Method')
                else:
                    response['APIs failing'].append('DELETE Payment Method')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Payment Method API')


            # ------------------------- Add Purchases ------------------------------
            purchase_uid = ""
            curr_pur_pay_api = ""
            try:
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
                post_add_purchase_response = requests.post(ENDPOINT + "/addPurchase", data=post_add_purchase_payload, headers=headers)
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
                put_add_purchase_response = requests.put(ENDPOINT + "/addPurchase", data=put_add_purchase_payload, headers=headers)
                if (put_add_purchase_response.status_code == 200):
                    response['APIs running successfully'].append('PUT Add Purchases')
                else:
                    response['APIs failing'].append('PUT Add Purchases')
                response['No of APIs tested'] += 1

                if purchase_uid == "":
                    raise Exception('No Purchase UID')

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
                post_payment_response = requests.post(ENDPOINT + "/makePayment", data=json.dumps(post_payment_payload), headers=json_headers)
                if (post_payment_response.status_code == 200):
                    response['APIs running successfully'].append('POST New Payments')
                else:
                    response['APIs failing'].append('POST New Payments')
                response['No of APIs tested'] += 1
                curr_pur_pay_api = "Completed"
            
            except:
                response['Error in running APIs'].append('Purchase & Payment API')
            
            finally:
                # -------- test DELETE Add purchases and New Payments --------
                print("\nIn DELETE add purchase")
                print(f"\nDeleting purchase_uid: {purchase_uid} from Purchases Table and payments with same purchase_uid from Payments Table")
                with connect() as db:
                    if purchase_uid != "":
                        delQuery_add_purchase = ("""
                                        DELETE FROM space_dev.purchases
                                        WHERE purchase_uid = \'""" + purchase_uid + """\';
                                    """)
                        del_add_purchase_response = db.delete(delQuery_add_purchase)

                    if purchase_uid != "" and curr_pur_pay_api == "Completed":
                        delQuery_payment = ("""
                                DELETE FROM space_dev.payments
                                WHERE pay_purchase_id = \'""" + purchase_uid + """\' AND paid_by = '350-000000'
                            """)
                        del_payment_response = db.delete(delQuery_payment)
            

            # ------------------------- Dashboard ------------------------------
            try:
                # -------- test GET Dashboard --------
                print("\nIn GET Dashboard")
                business_response = requests.get(ENDPOINT + "/dashboard/600-000000", headers=headers)
                owner_response = requests.get(ENDPOINT + "/dashboard/110-000000", headers=headers)
                tenant_response = requests.get(ENDPOINT + "/dashboard/350-000000", headers=headers)
                if (business_response.status_code == 200 and owner_response.status_code == 200 and tenant_response.status_code == 200):
                    response['APIs running successfully'].append('GET Dashboard')
                else:
                    response['APIs failing'].append('GET Dashboard')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Dashboard API')


            # ------------------------- Profiles ------------------------------
            owner_uid = ""
            business_uid = ""
            employee_uid = ""
            tenant_uid = ""
            try:
                # -------- test POST Profile --------
                print("\nIn test POST Profile")
                post_owner_profile_payload = {
                    "owner_user_id": "100-000000",
                    "owner_first_name": "Test",
                    "owner_last_name": "Owner Account",
                    "owner_phone_number": "(000) 000-0000",
                    "owner_email": "test@gmail.com"
                }
                post_owner_profile_response = requests.post(ENDPOINT + "/profile", data=post_owner_profile_payload, headers=headers)
                owner_uid = post_owner_profile_response.json()["owner_uid"]
                
                post_business_profile_payload = {
                    "business_user_id": "100-000000",
                    "business_type": "Management",
                    "business_name": "Test Business Account",
                    "business_email": "test@gmail.com",
                }
                post_business_profile_response = requests.post(ENDPOINT + "/profile", data=post_business_profile_payload, headers=headers)
                business_uid = post_business_profile_response.json()["business_uid"]
                employee_uid = post_business_profile_response.json()["employee_uid"]

                post_tenant_profile_payload = {
                    "tenant_user_id": "100-000000",
                    "tenant_first_name": "Test",
                    "tenant_last_name": "Tenant Account",
                    "tenant_email": "test@gmail.com",
                    "tenant_phone_number": "(000) 000-0000"
                }
                post_tenant_profile_response = requests.post(ENDPOINT + "/profile", data=post_tenant_profile_payload, headers=headers)
                tenant_uid = post_tenant_profile_response.json()["tenant_uid"]

                if (post_owner_profile_response.status_code == 200 and post_business_profile_response.status_code == 200 and post_tenant_profile_response.status_code == 200):
                    response['APIs running successfully'].append('POST Profile')
                else:
                    response['APIs failing'].append('POST Profile')
                response['No of APIs tested'] += 1

                # -------- test GET after POST Profile --------
                print("\nIn GET after POST Profile")
                post_get_owner_profile_response = requests.get(ENDPOINT + f"/profile/{owner_uid}", headers=headers)
                data = post_get_owner_profile_response.json()['profile']['result'][0]
                if data["owner_first_name"] != "Test":
                    print("Not Match")

                post_get_business_profile_response = requests.get(ENDPOINT + f"/profile/{business_uid}", headers=headers)
                data = post_get_business_profile_response.json()['profile']['result'][0]
                if data["business_type"] != "Management":
                    print("Not Match")

                post_get_tenant_profile_response = requests.get(ENDPOINT + f"/profile/{tenant_uid}", headers=headers)
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
                put_owner_profile_response = requests.put(ENDPOINT + "/profile", data=put_owner_profile_payload, headers=headers)

                put_business_profile_payload = {
                    "business_uid": f"{business_uid}",
                    "business_type": "Maintenance",
                }
                put_business_profile_response = requests.put(ENDPOINT + "/profile", data=put_business_profile_payload, headers=headers)

                put_tenant_profile_payload = {
                    "tenant_uid": f"{tenant_uid}",
                    "tenant_first_name": "Test Tenant",
                }
                put_tenant_profile_response = requests.put(ENDPOINT + "/profile", data=put_tenant_profile_payload, headers=headers)
                
                if (put_owner_profile_response.status_code == 200 and put_business_profile_response.status_code == 200 and put_tenant_profile_response.status_code == 200):
                    response['APIs running successfully'].append('PUT Profile')
                else:
                    response['APIs failing'].append('PUT Profile')
                response['No of APIs tested'] += 1

                # -------- test GET after PUT Profile --------
                print("\nIn test GET after PUT Profile")
                put_get_owner_profile_response = requests.get(ENDPOINT + f"/profile/{owner_uid}", headers=headers)
                data = put_get_owner_profile_response.json()['profile']['result'][0]
                if data["owner_first_name"] != "Test Owner":
                    print("Not Match")

                put_get_business_profile_response = requests.get(ENDPOINT + f"/profile/{business_uid}", headers=headers)
                data = put_get_business_profile_response.json()['profile']['result'][0]
                if data["business_type"] != "Maintenance":
                    print("Not Match")

                put_get_tenant_profile_response = requests.get(ENDPOINT + f"/profile/{tenant_uid}", headers=headers)
                data = put_get_tenant_profile_response.json()['profile']['result'][0]
                if data["tenant_first_name"] != "Test Tenant":
                    print("Not Match")

                if (put_get_owner_profile_response.status_code == 200 and put_get_business_profile_response.status_code == 200 and put_get_tenant_profile_response.status_code == 200):
                    response['APIs running successfully'].append('GET after PUT Profile')
                else:
                    response['APIs failing'].append('GET after PUT Profile')
                response['No of APIs tested'] += 1

            except:
                response['Error in running APIs'].append('Profile API')
            
            finally:
                # -------- test DELETE Profile --------
                print("\nIn DELETE Profile")
                print(f"Deleting {owner_uid} from Owner Table, {business_uid} from Business Table, {employee_uid} from Employee Table & {tenant_uid} from Tenant Table")
                with connect() as db:
                    if owner_uid != "":
                        delQuery_owner = ("""
                                        DELETE FROM space_dev.ownerProfileInfo
                                        WHERE owner_uid = \'""" + owner_uid + """\';
                                    """)
                        del_owner_profile_response = db.delete(delQuery_owner)
                    if business_uid != "":
                        delQuery_business = ("""
                                        DELETE FROM space_dev.businessProfileInfo
                                        WHERE business_uid = \'""" + business_uid + """\';
                                    """)
                        del_business_profile_response = db.delete(delQuery_business)
                    if employee_uid != "":
                        delQuery_employee = ("""
                                        DELETE FROM space_dev.employees
                                        WHERE employee_uid = \'""" + employee_uid + """\';
                                    """)
                        del_employee_profile_response = db.delete(delQuery_employee)
                    if tenant_uid != "":
                        delQuery_tenant = ("""
                                        DELETE FROM space_dev.tenantProfileInfo
                                        WHERE tenant_uid = \'""" + tenant_uid + """\';
                                    """)
                        del_tenant_profile_response = db.delete(delQuery_tenant)
            

            # ------------------------- Add Expense / Add Revenue ------------------------------
            expense_uid = ""
            revenue_uid = ""
            try:
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
                post_add_expense_response = requests.post(ENDPOINT + "/addExpense", data=json.dumps(post_add_expense_payload), headers=json_headers)
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
                put_add_purchase_response = requests.put(ENDPOINT + "/addExpense", data=put_add_purchase_payload, headers=headers)
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
                post_add_revenue_response = requests.post(ENDPOINT + "/addRevenue", data=json.dumps(post_add_revenue_payload), headers=json_headers)
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
                put_add_revenue_response = requests.put(ENDPOINT + "/addRevenue", data=put_add_revenue_payload, headers=headers)
                if (put_add_revenue_response.status_code == 200):
                    response['APIs running successfully'].append('PUT Add Revenue')
                else:
                    response['APIs failing'].append('PUT Add Revenue')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Add Expense / Add Revenue API')
            
            finally:
                # -------- test DELETE add expense / add revenue --------
                print("\nIn DELETE add expense / add revenue")
                print(f"\nDeleting purchase_uid: {expense_uid} from Purchases Table (for expense) and purchase_uid: {revenue_uid} from Purchases Table (for revenue)")
                with connect() as db:
                    if expense_uid != "":
                        delQuery_add_expense = ("""
                                        DELETE FROM space_dev.purchases
                                        WHERE purchase_uid = \'""" + expense_uid + """\';
                                    """)
                        del_add_expense_response = db.delete(delQuery_add_expense)
                    if revenue_uid != "":
                        delQuery_add_revenue = ("""
                                        DELETE FROM space_dev.purchases
                                        WHERE purchase_uid = \'""" + revenue_uid + """\';
                                    """)
                        del_add_revenue_response = db.delete(delQuery_add_revenue)


            # ------------------------- Cashflow Transaction ------------------------------
            try:
                # -------- test GET Cashflow Transaction --------
                print("\nIn GET Cashflow Transaction")
                get_cashflow_response = requests.get(ENDPOINT + "/cashflowTransactions/600-000000/all", headers=headers)
                if (get_cashflow_response.status_code == 200):
                    response['APIs running successfully'].append('GET Cashflow Transaction')
                else:
                    response['APIs failing'].append('GET Cashflow Transaction')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Cashflow Transaction API')


            # ------------------------- Payment Verification ------------------------------
            try:
                # -------- test GET Payment Verification --------
                print("\nIn GET Payment Verification")
                get_payment_verification_response = requests.get(ENDPOINT + "/paymentVerification/600-000000", headers=headers)
                if (get_payment_verification_response.status_code == 200):
                    response['APIs running successfully'].append('GET Payment Verification')
                else:
                    response['APIs failing'].append('GET Payment Verification')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Payment Verification API')


            # ------------------------- Rents / Rent Deatils ------------------------------
            try:
                # -------- test GET Rents --------
                get_rents_response = requests.get(ENDPOINT + "/rents/110-000000", headers=headers)
                if (get_rents_response.status_code == 200):
                    response['APIs running successfully'].append('GET Rents')
                else:
                    response['APIs failing'].append('GET Rents')
                response['No of APIs tested'] += 1

                # -------- test GET Rent Details --------
                get_rent_details_response = requests.get(ENDPOINT + "/rentDetails/110-000000", headers=headers)
                if (get_rent_details_response.status_code == 200):
                    response['APIs running successfully'].append('GET Rent Details')
                else:
                    response['APIs failing'].append('GET Rent Details')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Rents / Rent Details API')


            # ------------------------- Appliances ------------------------------
            try:
                # -------- test POST Appliance --------
                print("\nIn POST Appliances")
                post_appliance_payload= {
                    "appliance_property_id":"200-000000",
                    "appliance_type":"050-000023",
                    "appliance_desc":"Test Appliance Description"
                }
                post_appliance_response = requests.post(ENDPOINT + "/appliances", data=post_appliance_payload, headers=headers)
                appliance_uid = post_appliance_response.json()['appliance_UID']
                if (post_appliance_response.status_code == 200):
                    response['APIs running successfully'].append('POST Appliances')
                else:
                    response['APIs failing'].append('POST Appliances')
                response['No of APIs tested'] += 1

                # -------- test GET after POST Appliance --------
                print("\nIn GET after POST Appliances")          
                get_post_appliance_response = requests.get(ENDPOINT + "/appliances/200-000000", headers=headers)
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
                put_appliance_response = requests.put(ENDPOINT + "/appliances", data=put_appliance_payload, headers=headers)
                if (put_appliance_response.status_code == 200):
                    response['APIs running successfully'].append('PUT Appliances')
                else:
                    response['APIs failing'].append('PUT Appliances')
                response['No of APIs tested'] += 1

                # -------- test GET after PUT Appliance --------
                print("\nIn GET after PUT Appliances")            
                get_put_appliance_response = requests.get(ENDPOINT + "/appliances/200-000000", headers=headers)
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
                delete_appliance_response = requests.delete(ENDPOINT + f"/appliances/{appliance_uid}", headers=headers)
                if (delete_appliance_response.status_code == 200):
                    response['APIs running successfully'].append('DELETE Appliances')
                else:
                    response['APIs failing'].append('DELETE Appliances')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Appliances API')


            # ------------------------- Employee / Employee Verification ------------------------------
            employee_uid = ""
            try:
                # -------- test POST Employee --------
                print("\nIn POST Employee")
                post_employee_payload = {
                    "employee_user_id":"100-000000",
                    "employee_business_id":"600-000000",
                    "employee_first_name":"Test",
                    "employee_last_name":"Employee"
                }
                post_employee_response = requests.post(ENDPOINT + "/employee", data=post_employee_payload, headers=headers)
                employee_uid = post_employee_response.json()['employee_uid']
                if (post_employee_response.status_code == 200):
                    response['APIs running successfully'].append('POST Employee')
                else:
                    response['APIs failing'].append('POST Employee')
                response['No of APIs tested'] += 1

                # -------- test GET after POST Employee --------
                print("\nIn GET after POST Employee")
                get_post_employee_response = requests.get(ENDPOINT + f"/employee/{employee_uid}", headers=headers)
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
                put_employee_verification_response = requests.put(ENDPOINT + "/employeeVerification", data=json.dumps(put_employee_verification_payload), headers=json_headers)
                if (put_employee_verification_response.status_code == 200):
                    response['APIs running successfully'].append('PUT Employee Verification')
                else:
                    response['APIs failing'].append('PUT Employee Verification')
                response['No of APIs tested'] += 1

                # -------- test GET after PUT Employee --------
                print("\nIn GET after PUT Employee")
                get_put_employee_response = requests.get(ENDPOINT + f"/employee/{employee_uid}", headers=headers)
                data = get_put_employee_response.json()['employee']['result'][0]
                if data["employee_first_name"] != "Test Account":
                    print("Not Match")
                if (get_put_employee_response.status_code == 200):
                    response['APIs running successfully'].append('GET after PUT Employee Verification')
                else:
                    response['APIs failing'].append('GET after PUT Employee Verification')
                response['No of APIs tested'] += 1

            except:
                response['Error in running APIs'].append('Employee API')
            
            finally:
                # -------- test DELETE Employee --------
                print("\nIn DELETE Employee")
                print(f"\nDeleting employee_uid: {employee_uid} from Employees Table")
                with connect() as db:
                    if employee_uid != "":
                        delQuery_employee = ("""
                                        DELETE FROM space_dev.employees
                                        WHERE employee_uid = \'""" + employee_uid + """\';
                                    """)
                        del_employee_response = db.delete(delQuery_employee)
            

            # ------------------------- Contacts ------------------------------
            try:
                # -------- test GET Contacts --------
                print("\nIn GET Contacts")
                get_contacts_business_response = requests.get(ENDPOINT + "/contacts/600-000000", headers=headers)
                get_contacts_owner_response = requests.get(ENDPOINT + "/contacts/110-000000", headers=headers)
                get_contacts_tenant_response = requests.get(ENDPOINT + "/contacts/350-000000", headers=headers)
                if (get_contacts_business_response.status_code == 200 and get_contacts_owner_response.status_code == 200 and get_contacts_tenant_response.status_code == 200):
                    response['APIs running successfully'].append('GET Contacts')
                else:
                    response['APIs failing'].append('GET Contacts')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Contacts API')


            # ------------------------- SearchManager ------------------------------
            try:
                # -------- test GET SearchManager --------
                print("\nIn GET Search Manager")
                get_search_manager_response = requests.get(ENDPOINT + "/searchManager", headers=headers)
                if (get_search_manager_response.status_code == 200):
                    response['APIs running successfully'].append('GET Search Manager')
                else:
                    response['APIs failing'].append('GET Search Manager')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Search Manager API')


            # ------------------------- Utility ------------------------------
            curr_uti_api = ""
            try:
                # -------- test POST Utility --------
                print("\nIn POST Utility")
                post_utility_payload = {
                    "property_uid": "200-000000",
                    "property_utility":json.dumps({
                        "050-000000":"050-000000"
                    })
                }
                post_utility_payload_response = requests.post(ENDPOINT + "/utilities", data=post_utility_payload, headers=headers)
                if (post_utility_payload_response.status_code == 200):
                    response['APIs running successfully'].append('POST Utility')
                else:
                    response['APIs failing'].append('POST Utility')
                response['No of APIs tested'] += 1
                curr_uti_api = "Completed"

                # -------- test GET after POST Utility --------
                print("\nIn GET after POST Utility")
                post_get_utility_response = requests.get(ENDPOINT + "/utilities?utility_property_id=200-000000", headers=headers)
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
                put_utility_payload_response = requests.put(ENDPOINT + "/utilities", data=put_utility_payload, headers=headers)
                if (put_utility_payload_response.status_code == 200):
                    response['APIs running successfully'].append('PUT Utility')
                else:
                    response['APIs failing'].append('PUT Utility')
                response['No of APIs tested'] += 1

                # -------- test GET after PUT Utility --------
                print("\nIn GET after PUT Utility")
                put_get_utility_response = requests.get(ENDPOINT + "/utilities?utility_property_id=200-000000", headers=headers)
                data = put_get_utility_response.json()['result'][0]
                if data['utility_payer_id'] != "050-100000":
                    print("Not Match")
                if (put_get_utility_response.status_code == 200):
                    response['APIs running successfully'].append('GET after PUT Utility')
                else:
                    response['APIs failing'].append('GET after PUT Utility')
                response['No of APIs tested'] += 1
            
            except:
                response["Error in running APIs"].append('Utilities API')
            
            finally:
                # -------- test DELETE Utility --------
                print("\nIn DELETE Utility")
                print(f"\nDeleting utility_property_id: '200-000000', utility_type_id: '050-000000' & utility_payer_id: '050-100000' from Employees Table")
                with connect() as db:
                    if curr_uti_api == "Completed":
                        delQuery_property_utility = ("""
                                        DELETE FROM space_dev.property_utility
                                        WHERE utility_property_id = "200-000000"
                                        AND utility_type_id = "050-000000"
                                        AND utility_payer_id = "050-100000";
                                    """)
                        del_property_utility_response = db.delete(delQuery_property_utility)


            # ------------------------- Bills ------------------------------
            bill_uid = ""
            purchase_uids = []
            try:
                # -------- test POST Bills --------
                print("\nIn POST Bills")
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
                post_bill_response = requests.post(ENDPOINT + "/bills", data=post_bill_payload, headers=headers)
                bill_uid = post_bill_response.json()['maibill_uidtenance_request_uid']
                purchase_uids = post_bill_response.json()['purchase_ids_add']
                if (post_bill_response.status_code == 200):
                    response['APIs running successfully'].append('POST Bills')
                else:
                    response['APIs failing'].append('POST Bills')
                response['No of APIs tested'] += 1

                # -------- test GET after POST Bills --------
                print("\nIn GET after POST Bills")
                get_post_response = requests.get(ENDPOINT + f"/bills/{bill_uid}", headers=headers)
                data = get_post_response.json()['result'][0]
                if data['bill_description'] != "Test Bill Description":
                    print("Not Match")
                if (get_post_response.status_code == 200):
                    response['APIs running successfully'].append('GET after POST Bills')
                else:
                    response['APIs failing'].append('GET after POST Bills')
                response['No of APIs tested'] += 1

                # -------- test PUT Bills --------
                print("\nIn PUT Bills")
                put_bill_payload = {
                    "bill_uid":f"{bill_uid}",
                    "bill_description":"Test Bill Description 1"
                }
                put_bill_response = requests.put(ENDPOINT + "/bills", data=put_bill_payload, headers=headers)
                if (put_bill_response.status_code == 200):
                    response['APIs running successfully'].append('PUT Bills')
                else:
                    response['APIs failing'].append('PUT Bills')
                response['No of APIs tested'] += 1

                # -------- test GET after PUT Bills --------
                print("\nIn GET after PUT Bills")
                get_put_response = requests.get(ENDPOINT + f"/bills/{bill_uid}", headers=headers)
                data = get_put_response.json()['result'][0]
                if data['bill_description'] != "Test Bill Description 1":
                    print("Not Match")
                if (get_put_response.status_code == 200):
                    response['APIs running successfully'].append('GET after PUT Bills')
                else:
                    response['APIs failing'].append('GET after PUT Bills')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Bills API')
            
            finally:
                # -------- test DELETE Bills --------
                print("\nIn DELETE Bills")
                print(f"\nDeleting bill_uid: {bill_uid} from Bills Table and purchase_uids: {purchase_uids} from Purchases Table")
                with connect() as db:
                    if bill_uid != "":
                        delQuery_bill = ("""
                                        DELETE FROM space_dev.bills
                                        WHERE bill_uid = \'""" + bill_uid + """\';
                                    """)
                        del_bill_response = db.delete(delQuery_bill)
                    if purchase_uids != []:
                        for pur_id in purchase_uids:
                            delQuery_purchase = ("""
                                        DELETE FROM space_dev.purchases
                                        WHERE purchase_uid = \'""" + pur_id + """\';
                                    """)
                            del_purchase_response = db.delete(delQuery_purchase)


            # ------------------------- Listings ------------------------------
            try:
                # -------- test GET Listings --------
                print("\nIn GET Listings")
                get_listings_response = requests.get(ENDPOINT + "/listings/350-000000", headers=headers)
                if (get_listings_response.status_code == 200):
                    response['APIs running successfully'].append('GET Listings')
                else:
                    response['APIs failing'].append('GET Listings')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Listings API')


            # ------------------------- Announcements ------------------------------
            announcement_uid = ""
            try:
                # -------- test POST Announcement --------
                print("\nIn POST Announcement")
                global post_announcement_payload
                post_announcement_payload = {
                        "announcement_title": "Test Announcement",
                        "announcement_msg": "Hi! This is a test announcement",
                        "announcement_properties": "{\"350-000000\":[\"200-000000\"]}",
                        "announcement_mode": "LEASE",
                        "announcement_receiver": "350-000000",
                        "announcement_type": []
                    }
                post_announcement_response = requests.post(ENDPOINT + "/announcements/110-000000", data=json.dumps(post_announcement_payload), headers=json_headers)
                if (post_announcement_response.status_code == 200):
                    response['APIs running successfully'].append('POST Announcement')
                else:
                    response['APIs failing'].append('POST Announcement')
                response['No of APIs tested'] += 1

                # -------- test GET after POST Announcement --------
                print("\nIn GET after POST Announcement")
                post_get_announcement_response = requests.get(ENDPOINT + "/announcements/110-000000", headers=headers)
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
                print("\nIn PUT Announcement")
                put_announcement_payload = {
                    "announcement_uid": [f"{announcement_uid}"],
                    "announcement_title": "Test Announcement 1"
                }
                put_announcement_response = requests.put(ENDPOINT + "/announcements", data=json.dumps(put_announcement_payload), headers=json_headers)
                if (put_announcement_response.status_code == 200):
                    response['APIs running successfully'].append('PUT Announcement')
                else:
                    response['APIs failing'].append('PUT Announcement')
                response['No of APIs tested'] += 1

                # -------- test GET after PUT Announcement --------
                print("\nIn GET after PUT Announcement")
                put_get_announcement_response = requests.get(ENDPOINT + "/announcements/110-000000", headers=headers)
                data = put_get_announcement_response.json()['sent']['result'][0]
                if data['announcement_title'] != "Test Announcement 1":
                    print("Not Match")
                if (put_get_announcement_response.status_code == 200):
                    response['APIs running successfully'].append('GET after PUT Announcement')
                else:
                    response['APIs failing'].append('GET after PUT Announcement')
                response['No of APIs tested'] += 1
            
            except:
                response['Error in running APIs'].append('Announcements API')
            
            finally:
                # -------- test DELETE Announcement --------
                print("\nIn DELETE Announcement")
                print(f"\nDeleting announcement_uid: {announcement_uid} from Announcements Table")
                with connect() as db:
                    if announcement_uid != "":
                        delQuery_announcement = ("""
                                        DELETE FROM space_dev.announcements
                                        WHERE announcement_uid = \'""" + announcement_uid + """\';
                                    """)
                        del_announcement_response = db.delete(delQuery_announcement)
            

            # ------------------------- UserInfo ------------------------------
            try:
                # -------- test PUT UserInfo --------
                print("\nIn PUT UserInfo")
                put_userinfo_payload = {
                    "user_uid": "100-000000",
                    "first_name": "test user"
                }
                put_userinfo_response = requests.put(ENDPOINT + "/userInfo", data=json.dumps(put_userinfo_payload), headers=json_headers)
                if (put_userinfo_response.status_code == 200):
                    response['APIs running successfully'].append('PUT UserInfo')
                else:
                    response['APIs failing'].append('PUT UserInfo')
                response['No of APIs tested'] += 1
                
                # -------- test GET after PUT UserInfo --------
                print("\nIn GET after PUT UserInfo")
                get_userinfo_response = requests.get(ENDPOINT + "/userInfo/100-000000", headers=headers)
                data = get_userinfo_response.json()['result'][0]
                if data['first_name'] != "test user":
                    print("Not Match")
                if (get_userinfo_response.status_code == 200):
                    response['APIs running successfully'].append('GET after PUT UserInfo')
                else:
                    response['APIs failing'].append('GET after PUT UserInfo')
                response['No of APIs tested'] += 1

            except:
                response['Error in running APIs'].append('UserInfo API')

        except:
            response["cron fail"] = {'message': f'MySpace Test API CRON Job failed for {dt}' ,'code': 500}

        try:
            print("\n\n*** Deleting temporary data from the database ***\n")
            delete_property_query = """
                                        DELETE FROM space_dev.properties
                                        WHERE property_uid = "200-000000";
                                    """
            
            delete_property_owner_query = """
                                        DELETE FROM space_dev.property_owner
                                        WHERE property_id = "200-000000" AND property_owner_id = "110-000000";
                                    """
            
            delete_user_query = """
                                        DELETE FROM space_dev.users
                                        WHERE user_uid = "100-000000";
                                    """
            
            delete_business_profile_query = """
                                        DELETE FROM space_dev.businessProfileInfo
                                        WHERE business_uid = "600-000000";
                                    """
            
            delete_owner_profile_query = """
                                        DELETE FROM space_dev.ownerProfileInfo
                                        WHERE owner_uid = "110-000000";
                                    """
            
            delete_tenant_profile_query = """
                                        DELETE FROM space_dev.tenantProfileInfo
                                        WHERE tenant_uid = "350-000000";
                                    """
            
            delete_purchases_query = """
                                        DELETE FROM space_dev.purchases
                                        WHERE purchase_uid = "400-000000";
                                    """
            
            delete_maintenance_requests_query = """
                                        DELETE FROM space_dev.maintenanceRequests
                                        WHERE maintenance_request_uid = "800-000000";
                                    """
            
            delete_maintenance_quotes_query = """
                                        DELETE FROM space_dev.maintenanceQuotes
                                        WHERE maintenance_quote_uid = "900-000000";
                                    """
            
            delete_contracts_query = """
                                        DELETE FROM space_dev.contracts
                                        WHERE contract_uid = "010-000000";
                                    """
            delete_leases_query = """
                                        DELETE FROM space_dev.leases
                                        WHERE lease_uid = "300-000000";
                                    """
            delete_lease_tenant_query = """
                                        DELETE FROM space_dev.lease_tenant
                                        WHERE lt_lease_id = "300-000000" AND lt_tenant_id = "350-000000";
                                    """
            delete_lease_fees_query = """
                                        DELETE FROM space_dev.leaseFees
                                        WHERE leaseFees_uid = "370-000000";
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
                # delete_leases_query_response = db.delete(delete_leases_query)
                # delete_lease_tenant_query_response = db.delete(delete_lease_tenant_query)
                # delete_lease_fees_query_response = db.delete(delete_lease_fees_query)

            print("\n*** Completed ***\n")
            response['delete_temporary_data'] = 'Passed'
            
        except:
            response['delete_temporary_data'] = 'Failed'

        return response

# def test_clean_up_Database():
#     print(os.getenv('RDS_PORT'))
#     print(os.getenv('RDS_HOST'))
#     print(os.getenv('RDS_USER'))
#     print(os.getenv('RDS_PW'))
#     # Define the patterns to search for
#     patterns = [r"200-000000", r"600-000000", r"110-000000", r"350-000000", r"100-000000", r"050-000000", r"800-000000", r"900-000000", r"400-000000", r"010-000000", r"370-000000", r"300-000000"]

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

#     cursor = connection.cursor()

#     try:
#         print("in new try")

#         cursor.execute("""
#             SELECT TABLE_NAME 
#             FROM information_schema.TABLES 
#             WHERE TABLE_SCHEMA = 'space_dev' AND TABLE_TYPE = 'BASE TABLE';
#         """)
#         tables = cursor.fetchall()

#         print(tables)
#         for table_name in tables:
#             print(f"Processing table: {table_name}")
#             value = table_name['TABLE_NAME']

#             cursor.execute(f"DESCRIBE `{value}`") 

#             columns = cursor.fetchall()
#             # print(columns)

#             for column_info in columns:
#                 column_name = column_info['Field']
#                 print(f"Checking column: {column_name} in table: {value}")

#                 for pattern in patterns:
#                     delete_query = f"""
#                     DELETE FROM `{value}`
#                     WHERE `{column_name}` REGEXP %s
#                     """
#                     cursor.execute(delete_query, (pattern,))
#                     rows_deleted = cursor.rowcount 
#                     connection.commit()
#                     if rows_deleted > 0:
#                         print(f"Deleted {rows_deleted} rows in table `{value}` where column `{column_name}` matched '{pattern}'")


#         print("Cleanup completed.")

#     except Exception as e:
#         print(f"Error: {e}")
#     finally:
#         cursor.close()
#         connection.close()

