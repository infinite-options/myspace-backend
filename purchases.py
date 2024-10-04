from datetime import date, timedelta, datetime

from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity


from data_pm import connect, uploadImage, s3, processImage, processDocument
import boto3
import json
# from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
from werkzeug.exceptions import BadRequest



ALLOWED_EXTENSIONS = {'txt', 'pdf', 'doc', 'docx'}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


class Bills(Resource):
    def get(self,user_id):
        print("In Get Bills", user_id)
        response = {}

        if user_id[:3] == '040':
            with connect() as db:
                queryResponse = (""" 
                        
                        SELECT *
                        FROM space.bills
                        WHERE bill_uid = \'""" + user_id + """\';
                        """)
                response = db.execute(queryResponse)
            return response

        elif user_id[:3] == '900':
            with connect() as db:
                queryResponse = ("""                
                        SELECT * 
                        FROM space.bills 
                        WHERE bill_maintenance_quote_id = \'""" + user_id + """\';
                        """)
                response = db.execute(queryResponse)
            return response
    
    def post(self):
        print("In Add Bill POST")
        response = {}
        response['message'] = []
        payload = request.form.to_dict()
        print("Maintenance Quotes Add Payload: ", payload)

        if payload.get('bill_uid'):
            print("bill_uid found.  Please call PUT endpoint")
            raise BadRequest("Request failed, UID found in payload.")

        with connect() as db:
            newBillUID = db.call('space.new_bill_uid')['result'][0]['new_id']
            key = {'bill_uid': newBillUID}
            new_bill_uid = newBillUID
            print("Bill Key: ", key)

            # --------------- PROCESS IMAGES ------------------

            processImage(key, payload)
            print("Payload after function: ", payload)
            
            # --------------- PROCESS IMAGES ------------------


            # --------------- PROCESS DOCUMENTS ------------------

            processDocument(key, payload)
            print("Payload after function: ", payload)
            
            # --------------- PROCESS DOCUMENTS ------------------

            # bill_property_id = json.loads(payload["bill_property_id"])
            # print("property_id is ", bill_property_id)                  

            # Add BillInfo
            
            payload['bill_images'] = '[]' if payload.get('bill_images') in {None, '', 'null'} else payload.get('bill_images', '[]')
            print("Add Bill Payload: ", payload) 

            payload["bill_uid"] = newBillUID  
            payload["bill_timestamp"] = datetime.today().date().strftime("%m-%d-%Y")
            response['Add Bill'] = db.insert('bills', payload)
            response['maibill_uidtenance_request_uid'] = newBillUID 
            response['Bill Images Added'] = payload.get('bill_images', "None")
            print("\nNew Bill Added")
            
            # Works to this point

            # Split the bill across multiple properties
            bill_property_id = json.loads(payload["bill_property_id"])
            print("property_id is ", bill_property_id)
            bill_amount = payload["bill_amount"]
            bill_created_by = payload["bill_created_by"]
            bill_description = payload["bill_description"]
            bill_utility_type = payload["bill_utility_type"]
            bill_split = payload["bill_split"]
            bill_property_id = json.loads(payload["bill_property_id"])
            print("property_id is ", bill_property_id)
            bill_maintenance_quote_id  = payload["bill_maintenance_quote_id"]
            bill_maintenance_request_id  = payload["bill_maintenance_request_id"]
            bill_notes = payload["bill_notes"]

            pur_ids = []
            split_num = len(bill_property_id)
            # print(split_num)
            split_bill_amount = round(float(bill_amount)/split_num,2)

            for data_dict in bill_property_id:
                for key, value in data_dict.items():
                    # print(f"{key}: {value}")
                    # print(value)
                    pur_property_id = value
                    bill_utility_type = payload.get('bill_utility_type')
                    print("Input to Find Responsible Party Query:  ", pur_property_id, bill_utility_type)

                    # Find Responsible Party:  For each property ID and utility, identify the responsible party
                    if bill_utility_type == "maintenance": 
                        queryResponse = (""" 
                            -- MAINTENANCE REPOSONSIBILITY BY PROPERTY 
                            SELECT *, 
                                IF(contract_status = "ACTIVE",contract_business_id,property_owner_id) AS responsible_party
                            FROM space.property_owner
                            LEFT JOIN (
                                SELECT * 
                                FROM space.contracts
                                WHERE contract_status = 'ACTIVE'
                                ) as c
                            ON property_id = contract_property_id
                            WHERE property_id = \'""" + pur_property_id + """\';
                            """)

                    else:
                        queryResponse = (""" 
                            -- UTILITY PAYMENT REPOSONSIBILITY BY PROPERTY
                            SELECT *
                            FROM (
                                SELECT u.*
                                    , list_item AS utility_type
                                    , CASE
                                        WHEN contract_status = "ACTIVE" AND utility_payer = "property manager" THEN contract_business_id
                                        WHEN lease_status = "ACTIVE" AND utility_payer = "tenant" THEN lt_tenant_id
                                        ELSE property_owner_id
                                    END AS responsible_party
                                FROM (
                                    SELECT -- *,
                                        property_uid, property_address, property_unit
                                        , utility_type_id, utility_payer_id
                                        , list_item AS utility_payer
                                        , property_owner_id
                                        , contract_business_id, contract_status, contract_start_date, contract_end_date
                                        , lease_status, lease_start, lease_end
                                        , lt_tenant_id, lt_responsibility 
                                    FROM space.properties
                                    LEFT JOIN space.property_utility ON property_uid = utility_property_id		-- TO FIND WHICH UTILITES TO PAY AND WHO PAYS THEM

                                    LEFT JOIN space.lists ON utility_payer_id = list_uid				-- TO TRANSLATE WHO PAYS UTILITIES TO ENGLISH
                                    LEFT JOIN space.property_owner ON property_uid = property_id		-- TO FIND PROPERTY OWNER
                                    LEFT JOIN space.contracts ON property_uid = contract_property_id    -- TO FIND PROPERTY MANAGER
                                    LEFT JOIN space.leases ON property_uid = lease_property_id			-- TO FIND CONTRACT START AND END DATES
                                    LEFT JOIN space.lease_tenant ON lease_uid = lt_lease_id				-- TO FIND TENANT IDS AND RESPONSIBILITY PERCENTAGES
                                    WHERE contract_status = "ACTIVE"
                                    ) u 

                                LEFT JOIN space.lists ON utility_type_id = list_uid					-- TO TRANSLATE WHICH UTILITY TO ENGLISH
                                ) u_all

                            WHERE property_uid = \'""" + pur_property_id + """\'
                                AND utility_type = \'""" + bill_utility_type + """\';
            
                            """)

                    # print("queryResponse is: ", queryResponse)
                    responsibleArray = db.execute(queryResponse)
                    # print("Responsible Party is: ", responsibleArray)
                    responsibleParty = responsibleArray['result'][0]['responsible_party']
                    responsibleOwner = responsibleArray['result'][0]['property_owner_id']
                    responsibleManager = responsibleArray['result'][0]['contract_business_id']
                    # print("Responsible Party is: ", responsibleParty)
                    # print("Responsible Owner is: ", responsibleOwner)
                    # print("Responsible Manager is: ", responsibleManager)
                

                    # STILL NEED TO ADD A LOOP FOR EACH RESPONSIBLE PARTY   

                    # for data_dict2 in responsibleArray:
                    #     for key, value in data_dict2.items():
                    #         print(f"{key}: {value}")
                    #         print(value)
            
                    # post a Purchase for each property

                    # FOR MAINTENANCE ITEM, POST MAINTENACE-PM AND PM-OWNER
                    # FOR PM MAINTENANCE ITEM, POST PM-OWNER
                    # FOR UTILITY THAT TENANT PAYS TO PM PAYS POST, TENANT-PM AND PM-OWNER
                    # FOR UTILITY THAT PM PAYS POST, TENANT-PM AND PM-OWNER

                    if bill_utility_type == "maintenance":
                        print("In Maitenance Bill")
                        pur_cf_type = "expense"
                        if bill_maintenance_quote_id[:3] == "900": 
                            print("In Maintenance Item performed by Maintenance Role")
                            #FOR MAINTENANCE ITEM, POST MAINTENACE-PM AND PM-OWNER
                            #POST MAINTENANCE-PM PURCHASE
                            #  Get New PURCHASE UID
                            new_purchase_uid = db.call('space.new_purchase_uid')['result'][0]['new_id']  
                            print("New Purchase ID: ", new_purchase_uid)                        

                            purchaseQuery = (""" 
                                INSERT INTO space.purchases
                                SET purchase_uid = \'""" + new_purchase_uid + """\'
                                    , pur_timestamp = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                    , pur_property_id = \'""" + pur_property_id  + """\'
                                    , purchase_type = "MAINTENANCE"
                                    , pur_cf_type = \'""" + pur_cf_type  + """\'
                                    , pur_bill_id = \'""" + new_bill_uid + """\'
                                    , purchase_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                    , pur_due_date = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL 14 DAY), '%m-%d-%Y %H:%i')  
                                    , pur_amount_due = \'""" + str(split_bill_amount) + """\'
                                    , purchase_status = "UNPAID"
                                    , pur_notes = \'""" + bill_notes + """\'
                                    , pur_description = \'""" + bill_description + """\'
                                    , pur_receiver = \'""" + bill_created_by + """\'
                                    , pur_payer = \'""" + responsibleManager + """\'
                                    , pur_initiator = \'""" + bill_created_by + """\';
                                """)

                            # print("Query: ", purchaseQuery)
                            queryResponse = db.execute(purchaseQuery, [], 'post')
                            # print("queryResponse is: ", queryResponse)


                        #POST PM-OWNER OR FOR PM MAINTENANCE ITEM, POST PM-OWNER
                        new_purchase_uid = db.call('space.new_purchase_uid')['result'][0]['new_id']                          
                        print("New PM-OWNER Purchase ID: ", new_purchase_uid)                
                        purchaseQuery = (""" 
                            INSERT INTO space.purchases
                            SET purchase_uid = \'""" + new_purchase_uid + """\'
                                , pur_timestamp = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                , pur_property_id = \'""" + pur_property_id  + """\'
                                , purchase_type = "MAINTENANCE"
                                , pur_cf_type = \'""" + pur_cf_type  + """\'
                                , pur_bill_id = \'""" + new_bill_uid + """\'
                                , purchase_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                , pur_due_date = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL 14 DAY), '%m-%d-%Y %H:%i')  
                                , pur_amount_due = \'""" + str(split_bill_amount) + """\'
                                , purchase_status = "UNPAID"
                                , pur_notes = \'""" + bill_notes + """\'
                                , pur_description = \'""" + bill_description + """\'
                                , pur_receiver = \'""" + responsibleManager + """\'
                                , pur_payer = \'""" + responsibleOwner + """\'
                                , pur_initiator = \'""" + bill_created_by + """\';
                            """)

                        print("Query: ", purchaseQuery)
                        queryResponse = db.execute(purchaseQuery, [], 'post')
                        print("queryResponse is: ", queryResponse)




                    if bill_utility_type != "maintenance":
                        print("In Utility Bill")
                        if responsibleParty[:3] == '350':
                            print("Tenant Responsible")
                            pur_cf_type = "revenue"

                            purchaseQuery = (""" 
                                INSERT INTO space.purchases
                                SET purchase_uid = \'""" + new_purchase_uid + """\'
                                    , pur_timestamp = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                    , pur_property_id = \'""" + pur_property_id  + """\'
                                    , purchase_type = "MAINTENANCE"
                                    , pur_cf_type = \'""" + pur_cf_type  + """\'
                                    , pur_bill_id = \'""" + new_bill_uid + """\'
                                    , purchase_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                    , pur_due_date = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL 14 DAY), '%m-%d-%Y %H:%i')  
                                    , pur_amount_due = \'""" + str(split_bill_amount) + """\'
                                    , purchase_status = "UNPAID"
                                    , pur_notes = \'""" + bill_notes + """\'
                                    , pur_description = \'""" + bill_description + """\'
                                    , pur_receiver = \'""" + bill_created_by + """\'
                                    , pur_payer = \'""" + responsibleParty + """\'
                                    , pur_initiator = \'""" + bill_created_by + """\';
                                """)

                            # print("Query: ", purchaseQuery)
                            queryResponse = db.execute(purchaseQuery, [], 'post')
                            # print("queryResponse is: ", queryResponse)

                        #POST OWNER-PM REIMBURSEMENT
                        pur_cf_type = "expense"

                        purchaseQuery = (""" 
                            INSERT INTO space.purchases
                            SET purchase_uid = \'""" + new_purchase_uid + """\'
                                , pur_timestamp = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                , pur_property_id = \'""" + pur_property_id  + """\'
                                , purchase_type = "MAINTENANCE"
                                , pur_cf_type = \'""" + pur_cf_type  + """\'
                                , pur_bill_id = \'""" + new_bill_uid + """\'
                                , purchase_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                , pur_due_date = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL 14 DAY), '%m-%d-%Y %H:%i')  
                                , pur_amount_due = \'""" + str(split_bill_amount) + """\'
                                , purchase_status = "UNPAID"
                                , pur_notes = \'""" + bill_notes + """\'
                                , pur_description = \'""" + bill_description + """\'
                                , pur_receiver = \'""" + bill_created_by + """\'
                                , pur_payer = \'""" + responsibleOwner + """\'
                                , pur_initiator = \'""" + bill_created_by + """\';
                            """)

                        # print("Query: ", purchaseQuery)
                        queryResponse = db.execute(purchaseQuery, [], 'post')
                        # print("queryResponse is: ", queryResponse)


                    # # THESE STATEMENTS DO THE SAME THING
                    # responsibleParty = queryResponse['result'][0]['responsible_party']
                    # print("Responsible Party is: ", responsibleParty)
                    # responsibleParty = responsibleArray['responsible_party']
                    # print("Responsible Party is: ", responsibleParty)

                    # STORE PURCHASE IDS ADDED
                    
                    # print(queryResponse['code'])
                    if (queryResponse['code'] == 200):
                        print("In append function")
                        pur_ids.append(new_purchase_uid)


                    continue

            # print(pur_ids)
            response["purchase_ids_add"] = pur_ids
            # response["purchase_ids added"] = json.dumps(pur_ids)
            return response

    def put(self):
        print('in bills')

        payload = request.form
        
        if payload.get('bill_uid') in {None, '', 'null'}:
            print("No bill_uid")
            raise BadRequest("Request failed, no UID in payload.")
        key = {'bill_uid': payload['bill_uid']}
        print("Key: ", key)
        bills = {k: v for k, v in payload.items()}
        print("KV Pairs: ", bills)
        with connect() as db:
            print("In actual PUT")
            response = db.update('bills', key, bills)
        return response
        
    def delete(self):
        print("In delete Bill - works but need to delete from Purchases as well")
        response = {}

        with connect() as db:
            response['message'] = []
            data = request.get_json(force=True)
            print(data)

            #  Get Bill UID
            bill_uid = data["bill_uid"]
            print(bill_uid)

            # Query
            delBillQuery = (""" 
                    -- DELETE BILL
                    DELETE FROM space.bills 
                    WHERE bill_uid = \'""" + bill_uid + """\';         
                    """)

            # print("Query: ", delBillQuery)
            response = db.delete(delBillQuery) 
            # print("Query out", response["code"])
            response["Deleted bill_uid"] = bill_uid


            return response
        

class AddPurchase(Resource):
    def post(self):
        print("In Add Purchase FORM")
        response = {}
        payload = request.form.to_dict()
        print("Property Add Payload: ", payload)

        # Verify uid has NOT been included in the data
        if payload.get('purchase_uid'):
            print("purchase_uid found.  Please call PUT endpoint")
            raise BadRequest("Request failed, UID found in payload.")
        
        with connect() as db:
            newPurchaseUID = db.call('new_purchase_uid')['result'][0]['new_id']
            key = {'purchase_uid': newPurchaseUID}
            print("Purchase Key: ", key)

            # --------------- PROCESS DOCUMENTS ------------------
            # processDocument(key, payload)
            # print("Payload after function: ", payload)
            
            # --------------- PROCESS DOCUMENTS ------------------


           # Add Purachse Info

            # payload['bill_documents'] = '[]' if payload.get('bill_documents') in {None, '', 'null'} else payload.get('bill_documents', '[]')
            # print("Add Appliance Payload: ", payload)  

            payload["purchase_uid"] = newPurchaseUID  

            # SET TRANSACTION DATE TO NOW
            # newRequest['pur_timestamp'] = datetime.today().date().strftime('%m-%d-%Y %H:%M')
            payload['pur_timestamp'] = datetime.today().strftime('%m-%d-%Y %H:%M')
            # SET ADDITIONAL FIELDS           
            payload['pur_status_value'] = "5" if payload.get('purchase_status') == "UNPAID" else \
                              "1" if payload.get('purchase_status') == "PARTIALLY PAID" else \
                              "0" if payload.get('purchase_status') == "PAID" else "5"

            # FORMAT DATE FIELDS
            payload['purchase_date'] = f"{payload.get('purchase_date')} 12:00"
            payload['pur_due_date'] = f"{payload.get('pur_due_date')} 12:00"
            # print(datetime.date.today())

            payer = payload.get('pur_payer') 
            payload['pur_cf_type'] = 'revenue' if payer.startswith(('110', '350')) else 'expense'

            response['Purchases_UID'] = newPurchaseUID
            response['Add Purchase'] = db.insert('purchases', payload)
            response['purchase_UID'] = newPurchaseUID 
        
            # response['Appliance Documents Added'] = payload.get('appliance_documents', "None")
            print("1")
            print(response)
            print("\nNew Purchase Added")



        return response



            # Add Purchase Info
        #     fields = [
        #         "pur_property_id"
        #         , "purchase_type"
        #         , "pur_cf_type"
        #         , "purchase_date"
        #         , "pur_due_date"
        #         , "pur_amount_due"
        #         , "purchase_status"
        #         , "pur_notes"
        #         , "pur_description"
        #         , "pur_receiver"
        #         , "pur_initiator"
        #         , "pur_payer"
        #     ]


        #     # PUTS FROM DATA INTO EACH FIELD
        #     newRequest = {}
        #     newRequest['pur_group'] = newPurchaseUID
        #     payload['appliance_documents'] = '[]' if payload.get('appliance_documents') in {None, '', 'null'} else payload.get('appliance_documents', '[]')
        #     for field in fields:
        #         if field in payload:
        #             newRequest[field] = payload.get(field)
        #         print(field, " = ", newRequest[field])
        #     print("Payload at this stage: ", newRequest)
            

        # return response
    




        
    def put(self):
        print('in purchases')
        payload = request.form
        if payload.get('purchase_uid') in {None, '', 'null'}:
            print("No purchase_uid")
            raise BadRequest("Request failed, no UID in payload.")
        key = {'purchase_uid': payload['purchase_uid']}
        print("Key: ", key)
        purchases = {k: v for k, v in payload.items()}
        print("KV Pairs: ", purchases)
        with connect() as db:
            print("In actual PUT")
            response = db.update('purchases', key, purchases)
        return response
    

    # IF PAYER = 3RD PARTY
                # IF RECIEVER = TENANT
                    # 3RD PARTY - OWNER
                    # OWNER - PM
                    # PM - TENANT
                        # ----OR-----
                    # 3RD PARTY - PM
                    # PM - TENANT
                # IF RECEIVER = PM
                    # 3RD PARTY - OWNER
                    # OWNER - PM
                        # ----OR-----
                    # 3RD PARTY - PM
                # IF RECEIVER = OWNER
                    # 3RD PARTY - OWNER
                        # ----OR-----
                    # 3RD PARTY - PM
                    # PM - OWNER
                # IF RECEIVER = 3RD PARTY
                    # 3RD PARTY - OWNER
                    # OWNER - PM
                    # PM - 3RD PARTY
                        # ----OR-----
                    # 3RD PARTY - PM
                    # PM - 3RD PARTY
                    
            # IF PAYER = TENANT
            # IF PERSON WHO ACTUALLY MADE PAYEMENT (PAYER) = TENANT
                # IF RECEIVER = PM
                    # TENANT - PM
                # IF RECEIVER = OWNER
                    # TENANT - PM
                    # PM - OWNER
                # IF RECEIVER = 3RD PARTY
                    # TENANT - PM
                    # PM - OWNER
                    # OWNER - 3RD PARTY
                        # ----OR-----
                    # TENANT - PM
                    # PM - 3RD PARTY
            # IF PERSON WHO ACTUALLY MADE PAYEMENT (PAYER) = PM
                # IF RECEIVER = TENANT
                    # PM - TENANT
                # IF RECEIVER = OWNER
                    # PM - OWNER
                # IF RECEIVER = 3RD PARTY
                    # PM - 3RD PARTY
                        # ----OR-----  (IF Reimbursable  WHO ACTUALLY PAID?  WHO IS ULTIMATELY RESPONSIBLE?)
                    # OWNER - PM
                    # PM - 3RD PARTY
      
            # IF PAYER = OWNER
            # IF PERSON WHO ACTUALLY MADE PAYEMENT (PAYER) = OWNER
                # IF RECEIVER = TENANT
                    # OWNER - PM
                    # PM - TENANT
                # IF RECEIVER = PM
                    # OWNER - PM
                # IF RECEIVER = 3RD PARTY
                    # OWNER - 3RD PARTY
                        # ----OR-----  (IF Reimbursable)
                    # PM - OWNER
                    # OWNER - 3RD PARTY
            # WHO ACTUALLY MADE PAYEMENT (PAYER) = OWNER
            # WHO IS UTLIMATELY RESPONSIBLE FOR PAYMENT = 
            # WHO IS THE RECEIVER
            
                # IF RECEIVER = TENANT
                    # OWNER - PM
                    # PM - TENANT
                # IF RECEIVER = PM
                    # OWNER - PM
                # IF RECEIVER = 3RD PARTY
                    # OWNER - 3RD PARTY
                        # ----OR-----  (IF Reimbursable)
                    # PM - OWNER
                    # OWNER - 3RD PARTY
    
class AddPurchaseJSON(Resource):
    def post(self):
        print("In Add Purchase")
        response = {}
        with connect() as db:
            data = request.get_json(force=True)
            # print(data)

            fields = [
                "pur_property_id"
                , "purchase_type"
                , "pur_cf_type"
                , "purchase_date"
                , "pur_due_date"
                , "pur_amount_due"
                , "purchase_status"
                , "pur_notes"
                , "pur_description"
                , "pur_receiver"
                , "pur_initiator"
                , "pur_payer"
            ]

            # PUTS JSON DATA INTO EACH FIELD
            newRequest = {}
            for field in fields:
                newRequest[field] = data.get(field)
                # print(field, " = ", newRequest[field])


            # # GET NEW UID
            # print("Get New Request UID")
            newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
            newRequest['purchase_uid'] = newRequestID
            newRequest['pur_group'] = newRequestID
            # print(newRequestID)

            # SET TRANSACTION DATE TO NOW
            # newRequest['pur_timestamp'] = datetime.today().date().strftime('%m-%d-%Y %H:%M')
            newRequest['pur_timestamp'] = datetime.today().strftime('%m-%d-%Y %H:%M')

            # SET ADDITIONAL FIELDS
            newRequest['pur_status_value'] = "5"

            # FORMAT DATE FIELDS
            newRequest['purchase_date'] = f"{data.get('purchase_date')} 12:00"
            newRequest['pur_due_date'] = f"{data.get('pur_due_date')} 12:00"

            # print(datetime.date.today())

            response = db.insert('purchases', newRequest)
            response['Purchases_UID'] = newRequestID

        return response
        
    def put(self):
        print('in purchases')
        payload = request.form
        if payload.get('purchase_uid') in {None, '', 'null'}:
            print("No purchase_uid")
            raise BadRequest("Request failed, no UID in payload.")
        key = {'purchase_uid': payload['purchase_uid']}
        print("Key: ", key)
        purchases = {k: v for k, v in payload.items()}
        print("KV Pairs: ", purchases)
        with connect() as db:
            print("In actual PUT")
            response = db.update('purchases', key, purchases)
        return response

        
class AddExpense(Resource):
    def post(self):
        print("In Add Expense")
        response = {}
        with connect() as db:
            data = request.get_json(force=True)
            # print(data)

            fields = [
                "pur_property_id"
                , "purchase_type"
                , "pur_cf_type"
                , "purchase_date"
                , "pur_due_date"
                , "pur_amount_due"
                , "purchase_status"
                , "pur_notes"
                , "pur_description"
                , "pur_receiver"
                , "pur_initiator"
                , "pur_payer"
            ]

            # PUTS JSON DATA INTO EACH FIELD
            newRequest = {}
            for field in fields:
                newRequest[field] = data.get(field)
                # print(field, " = ", newRequest[field])


            # # GET NEW UID
            # print("Get New Request UID")
            newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
            newRequest['purchase_uid'] = newRequestID
            newRequest['pur_group'] = newRequestID
            # print(newRequestID)

            # SET TRANSACTION DATE TO NOW
            # newRequest['pur_timestamp'] = datetime.today().date().strftime('%m-%d-%Y %H:%M')
            newRequest['pur_timestamp'] = datetime.today().strftime('%m-%d-%Y %H:%M')

            # SET ADDITIONAL FIELDS
            newRequest['pur_status_value'] = "5"

            # FORMAT DATE FIELDS
            newRequest['purchase_date'] = f"{data.get('purchase_date')} 12:00"
            newRequest['pur_due_date'] = f"{data.get('pur_due_date')} 12:00"

            # print(datetime.date.today())

            response = db.insert('purchases', newRequest)
            response['Purchases_UID'] = newRequestID

        return response
        
    def put(self):
        print('in purchases')
        payload = request.form
        if payload.get('purchase_uid') in {None, '', 'null'}:
            print("No purchase_uid")
            raise BadRequest("Request failed, no UID in payload.")
        key = {'purchase_uid': payload['purchase_uid']}
        print("Key: ", key)
        purchases = {k: v for k, v in payload.items()}
        print("KV Pairs: ", purchases)
        with connect() as db:
            print("In actual PUT")
            response = db.update('purchases', key, purchases)
        return response
    

class AddRevenue(Resource):
    def post(self):
        print("In Add Revenue")
        response = {}
        with connect() as db:
            data = request.get_json(force=True)
            # print(data)

            fields = [
                "pur_property_id"
                , "purchase_type"
                , "pur_cf_type"
                , "purchase_date"
                , "pur_due_date"
                , "pur_amount_due"
                , "purchase_status"
                , "pur_notes"
                , "pur_description"
                , "pur_receiver"
                , "pur_initiator"
                , "pur_payer"
            ]

            # PUTS JSON DATA INTO EACH FIELD
            newRequest = {}
            for field in fields:
                newRequest[field] = data.get(field)
                # print(field, " = ", newRequest[field])


            # # GET NEW UID
            # print("Get New Request UID")
            newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
            newRequest['purchase_uid'] = newRequestID
            newRequest['pur_group'] = newRequestID
            # print(newRequestID)

            # SET TRANSACTION DATE TO NOW
            # newRequest['pur_timestamp'] = datetime.today().date().strftime('%m-%d-%Y %H:%M')
            newRequest['pur_timestamp'] = datetime.today().strftime('%m-%d-%Y %H:%M')

            # SET ADDITIONAL FIELDS
            newRequest['pur_status_value'] = "5"

            # FORMAT DATE FIELDS
            newRequest['purchase_date'] = f"{data.get('purchase_date')} 12:00"
            newRequest['pur_due_date'] = f"{data.get('pur_due_date')} 12:00"
            
            # print(newRequest)

            response = db.insert('purchases', newRequest)
            response['Purchases_UID'] = newRequestID

        return response
        
    def put(self):
        print('in purchases')
        payload = request.form
        if payload.get('purchase_uid') in {None, '', 'null'}:
            print("No purchase_uid")
            raise BadRequest("Request failed, no UID in payload.")
        key = {'purchase_uid': payload['purchase_uid']}
        print("Key: ", key)
        purchases = {k: v for k, v in payload.items()}
        print("KV Pairs: ", purchases)
        with connect() as db:
            print("In actual PUT")
            response = db.update('purchases', key, purchases)
        return response


class RentPurchase(Resource):
    def post(self):
        print("In Rent Purchase")
        data = request.get_json(force=True)
        print("Data Received: ", data)

        lease_uid = data["lease_uid"]
        today = date.today().strftime('%m-%d-%Y')

        response = {}

        with connect() as db:
            

            newRequest = {}

            # GET LEASE FEES
            feesResponse = (""" 
                SELECT * FROM space.leases
                LEFT JOIN space.leaseFees ON fees_lease_id = lease_uid
                LEFT JOIN space.lease_tenant ON lt_lease_id = lease_uid
                LEFT JOIN space.contracts ON contract_property_id = lease_property_id
                LEFT JOIN space.property_owner ON property_id = lease_property_id
                -- WHERE lease_uid = '300-000001'
                WHERE lease_uid = \'""" + lease_uid + """\'
                """)
            response = db.execute(feesResponse)
            fees = response['result']

            print("\nGet Fees Response: ", len(fees), fees)




            # STILL NEED A LOOP FOR LEASES THAT STARTED MONTHS AGO

            # print("\nLease Start Date: ", type(fees[0]["lease_start"]), datetime.strptime(fees[0]["lease_start"], "%m-%d-%Y"))
            # print("Lease Effective Date: ", type(fees[0]["lease_effective_date"]), datetime.strptime(fees[0]["lease_effective_date"], "%m-%d-%Y"))

            # lease_start_date = datetime.strptime(fees[0]["lease_start"], "%m-%d-%Y")
            # current_date = datetime.now()

            # if lease_start_date.year < current_date.year or (lease_start_date.year == current_date.year and lease_start_date.month < current_date.month):
            #     print("Need to run loop multiple times")
            #     lease_month = lease_start_date.month
            #     print("Lease Start Month: ", type(lease_month), lease_month)

            #     while lease_month <= current_date.month:
            #         print("lease_month", lease_month)
            #         lease_month = lease_month + 1





            for fee in fees:  
                print("\nFee: ", fee)
                # print("Frequency: ", fee["frequency"] )
                # print("\nLease Start Date: ", type(fees[0]["lease_start"]), datetime.strptime(fees[0]["lease_start"], "%m-%d-%Y"))
                # print("Lease Effective Date: ", type(fees[0]["lease_effective_date"]), datetime.strptime(fees[0]["lease_effective_date"], "%m-%d-%Y"))

                # # CALC NUMBER OF TIMES TO RUN THIS FEE
                # if fee["frequency"] == 'Monthly':
                #     lease_start_date = datetime.strptime(fees[0]["lease_start"], "%m-%d-%Y")
                #     lease_month = lease_start_date.month
                #     current_date = datetime.now()

                #     while lease_month <= current_date.month and lease_start_date.year <= current_date.year:
                #         lease_month = lease_month + 1

                #     if lease_start_date.year < current_date.year or (lease_start_date.year == current_date.year and lease_month <= current_date.month):
                #         print("Need to run loop multiple times")
                #         while lease_month <= current_date.month:
                #             print("lease_month", lease_month)
                #             lease_month = lease_month + 1
                    


                # ADD TENANT PURCHASE TO PURCHASE TABLE
                # GET NEW PURCHASE UID
                newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                newRequest['purchase_uid'] = newRequestID
                print("New UID: ", newRequest['purchase_uid'])
                purchase_group = newRequestID


                newRequest['pur_timestamp'] = today.strftime('%m-%d-%Y %H:%M')
                newRequest['pur_property_id'] = fee["lease_property_id"]
                newRequest['purchase_type'] = fee["fee_name"]
                newRequest['pur_cf_type'] = "revenue"
                # print("New Request: ", newRequest)
                # dt = datetime.datetime(2023,9,21)  # To Test Back Dating a Lease
                # dt = fee["lease_start"]
                dt = datetime.strptime(fee["lease_start"], '%m-%d-%Y')

                print("Lease Start Date: ", dt)
                # newRequest['purchase_date'] = dt
                # newRequest['pur_due_date'] = dt

                newRequest['purchase_date'] = dt.strftime('%m-%d-%Y') + ' 12:00'
                newRequest['pur_due_date'] = dt.strftime('%m-%d-%Y') + ' 12:00'
                print("DateTime: ", newRequest['purchase_date'], newRequest['pur_due_date'])
                newRequest['pur_amount_due'] = fee["charge"]
                newRequest['purchase_status'] = "UNPAID"
                newRequest['pur_status_value'] = "0"
                newRequest['pur_notes'] = "New Lease Charge"
                newRequest['pur_description'] = "New Lease Charge"
                newRequest['pur_receiver'] = fee["contract_business_id"]
                newRequest['pur_initiator'] = fee["contract_business_id"]
                newRequest['pur_payer'] = fee["lt_tenant_id"]
                newRequest['pur_late_Fee'] = fee["late_fee"]
                newRequest['pur_perDay_late_fee'] = fee["perDay_late_fee"]
                newRequest['pur_due_by'] = fee["due_by"]
                newRequest['pur_late_by'] = fee["late_by"]
                newRequest['pur_group'] = purchase_group
                print("\n","Complete New Request: ", newRequest)

                response = db.insert('purchases', newRequest)


                # ADD MANAGER PURCHASE TO PURCHASE TABLE
                # GET NEW PURCHASE UID
                newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                newRequest['purchase_uid'] = newRequestID
                print("New UID: ", newRequest['purchase_uid'])


                newRequest['pur_timestamp'] = today.strftime('%m-%d-%Y %H:%M')
                newRequest['pur_property_id'] = fee["lease_property_id"]
                newRequest['purchase_type'] = fee["fee_name"]
                newRequest['pur_cf_type'] = "revenue"
                # print("New Request: ", newRequest)

                # dt = datetime.datetime(2023,9,21)  # To Test Back Dating a Lease
                # dt = fee["lease_start"]
                dt = datetime.strptime(fee["lease_start"], '%m-%d-%Y')
                # print("Lease Start Date: ", dt)
                # newRequest['purchase_date'] = dt
                # newRequest['pur_due_date'] = dt

                newRequest['purchase_date'] = dt.strftime('%m-%d-%Y') + ' 12:00'
                newRequest['pur_due_date'] = dt.strftime('%m-%d-%Y') + ' 12:00'
                print("DateTime: ", newRequest['purchase_date'], newRequest['pur_due_date'])
                newRequest['pur_amount_due'] = fee["charge"]
                newRequest['purchase_status'] = "UNPAID"
                newRequest['pur_status_value'] = "0"
                newRequest['pur_notes'] = "New Lease Charge"
                newRequest['pur_description'] = "New Lease Charge"
                newRequest['pur_receiver'] = fee["property_owner_id"]
                newRequest['pur_initiator'] = fee["contract_business_id"]
                newRequest['pur_payer'] = fee["contract_business_id"]
                newRequest['pur_late_Fee'] = 0
                newRequest['pur_perDay_late_fee'] = 0
                newRequest['pur_due_by'] = fee["due_by"]
                newRequest['pur_late_by'] = fee["late_by"]
                newRequest['pur_group'] = purchase_group
                print("\n","Complete Manager Payment Request: ", newRequest)

                response = db.insert('purchases', newRequest)



                # ADD MANAGER FEE TO PURCHASE TABLE
                # Based on Fee Frequency determine which contract_fees to apply
                charges = json.loads(fee["contract_fees"])
                print("\n", "Charges: ", type(charges), charges)
                # json_string = json.loads(fee["contract_fees"])
                # print("\n", "JSON String: ", type(json_string), json_string)
                number_of_charges = len(charges)
                print("Number of charges:", number_of_charges)
                # print("Number of charges: ", len(charges["Management Contract Fees"]))
                for charge in charges: 
                    print("\n", "Charge: ", charge)
                    print("Charge Frequency: ", charge['frequency'])
                    print("Fee Frequency: ", fee['frequency'])
                    if charge['frequency'] == fee['frequency']:
                        print("Match Frequency")

                        # GET NEW PURCHASE UID
                        newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                        newRequest['purchase_uid'] = newRequestID
                        print("New UID: ", newRequest['purchase_uid'])

                        newRequest['pur_timestamp'] = today.strftime('%m-%d-%Y %H:%M')
                        newRequest['pur_property_id'] = fee["lease_property_id"]
                        newRequest['purchase_type'] = charge["fee_name"]
                        newRequest['pur_cf_type'] = "expense"
                        # print("New Request: ", newRequest)

                        # dt = datetime.datetime(2023,9,21)  # To Test Back Dating a Lease
                        dt = fee["lease_start"]
                        dt = datetime.strptime(fee["lease_start"], '%m-%d-%Y')
                        # print("Lease Start Date: ", dt)

                        # newRequest['purchase_date'] = dt
                        # newRequest['pur_due_date'] = dt

                        newRequest['purchase_date'] = dt.strftime('%m-%d-%Y') + ' 12:00'
                        newRequest['pur_due_date'] = dt.strftime('%m-%d-%Y') + ' 12:00'
                        print("DateTime: ", newRequest['purchase_date'], newRequest['pur_due_date'])
                        
                        newRequest['purchase_status'] = "UNPAID"
                        newRequest['pur_status_value'] = "0"
                        newRequest['pur_notes'] = "New Lease Charge"
                        newRequest['pur_description'] = "New Lease Charge"
                        newRequest['pur_receiver'] = fee["contract_business_id"]
                        newRequest['pur_initiator'] = fee["contract_business_id"]
                        newRequest['pur_payer'] = fee["property_owner_id"]
                        newRequest['pur_late_Fee'] = 0
                        newRequest['pur_perDay_late_fee'] = 0
                        newRequest['pur_due_by'] = fee["due_by"]
                        newRequest['pur_late_by'] = fee["late_by"]
                        newRequest['pur_group'] = purchase_group

                        if fee['frequency'] == 'Monthly':
                            newRequest['pur_amount_due'] = format(float(fee["charge"])*float(charge["charge"])/100, ".2f")
                        elif fee['frequency'] == 'One-time':
                            newRequest['pur_amount_due'] = charge["charge"]
                        
                            
                        print("\n","Complete Manager Payment Request: ", newRequest)

                        response = db.insert('purchases', newRequest)


            return response


            # # GET NEW UID
            newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
            newRequest['purchase_uid'] = newRequestID
            print("New UID: ", newRequest['purchase_uid'])



            newRequest['pur_timestamp'] = datetime.date.today()
            newRequest['pur_property_id'] = data.get("property_id")
            newRequest['purchase_type'] = "RENT"
            newRequest['pur_cf_type'] = "REVENUE"
            dt = datetime.datetime(2023,9,21)
            newRequest['purchase_date'] = (dt.replace(day=1) + datetime.timedelta(days=32)).replace(day=1)
            newRequest['pur_due_date'] = (dt.replace(day=1) + datetime.timedelta(days=32)).replace(day=1)
            print("DateTime: ", newRequest['purchase_date'], newRequest['s'])

            # THINGS THAT ARE MISSING:
            # 1. NEED TO GET LEASE INFO TO FIND RENT, RENT START DATE, DEPOSIT, DUE DATES AND LATE DATES
            # 2. NEED TO RECURSIVELY POST TO PURCHASES FOR EACH MONTH DUE
            # 3. NEED TO WAIVE LATE FEES?  SEND FLAG FROM FRONTEND?

            #get the rent amount  - NEED TO GET THIS FROM THE LEASE
            rent_amt_st = db.select('properties',
                                          {'property_uid': data.get("property_id")})
            rent_amt = rent_amt_st.get('result')[0]['property_listed_rent']
            print("Rent Amount: ", rent_amt)
            newRequest['pur_amount_due'] = rent_amt
            newRequest['purchase_status'] = "UNPAID"
            newRequest['pur_notes'] = "RENT FOR NEXT MONTH"
            newRequest['pur_description'] = "RENT FOR NEXT MONTH"

            #get property owner id
            owner_id_st = db.select('property_owner',
                                    {'property_id': data.get("property_id")})
            owner_id = owner_id_st.get('result')[0]['property_owner_id']
            newRequest['pur_receiver'] = owner_id

            #get property manager id
            manager_id_st = db.select('b_details',
                                    {'contract_property_id': data.get("property_id"),
                                     'business_type':"MANAGEMENT"})
            manager_id = manager_id_st.get('result')[0]['business_user_id']
            newRequest['pur_initiator'] = manager_id

            #get the tenant id
            lease_id_st = db.select('leases',
                                      {'lease_property_id': data.get("property_id")})
            lease_id = lease_id_st.get('result')[0]['lease_uid']
            tenant_id_st = db.select('lease_tenant',
                                      {'lt_lease_id': lease_id})
            tenant_id = tenant_id_st.get('result')[0]['lt_tenant_id']
            newRequest['pur_payer'] = tenant_id


            response = db.insert('purchases', newRequest)

        return response
