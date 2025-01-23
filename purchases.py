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
                        FROM bills
                        WHERE bill_uid = \'""" + user_id + """\';
                        """)
                response = db.execute(queryResponse)
            return response

        elif user_id[:3] == '900':
            with connect() as db:
                queryResponse = ("""                
                        SELECT * 
                        FROM bills 
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
            newBillUID = db.call('new_bill_uid')['result'][0]['new_id']
            key = {'bill_uid': newBillUID}
            new_bill_uid = newBillUID
            print("Bill Key: ", key)

            # --------------- PROCESS IMAGES ------------------

            processImage(key, payload)
            # print("Payload after processImage function: ", payload, type(payload))
            
            # --------------- PROCESS IMAGES ------------------


            # --------------- PROCESS DOCUMENTS ------------------

            processDocument(key, payload)
            # print("Payload after processDocument function: ", payload, type(payload))
            
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
                            FROM property_owner
                            LEFT JOIN (
                                SELECT * 
                                FROM contracts
                                WHERE contract_status = 'ACTIVE'
                                ) as c
                            ON property_id = contract_property_id
                            WHERE property_id = \'""" + pur_property_id + """\';
                            """)

                    else:
                        # queryResponse = (""" 
                        #     -- UTILITY PAYMENT REPOSONSIBILITY BY PROPERTY
                        #     SELECT *
                        #     FROM (
                        #         SELECT u.*
                        #             , list_item AS utility_type
                        #             , CASE
                        #                 WHEN contract_status = "ACTIVE" AND utility_payer = "property manager" THEN contract_business_id
                        #                 WHEN (lease_status IN ('ACTIVE', 'ACTIVE M2M')) AND utility_payer = "tenant" THEN lt_tenant_id
                        #                 ELSE property_owner_id
                        #             END AS responsible_party
                        #         FROM (
                        #             SELECT -- *,
                        #                 property_uid, property_address, property_unit
                        #                 , utility_type_id, utility_payer_id
                        #                 , list_item AS utility_payer
                        #                 , property_owner_id
                        #                 , contract_business_id, contract_status, contract_start_date, contract_end_date
                        #                 , lease_status, lease_start, lease_end
                        #                 , lt_tenant_id, lt_responsibility 
                        #             FROM properties
                        #             LEFT JOIN property_utility ON property_uid = utility_property_id		-- TO FIND WHICH UTILITES TO PAY AND WHO PAYS THEM

                        #             LEFT JOIN lists ON utility_payer_id = list_uid				-- TO TRANSLATE WHO PAYS UTILITIES TO ENGLISH
                        #             LEFT JOIN property_owner ON property_uid = property_id		-- TO FIND PROPERTY OWNER
                        #             LEFT JOIN contracts ON property_uid = contract_property_id    -- TO FIND PROPERTY MANAGER
                        #             LEFT JOIN leases ON property_uid = lease_property_id			-- TO FIND CONTRACT START AND END DATES
                        #             LEFT JOIN lease_tenant ON lease_uid = lt_lease_id				-- TO FIND TENANT IDS AND RESPONSIBILITY PERCENTAGES
                        #             WHERE contract_status = "ACTIVE"
                        #             ) u 

                        #         LEFT JOIN lists ON utility_type_id = list_uid					-- TO TRANSLATE WHICH UTILITY TO ENGLISH
                        #         ) u_all

                        #     WHERE property_uid = \'""" + pur_property_id + """\'
                        #         AND utility_type = \'""" + bill_utility_type + """\';
            
                        #     """)

                        queryResponse = (""" 
                            -- UTILITY PAYMENT REPOSONSIBILITY BY PROPERTY
                            SELECT *
                            , CASE
                                WHEN (lease_status IN ('ACTIVE', 'ACTIVE M2M')) and payer = "owner" THEN property_owner_id
                                WHEN (lease_status IN ('ACTIVE', 'ACTIVE M2M')) and payer = "tenant" THEN lt_tenant_id
                                WHEN (lease_status IN ('ACTIVE', 'ACTIVE M2M')) and payer = "property manager" THEN contract_business_id
                                ELSE property_owner_id
                                    END AS responsible_party
                            , IF((lease_status IN ('ACTIVE', 'ACTIVE M2M')) AND lt_responsibility IS NOT NULL, lt_responsibility, 1) AS lt_responsibility
                            FROM (
                            SELECT -- *
                                property_utility.*
                                , utility.list_item as utility
                                , payer.list_item AS payer
                                , l.lease_status
                                , property_owner.property_owner_id
                                , contracts.contract_business_id
                            --     , l.lease_assigned_contacts
                                , lease_tenant.*
                            FROM property_utility
                            LEFT JOIN lists AS utility ON utility_type_id = utility.list_uid
                            LEFT JOIN lists AS payer ON utility_payer_id = payer.list_uid
                            LEFT JOIN (SELECT * FROM leases WHERE lease_status IN ('ACTIVE', 'ACTIVE M2M')) AS l ON utility_property_id = lease_property_id
                            LEFT JOIN property_owner ON utility_property_id = property_id		-- TO FIND PROPERTY OWNER
                            LEFT JOIN contracts ON utility_property_id = contract_property_id    -- TO FIND PROPERTY MANAGER
                            LEFT JOIN lease_tenant ON lease_uid = lt_lease_id				-- TO FIND TENANT IDS AND RESPONSIBILITY PERCENTAGES
                            ) AS rp
                            WHERE utility_property_id = '200-000002' AND utility = 'electricity';
                            """)

                    # print("queryResponse is: ", queryResponse)
                    responsibleArray = db.execute(queryResponse)
                    print("Responsible Party is: ", responsibleArray)
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
                        
                        if bill_maintenance_quote_id[:3] == "900": 
                            print("In Maintenance Item performed by Maintenance Role")
                            #FOR MAINTENANCE ITEM, POST MAINTENACE-PM AND PM-OWNER
                            #POST MAINTENANCE-PM PURCHASE
                            #  Get New PURCHASE UID
                            new_purchase_uid = db.call('new_purchase_uid')['result'][0]['new_id'] 
                            purchase_group =  new_purchase_uid
                            print("New Purchase ID: ", new_purchase_uid) 


                            purchaseQuery = (""" 
                                INSERT INTO purchases
                                SET purchase_uid = \'""" + new_purchase_uid + """\'
                                    , pur_group = \'""" + purchase_group + """\'
                                    , pur_timestamp = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                    , pur_property_id = \'""" + pur_property_id  + """\'
                                    , purchase_type = "MAINTENANCE"
                                    , pur_bill_id = \'""" + new_bill_uid + """\'
                                    , purchase_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                    , pur_due_date = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL 14 DAY), '%m-%d-%Y %H:%i')  
                                    , pur_amount_due = \'""" + str(split_bill_amount) + """\'
                                    , purchase_status = "UNPAID"
                                    , pur_status_value = "0"
                                    , pur_notes = \'""" + bill_notes + """\'
                                    , pur_description = \'""" + bill_description + """\'
                                    , pur_cf_type = "expense"
                                    , pur_receiver = \'""" + bill_created_by + """\'
                                    , pur_payer = \'""" + responsibleManager + """\'
                                    , pur_initiator = \'""" + bill_created_by + """\';
                                """)

                            # print("Query: ", purchaseQuery)
                            queryResponse = db.execute(purchaseQuery, [], 'post')
                            # print("queryResponse is: ", queryResponse)
                            if (queryResponse['code'] == 200):
                                print("In append function")
                                pur_ids.append(new_purchase_uid)


                        #POST PM-OWNER OR FOR PM MAINTENANCE ITEM, POST PM-OWNER
                        new_purchase_uid = db.call('new_purchase_uid')['result'][0]['new_id']                          
                        print("New PM-OWNER Purchase ID: ", new_purchase_uid)                
                        purchaseQuery = (""" 
                            INSERT INTO purchases
                            SET purchase_uid = \'""" + new_purchase_uid + """\'
                                , pur_group = \'""" + purchase_group + """\'
                                , pur_timestamp = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                , pur_property_id = \'""" + pur_property_id  + """\'
                                , purchase_type = "MAINTENANCE"
                                , pur_bill_id = \'""" + new_bill_uid + """\'
                                , purchase_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                , pur_due_date = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL 14 DAY), '%m-%d-%Y %H:%i')  
                                , pur_amount_due = \'""" + str(split_bill_amount) + """\'
                                , purchase_status = "UNPAID"
                                , pur_status_value = "0"
                                , pur_notes = \'""" + bill_notes + """\'
                                , pur_description = \'""" + bill_description + """\'
                                , pur_cf_type = "revenue"
                                , pur_receiver = \'""" + responsibleManager + """\'
                                , pur_payer = \'""" + responsibleOwner + """\'
                                , pur_initiator = \'""" + bill_created_by + """\';
                            """)

                        print("Query: ", purchaseQuery)
                        queryResponse = db.execute(purchaseQuery, [], 'post')
                        print("queryResponse is: ", queryResponse)
                        if (queryResponse['code'] == 200):
                            print("In append function")
                            pur_ids.append(new_purchase_uid)




                    if bill_utility_type != "maintenance":
                        print("In Utility Bill", responsibleParty)
                        if responsibleParty[:3] == '350':
                            print("Tenant Responsible")
                            pur_cf_type = "revenue"

                            new_purchase_uid = db.call('new_purchase_uid')['result'][0]['new_id']   
                            purchase_group =  new_purchase_uid                       
                            print("New PM-OWNER Purchase ID: ", new_purchase_uid)  

                            purchaseQuery = (""" 
                                INSERT INTO purchases
                                SET purchase_uid = \'""" + new_purchase_uid + """\'
                                    , pur_group = \'""" + purchase_group + """\'
                                    , pur_timestamp = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                    , pur_property_id = \'""" + pur_property_id  + """\'
                                    , purchase_type = "MAINTENANCE"
                                    , pur_cf_type = \'""" + pur_cf_type  + """\'
                                    , pur_bill_id = \'""" + new_bill_uid + """\'
                                    , purchase_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                    , pur_due_date = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL 14 DAY), '%m-%d-%Y %H:%i')  
                                    , pur_amount_due = \'""" + str(split_bill_amount) + """\'
                                    , purchase_status = "UNPAID"
                                    , pur_status_value = "0"
                                    , pur_notes = \'""" + bill_notes + """\'
                                    , pur_description = \'""" + bill_description + """\'
                                    , pur_receiver = \'""" + bill_created_by + """\'
                                    , pur_payer = \'""" + responsibleParty + """\'
                                    , pur_initiator = \'""" + bill_created_by + """\';
                                """)

                            # print("Query: ", purchaseQuery)
                            queryResponse = db.execute(purchaseQuery, [], 'post')
                            # print("queryResponse is: ", queryResponse)
                            if (queryResponse['code'] == 200):
                                print("In append function")
                                pur_ids.append(new_purchase_uid)

                            

                        #POST OWNER-PM REIMBURSEMENT
                        pur_cf_type = "expense"

                        new_purchase_uid = db.call('new_purchase_uid')['result'][0]['new_id']                          
                        print("New PM-OWNER Purchase ID: ", new_purchase_uid)

                        purchaseQuery = (""" 
                            INSERT INTO purchases
                            SET purchase_uid = \'""" + new_purchase_uid + """\'
                                , pur_group = \'""" + purchase_group + """\'
                                , pur_timestamp = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                , pur_property_id = \'""" + pur_property_id  + """\'
                                , purchase_type = "MAINTENANCE"
                                , pur_cf_type = \'""" + pur_cf_type  + """\'
                                , pur_bill_id = \'""" + new_bill_uid + """\'
                                , purchase_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                , pur_due_date = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL 14 DAY), '%m-%d-%Y %H:%i')  
                                , pur_amount_due = \'""" + str(split_bill_amount) + """\'
                                , purchase_status = "UNPAID"
                                , pur_status_value = "0"
                                , pur_notes = \'""" + bill_notes + """\'
                                , pur_description = \'""" + bill_description + """\'
                                , pur_receiver = \'""" + bill_created_by + """\'
                                , pur_payer = \'""" + responsibleOwner + """\'
                                , pur_initiator = \'""" + bill_created_by + """\';
                            """)

                        # print("Query: ", purchaseQuery)
                        queryResponse = db.execute(purchaseQuery, [], 'post')
                        # print("queryResponse is: ", queryResponse)
                        if (queryResponse['code'] == 200):
                            print("In append function")
                            pur_ids.append(new_purchase_uid)
                        


                    # # THESE STATEMENTS DO THE SAME THING
                    # responsibleParty = queryResponse['result'][0]['responsible_party']
                    # print("Responsible Party is: ", responsibleParty)
                    # responsibleParty = responsibleArray['responsible_party']
                    # print("Responsible Party is: ", responsibleParty)

                    # STORE PURCHASE IDS ADDED
                    
                    # print(queryResponse['code'])
                    # if (queryResponse['code'] == 200):
                    #     print("In append function")
                    #     pur_ids.append(new_purchase_uid)


                    continue

            # print(pur_ids)
            response["purchase_ids_add"] = pur_ids
            # response["purchase_ids added"] = json.dumps(pur_ids)
            return response

    # def put(self):
    #     print('in bills')

    #     payload = request.form
        
    #     if payload.get('bill_uid') in {None, '', 'null'}:
    #         print("No bill_uid")
    #         raise BadRequest("Request failed, no UID in payload.")
    #     key = {'bill_uid': payload['bill_uid']}
    #     print("Key: ", key)
    #     bills = {k: v for k, v in payload.items()}
    #     print("KV Pairs: ", bills)
    #     with connect() as db:
    #         print("In actual PUT")
    #         response = db.update('bills', key, bills)
    #     return response
    

    def put(self):
        print("\nIn Bills PUT")
        response = {}

        payload = request.form.to_dict()
        print("Bills Update Payload: ", payload)
        
         # Verify uid has been included in the data
        if payload.get('bill_uid') in {None, '', 'null'}:
            print("No bill_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        # bill_uid = payload.get('bill_uid')
        key = {'bill_uid': payload.pop('bill_uid')}
        print("Bill Key: ", key) 


        # --------------- PROCESS IMAGES ------------------

        processImage(key, payload)
        # print("Payload after processImage function: ", payload, type(payload))
        
        # --------------- PROCESS IMAGES ------------------
        

        # --------------- PROCESS DOCUMENTS ------------------

        processDocument(key, payload)
        # print("Payload after processDocument function: ", payload, type(payload))
        
        # --------------- PROCESS DOCUMENTS ------------------



        # Write to Databaseè
        with connect() as db:
            # print("Checking Inputs: ", key, payload)
            response['bill_info'] = db.update('bills', key, payload)
            # print("Response:" , response)



        # Update PURCHASES Table to reflect new values
        if payload.get('bill_notes') not in {None, '', 'null'} or payload.get('bill_amount') not in {None, '', 'null'}:
            print("purchase table needs to change")

            with connect() as db:
                bill_purchase_query = db.execute(""" SELECT * FROM purchases WHERE pur_bill_id = \'""" + key['bill_uid'] + """\' """)     # Current Purchase associated with Bill
                # print(bill_purchase_query)

                for purchase in bill_purchase_query["result"]:
                    #  print(purchase)
                    #  print("Purchase UID: ", purchase["purchase_uid"], type(purchase["purchase_uid"]))
                     bill_pur_update = {}
                     bill_pur_update["pur_amount_due"] = payload.get('bill_amount')
                     bill_pur_update["pur_notes"] = payload.get('bill_notes')
                    #  print(bill_pur_update)

                     response['bill_update'] = db.update('purchases', {'purchase_uid':purchase["purchase_uid"]}, bill_pur_update)

            
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
                    DELETE FROM bills 
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
            # print("Payload after processDocument function: ", payload, type(payload))
            
            # --------------- PROCESS DOCUMENTS ------------------


           # Add Purachse Info

            # payload['bill_documents'] = '[]' if payload.get('bill_documents') in {None, '', 'null'} else payload.get('bill_documents', '[]')
            # print("Add Appliance Payload: ", payload)  

            payload["purchase_uid"] = newPurchaseUID
            payload["pur_group"] = newPurchaseUID 

            # SET TRANSACTION DATE TO NOW
            # newRequest['pur_timestamp'] = datetime.today().date().strftime('%m-%d-%Y %H:%M')
            payload['pur_timestamp'] = datetime.today().strftime('%m-%d-%Y %H:%M')
            # SET ADDITIONAL FIELDS           
            payload['pur_status_value'] = "0" if payload.get('purchase_status') == "UNPAID" else \
                              "4" if payload.get('purchase_status') == "PARTIALLY PAID" else \
                              "0" if payload.get('purchase_status') == "PAID" else "5"

            # FORMAT DATE FIELDS
            payload['purchase_date'] = f"{payload.get('purchase_date')} 12:00"
            payload['pur_due_date'] = f"{payload.get('pur_due_date')} 12:00"
            # print(datetime.date.today())

            payer = payload.get('pur_payer') 
            payload['pur_cf_type'] = 'revenue' if payer.startswith(('110', '350')) else 'expense'

            response['Add Purchase'] = db.insert('purchases', payload)
            response['purchase_UID'] = newPurchaseUID 
        
            # response['Purchase Documents Added'] = payload.get('purchase_documents', "None")
            # print(response)
            # print("\nNew Purchase Added")



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
