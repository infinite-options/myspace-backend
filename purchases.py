
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar


# OVERVIEW
#           TENANT      OWNER     PROPERTY MANAGER     
# BY MONTH    X           X               X
# BY YEAR     X           X               X


class Bills(Resource):
    def post(self):
        print("In add Bill")

        try:
            conn = connect()
            response = {}
            response['message'] = []
            data = request.get_json(force=True)
            print(data)

            #  Get New Bill UID
            new_bill_uid = get_new_billUID(conn)
            print(new_bill_uid)


            # Set Variables from JSON OBJECT
            bill_description = data["bill_description"]
            bill_amount = data["bill_amount"]
            bill_created_by = data["bill_created_by"]
            bill_utility_type = data["bill_utility_type"]
            bill_split = data["bill_split"]
            bill_property_id = data["bill_property_id"]
            print(str(json.dumps(bill_property_id)))
            bill_docs = data["bill_docs"]

            billQuery = (""" 
                    -- CREATE NEW BILL
                    INSERT INTO space.bills
                    SET bill_uid = \'""" + new_bill_uid + """\'
                    , bill_timestamp = CURRENT_TIMESTAMP()
                    , bill_description = \'""" + bill_description + """\'
                    , bill_amount = \'""" + str(bill_amount) + """\'
                    , bill_created_by = \'""" + bill_created_by + """\'
                    , bill_utility_type = \'""" + bill_utility_type + """\'
                    , bill_split = \'""" + bill_split + """\'
                    , bill_property_id = \'""" + json.dumps(bill_property_id, sort_keys=False) + """\'
                    , bill_docs = \'""" + json.dumps(bill_docs, sort_keys=False) + """\';          
                    """)

            # print("Query: ", billQuery)
            response = execute(billQuery, "post", conn) 
            # print("Query out", response["code"])
            response["bill_uid"] = new_bill_uid

            # Works to this point


            try:
                print("made it here", bill_property_id)
                split_num = len(bill_property_id)
                print(split_num)
                split_bill_amount = round(bill_amount/split_num,2)
                for data_dict in bill_property_id:
                    for key, value in data_dict.items():
                        # print(f"{key}: {value}")
                        # print(value)
                        pur_property_id = value
                        print("Input to Find Responsible Party Query:  ", pur_property_id, bill_utility_type)

                        # For each property ID and utility, identify the responsible party

                        responsibleQuery = (""" 
                            -- UTILITY PAYMENT REPOSONSIBILITY BY PROPERTY
                            SELECT responsible_party
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

                        # print("Query: ", responsibleQuery)
                        queryResponse = execute(responsibleQuery, "get", conn)
                        print("queryResponse is: ", queryResponse)
                        responsibleArray = queryResponse['result'][0]
                        print("Responsible Party is: ", responsibleArray)


                        # THESE STATEMENTS DO THE SAME THING
                        responsibleParty = queryResponse['result'][0]['responsible_party']
                        print("Responsible Party is: ", responsibleParty)
                        responsibleParty = responsibleArray['responsible_party']
                        print("Responsible Party is: ", responsibleParty)

                        # STILL NEED TO ADD A LOOP FOR EACH RESPONSIBLE PARTY   

                        # for data_dict2 in responsibleArray:
                        #     for key, value in data_dict2.items():
                        #         print(f"{key}: {value}")
                        #         print(value)
                
                        # post a Purchase for each property

                        #  Get New Bill UID
                        new_purchase_uid = get_new_purchaseUID(conn)
                        print(new_purchase_uid)

                        # Determine if this is a revenue or expense
                        if responsibleParty[:3] == "350": pur_cf_type = "revenue"
                        else: pur_cf_type = "expense"


                        purchaseQuery = (""" 
                            INSERT INTO space.purchases
                            SET purchase_uid = \'""" + new_purchase_uid + """\'
                                , pur_timestamp = CURRENT_TIMESTAMP()
                                , pur_property_id = \'""" + pur_property_id  + """\'
                                , purchase_type = "BILL POSTING"
                                , pur_cf_type = \'""" + pur_cf_type  + """\'
                                , pur_bill_id = \'""" + new_bill_uid + """\'
                                , purchase_date = CURRENT_DATE()
                                , pur_due_date = DATE_ADD(LAST_DAY(CURRENT_DATE()), INTERVAL 1 DAY)
                                , pur_amount_due = \'""" + str(split_bill_amount) + """\'
                                , purchase_status = "UNPAID"
                                , pur_notes = "THIS IS A TEST"
                                , pur_description = "THIS IS ONLY A TEST"
                                , pur_receiver = "600-000003"
                                , pur_payer = \'""" + responsibleParty + """\'
                                , pur_initiator = \'""" + bill_created_by + """\';
                            """)

                        print("Query: ", purchaseQuery)
                        queryResponse = execute(purchaseQuery, "post", conn)
                        print("queryResponse is: ", queryResponse)
                        # responsibleArray = queryResponse['result'][0]
                        # print("Responsible Party is: ", responsibleArray)


                        # # THESE STATEMENTS DO THE SAME THING
                        # responsibleParty = queryResponse['result'][0]['responsible_party']
                        # print("Responsible Party is: ", responsibleParty)
                        # responsibleParty = responsibleArray['responsible_party']
                        # print("Responsible Party is: ", responsibleParty)



                        continue

            except json.JSONDecodeError:
                print("Invalid JSON format.")


            return response

        except:
            print("Error in Add Bill Query")
        finally:
            disconnect(conn)

    
    def delete(self):
        print("In delete Bill")

        try:
            conn = connect()
            response = {}
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
            response = execute(delBillQuery, "del", conn) 
            # print("Query out", response["code"])
            response["Deleted bill_uid"] = bill_uid


            return response

        except:
            print("Error in Add Bill Query")
        finally:
            disconnect(conn)