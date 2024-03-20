
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest


# NOT SURE I NEED THIS
# def get_new_paymentUID(conn):
#     print("In new UID request")
#     with connect() as db:
#         newPaymentQuery = db.execute("CALL space.new_payment_uid;", "get", conn)
#         if newPaymentQuery["code"] == 280:
#             return newPaymentQuery["result"][0]["new_id"]
#     return "Could not generate new payment UID", 500


class NewPayments(Resource):
    def post(self):
        print("In Make Payment")
        data = request.get_json(force=True)
        print(data)
        
        # data contains a list of dictionaries
        # PART 1: For each purchase id: Change purchase status and record payment
        # PART 2: If payment was for RENT, pay Management Fees
        # PART 3: If there is a convenience fee, add covnenience fee purchase and record payment

        with connect() as db:

            purchase_ids = data['pay_purchase_id']
            num_pi = len(purchase_ids)
            print("number of items: ", num_pi)

            # Iterating over the list
            for item in purchase_ids:
                print("Current Item: ", item)

                # PART 1
                # INSERT ITEM PAYMENT INTO PAYMENTS TABLE
                print("UPDATE PAYMENTS TABLE")

                # GET NEW PAYMENT UID
                # print("Get New Request UID")
                newPaymentRequestID = db.call('new_payment_uid')['result'][0]['new_id']
                # print(newPaymentRequestID)

                paymentQuery = (""" 
                        -- INSERT ITEM PAYMENT INTO PAYMENTS TABLE
                        INSERT INTO space.payments
                        SET payment_uid = \'""" + newPaymentRequestID + """\'
                            , pay_purchase_id =  \'""" + item['purchase_uid'] + """\'
                            , pay_amount = \'""" + item['pur_amount_due'] + """\'
                            , payment_notes = \'""" + data['payment_notes'] + """\'
                            , pay_charge_id = \'""" + data['pay_charge_id'] + """\'
                            , payment_type = \'""" + data['payment_type'] + """\'
                            , payment_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                            , paid_by = \'""" + data['paid_by'] + """\'
                            , payment_intent = \'""" + data['payment_intent'] + """\'
                            , payment_method = \'""" + data['payment_method'] + """\';     
                        """)

                # print("Item Payment Query: ", paymentQuery)
                response = db.execute(paymentQuery, [], 'post')


                #  UPDATE PURCHASES TABLE
                print("UPDATE PURCHASES TABLE")

                # DETERMINE WHAT VALUE TO INSERT INTO purchase-status (UNPAID, PAID, PARTIALLY PAID)
                purchaseInfo = db.execute("""
                            -- QUERY ORIGINAL PURCHASE TO CHECK HOW MUCH IS SUPPOSED TO BE PAID
                            SELECT *
                            FROM space.pp_status
                            -- WHERE purchase_uid = '400-000703';
                            WHERE purchase_uid = \'""" + item['purchase_uid'] + """\';
                            """)
                # print(purchaseInfo)
                amt_remaining_str = purchaseInfo['result'][0]['amt_remaining']
                # print('amt_remaining: ', amt_remaining_str, type(amt_remaining_str))
                amt_remaining = float(amt_remaining_str)
                # print('amt_remaining: ', amt_remaining, type(amt_remaining))
                amt_due = float(purchaseInfo['result'][0]['pur_amount_due'])
                # print('amt_due: ', amt_due, type(amt_due))

                # print(purchaseInfo['result'][0]['pur_due_date'], type(purchaseInfo['result'][0]['pur_due_date']))
                # print(datetime.now(), type(datetime.now()))

                # if purchaseInfo['result'][0]['pur_due_date'] == datetime.now():
                #     print("YES")

                pur_due_date = datetime.strptime(purchaseInfo['result'][0]['pur_due_date'], '%m-%d-%Y')
                # print('pur_due_date: ', pur_due_date, type(pur_due_date))
                
                purchase_status = "UNPAID"
                pur_status_value = "0"
                if amt_remaining <= 0: 
                    if pur_due_date >= datetime.now():
                        purchase_status = "PAID"
                        pur_status_value = "5"
                    else: 
                        purchase_status = "PAID LATE"
                        pur_status_value = "4"
                elif amt_remaining == amt_due: 
                    purchase_status = "UNPAID"
                    pur_status_value = "0"
                else:
                    purchase_status = "PARTIALLY PAID"
                    pur_status_value = "1"
                # print(purchase_status)

                # DEFINE KEY VALUE PAIR
                key = {'purchase_uid': item['purchase_uid']}
                payload = {'purchase_status': purchase_status}
                payload2= {'pur_status_value': pur_status_value}    
                # print(key, payload)

                # UPDATE PURCHASE TABLE WITH PURCHASE STATUS
                response['purchase_table_update'] = db.update('purchases', key, payload)
                response['purchase_status_update'] = db.update('purchases', key, payload2)
                # print(response)
                



                # PART 2
                # DETERMINE IF PAYMENT WAS FOR RENT
                print("Purchase Type: ", purchaseInfo['result'][0]['purchase_type'])
                purchaseType = purchaseInfo['result'][0]['purchase_type']
                if purchaseType == "Rent":
                    #Pay Management Fees associated with purchase_uid

                    managementInfo = db.execute("""
                            -- DETERMINE WHICH PURCHASES ARE MONTHLY MANAGEMENT FEES
                            SELECT *
                            FROM space.purchases
                            WHERE purchase_type = "Management" AND
                                -- pur_description = '400-000023';
                                pur_description = \'""" + item['purchase_uid'] + """\';
                            """)
                    # print(managementInfo)

                    for managementFee in managementInfo['result']:
                        # print("Current Item: ", managementFee)
                        # print(managementFee['purchase_uid'])

                        # SET STATUS AS MAIN PURCHASE UID
                        payload = {'purchase_status': purchase_status}
                        payload2= {'pur_status_value': pur_status_value}  

                        # DEFINE KEY VALUE PAIR
                        key = {'purchase_uid': managementFee['purchase_uid']}
                        payload = {'purchase_status': purchase_status}
                        payload2= {'pur_status_value': pur_status_value}    
                        # print(key, payload)

                        # UPDATE PURCHASE TABLE WITH PURCHASE STATUS
                        response['purchase_table_update'] = db.update('purchases', key, payload)
                        response['purchase_status_update'] = db.update('purchases', key, payload2)
                        # print(response)





                # PART 3
                # DETERMINE IF CONVENIENCE FEES WERE PAID
                print("Convenience fee paid: ", data['pay_fee'], type(data['pay_fee']))
                if data['pay_fee'] > 0:

                    # PART 2A: ENTER PURCHASE AND PAYMENT INFO FOR RECEIPT OF CONVENIENCE FEE
                    # INSERT INTO PURCHASES TABLE
                    newPurchaseRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                    # print(newPurchaseRequestID)

                    # DETERMINE HOW MUCH OF THE CONVENIENCE FEES WAS DUE TO EACH PURCHASE
                    # print(item['pur_amount_due'], type(item['pur_amount_due']))
                    # print(data['pay_total'], type(data['pay_total']))
                    # print(data['pay_fee'], type(data['pay_fee']))
                    itemFee = float(item['pur_amount_due']) / (data['pay_total'] - data['pay_fee']) * data['pay_fee']
                    itemFee = round(itemFee, 2)
                    print("Item Convenience Fee: ", itemFee)

                    feePurchaseQuery = (""" 
                            INSERT INTO space.purchases
                            SET purchase_uid = \'""" + newPurchaseRequestID + """\'
                                , pur_timestamp = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , pur_property_id = \'""" + purchaseInfo['result'][0]['pur_property_id'] + """\'  
                                , purchase_type = "Extra Charges"
                                , pur_cf_type = "revenue"
                                , purchase_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , pur_due_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , pur_amount_due = """ + str(itemFee) + """
                                , purchase_status = "PAID"
                                , pur_notes = "Credit Card Fee - Auto Posted"
                                , pur_description =  \'""" + item['purchase_uid'] + """\'
                                , pur_receiver = \'""" + purchaseInfo['result'][0]['pur_receiver'] + """\'
                                , pur_payer = \'""" + purchaseInfo['result'][0]['pur_payer'] + """\'
                                , pur_initiator = \'""" + purchaseInfo['result'][0]['pur_initiator'] + """\'
                                ;
                            """)

                    # print("Query: ", feePurchaseQuery)
                    queryResponse = db.execute(feePurchaseQuery, [], 'post')
                    # print(queryResponse)

                
                    # INSERT INTO PAYMENTS TABLE
                    # GET NEW UID
                    # print("Get New Request UID")
                    newPaymentRequestID = db.call('new_payment_uid')['result'][0]['new_id']
                    # print(newPaymentRequestID)

                    feePaymentQuery = (""" 
                            -- PAY PURCHASE UID
                            INSERT INTO space.payments
                            SET payment_uid = \'""" + newPaymentRequestID + """\'
                                , pay_purchase_id =  \'""" + newPurchaseRequestID + """\'
                                , pay_amount = """ + str(itemFee) + """
                                , payment_notes = \'""" + data['payment_notes'] + """\'
                                , pay_charge_id = \'""" + data['pay_charge_id'] + """\'
                                , payment_type = \'""" + data['payment_type'] + """\'
                                , payment_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , paid_by = \'""" + data['paid_by'] + """\'
                                , payment_intent = \'""" + data['payment_intent'] + """\'
                                , payment_method = \'""" + data['payment_method'] + """\';     
                            """)

                    # print("Query: ", feePaymentQuery)
                    response = db.execute(feePaymentQuery, [], 'post')


                    # PART 2B: ENTER PURCHASE AND PAYMENT INFO FOR PAYMENT OF CONVENIENCE FEE
                    # INSERT INTO PURCHASES TABLE
                    newPurchaseRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                    # print(newPurchaseRequestID)

                    feePurchaseQuery = (""" 
                            INSERT INTO space.purchases
                            SET purchase_uid = \'""" + newPurchaseRequestID + """\'
                                , pur_timestamp = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , pur_property_id = \'""" + purchaseInfo['result'][0]['pur_property_id'] + """\'  
                                , purchase_type = "Bill Posting"
                                , pur_cf_type = "expense"
                                , purchase_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , pur_due_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , pur_amount_due = """ + str(itemFee) + """
                                , purchase_status = "PAID"
                                , pur_notes = "Credit Card Fee - Auto Posted"
                                , pur_description =  \'""" + item['purchase_uid'] + """\'
                                , pur_receiver = "STRIPE"
                                , pur_payer = \'""" + purchaseInfo['result'][0]['pur_receiver'] + """\'
                                , pur_initiator = \'""" + purchaseInfo['result'][0]['pur_initiator'] + """\'
                                ;
                            """)

                    # print("Query: ", feePurchaseQuery)
                    queryResponse = db.execute(feePurchaseQuery, [], 'post')
                    # print(queryResponse)

                
                    # INSERT INTO PAYMENTS TABLE
                    # GET NEW UID
                    # print("Get New Request UID")
                    newPaymentRequestID = db.call('new_payment_uid')['result'][0]['new_id']
                    # print(newPaymentRequestID)

                    feePaymentQuery = (""" 
                            -- PAY PURCHASE UID
                            INSERT INTO space.payments
                            SET payment_uid = \'""" + newPaymentRequestID + """\'
                                , pay_purchase_id =  \'""" + newPurchaseRequestID + """\'
                                , pay_amount = """ + str(itemFee) + """
                                , payment_notes = \'""" + data['payment_notes'] + """\'
                                , pay_charge_id = \'""" + data['pay_charge_id'] + """\'
                                , payment_type = \'""" + data['payment_type'] + """\'
                                , payment_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , paid_by = \'""" + purchaseInfo['result'][0]['pur_receiver'] + """\'
                                , payment_intent = \'""" + data['payment_intent'] + """\'
                                , payment_method = \'""" + data['payment_method'] + """\';     
                            """)

                    # print("Query: ", feePaymentQuery)
                    response = db.execute(feePaymentQuery, [], 'post')


                else:
                    print("No convenience fee")


        return data 
    
    















class Payments(Resource):
    def post(self):
        print("In Make Payment")
        response = {}
        with connect() as db:
            data = request.get_json(force=True)
            # print(data)

            fields = [
                'pay_purchase_id'
                , 'pay_amount'
                , 'payment_notes'
                , 'pay_charge_id'
                , 'payment_type'
                , 'payment_date'
                , 'payment_verify'
                , 'paid_by'
                , 'payment_intent'
                , 'payment_method'
                , 'payment_date_cleared'
                , 'payment_client_secret'
            ]

            # PUTS JSON DATA INTO EACH FIELD
            newRequest = {}
            for field in fields:
                newRequest[field] = data.get(field)
                # print(field, " = ", newRequest[field])


            # GET NEW UID
            # print("Get New Request UID")
            newRequestID = db.call('new_payment_uid')['result'][0]['new_id']
            newRequest['payment_uid'] = newRequestID
            # print(newRequestID)

            # SET TRANSACTION DATE TO NOW
            newRequest['payment_date'] = date.today()
            newRequest['payment_verify'] = "Unverified"

            # INSERT DATA INTO PAYMENTS TABLE
            # print(newRequest)
            response['Payment_Insert'] = db.insert('payments', newRequest)
            response['Payments_UID'] = newRequestID
            # print(response)

            # DETERMINE WHAT VALUE TO INSERT INTO purchase-status (UNPAID, PAID, PARTIALLY PAID)
            amt_due = db.execute("""
                        -- CHECK HOW MUCH IS SUPPOSED TO BE PAID
                        SELECT -- *,
                            amt_remaining
                        FROM space.pp_status
                        WHERE purchase_uid = \'""" + newRequest['pay_purchase_id'] + """\';
                        """)
            # print(amt_due)
            # print("Amount Due: ", amt_due['result'][0]['amt_remaining'])

            purchase_status = "UNPAID"
            print(type(amt_due['result'][0]['amt_remaining']))
            if float(amt_due['result'][0]['amt_remaining']) <= 0: 
                purchase_status = "PAID"
                # print(purchase_status)

                # payload = {'purchase_uid': newRequest['pay_purchase_id'], 'purchase_status': purchase_status}
                payload = {'purchase_status': purchase_status}
                key = {'purchase_uid': newRequest['pay_purchase_id']}
                # print(key, payload)

                
                # response['b'] = db.update('purchases', key, purchase_status)
                response['purchase_table_update'] = db.update('purchases', key, payload)
                response['purchase_status'] = purchase_status

            else:
                response['purchase_status'] = 'UNPAID'


            # # DETERMINE PURCHASE STATUS

            # # DETERMINE AMOUNT DUE
            # # print("Purchase ID: ", newRequest['pay_purchase_id'])
            # amt_due = db.execute("""
            #         -- CHECK HOW MUCH IS SUPPOSED TO BE PAID
            #         SELECT -- *,
            #             pur_amount_due
            #         FROM space.purchases
            #         WHERE purchase_uid = \'""" + newRequest['pay_purchase_id'] + """\';
            #         """)
            # # print("Amount Due: ", amt_due['result'][0]['pur_amount_due'])

            # # DETERMINE HOW MUCH WAS PAID BEFORE
            # purchase_status = "Payment Error"

            # # print(type(newRequest['pay_amount']))
            # # print(type(amt_due['result'][0]['pur_amount_due']))

            # if newRequest['pay_amount'] >= float(amt_due['result'][0]['pur_amount_due']): purchase_status = "PAID"
            # elif newRequest['pay_amount'] < float(amt_due['result'][0]['pur_amount_due']): purchase_status = "PARTIALLY PAID"
            
            # # print(purchase_status)
            # # print(newRequest['pay_purchase_id'])

            # # purRequest = {}
            # # purRequest['purchase_status'] = purchase_status
            # # pur_response = db.insert('purchases', purRequest)

            # key = {'purchase_uid': newRequest['pay_purchase_id']}
            # # print(key)
            # payload = {'purchase_status': purchase_status}
            # # print(payload)
            # response['Purchase_Status_Update'] = db.update('purchases', key, payload)
            # response['Purchase_Status'] = purchase_status

        return response  



    def put(self):
        print('in Update Payment Status')
        with connect() as db:
            payload = request.get_json()
            # print(payload)

            if payload.get('pay_purchase_id') is None:
                raise BadRequest("Request failed, no UID in payload.")
        
            payload_purchase_uid = {'purchase_uid': payload.pop('pay_purchase_id')}
            # print(payload_purchase_uid)
            purchase_uid = payload_purchase_uid['purchase_uid']
            # print(purchase_uid)


            # DETERMINE WHAT VALUE TO INSERT INTO purchase-status (UNPAID, PAID, PARTIALLY PAID)
            amt_due = db.execute("""
                        -- CHECK HOW MUCH IS SUPPOSED TO BE PAID
                        SELECT -- *,
                            amt_remaining
                        FROM space.pp_status
                        WHERE purchase_uid = \'""" + purchase_uid + """\';
                        """)
            # print(amt_due)
            # print("Amount Due: ", amt_due['result'][0]['amt_remaining'])

            purchase_status = "UNPAID"
            # print(type(amt_due['result'][0]['amt_remaining']))
            if float(amt_due['result'][0]['amt_remaining']) <= 0: purchase_status = "PAID"
            # print(purchase_status)

            payload = {'purchase_uid': '400-000531', 'purchase_status': purchase_status}

            
            response = db.update('purchases', payload_purchase_uid, payload)
            response['purchase_status'] = purchase_status
        return response



class PaymentStatus(Resource):
    # decorators = [jwt_required()]

    def get(self, user_id):
        print('in PaymentStatus')
        response = {}

        # print("User ID: ", user_id)

        with connect() as db:
            # print("in connect loop")
            paymentStatus = db.execute(""" 
                    -- FIND TENANT PAYABLES
                    SELECT pp_details.*, bill_maintenance_quote_id
                    FROM space.pp_details
                    LEFT JOIN space.bills ON bill_uid = pur_bill_id
                    WHERE pur_payer = \'""" + user_id + """\' and purchase_status IN ('UNPAID','PARTIALLY PAID');
                    """)

            
            # print("Query: ", paymentStatus)
            response["PaymentStatus"] = paymentStatus

            rentStatus = db.execute("""
                    -- GET RENT DETAILS
                    SELECT * from space.leaseFees
                    LEFT JOIN space.leases ON lease_uid = fees_lease_id
                    LEFT JOIN space.lease_tenant ON lt_lease_id = lease_uid
                    WHERE lease_status = "ACTIVE" AND lt_tenant_id = \'""" + user_id + """\' """)

            response["RentStatus"] = rentStatus
            
            paidStatus = db.execute("""
                    -- FIND TENANT PAYMENT HISTORY
                    SELECT * FROM space.payments
                    LEFT JOIN space.purchases ON pay_purchase_id = purchase_uid
                    -- WHERE paid_by = '350-000002'
                    WHERE paid_by = \'""" + user_id + """\'
                    """)

            
            # print("Query: ", paidStatus)
            response["PaidStatus"] = paidStatus


            return response

class PaymentMethod(Resource):
    def post(self):
        response = []
        payload = request.get_json()
        print(payload)
        with connect() as db:
            query_response = db.insert('paymentMethods', payload)
            print(query_response)
            response.append(query_response)
        return response
    
    def put(self):
        response = {}
        payload = request.get_json()
        if payload.get('paymentMethod_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'paymentMethod_uid': payload.pop('paymentMethod_uid')}
        with connect() as db:
            response = db.update('paymentMethods', key, payload)
        return response

    def get(self, user_id):
        print('in PaymentMethod GET')
        with connect() as db:
            paymentMethodQuery = db.execute("""
                -- FIND APPLICATIONS CURRENTLY IN PROGRESS
                SELECT *
                FROM space.paymentMethods
                WHERE paymentMethod_profile_id = \'""" + user_id + """\';
                 """)
        return paymentMethodQuery

    def delete(self, user_id):
        print("In paymentMethods DELETE")
        print(user_id)
        response = {}
        with connect() as db:

            paymentQuery = ("""
                    DELETE 
                    FROM space.paymentMethods
                    WHERE paymentMethod_uid = \'""" + user_id + """\';
                    """)

            response["delete_paymentMethods"] = db.delete(paymentQuery)


        return response

class RequestPayment(Resource):
    def post(self):
        print('in Request Payment')
        with connect() as db:
            data = request.get_json()

            new_bill_uid = db.call('space.new_bill_uid')['result'][0]['new_id']
            bill_description = data["bill_description"]
            bill_amount = data["bill_amount"]
            bill_created_by = data["bill_created_by"]
            bill_utility_type = data["bill_utility_type"]
            bill_split = data["bill_split"]
            bill_property_id = data["bill_property_id"]
            bill_docs = data["bill_docs"]
            bill_maintenance_quote_id = data["bill_maintenance_quote_id"]
            bill_notes = data["bill_notes"]

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
                    , bill_docs = \'""" + json.dumps(bill_docs, sort_keys=False) + """\'
                    , bill_notes = \'""" + bill_description + """\'
                    , bill_maintenance_quote_id = \'""" + bill_maintenance_quote_id + """\';          
                    """)

            response = db.execute(billQuery, [], 'post')

            newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
            purchase_uid = newRequestID

            pur_payer_st = db.select('property_owner', {'property_id': data["bill_property_id"]})
            pur_payer = pur_payer_st.get('result')[0]['property_owner_id']

            maintenance_request_id_st = db.select('maintenanceQuotes', {'maintenance_quote_uid': bill_maintenance_quote_id})
            quote_maintenance_request_id = maintenance_request_id_st.get('result')[0]['quote_maintenance_request_id']
            quote_maintenance_close_dt = db.select('maintenanceRequests', {'maintenance_request_uid': quote_maintenance_request_id})
            quote_close_date = quote_maintenance_close_dt.get('result')[0]['maintenance_request_closed_date']

            purchaseQuery = (""" 
                                    INSERT INTO space.purchases
                                    SET purchase_uid = \'""" + purchase_uid + """\'
                                        , pur_timestamp = CURRENT_TIMESTAMP()
                                        , pur_property_id = \'""" + bill_property_id + """\'
                                        , purchase_type = "MAINTENANCE"
                                        , pur_cf_type = "expense"
                                        , pur_bill_id = \'""" + new_bill_uid + """\'
                                        , purchase_date = \'""" + quote_close_date + """\'
                                        , pur_due_date = DATE_ADD(LAST_DAY(CURRENT_DATE()), INTERVAL 30 DAY)
                                        , pur_amount_due = \'""" + str(bill_amount) + """\'
                                        , purchase_status = "UNPAID"
                                        , pur_notes = "AUTO CREATED WHEN MAINTENANCE WAS COMPLETED"
                                        , pur_description = "AUTO CREATED WHEN MAINTENANCE WAS COMPLETED"
                                        , pur_receiver = \'""" + bill_created_by + """\'
                                        , pur_payer = \'""" + pur_payer + """\'
                                        , pur_initiator = \'""" + bill_created_by + """\';
                                    """)

            # print("Query: ", purchaseQuery)
            queryResponse = db.execute(purchaseQuery, [], 'post')

        return queryResponse
