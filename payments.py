
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# newRequest['pur_timestamp'] = datetime.now().strftime("%m-%d-%Y %H:%M")
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
#         newPaymentQuery = db.execute("CALL space_dev.new_payment_uid;", "get", conn)
#         if newPaymentQuery["code"] == 280:
#             return newPaymentQuery["result"][0]["new_id"]
#     return "Could not generate new payment UID", 500


class NewPayments(Resource):
    def post(self):
        print("In New Payment")
        data = request.get_json(force=True)
        print(data)
        
        # data contains a list of dictionaries
        # PART 1: For each purchase id: Change purchase status and record payment
        # PART 2: If payment was for RENT, pay Management Fees
        # PART 3: If there is a convenience fee, add covnenience fee purchase and record payment

        with connect() as db:

            purchase_ids = data['pay_purchase_id']
            # total_paid = data['cashflow_total'] if data['cashflow_total'] else data['pay_total']
            # Check if 'cashflow_total' exists and is not None
            if 'cashflow_total' in data and data['cashflow_total'] is not None:
                total_paid = data.pop('cashflow_total')  # Pop the value from the dictionary
            else:
                total_paid = data.get('pay_total', 0)  # Use 'pay_total' or default to 0

            num_pi = len(purchase_ids)
            print("number of items: ", num_pi, total_paid)

            # Iterating over the list
            for item in purchase_ids:
                print("Current Item: ", item)

                # PART 1
                # INSERT ITEM PAYMENT INTO PAYMENTS TABLE
                print("UPDATE PAYMENTS TABLE")

                # GET NEW PAYMENT UID
                # print("Get New Request UID")
                newPaymentRequestID = db.call('space_dev.new_payment_uid')['result'][0]['new_id']
                print(newPaymentRequestID)

                if total_paid > round(float(item['pur_amount_due']), 2):
                    paid_amt = item['pur_amount_due']
                    print("Paid Amount: ", paid_amt, type(paid_amt))
                    total_paid = total_paid - round(float(item['pur_amount_due']), 2)
                    print("Total Paid: ", total_paid)
                else:
                    paid_amt = str(total_paid)
                    print("Paid Amount: ", paid_amt, type(paid_amt))
                    total_paid = 0
                    print("Total Paid Else: ", total_paid)

                paymentQuery = (""" 
                        -- INSERT ITEM PAYMENT INTO PAYMENTS TABLE
                        INSERT INTO space_dev.payments
                        SET payment_uid = \'""" + newPaymentRequestID + """\'
                            , pay_purchase_id =  \'""" + item['purchase_uid'] + """\'
                            , pay_amount = \'""" + paid_amt + """\'
                            , payment_notes = \'""" + data['payment_notes'] + """\'
                            , pay_charge_id = \'""" + data['pay_charge_id'] + """\'
                            , payment_type = \'""" + data['payment_type'] + """\'
                            , payment_date = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 8 HOUR), '%m-%d-%Y %H:%i')
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
                            FROM space_dev.pp_status
                            -- WHERE purchase_uid = '400-000703';
                            WHERE purchase_uid = \'""" + item['purchase_uid'] + """\';
                            """)
                print(purchaseInfo)
                amt_remaining_str = purchaseInfo['result'][0]['amt_remaining']
                # print('amt_remaining: ', amt_remaining_str, type(amt_remaining_str))
                amt_remaining = round(float(amt_remaining_str), 2)
                print('amt_remaining: ', amt_remaining, type(amt_remaining))
                amt_due = round(float(purchaseInfo['result'][0]['pur_amount_due']), 2)
                print('amt_due: ', amt_due, type(amt_due))

                
                print("Date Time Stamp: ", datetime.now(), type(datetime.now()))


                if purchaseInfo['result'][0]['pur_due_date'] not in (None, '', 'None'):
                    try:
                        print("Due Date Provided? ", purchaseInfo['result'][0]['pur_due_date'], type(purchaseInfo['result'][0]['pur_due_date']))
                        # Parse the date and time, handle cases where time may not be included
                        if ' ' in purchaseInfo['result'][0]['pur_due_date']:
                            pur_due_date = datetime.strptime(purchaseInfo['result'][0]['pur_due_date'], '%m-%d-%Y %H:%M')
                        else:
                            pur_due_date = datetime.strptime(purchaseInfo['result'][0]['pur_due_date'], '%m-%d-%Y')  # No time included, defaults to 00:00
                    except ValueError as e:
                        print("Error:", e)
                else:
                    pur_due_date = datetime.today().strftime('%m-%d-%Y %H:%M')

                print("Purchase Due Date: ", pur_due_date, type(pur_due_date))

            
                
                purchase_status = "UNPAID"
                pur_status_value = "0"
                # print("Amount Reamining: ", amt_remaining, type(amt_remaining))
                if amt_remaining <= 0: 
                    print("Date Check: ", pur_due_date, datetime.now())
                    if pur_due_date.date() >= datetime.now().date():
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
                # print("Purchase Status: ", purchase_status)

                # DEFINE KEY VALUE PAIR
                key = {'purchase_uid': item['purchase_uid']}
                payload = {'purchase_status': purchase_status}
                payload2= {'pur_status_value': pur_status_value}    
                # print(key, payload)

                # UPDATE PURCHASE TABLE WITH PURCHASE STATUS
                response['purchase_table_update'] = db.update('space_dev.purchases', key, payload)
                response['purchase_status_update'] = db.update('space_dev.purchases', key, payload2)
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
                            FROM space_dev.purchases
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
                        response['purchase_table_update'] = db.update('space_dev.purchases', key, payload)
                        response['purchase_status_update'] = db.update('space_dev.purchases', key, payload2)
                        # print(response)





                # PART 3
                # DETERMINE IF CONVENIENCE FEES WERE PAID
                print("Convenience fee paid: ", data['pay_fee'], type(data['pay_fee']))
                if data['pay_fee'] > 0:

                    # PART 2A: ENTER PURCHASE AND PAYMENT INFO FOR RECEIPT OF CONVENIENCE FEE
                    # INSERT INTO PURCHASES TABLE
                    newPurchaseRequestID = db.call('space_dev.new_purchase_uid')['result'][0]['new_id']
                    # print("Part 2A: ", newPurchaseRequestID)

                    # DETERMINE HOW MUCH OF THE CONVENIENCE FEES WAS DUE TO EACH PURCHASE
                    # print(item['pur_amount_due'], type(item['pur_amount_due']))
                    # print(data['pay_total'], type(data['pay_total']))
                    # print(data['pay_fee'], type(data['pay_fee']))

                    if float(item['pur_amount_due']) >= (data['pay_total'] - data['pay_fee']):
                        itemFee = data['pay_fee']
                        itemFee = round(itemFee, 2)
                        print("Item Convenience Fee: ", itemFee)
                    else: 
                        itemFee = float(item['pur_amount_due']) / (data['pay_total'] - data['pay_fee']) * data['pay_fee']
                        itemFee = round(itemFee, 2)
                        print("Item Convenience Fee: ", itemFee)

                    feePurchaseQuery = (""" 
                            INSERT INTO space_dev.purchases
                            SET purchase_uid = \'""" + newPurchaseRequestID + """\'
                                , pur_group = \'""" + newPurchaseRequestID + """\'
                                , pur_timestamp = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                , pur_property_id = \'""" + purchaseInfo['result'][0]['pur_property_id'] + """\'  
                                , purchase_type = "Extra Charges"
                                , pur_cf_type = "revenue"
                                , purchase_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                , pur_due_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                , pur_amount_due = """ + str(itemFee) + """
                                , purchase_status = "PAID"
                                , pur_status_value = '5'
                                , pur_notes = \'""" + item['purchase_uid'] + """\'
                                , pur_description =  "Credit Card Fee - Auto Posted"
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
                    newPaymentRequestID = db.call('space_dev.new_payment_uid')['result'][0]['new_id']
                    # print(newPaymentRequestID)

                    feePaymentQuery = (""" 
                            -- PAY PURCHASE UID
                            INSERT INTO space_dev.payments
                            SET payment_uid = \'""" + newPaymentRequestID + """\'
                                , pay_purchase_id =  \'""" + newPurchaseRequestID + """\'
                                , pay_amount = """ + str(itemFee) + """
                                , payment_notes = \'""" + data['payment_notes'] + """\'
                                , pay_charge_id = \'""" + data['pay_charge_id'] + """\'
                                , payment_type = \'""" + data['payment_type'] + """\'
                                , payment_date = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 8 HOUR), '%m-%d-%Y %H:%i')
                                , payment_verify = "Verified"
                                , paid_by = \'""" + data['paid_by'] + """\'
                                , payment_intent = \'""" + data['payment_intent'] + """\'
                                , payment_method = \'""" + data['payment_method'] + """\';     
                            """)

                    # print("Query: ", feePaymentQuery)
                    response = db.execute(feePaymentQuery, [], 'post')


                    # PART 2B: ENTER PURCHASE AND PAYMENT INFO FOR PAYMENT OF CONVENIENCE FEE
                    # INSERT INTO PURCHASES TABLE
                    newPurchaseRequestID = db.call('space_dev.new_purchase_uid')['result'][0]['new_id']
                    # print("Part 2B: ", newPurchaseRequestID)

                    feePurchaseQuery = (""" 
                            INSERT INTO space_dev.purchases
                            SET purchase_uid = \'""" + newPurchaseRequestID + """\'
                                , pur_group = \'""" + newPurchaseRequestID + """\'
                                , pur_timestamp = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                , pur_property_id = \'""" + purchaseInfo['result'][0]['pur_property_id'] + """\'  
                                , purchase_type = "Bill Posting"
                                , pur_cf_type = "expense"
                                , purchase_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                , pur_due_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y %H:%i')
                                , pur_amount_due = """ + str(itemFee) + """
                                , purchase_status = "PAID"
                                , pur_status_value = '5'
                                , pur_notes = \'""" + item['purchase_uid'] + """\'
                                , pur_description =  "Credit Card Fee - Auto Posted"
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
                    newPaymentRequestID = db.call('space_dev.new_payment_uid')['result'][0]['new_id']
                    # print(newPaymentRequestID)

                    feePaymentQuery = (""" 
                            -- PAY PURCHASE UID
                            INSERT INTO space_dev.payments
                            SET payment_uid = \'""" + newPaymentRequestID + """\'
                                , pay_purchase_id =  \'""" + newPurchaseRequestID + """\'
                                , pay_amount = """ + str(itemFee) + """
                                , payment_notes = \'""" + data['payment_notes'] + """\'
                                , pay_charge_id = \'""" + data['pay_charge_id'] + """\'
                                , payment_type = \'""" + data['payment_type'] + """\'
                                , payment_date = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 8 HOUR), '%m-%d-%Y %H:%i')
                                , payment_verify = "Verified"
                                , paid_by = \'""" + purchaseInfo['result'][0]['pur_receiver'] + """\'
                                , payment_intent = \'""" + data['payment_intent'] + """\'
                                , payment_method = \'""" + data['payment_method'] + """\';     
                            """)

                    # print("Query: ", feePaymentQuery)
                    response = db.execute(feePaymentQuery, [], 'post')


                else:
                    print("No convenience fee")

                if total_paid == 0:
                    break
        

        return data 
    

class PaymentMethod(Resource):
    def post(self):
        print("In Payment Method POST")
        response = {}
        payload = request.get_json(force=True)
        print("POST Payload: ", type(payload), payload)
        i = 0

        with connect() as db:

            if isinstance(payload, list):
                for item in payload:
                    print(item)
                    query_response = db.insert('space_dev.paymentMethods', item)
                    print(query_response)
                    response[i] = query_response
                    i += 1

            else:
                query_response = db.insert('space_dev.paymentMethods', payload)
                print(query_response)
                response = query_response

        return response
    
    def put(self):
        print("In Payment Method PUT")
        response = {}
        payload = request.get_json(force=True)
        print("PUT Payload: ", type(payload), payload)
        i = 0

        with connect() as db:

            if isinstance(payload, list):
                print("In List")
                for item in payload:
                    if item.get('paymentMethod_uid') is None:
                        print("Bad Request: ", item)
                        raise BadRequest("Request failed, no UID in payload.")
                    else:
                        print(item)
                        key = {'paymentMethod_uid': item.pop('paymentMethod_uid')}
                        print("Key: ", type(key), key)
                        query_response = db.update('space_dev.paymentMethods', key, item)
                        print(query_response)
                        response[i] = query_response
                        i += 1

            else:
                if payload.get('paymentMethod_uid') in {None, '', 'null'}:
                    print("No paymentMethod_uid")
                    raise BadRequest("Request failed, no UID in payload.")
                key = {'paymentMethod_uid': payload.pop('paymentMethod_uid')}
                query_response = db.update('space_dev.paymentMethods', key, payload)
                print(query_response)
                response = query_response

        return response

    def get(self, user_id):
        print('in PaymentMethod GET')
        with connect() as db:
            paymentMethodQuery = db.execute("""
                -- FIND APPLICATIONS CURRENTLY IN PROGRESS
                SELECT *
                FROM space_dev.paymentMethods
                WHERE paymentMethod_profile_id = \'""" + user_id + """\';
                 """)
        return paymentMethodQuery

    def delete(self, user_id, payment_id):
        print("In paymentMethods DELETE")
        print(user_id)
        response = {}
        with connect() as db:

            paymentQuery = ("""
                    DELETE 
                    FROM space_dev.paymentMethods
                    -- WHERE paymentMethod_profile_id = '350-000007' AND paymentMethod_uid = '070-000075';
                    WHERE paymentMethod_profile_id = \'""" + user_id + """\' AND paymentMethod_uid = \'""" + payment_id + """\';
                    """)

            response["delete_paymentMethods"] = db.delete(paymentQuery)


        return response
