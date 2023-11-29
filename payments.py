
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

            # PUTS JSON DATA INTO EACH FILE
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
                    SELECT *
                    FROM space.pp_details
                    WHERE pur_payer = \'""" + user_id + """\';
                    """)

            
            # print("Query: ", paymentStatus)
            response["PaymentStatus"] = paymentStatus

            paidStatus = db.execute("""
                    -- FIND TENANT PAYMENT HISTORY
                    SELECT *
                    FROM space.pp_details
                    WHERE pur_payer = \'""" + user_id + """\' AND latest_date >= DATE_SUB(NOW(), INTERVAL 365 DAY);
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
