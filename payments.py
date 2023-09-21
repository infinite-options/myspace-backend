
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar



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


            # # GET NEW UID
            # print("Get New Request UID")
            newRequestID = db.call('new_payment_uid')['result'][0]['new_id']
            newRequest['payment_uid'] = newRequestID
            print(newRequestID)

            # SET TRANSACTION DATE TO NOW
            newRequest['payment_date'] = date.today()

            # print(newRequest)

            response = db.insert('payments', newRequest)
            response['Payments_UID'] = newRequestID

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
                    SELECT DISTINCT *
                    FROM space.pp_details
                    WHERE pur_payer = \'""" + user_id + """\' AND ISNULL(payment_uid);
                    """)

            
            # print("Query: ", paymentStatus)
            response["PaymentStatus"] = paymentStatus

            paidStatus = db.execute(""" 
                    SELECT DISTINCT *
                    FROM space.pp_details
                    WHERE pur_payer = \'""" + user_id + """\' AND payment_uid != "NULL";
                    """)

            
            # print("Query: ", paidStatus)
            response["PaidStatus"] = paidStatus


            return response

class PaymentMethod(Resource):
    # decorators = [jwt_required()]

    def post(self):
        print('in PaymentStatus')
        response = {}

        # print("User ID: ", user_id)
        with connect() as db:
            print("in owner dashboard")
            rows = db.execute(""" 
                    SELECT * FROM space.businessProfileInfo
                    """)
        for i in range(0, len(rows["result"])):
            if rows["result"][i]["business_zelle"]!="":
                with connect() as db:
                    cur = db.conn.cursor()

                    sql = """insert into space.paymentMethods (paymentMethod_profile_id, paymentMethod_type, paymentMethod_name, 
                                                                  paymentMethod_billingzip)
                                         values (%s, %s, %s, %s) 
                                    """
                    cur.execute(sql,
                                (rows["result"][i]["business_uid"], 'venmo', rows["result"][i]["business_venmo"],
                                 rows["result"][i]["business_zip"]))
                    db.conn.commit()
            if rows["result"][i]["business_zelle"]!="":
                with connect() as db:
                    cur = db.conn.cursor()

                    sql = """insert into space.paymentMethods (paymentMethod_profile_id, paymentMethod_type, paymentMethod_name, 
                                                                  paymentMethod_billingzip)
                                         values (%s, %s, %s, %s) 
                                    """
                    cur.execute(sql,
                                (rows["result"][i]["business_uid"], 'zelle', rows["result"][i]["business_zelle"],
                                 rows["result"][i]["business_zip"]))
                    db.conn.commit()
            if rows["result"][i]["business_apple_pay"]!="":
                with connect() as db:
                    cur = db.conn.cursor()

                    sql = """insert into space.paymentMethods (paymentMethod_profile_id, paymentMethod_type, paymentMethod_name, 
                                                                  paymentMethod_billingzip)
                                         values (%s, %s, %s, %s) 
                                    """
                    cur.execute(sql,
                                (rows["result"][i]["business_uid"], 'apple_pay', rows["result"][i]["business_apple_pay"],
                                 rows["result"][i]["business_zip"]))
                    db.conn.commit()
            if rows["result"][i]["business_paypal"]!="":
                with connect() as db:
                    cur = db.conn.cursor()

                    sql = """insert into space.paymentMethods (paymentMethod_profile_id, paymentMethod_type, paymentMethod_name, 
                                                                  paymentMethod_billingzip)
                                         values (%s, %s, %s, %s) 
                                    """
                    cur.execute(sql,
                                (
                                rows["result"][i]["business_uid"], 'paypal', rows["result"][i]["business_paypal"],
                                rows["result"][i]["business_zip"]))
                    db.conn.commit()
            if rows["result"][i]["business_account_number"]!="":
                with connect() as db:
                    cur = db.conn.cursor()

                    sql = """insert into space.paymentMethods (paymentMethod_profile_id, paymentMethod_type, paymentMethod_acct, 
                                                                  paymentMethod_routing_number ,paymentMethod_billingzip)
                                         values (%s, %s, %s, %s, %s) 
                                    """
                    cur.execute(sql,
                                (
                                    rows["result"][i]["business_uid"], 'bank', rows["result"][i]["business_account_number"],rows["result"][i]["business_routing_number"]
                                    ,rows["result"][i]["business_zip"]))
                    db.conn.commit()
            #
            #
            # print(rows["result"][i]["business_uid"])

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
