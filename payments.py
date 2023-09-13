
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

    def post(self):
        response = {}

        # print("User ID: ", user_id)
        data = request.get_json(force=True)
        if data["paymentMethod_type"] == "bank":
            with connect() as db:

                # print(data)

                fields = [
                    'paymentMethod_uid'
                    , 'paymentMethod_profile_id'
                    , 'paymentMethod_type'
                    , 'paymentMethod_name'
                    , 'paymentMethod_acct'
                    , 'paymentMethod_routing_number'
                    , 'paymentMethod_micro_deposits'
                    , 'paymentMethod_billingzip'
                    , 'paymentMethod_status'
                ]

                newRequest = {}
                for field in fields:
                    newRequest[field] = data.get(field)

                

                response = db.insert('paymentMethods', newRequest)


                return response
        elif data["paymentMethod_type"] == "card":
            with connect() as db:

                

                fields = [
                    'paymentMethod_uid'
                    , 'paymentMethod_profile_id'
                    , 'paymentMethod_type'
                    , 'paymentMethod_name'
                    , 'paymentMethod_exp_date'
                    , 'paymentMethod_cvv'
                    , 'paymentMethod_billingzip'
                    , 'paymentMethod_status'
                ]

                newRequest = {}
                for field in fields:
                    newRequest[field] = data.get(field)

                

                response = db.insert('paymentMethods', newRequest)

                return response
