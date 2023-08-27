
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


class Rents(Resource):
    def get(self, owner_id):
        print("in Get Rent Status")

        response = {}

        # print("Property UID: ", property_id)

        with connect() as db:
            print("in connect loop")
            rentQuery = db.execute(""" 
                    -- RENT STATUS BY PROPERTY FOR OWNER PAGE
                    SELECT property_id, property_owner_id, po_owner_percent
                        , property_address, property_unit, property_city, property_state, property_zip, property_images
                        , pp_status.*
                        , IF (ISNULL(payment_status), "VACANT", payment_status) AS rent_status
                    FROM space.property_owner
                    LEFT JOIN space.properties ON property_uid = property_id
                    LEFT JOIN space.pp_status ON pur_property_id = property_id
                    WHERE property_owner_id = \'""" + owner_id + """\'
                        AND (purchase_type = "RENT" OR ISNULL(purchase_type))
                        AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                        AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                    GROUP BY property_id
                    ORDER BY rent_status
                    """)

            
            # print("Query: ", maintenanceQuery)
            response["RentStatus"] = rentQuery
            return response



class RentDetails(Resource):
    def get(self, owner_id):
        print("in Get Rent Status")

        response = {}

        # print("Property UID: ", property_id)

        with connect() as db:
            print("in connect loop")
            rentQuery = db.execute(""" 
                    -- RENT STATUS BY PROPERTY BY MONTH FOR OWNER PAGE
                    SELECT property_id, property_owner_id, po_owner_percent
                        , property_address, property_unit, property_city, property_state, property_zip
                        , pp_status.*
                        , IF (ISNULL(payment_status), "VACANT", payment_status) AS rent_status
                        , IF (payment_status = "UNPAID", DATEDIFF(NOW(),pur_due_date), "") AS overdue
                    FROM space.property_owner
                    LEFT JOIN space.properties ON property_uid = property_id
                    LEFT JOIN space.pp_status ON pur_property_id = property_id
                    WHERE property_owner_id = \'""" + owner_id + """\'
                        AND (purchase_type = "RENT" OR purchase_type = "LATE FEE" OR ISNULL(purchase_type))
                    ORDER BY property_id, pur_due_date
                    """)

            
            # print("Query: ", maintenanceQuery)
            response["RentStatus"] = rentQuery
            return response




    # def post(self):
    #     print("In Make Payment")
    #     response = {}
    #     with connect() as db:
    #         data = request.get_json(force=True)
    #         # print(data)

    #         fields = [
    #             'pay_purchase_id'
    #             , 'pay_amount'
    #             , 'payment_notes'
    #             , 'pay_charge_id'
    #             , 'payment_type'
    #             , 'payment_date'
    #             , 'payment_verify'
    #             , 'paid_by'
    #         ]

    #         # PUTS JSON DATA INTO EACH FILE
    #         newRequest = {}
    #         for field in fields:
    #             newRequest[field] = data.get(field)
    #             # print(field, " = ", newRequest[field])


    #         # # GET NEW UID
    #         # print("Get New Request UID")
    #         newRequestID = db.call('new_payment_uid')['result'][0]['new_id']
    #         newRequest['payment_uid'] = newRequestID
    #         print(newRequestID)

    #         # SET TRANSACTION DATE TO NOW
    #         newRequest['payment_date'] = date.today()

    #         # print(newRequest)

    #         response = db.insert('payments', newRequest)
    #         response['Payments_UID'] = newRequestID

    #     return response