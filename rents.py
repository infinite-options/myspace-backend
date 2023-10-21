
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
                    SELECT -- *,
                        property_uid, p.property_address, p.property_unit, p.property_city, p.property_state, p.property_zip, p.property_type
                        , property_num_beds, property_num_baths, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, p.property_images, p.property_description, p.property_notes
                        , lease_uid, lease_start, lease_end, lease_status, lease_rent, lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, lease_actual_rent
                        , tenant_uid, p.tenant_first_name, p.tenant_last_name, p.tenant_email, p.tenant_phone_number
                        , p.owner_uid, p.owner_first_name, p.owner_last_name, p.owner_email, p.owner_phone_number
                        , contract_documents
                        , business_uid, p.business_name, p.business_email, p.business_phone_number
                        , latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_year
                    -- 	, num AS num_open_maintenace_req
                        , CASE
                                WHEN (lease_status = 'ACTIVE' AND payment_status IS NOT NULL) THEN payment_status
                                WHEN (lease_status = 'ACTIVE' AND payment_status IS NULL) THEN 'UNPAID'
                                ELSE 'VACANT'
                            END AS rent_status
                    FROM (
                        SELECT * FROM space.p_details
                        WHERE owner_uid = \'""" + owner_id + """\'
                        ) as p
                    LEFT JOIN (
                        SELECT * 
                        FROM space.pp_details 
                        WHERE (purchase_type = "RENT" OR ISNULL(purchase_type))
                        AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                        AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                        ) AS r ON property_uid = pur_property_id
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