
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
    def get(self, uid):
        print("in Get Rent Status")

        response = {}

        # print("Property UID: ", property_id)

        if uid[:3] == '110':
            print("In Owner ID")
            with connect() as db:
                # print("in connect loop")
                rentsQuery = db.execute("""  
                    -- RENT STATUS BY PROPERTY FOR OWNER PAGE
                    SELECT -- *,
                        p.property_uid, p.property_address, p.property_unit, p.property_city, p.property_state, p.property_zip, p.property_type
                        , p.property_num_beds, p.property_num_baths, p.property_area, p.property_listed_rent, p.property_deposit, p.property_pets_allowed, p.property_deposit_for_rent, p.property_images, p.property_description, p.property_notes
                        , p.lease_uid, p.lease_start, p.lease_end, p.lease_status, p.leaseFees, p.lease_assigned_contacts, p.lease_documents, p.lease_early_end_date, p.lease_renew_status
                        , p.tenant_uid, p.tenant_first_name, p.tenant_last_name, p.tenant_email, p.tenant_phone_number
                        , p.owner_uid, p.owner_first_name, p.owner_last_name, p.owner_email, p.owner_phone_number
                        , p.contract_documents
                        , p.business_uid, p.business_name, p.business_email, p.business_phone_number
                        , r.latest_date, r.total_paid, r.payment_status, r.amt_remaining, r.cf_month, r.cf_year
                        , CASE
                                WHEN (p.lease_status = 'ACTIVE' AND r.payment_status IS NOT NULL) THEN payment_status
                                WHEN (p.lease_status = 'ACTIVE' AND r.payment_status IS NULL) THEN 'UNPAID'
                                ELSE 'VACANT'
                            END AS rent_status
                    FROM (
                        SELECT * FROM space.p_details
                        WHERE owner_uid = \'""" + uid + """\'
                        ) as p
                    LEFT JOIN (
                        SELECT * 
                        FROM space.pp_details 
                        WHERE (purchase_type = "RENT" OR ISNULL(purchase_type))
                        AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                        AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                        ) AS r ON p.property_uid = r.pur_property_id
                    """)

            # print("Query: ", propertiesQuery)
            response["RentStatus"] = rentsQuery
            return response
        
        elif uid[:3] == '600':
            print("In Business ID")
            with connect() as db:
                # print("in connect loop")
                rentsQuery = db.execute(""" 
                    -- RENT STATUS BY MANAGER FOR MANAGER PAGE
                    SELECT -- *,
                        p.property_uid, p.property_address, p.property_unit, p.property_city, p.property_state, p.property_zip, p.property_type
                        , p.property_num_beds, p.property_num_baths, p.property_area, p.property_listed_rent, p.property_deposit, p.property_pets_allowed, p.property_deposit_for_rent, p.property_images, p.property_description, p.property_notes
                        , p.lease_uid, p.lease_start, p.lease_end, p.lease_status, p.leaseFees, p.lease_assigned_contacts, p.lease_documents, p.lease_early_end_date, p.lease_renew_status
                        , p.tenant_uid, p.tenant_first_name, p.tenant_last_name, p.tenant_email, p.tenant_phone_number
                        , p.owner_uid, p.owner_first_name, p.owner_last_name, p.owner_email, p.owner_phone_number
                        , p.contract_documents
                        , p.business_uid, p.business_name, p.business_email, p.business_phone_number
                        , r.latest_date, r.total_paid, r.payment_status, r.amt_remaining, r.cf_month, r.cf_year
                        , CASE
                                WHEN (p.lease_status = 'ACTIVE' AND r.payment_status IS NOT NULL) THEN payment_status
                                WHEN (p.lease_status = 'ACTIVE' AND r.payment_status IS NULL) THEN 'UNPAID'
                                ELSE 'VACANT'
                            END AS rent_status
                    FROM (
                        SELECT * FROM space.p_details
                        WHERE business_uid = \'""" + uid + """\'
                        ) as p
                    LEFT JOIN (
                        SELECT * 
                        FROM space.pp_details 
                        WHERE (purchase_type = "RENT" OR ISNULL(purchase_type))
                        AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                        AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                        ) AS r ON p.property_uid = r.pur_property_id;
                    """) 

            # print("Query: ", propertiesQuery)
            response["RentStatus"] = rentsQuery
            return response
        
        # elif uid[:3] == '350':
        #     print("In Tenant ID")
            

        #     # print("Query: ", propertiesQuery)
        #     response["Property"] = propertiesQuery
        #     return response
        
        else:
            print("UID Not found")
            response["RentStatus"] = "UID Not Found"
            return response







       



class RentDetails(Resource):
    def get(self, uid):
        print("in Get Rent Status")

        response = {}

        # print("Property UID: ", property_id)

        with connect() as db:
            print("in connect loop")
            if uid[:3] == '110':
                rentQuery = db.execute(""" 
                                SELECT
                                    property_uid, owner_uid, po_start_date, po_end_date,
                                    contract_business_id, contract_status, contract_start_date, contract_end_date, contract_early_end_date,
                                    rs.*, 
                                    IFNULL(lease_status,"VACANT") AS lease_status,
                                    late_fees.total_late_fees AS total_late_fees, 
                                    late_fees.total_late_fees_paid AS total_late_fees_paid
                                FROM
                                    space.p_details
                                LEFT JOIN (
                                    -- PROPERTY RENT STATUS
                                    -- GROUP BY PROPERTY
                                    SELECT -- *
                                        purchase_uid, pur_property_id, purchase_type, pur_cf_type, purchase_status, pur_receiver, pur_initiator, pur_due_date, pur_payer, latest_pay_date,
                                        payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, paid_by, payment_method, payment_date_cleared, cf_month, cf_year,
                                        SUM(pur_amount_due) AS pur_amount_due,
                                        SUM(total_paid) AS total_paid,
                                        MIN(pur_status_value) AS pur_status_value
                                    FROM (
                                        -- GET PURCHASES AND AMOUNT REMAINING
                                        SELECT *,
                                               MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_month,
                                               YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_year
                                        FROM space.purchases
                                        LEFT JOIN (
                                            -- GET PAYMENTS BY PURCHASE ID
                                            SELECT
                                                payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, 
                                                payment_type, payment_date, paid_by, 
                                                payment_method, payment_date_cleared,
                                                payment_date AS latest_pay_date,
                                                pay_amount AS total_paid
                                            FROM
                                                space.payments
                                            GROUP BY
                                                pay_purchase_id
                                        ) pay ON pay.pay_purchase_id = purchase_uid
                                    ) pp
                                    WHERE
                                        purchase_type LIKE "%Rent%" 
                                    GROUP BY
                                        purchase_uid
                                ) AS rs ON property_uid = pur_property_id
                                LEFT JOIN (
                                    SELECT
                                        pur_description,
                                        SUM(pur_amount_due) AS total_late_fees,
                                        SUM(total_paid) AS total_late_fees_paid
                                    FROM
                                        space.pp_details
                                    WHERE
                                        purchase_type = 'Late Fee'
                                --         AND pur_description IN (
                                --             SELECT
                                --                 purchase_uid
                                --             FROM
                                --                 space.purchases
                                --             WHERE
                                --                 purchase_type = 'Rent'
                                --         )
                                    GROUP BY
                                        pur_description
                                ) AS late_fees ON rs.purchase_uid = late_fees.pur_description
                                WHERE
                                    owner_uid = \'""" + uid + """\';
                        """)

            elif uid[:3] == '600':
                rentQuery = db.execute("""
                                SELECT
                                    property_uid, owner_uid, po_start_date, po_end_date,
                                    contract_business_id, contract_status, contract_start_date, contract_end_date, contract_early_end_date,
                                    rs.*, 
                                    IFNULL(lease_status,"VACANT") AS lease_status,
                                    late_fees.total_late_fees AS total_late_fees, 
                                    late_fees.total_late_fees_paid AS total_late_fees_paid
                                FROM
                                    space.p_details
                                LEFT JOIN (
                                    -- PROPERTY RENT STATUS
                                    -- GROUP BY PROPERTY
                                    SELECT -- *
                                        purchase_uid, pur_property_id, purchase_type, pur_cf_type, purchase_status, pur_receiver, pur_initiator, pur_due_date, pur_payer, latest_pay_date,
                                        payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, paid_by, payment_method, payment_date_cleared, cf_month, cf_year,
                                        SUM(pur_amount_due) AS pur_amount_due,
                                        SUM(total_paid) AS total_paid,
                                        MIN(pur_status_value) AS pur_status_value
                                    FROM (
                                        -- GET PURCHASES AND AMOUNT REMAINING
                                        SELECT *,
                                               MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_month,
                                               YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_year
                                        FROM space.purchases
                                        LEFT JOIN (
                                            -- GET PAYMENTS BY PURCHASE ID
                                            SELECT
                                                payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, 
                                                payment_type, payment_date, paid_by, 
                                                payment_method, payment_date_cleared,
                                                payment_date AS latest_pay_date,
                                                pay_amount AS total_paid
                                            FROM
                                                space.payments
                                            GROUP BY
                                                pay_purchase_id
                                        ) pay ON pay.pay_purchase_id = purchase_uid
                                    ) pp
                                    WHERE
                                        purchase_type LIKE "%Rent%" 
                                    GROUP BY
                                        purchase_uid
                                ) AS rs ON property_uid = pur_property_id
                                LEFT JOIN (
                                    SELECT
                                        pur_description,
                                        SUM(pur_amount_due) AS total_late_fees,
                                        SUM(total_paid) AS total_late_fees_paid
                                    FROM
                                        space.pp_details
                                    WHERE
                                        purchase_type = 'Late Fee'
                                --         AND pur_description IN (
                                --             SELECT
                                --                 purchase_uid
                                --             FROM
                                --                 space.purchases
                                --             WHERE
                                --                 purchase_type = 'Rent'
                                --         )
                                    GROUP BY
                                        pur_description
                                ) AS late_fees ON rs.purchase_uid = late_fees.pur_description
                                WHERE
                                    business_uid = \'""" + uid + """\';
                        """)

            elif uid[:3] == '350':
                rentQuery = db.execute("""
                                SELECT
                                    property_uid, owner_uid, po_start_date, po_end_date,
                                    contract_business_id, contract_status, contract_start_date, contract_end_date, contract_early_end_date,
                                    rs.*, 
                                    IFNULL(lease_status,"VACANT") AS lease_status,
                                    late_fees.total_late_fees AS total_late_fees, 
                                    late_fees.total_late_fees_paid AS total_late_fees_paid
                                FROM
                                    space.p_details
                                LEFT JOIN (
                                    -- PROPERTY RENT STATUS
                                    -- GROUP BY PROPERTY
                                    SELECT -- *
                                        purchase_uid, pur_property_id, purchase_type, pur_cf_type, purchase_status, pur_receiver, pur_initiator, pur_due_date, pur_payer, latest_pay_date,
                                        payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, paid_by, payment_method, payment_date_cleared, cf_month, cf_year,
                                        SUM(pur_amount_due) AS pur_amount_due,
                                        SUM(total_paid) AS total_paid,
                                        MIN(pur_status_value) AS pur_status_value
                                    FROM (
                                        -- GET PURCHASES AND AMOUNT REMAINING
                                        SELECT *,
                                               MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_month,
                                               YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_year
                                        FROM space.purchases
                                        LEFT JOIN (
                                            -- GET PAYMENTS BY PURCHASE ID
                                            SELECT
                                                payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, 
                                                payment_type, payment_date, paid_by, 
                                                payment_method, payment_date_cleared,
                                                payment_date AS latest_pay_date,
                                                pay_amount AS total_paid
                                            FROM
                                                space.payments
                                            GROUP BY
                                                pay_purchase_id
                                        ) pay ON pay.pay_purchase_id = purchase_uid
                                    ) pp
                                    WHERE
                                        purchase_type LIKE "%Rent%" 
                                    GROUP BY
                                        purchase_uid
                                ) AS rs ON property_uid = pur_property_id
                                LEFT JOIN (
                                    SELECT
                                        pur_description,
                                        SUM(pur_amount_due) AS total_late_fees,
                                        SUM(total_paid) AS total_late_fees_paid
                                    FROM
                                        space.pp_details
                                    WHERE
                                        purchase_type = 'Late Fee'
                                --         AND pur_description IN (
                                --             SELECT
                                --                 purchase_uid
                                --             FROM
                                --                 space.purchases
                                --             WHERE
                                --                 purchase_type = 'Rent'
                                --         )
                                    GROUP BY
                                        pur_description
                                ) AS late_fees ON rs.purchase_uid = late_fees.pur_description
                                WHERE
                                    tenant_uid = \'""" + uid + """\';            
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
