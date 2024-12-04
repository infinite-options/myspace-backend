from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
from werkzeug.exceptions import BadRequest



class PaymentVerification(Resource):
    def get(self, user_id):
        print("In Payment Verification GET")

        with connect() as db:
            print("in connect loop")
            cashflow = db.execute("""                            
                    -- PAYMENT VERIFICATION GET
                    SELECT payments.*, cp.total_paid, purchases.*
                        , DATE_FORMAT(STR_TO_DATE(pur_due_date, '%m-%d-%Y'), "%M") AS cf_month
                        , DATE_FORMAT(STR_TO_DATE(pur_due_date, '%m-%d-%Y'), "%m") AS cf_month_num
                        , DATE_FORMAT(STR_TO_DATE(pur_due_date, '%m-%d-%Y'), "%Y") AS cf_year
                    FROM space.payments
                    LEFT JOIN (
                        SELECT payment_intent AS pi, payment_method AS pm, payment_date AS pd, SUM(pay_amount) AS total_paid
                        FROM space.payments
                        WHERE paid_by LIKE '350%'
                        GROUP BY payment_intent, payment_date
                        ) AS cp ON payment_intent = pi AND payment_method = pm AND payment_date = pd
                    LEFT JOIN space.purchases ON pay_purchase_id = purchase_uid
                    -- WHERE pur_receiver = '600-000043' AND pur_payer LIKE '350%'
                    WHERE pur_receiver = \'""" + user_id + """\' AND pur_payer LIKE '350%'
                        AND payment_verify = 'Unverified'
                    """)
        return cashflow
    

    def put(self):
        print("In Payment Verification PUT")
        response = {}
        update_counter = 0

        payload = request.form.to_dict()
        print("Profile Update Payload: ", payload)

         # Verify uid has been included in the data
        if payload.get('payment_uid') in {None, '', 'null'}:
            print("No payment_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        payment_uids = json.loads(payload.pop('payment_uid'))
        # payment_uids = payload.pop('payment_uid')
        print("Payment UIDs received: ", payment_uids, type(payment_uids))
        
        for payment_uid in payment_uids:
        
            # payment_uid = payload.get('payment_uid')
            key = {'payment_uid': payment_uid}
            print("Payment Key: ", key) 

            # Write to Database
            with connect() as db:
                print("Checking Inputs: ", key, payload)
                response['payment_info'] = db.update('payments', key, payload)
                update_counter = update_counter + 1
                print("Response:" , response)
        
        response['updates'] = update_counter
            
        return response

    

class CashflowTransactions(Resource):
    def get(self, user_id, filter):
        print("In Cashflow Transactions")

        response = {}
        property = {}
        pur_group = {}

        with connect() as db:

            if filter == 'property':
                print("In PM Cashflow by Property")
           
                response["revenue"] = db.execute("""                            
                        -- Cashflow by PROPERTY 
                        -- BY USER, by Month, by Year, by Property
                        SELECT 
                            cf_year,
                            cf_month,
                            SUM(pur_amount_due) AS received_expected,
                            SUM(pur_amount_due-amt_remaining) AS received_actual,
                            pur_property_id,
                            pur_receiver,
                            JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'purchase_uid', purchase_uid,
                                    'pur_timestamp', pur_timestamp,
                                    'pur_description', pur_description,
                                    'pur_notes', pur_notes,
                                    'pur_cf_type', pur_cf_type,
                                    'pur_bill_id', pur_bill_id,
                                    'purchase_date', purchase_date,
                                    'pur_due_date', pur_due_date,
                                    'pur_amount_due', pur_amount_due,
                                    'purchase_status', purchase_status,
                                    'pur_status_value', pur_status_value,
                                    'pur_receiver', pur_receiver,
                                    'pur_initiator', pur_initiator,
                                    'pur_payer', pur_payer,
                                    'pur_late_Fee', pur_late_Fee,
                                    'pur_perDay_late_fee', pur_perDay_late_fee,
                                    'pur_due_by', pur_due_by,
                                    'pur_late_by', pur_late_by,
                                    'pur_group', pur_group,
                                    'pur_leaseFees_id', pur_leaseFees_id,
                                    'pay_purchase_id', pay_purchase_id,
                                    'latest_date', latest_date,
                                    'total_paid', total_paid,
                                    'verified', verified,
                                    'payment_status', payment_status,
                                    'amt_remaining', amt_remaining,
                                    'cf_month', cf_month,
                                    'cf_month_num', cf_month_num,
                                    'cf_year', cf_year
                                )
                            ) AS individual_transactions
                        FROM space.pp_status
                        WHERE cf_month = DATE_FORMAT(NOW(), '%M')
                            AND cf_year = DATE_FORMAT(NOW(), '%Y')
                            -- AND pur_receiver = '600-000003'  
                            AND pur_receiver = \'""" + user_id + """\'
                            -- AND pur_payer = '600-000003'  
	                        -- AND pur_payer = \'""" + user_id + """\'
                        GROUP BY pur_property_id
                    """)
                
                response["expense"] = db.execute("""                            
                        -- Cashflow by PROPERTY 
                        -- BY USER, by Month, by Year, by Property
                        SELECT 
                            cf_year,
                            cf_month,
                            SUM(pur_amount_due) AS received_expected,
                            SUM(pur_amount_due-amt_remaining) AS received_actual,
                            pur_property_id,
                            pur_receiver,
                            JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'purchase_uid', purchase_uid,
                                    'pur_timestamp', pur_timestamp,
                                    'pur_description', pur_description,
                                    'pur_notes', pur_notes,
                                    'pur_cf_type', pur_cf_type,
                                    'pur_bill_id', pur_bill_id,
                                    'purchase_date', purchase_date,
                                    'pur_due_date', pur_due_date,
                                    'pur_amount_due', pur_amount_due,
                                    'purchase_status', purchase_status,
                                    'pur_status_value', pur_status_value,
                                    'pur_receiver', pur_receiver,
                                    'pur_initiator', pur_initiator,
                                    'pur_payer', pur_payer,
                                    'pur_late_Fee', pur_late_Fee,
                                    'pur_perDay_late_fee', pur_perDay_late_fee,
                                    'pur_due_by', pur_due_by,
                                    'pur_late_by', pur_late_by,
                                    'pur_group', pur_group,
                                    'pur_leaseFees_id', pur_leaseFees_id,
                                    'pay_purchase_id', pay_purchase_id,
                                    'latest_date', latest_date,
                                    'total_paid', total_paid,
                                    'verified', verified,
                                    'payment_status', payment_status,
                                    'amt_remaining', amt_remaining,
                                    'cf_month', cf_month,
                                    'cf_month_num', cf_month_num,
                                    'cf_year', cf_year
                                )
                            ) AS individual_transactions
                        FROM space.pp_status
                        WHERE cf_month = DATE_FORMAT(NOW(), '%M')
                            AND cf_year = DATE_FORMAT(NOW(), '%Y')
                            -- AND pur_receiver = '600-000003'  
                            -- AND pur_receiver = \'""" + user_id + """\'
                            -- AND pur_payer = '600-000003'  
	                        AND pur_payer = \'""" + user_id + """\'
                        GROUP BY pur_property_id
                    """)
                
            if filter == 'type':
                print("In PM Cashflow by Purchase Type")
           
                response["revenue"] = db.execute("""                          
                        -- Cashflow by PURCHASE TYPE 
                        -- BY USER, by Month, by Year, by Purchase Type
                        SELECT 
                            cf_year,
                            cf_month,
                            SUM(pur_amount_due) AS received_expected,
                            SUM(pur_amount_due-amt_remaining) AS received_actual,
                            purchase_type,
                            pur_receiver,
                            JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'purchase_uid', purchase_uid,
                                    'pur_timestamp', pur_timestamp,
                                    'pur_description', pur_description,
                                    'pur_notes', pur_notes,
                                    'pur_cf_type', pur_cf_type,
                                    'pur_bill_id', pur_bill_id,
                                    'purchase_date', purchase_date,
                                    'pur_due_date', pur_due_date,
                                    'pur_amount_due', pur_amount_due,
                                    'purchase_status', purchase_status,
                                    'pur_status_value', pur_status_value,
                                    'pur_receiver', pur_receiver,
                                    'pur_initiator', pur_initiator,
                                    'pur_payer', pur_payer,
                                    'pur_late_Fee', pur_late_Fee,
                                    'pur_perDay_late_fee', pur_perDay_late_fee,
                                    'pur_due_by', pur_due_by,
                                    'pur_late_by', pur_late_by,
                                    'pur_group', pur_group,
                                    'pur_leaseFees_id', pur_leaseFees_id,
                                    'pay_purchase_id', pay_purchase_id,
                                    'latest_date', latest_date,
                                    'total_paid', total_paid,
                                    'verified', verified,
                                    'payment_status', payment_status,
                                    'amt_remaining', amt_remaining,
                                    'cf_month', cf_month,
                                    'cf_month_num', cf_month_num,
                                    'cf_year', cf_year
                                )
                            ) AS individual_transactions
                        FROM space.pp_status
                        WHERE cf_month = DATE_FORMAT(NOW(), '%M')
                            AND cf_year = DATE_FORMAT(NOW(), '%Y')
                            -- AND pur_receiver = '600-000003'  
                            AND pur_receiver = \'""" + user_id + """\'
                            -- AND pur_payer = '600-000003'  
	                        -- AND pur_payer = \'""" + user_id + """\'
                        GROUP BY purchase_type
                    """)
                
                response["expense"] = db.execute("""                          
                        -- Cashflow by PURCHASE TYPE 
                        -- BY USER, by Month, by Year, by Purchase Type
                        SELECT 
                            cf_year,
                            cf_month,
                            SUM(pur_amount_due) AS received_expected,
                            SUM(pur_amount_due-amt_remaining) AS received_actual,
                            purchase_type,
                            pur_receiver,
                            JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'purchase_uid', purchase_uid,
                                    'pur_timestamp', pur_timestamp,
                                    'pur_description', pur_description,
                                    'pur_notes', pur_notes,
                                    'pur_cf_type', pur_cf_type,
                                    'pur_bill_id', pur_bill_id,
                                    'purchase_date', purchase_date,
                                    'pur_due_date', pur_due_date,
                                    'pur_amount_due', pur_amount_due,
                                    'purchase_status', purchase_status,
                                    'pur_status_value', pur_status_value,
                                    'pur_receiver', pur_receiver,
                                    'pur_initiator', pur_initiator,
                                    'pur_payer', pur_payer,
                                    'pur_late_Fee', pur_late_Fee,
                                    'pur_perDay_late_fee', pur_perDay_late_fee,
                                    'pur_due_by', pur_due_by,
                                    'pur_late_by', pur_late_by,
                                    'pur_group', pur_group,
                                    'pur_leaseFees_id', pur_leaseFees_id,
                                    'pay_purchase_id', pay_purchase_id,
                                    'latest_date', latest_date,
                                    'total_paid', total_paid,
                                    'verified', verified,
                                    'payment_status', payment_status,
                                    'amt_remaining', amt_remaining,
                                    'cf_month', cf_month,
                                    'cf_month_num', cf_month_num,
                                    'cf_year', cf_year
                                )
                            ) AS individual_transactions
                        FROM space.pp_status
                        WHERE cf_month = DATE_FORMAT(NOW(), '%M')
                            AND cf_year = DATE_FORMAT(NOW(), '%Y')
                            -- AND pur_receiver = '600-000003'  
                            -- AND pur_receiver = \'""" + user_id + """\'
                            -- AND pur_payer = '600-000003'  
	                        AND pur_payer = \'""" + user_id + """\'
                        GROUP BY purchase_type
                    """)
                
            if filter == 'all':
                print("In PM Cashflow All")
            
                response = db.execute("""                          
                        -- All Cashflow Transactions
                        SELECT *,
                            property_address, property_unit
                            , property_owner_id
                        FROM space.pp_status
                        LEFT JOIN properties ON pur_property_id = property_uid
                        LEFT JOIN property_owner ON pur_property_id = property_id
                        -- WHERE pur_receiver = '350-000007'  OR pur_payer = '350-000007'
                        -- WHERE pur_receiver = '600-000003'  OR pur_payer = '600-000003'
                        WHERE pur_receiver = \'""" + user_id + """\' OR pur_payer = \'""" + user_id + """\'
                            AND STR_TO_DATE(purchase_date, '%m-%d-%Y %H:%i') >= DATE_FORMAT(NOW() - INTERVAL 1 YEAR, '%Y-01-01')
                            -- AND cf_month = DATE_FORMAT(NOW(), '%M')
                            -- AND cf_year = DATE_FORMAT(NOW(), '%Y')
                        """)

            if filter == 'payment':
                print("In PM Cashflow Payment")
            
                response = db.execute("""                          
                        -- All Cashflow Transactions
                        SELECT pp_details.*,
                            property_address, property_unit, property_owner_id, owner_first_name, owner_last_name,
                            -- properties.*,
                            -- p_details.*,
                            CASE 
                                WHEN purchase_type = 'Rent' THEN 1
                                WHEN purchase_type = 'Rent due owner' THEN 2
                                WHEN purchase_type = 'Management' THEN 3
                                ELSE 5
                            END AS pur_type_code
                        FROM space.pp_details
                        -- LEFT JOIN space.properties ON property_uid = pur_property_id
                        LEFT JOIN space.p_details ON property_uid = pur_property_id
                        -- WHERE pur_receiver = '600-000043' OR pur_payer = '600-000043';
                        WHERE pur_receiver = \'""" + user_id + """\' OR pur_payer = \'""" + user_id + """\';
                        """)
            
            else:
                print("In New Cashflow All")
                response = db.execute("""                          
                        -- All Cashflow Transactions
                        SELECT *,
                            JSON_LENGTH(transactions) AS num
                        FROM (
                        SELECT -- *,
                            -- purchase_uid, pur_timestamp, 
                            pur_property_id, property_owner_id, property_address, property_unit, purchase_type, -- pur_description, pur_notes, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, 
                            pur_receiver, pur_initiator, pur_payer, -- pur_late_Fee, pur_perDay_late_fee, pur_due_by, pur_late_by, pur_group, pur_leaseFees_id, pay_purchase_id, latest_date, total_paid, verified, payment_status, amt_remaining, 
                            pur_group, cf_month, cf_month_num, cf_year,
                            purchase_date, pur_due_date,
                            -- IF(pur_receiver = "600-000003", SUM(pur_amount_due), "") AS expected,
                            -- IF(pur_receiver = "600-000003", SUM(total_paid), "") AS actual 
                            IF(pur_receiver = \'""" + user_id + """\', SUM(pur_amount_due), "") AS expected,
                            IF(pur_receiver = \'""" + user_id + """\', SUM(total_paid), "") AS actual 
                            , purchase_status, payment_status, verified
                            , JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'purchase_uid', purchase_uid,
                                    'pur_receiver', pur_receiver,
                                    'pur_payer', pur_payer,
                                    'purchase_status', purchase_status,
                                    'purchase_type', purchase_type,
                                    'pur_notes', pur_notes,
                                    'pur_description', pur_description,
                                    'pur_amount_due',  pur_amount_due,
                                    'total_paid', total_paid,
                                    'pur_group', pur_group,
                                    'pur_leaseFees_id', pur_leaseFees_id,
                                    'cf_month', cf_month,
                                    'cf_month_num', cf_month_num,
                                    'cf_year', cf_year,
                                    'payment_ids', payment_ids,
                                    -- 'pur_cf_type', IF(pur_receiver = '600-000003', "revenue", "expense")
                                    'pur_cf_type', IF(pur_receiver = \'""" + user_id + """\', "revenue", "expense")
                                )
                            ) AS transactions
                            , JSON_ARRAYAGG(purchase_uid) AS purchase_ids
                        FROM space.pp_status
                        LEFT JOIN space.properties ON pur_property_id = property_uid
                        LEFT JOIN space.property_owner ON pur_property_id = property_id
                        -- WHERE pur_receiver = "600-000003" -- AND pur_property_id LIKE '200-000034'
                        WHERE pur_receiver = \'""" + user_id + """\'
                        GROUP BY cf_month, cf_year, purchase_type, pur_property_id, pur_group
                        UNION
                        SELECT -- *,
                            pur_property_id, property_owner_id, property_address, property_unit, purchase_type, 
                            pur_receiver, pur_initiator, pur_payer,
                            pur_group, cf_month, cf_month_num, cf_year,
                            purchase_date, pur_due_date,
                            -- IF(pur_payer = "600-000003", SUM(pur_amount_due), "") AS expected,
                            -- IF(pur_payer = "600-000003", SUM(total_paid), "") AS actual 
                            IF(pur_payer = \'""" + user_id + """\', SUM(pur_amount_due), "") AS expected,
                            IF(pur_payer = \'""" + user_id + """\', SUM(total_paid), "") AS actual
                            , purchase_status, payment_status, verified
                            , JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'purchase_uid', purchase_uid,
                                    'pur_receiver', pur_receiver,
                                    'pur_payer', pur_payer,
                                    'purchase_status', purchase_status,
                                    'purchase_type', purchase_type,
                                    'pur_notes', pur_notes,
                                    'pur_description', pur_description,
                                    'pur_amount_due',  pur_amount_due,
                                    'total_paid', total_paid,
                                    'pur_group', pur_group,
                                    'pur_leaseFees_id', pur_leaseFees_id,
                                    'cf_month', cf_month,
                                    'cf_month_num', cf_month_num,
                                    'cf_year', cf_year,
                                    'payment_ids', payment_ids,
                                    -- 'pur_cf_type', IF(pur_payer = '600-000003', "expense", "revenue")
                                    'pur_cf_type', IF(pur_payer = \'""" + user_id + """\', "expense", "revenue")
                                )
                            ) AS transactions
                            , JSON_ARRAYAGG(purchase_uid) AS purchase_ids
                        FROM space.pp_status
                        LEFT JOIN space.properties ON pur_property_id = property_uid
                        LEFT JOIN space.property_owner ON pur_property_id = property_id
                        -- WHERE pur_payer = "600-000003" -- AND pur_property_id LIKE '200-000034'
                        WHERE pur_payer = \'""" + user_id + """\'
                        GROUP BY cf_month, cf_year, purchase_type, pur_property_id, pur_group
                        ) AS t
                        """)


        return response

