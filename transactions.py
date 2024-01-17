
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


# OVERVIEW
#           TENANT      OWNER     PROPERTY MANAGER     
# ALL         Y           Y               Y
# BY MONTH    X           X               X
# BY YEAR     X           X               X


class AllTransactions(Resource):
    # decorators = [jwt_required()]

    def get(self, uid):
        print('in Maintenance Request')
        response = {}

        print("UID: ", uid)

        if uid[:3] == '110':
            print("In Owner ID")
            with connect() as db:
                queryResponse = (""" 

                                                            SELECT purchase_uid,property_address,pur_timestamp,pur_property_id,purchase_type,pur_cf_type,pur_bill_id,purchase_date,pur_due_date,pur_amount_due,
                                                            purchase_status,pur_notes,pur_description,pur_receiver,pur_initiator,pur_payer,pay_purchase_id,latest_date,total_paid,payment_status
                                                            payment_status,amt_remaining,cf_month,cf_year,receiver_user_id,receiver_profile_uid,receiver_user_type,receiver_user_name,receiver_user_phone
                                                            receiver_user_email,initiator_user_id,initiator_user_id,initiator_profile_uid,initiator_user_type,initiator_user_name,initiator_user_phone,
                                                            initiator_user_email,payer_user_id,payer_profile_uid,payer_user_type,payer_user_name,payer_user_phone,payer_user_email,property_uid,property_owner_id
                                                            contract_uid,business_uid,lease_uid,tenant_uid FROM space.pp_details
                                                            WHERE pur_receiver=\'""" + uid + """\' OR pur_payer=\'""" + uid + """\'
                                                            OR property_owner_id =\'""" + uid + """\'
                                                            """)
                response = db.execute(queryResponse)
                
            return response

        elif uid[:3] == '600':
            print("In Business ID")
            with connect() as db:
                queryResponse = (""" 

                                                            SELECT purchase_uid,property_address,pur_timestamp,pur_property_id,purchase_type,pur_cf_type,pur_bill_id,purchase_date,pur_due_date,pur_amount_due,
                                                            purchase_status,pur_notes,pur_description,pur_receiver,pur_initiator,pur_payer,pay_purchase_id,latest_date,total_paid,payment_status
                                                            payment_status,amt_remaining,cf_month,cf_year,receiver_user_id,receiver_profile_uid,receiver_user_type,receiver_user_name,receiver_user_phone
                                                            receiver_user_email,initiator_user_id,initiator_user_id,initiator_profile_uid,initiator_user_type,initiator_user_name,initiator_user_phone,
                                                            initiator_user_email,payer_user_id,payer_profile_uid,payer_user_type,payer_user_name,payer_user_phone,payer_user_email,property_uid,property_owner_id
                                                            contract_uid,business_uid,lease_uid,tenant_uid FROM space.pp_details 
                                                            WHERE pur_receiver=\'""" + uid + """\' OR pur_payer=\'""" + uid + """\'
                                                            OR business_uid =\'""" + uid + """\'
                                                            """)
                response = db.execute(queryResponse)

            return response

        



class TransactionsByOwner(Resource):
    # decorators = [jwt_required()]

    def get(self, owner_id):
        print('in Transactions By Owner')
        response = {}

        print("Property Owner UID: ", owner_id)

        with connect() as db:
            print("in connect loop")
            transactionQuery = db.execute(""" 
                    -- ALL PURCHASE TRANSACTIONS BY OWNER
                    SELECT * FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\';
                    """)
            

            # print("Query: ", TransactionQuery)
            # items = execute(transactionQuery, "get", conn)
            response["Transactions"] = transactionQuery
            return response


class TransactionsByOwnerByProperty(Resource):
    # decorators = [jwt_required()]

    def get(self, owner_id, property_id):
        print('in Transactions By Owner')
        response = {}

        print("Property Owner UID: ", owner_id)

        with connect() as db:
            print("in connect loop")
            transactionQuery = db.execute(""" 
                    -- TRANSACTIONS BY OWNER BY PROPERTY
                    SELECT * FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND pur_property_id = \'""" + property_id + """\';
                    """)

            # print("Query: ", transactionQuery)
            # items = execute(transactionQuery, "get", conn)
            response["Transactions"] = transactionQuery
            return response
