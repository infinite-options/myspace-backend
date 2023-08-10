
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
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

    def get(self):
        print('in All Transactions')
        response = {}
        conn = connect()

        try:
            transactionQuery = (""" 
                    -- ALL PURCHASE TRANSACTIONS TO DETERMINE IF BILLS ARE PAID OR NOT PAID
                    SELECT * FROM space.pp_details;
                    """)
            

            print("Query: ", transactionQuery)
            items = execute(transactionQuery, "get", conn)
            print(items)
            response["Transactions"] = items["result"]


            return response

        except:
            print("Error in Cash Flow Query")
        finally:
            disconnect(conn)

class TransactionsByOwner(Resource):
    # decorators = [jwt_required()]

    def get(self, owner_id):
        print('in Transactions By Owner')
        response = {}
        conn = connect()

        print("Property Owner UID: ", owner_id)

        try:
            transactionQuery = (""" 
                    -- ALL PURCHASE TRANSACTIONS BY OWNER
                    SELECT * FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\';
                    """)
            

            # print("Query: ", TransactionQuery)
            items = execute(transactionQuery, "get", conn)
            response["Transactions"] = items["result"]


            return response

        except:
            print("Error in Transaction Query")
        finally:
            disconnect(conn)

class TransactionsByOwnerByProperty(Resource):
    # decorators = [jwt_required()]

    def get(self, owner_id, property_id):
        print('in Transactions By Owner')
        response = {}
        conn = connect()

        print("Property Owner UID: ", owner_id)

        try:
            transactionQuery = (""" 
                    -- TRANSACTIONS BY OWNER BY PROPERTY
                    SELECT * FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND pur_property_id = \'""" + property_id + """\';
                    """)





            # print("Query: ", transactionQuery)
            items = execute(transactionQuery, "get", conn)
            response["Transactions"] = items["result"]


            return response

        except:
            print("Error in Transaction Query")
        finally:
            disconnect(conn)