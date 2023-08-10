
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
# BY MONTH    X           X               X
# BY YEAR     X           X               X



class CashflowByOwner(Resource):
    def get(self, owner_id, year):
        print("In Cashflow")
        response = {}

        # filters = ['owner_id', 'year']
        # print(filters)
        # owner_id = request.args.get(filters[0])
        # year = request.args.get(filters[1])
        # print(filterValue)
        # print(year)


        today = date.today()
        conn = connect()

        try:
            # REVENUE
            response_revenue_by_year = ("""
                -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY YEAR
                SELECT -- * , 
                    cf_year -- , cf_month, pur_due_date
                    , pur_cf_type -- , purchase_type
                    , sum(pur_amount_due), sum(sum_paid_amount), sum(amt_remaining) -- , payment_status
                    , property_address, property_unit
                FROM space.pp_details
                  WHERE owner_uid = \'""" + owner_id + """\'
                    AND cf_year = \'""" + year + """\'
                    AND purchase_status != 'DELETED'
                    AND pur_cf_type = 'revenue'
                    GROUP BY property_address, property_unit
                    ORDER BY property_address ASC, property_unit ASC;
                """)
            
            print("Query: ", response_revenue_by_year)
            items = execute(response_revenue_by_year, "get", conn)
            print(items)
            response["response_revenue_by_year"] = items["result"]




            response_revenue_by_month = ("""
                -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH
                SELECT -- * , 
                    cf_year, cf_month -- , pur_due_date
                    , pur_cf_type -- , purchase_type
                    , sum(pur_amount_due), sum(sum_paid_amount), sum(amt_remaining) -- , payment_status
                    , property_address, property_unit
                FROM space.pp_details
                WHERE owner_uid = \'""" + owner_id + """\'
                    AND cf_year = \'""" + year + """\'
                    AND purchase_status != 'DELETED'
                    AND pur_cf_type = 'revenue'
                    GROUP BY property_address, property_unit, cf_month
                    ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                """)
            
            print("Query: ", response_revenue_by_month)
            items = execute(response_revenue_by_month, "get", conn)
            print(items)
            response["response_revenue_by_month"] = items["result"]




            response_revenue_by_month_by_type = ("""
                -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH BY PURCHASE TYPE
                SELECT -- * , 
                    cf_year, cf_month -- , pur_due_date
                    , pur_cf_type, purchase_type
                    , sum(pur_amount_due), sum(sum_paid_amount), sum(amt_remaining) -- , payment_status
                    , property_address, property_unit
                FROM space.pp_details
                WHERE owner_uid = \'""" + owner_id + """\'
                    AND cf_year = \'""" + year + """\'
                    AND purchase_status != 'DELETED'
                    AND pur_cf_type = 'revenue'
                    GROUP BY property_address, property_unit, cf_month,  purchase_type
                    ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                """)
            
            print("Query: ", response_revenue_by_month_by_type)
            items = execute(response_revenue_by_month_by_type, "get", conn)
            print(items)
            response["response_revenue_by_month_by_type"] = items["result"]





            response_revenue = ("""
                -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER
                SELECT -- * , 
                    cf_year, cf_month, pur_due_date
                    , pur_cf_type, purchase_type
                    , pur_amount_due, sum_paid_amount, amt_remaining, payment_status
                    , property_address, property_unit
                FROM space.pp_details
                WHERE owner_uid = \'""" + owner_id + """\'
                    AND cf_year = \'""" + year + """\'
                    AND purchase_status != 'DELETED'
                    AND pur_cf_type = 'revenue'
                ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                """)
            
            print("Query: ", response_revenue)
            items = execute(response_revenue, "get", conn)
            print(items)
            response["response_revenue"] = items["result"]





            # EXPENSES
            response_expense_by_year = ("""
                -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY YEAR
                SELECT -- * , 
                    cf_year -- , cf_month, pur_due_date
                    , pur_cf_type -- , purchase_type
                    , sum(pur_amount_due), sum(sum_paid_amount), sum(amt_remaining) -- , payment_status
                    , property_address, property_unit
                FROM space.pp_details
                  WHERE owner_uid = \'""" + owner_id + """\'
                    AND cf_year = \'""" + year + """\'
                    AND purchase_status != 'DELETED'
                    AND pur_cf_type = 'expense'
                    GROUP BY property_address, property_unit
                    ORDER BY property_address ASC, property_unit ASC;
                """)
            
            print("Query: ", response_expense_by_year)
            items = execute(response_expense_by_year, "get", conn)
            print(items)
            response["response_expense_by_year"] = items["result"]




            response_expense_by_month = ("""
                -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH
                SELECT -- * , 
                    cf_year, cf_month -- , pur_due_date
                    , pur_cf_type -- , purchase_type
                    , sum(pur_amount_due), sum(sum_paid_amount), sum(amt_remaining) -- , payment_status
                    , property_address, property_unit
                FROM space.pp_details
                WHERE owner_uid = \'""" + owner_id + """\'
                    AND cf_year = \'""" + year + """\'
                    AND purchase_status != 'DELETED'
                    AND pur_cf_type = 'expense'
                    GROUP BY property_address, property_unit, cf_month
                    ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                """)
            
            print("Query: ", response_expense_by_month)
            items = execute(response_expense_by_month, "get", conn)
            print(items)
            response["response_expense_by_month"] = items["result"]




            response_expense_by_month_by_type = ("""
                -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH BY PURCHASE TYPE
                SELECT -- * , 
                    cf_year, cf_month -- , pur_due_date
                    , pur_cf_type, purchase_type
                    , sum(pur_amount_due), sum(sum_paid_amount), sum(amt_remaining) -- , payment_status
                    , property_address, property_unit
                FROM space.pp_details
                WHERE owner_uid = \'""" + owner_id + """\'
                    AND cf_year = \'""" + year + """\'
                    AND purchase_status != 'DELETED'
                    AND pur_cf_type = 'expense'
                    GROUP BY property_address, property_unit, cf_month,  purchase_type
                    ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                """)
            
            print("Query: ", response_expense_by_month_by_type)
            items = execute(response_expense_by_month_by_type, "get", conn)
            print(items)
            response["response_expense_by_month_by_type"] = items["result"]





            response_expense = ("""
                -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER
                SELECT -- * , 
                    cf_year, cf_month, pur_due_date
                    , pur_cf_type, purchase_type
                    , pur_amount_due, sum_paid_amount, amt_remaining, payment_status
                    , property_address, property_unit
                FROM space.pp_details
                WHERE owner_uid = \'""" + owner_id + """\'
                    AND cf_year = \'""" + year + """\'
                    AND purchase_status != 'DELETED'
                    AND pur_cf_type = 'expense'
                ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                """)
            
            print("Query: ", response_expense)
            items = execute(response_expense, "get", conn)
            print(items)
            response["response_expense"] = items["result"]


            # OTHER
            response_other = ("""
                -- ALL OTHER TRANSACTIONS AFFECTING A PARTICULAR OWNER
                SELECT -- * , 
                    cf_year, cf_month, pur_due_date
                    , pur_cf_type, purchase_type
                    , pur_amount_due, sum_paid_amount, amt_remaining, payment_status
                    , property_address, property_unit
                FROM space.pp_details
                WHERE owner_uid = \'""" + owner_id + """\'
                    AND cf_year = \'""" + year + """\'
                    AND purchase_status != 'DELETED'
                    AND (pur_cf_type != 'expense' AND pur_cf_type != 'revenue')
                ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                """)
            
            print("Query: ", response_other)
            items = execute(response_other, "get", conn)
            print(items)
            response["response_other"] = items["result"]





            return response

            
        except:
            print("Error in Cash Flow Query")
        finally:
            disconnect(conn)
