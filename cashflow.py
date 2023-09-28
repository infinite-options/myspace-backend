
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
        if year != "TTM":

            print("In Cashflow year")
            with connect() as db:
                print("in connect loop")
                # REVENUE
                response_revenue_by_year = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY YEAR
                    SELECT -- * , 
                        cf_year -- , cf_month, pur_due_date
                        , pur_cf_type -- , purchase_type
                        , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'revenue'
                        GROUP BY property_address, property_unit
                        ORDER BY property_address ASC, property_unit ASC;
                    """)
                
                # print("Query: ", response_revenue_by_year)
                response["response_revenue_by_year"] = response_revenue_by_year




                response_revenue_by_month = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH
                    SELECT -- * , 
                        cf_year, cf_month -- , pur_due_date
                        , pur_cf_type -- , purchase_type
                        , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'revenue'
                        GROUP BY property_address, property_unit, cf_month
                        ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)
                
                # print("Query: ", response_revenue_by_month)
                response["response_revenue_by_month"] = response_revenue_by_month




                response_revenue_by_month_by_type = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH BY PURCHASE TYPE
                    SELECT -- * , 
                        cf_year, cf_month -- , pur_due_date
                        , pur_cf_type, purchase_type
                        , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'revenue'
                        GROUP BY property_address, property_unit, cf_month,  purchase_type
                        ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)
                
                # print("Query: ", response_revenue_by_month_by_type)
                response["response_revenue_by_month_by_type"] = response_revenue_by_month_by_type





                response_revenue = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER
                    SELECT -- * , 
                        cf_year, cf_month, pur_due_date
                        , pur_cf_type, purchase_type
                        , pur_amount_due, total_paid, amt_remaining, payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'revenue'
                    ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)
                
                # print("Query: ", response_revenue)
                response["response_revenue"] = response_revenue





                # # EXPENSES
                response_expense_by_year = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY YEAR
                    SELECT -- * , 
                        cf_year -- , cf_month, pur_due_date
                        , pur_cf_type -- , purchase_type
                        , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'expense'
                        GROUP BY property_address, property_unit
                        ORDER BY property_address ASC, property_unit ASC;
                    """)
                
                # print("Query: ", response_expense_by_year)
                response["response_expense_by_year"] = response_expense_by_year




                response_expense_by_month = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH
                    SELECT -- * , 
                        cf_year, cf_month -- , pur_due_date
                        , pur_cf_type -- , purchase_type
                        , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'expense'
                        GROUP BY property_address, property_unit, cf_month
                        ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)
                
                # print("Query: ", response_expense_by_month)
                response["response_expense_by_month"] = response_expense_by_month




                response_expense_by_month_by_type = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH BY PURCHASE TYPE
                    SELECT -- * , 
                        cf_year, cf_month -- , pur_due_date
                        , pur_cf_type, purchase_type
                        , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'expense'
                        GROUP BY property_address, property_unit, cf_month,  purchase_type
                        ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)
                
                # print("Query: ", response_expense_by_month_by_type)
                response["response_expense_by_month_by_type"] = response_expense_by_month_by_type





                response_expense = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER
                    SELECT -- * , 
                        cf_year, cf_month, pur_due_date
                        , pur_cf_type, purchase_type
                        , pur_amount_due, total_paid, amt_remaining, payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'expense'
                    ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)
                
                # print("Query: ", response_expense)
                response["response_expense"] = response_expense


                # OTHER
                response_other = db.execute("""
                    -- ALL OTHER TRANSACTIONS AFFECTING A PARTICULAR OWNER
                    SELECT -- * , 
                        cf_year, cf_month, pur_due_date
                        , pur_cf_type, purchase_type
                        , pur_amount_due, total_paid, amt_remaining, payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type != 'expense' AND pur_cf_type != 'revenue')
                    ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)
                
                # print("Query: ", response_other)
                response["response_other"] = response_other

                # print(response)
                return response
            
        else:

            print("In Cashflow TTM")
            with connect() as db:
                print("in connect loop")
                # REVENUE
                response_revenue_by_year = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY YEAR
                    SELECT -- * , 
                        cf_year -- , cf_month, pur_due_date
                        , pur_cf_type -- , purchase_type
                        , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND pur_due_date > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'revenue'
                        GROUP BY property_address, property_unit
                        ORDER BY property_address ASC, property_unit ASC;
                    """)
                
                # print("Query: ", response_revenue_by_year)
                response["response_revenue_by_year"] = response_revenue_by_year

                # print("In Cashflow TTM 1")


                response_revenue_by_month = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH
                    SELECT -- * , 
                        cf_year, cf_month -- , pur_due_date
                        , pur_cf_type -- , purchase_type
                        , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND pur_due_date > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'revenue'
                        GROUP BY property_address, property_unit, cf_month, cf_year
                        ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)
                
                # print("Query: ", response_revenue_by_month)
                response["response_revenue_by_month"] = response_revenue_by_month

                # print("In Cashflow TTM 2")


                response_revenue_by_month_by_type = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH BY PURCHASE TYPE
                    SELECT -- * , 
                        cf_year, cf_month -- , pur_due_date
                        , pur_cf_type, purchase_type
                        , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND pur_due_date > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'revenue'
                        GROUP BY property_address, property_unit, cf_month, cf_year, purchase_type
                        ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)
                
                # print("Query: ", response_revenue_by_month_by_type)
                response["response_revenue_by_month_by_type"] = response_revenue_by_month_by_type

                # print("In Cashflow TTM 3")



                response_revenue = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER
                    SELECT -- * , 
                        cf_year, cf_month, pur_due_date
                        , pur_cf_type, purchase_type
                        , pur_amount_due, total_paid, amt_remaining, payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND pur_due_date > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'revenue'
                    ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)
                
                # print("Query: ", response_revenue)
                response["response_revenue"] = response_revenue


                # print("In Cashflow TTM 4")


                # # EXPENSES
                response_expense_by_year = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY YEAR
                    SELECT -- * , 
                        cf_year -- , cf_month, pur_due_date
                        , pur_cf_type -- , purchase_type
                        , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND pur_due_date > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'expense'
                        GROUP BY property_address, property_unit
                        ORDER BY property_address ASC, property_unit ASC;
                    """)
                
                # print("Query: ", response_expense_by_year)
                response["response_expense_by_year"] = response_expense_by_year

                # print("In Cashflow TTM 5")


                response_expense_by_month = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH
                    SELECT -- * , 
                        cf_year, cf_month -- , pur_due_date
                        , pur_cf_type -- , purchase_type
                        , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND pur_due_date > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'expense'
                        GROUP BY property_address, property_unit, cf_month
                        ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)
                
                # print("Query: ", response_expense_by_month)
                response["response_expense_by_month"] = response_expense_by_month


                # print("In Cashflow TTM 6")


                response_expense_by_month_by_type = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH BY PURCHASE TYPE
                    SELECT -- * , 
                        cf_year, cf_month -- , pur_due_date
                        , pur_cf_type, purchase_type
                        , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND pur_due_date > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'expense'
                        GROUP BY property_address, property_unit, cf_month,  purchase_type
                        ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)
                
                # print("Query: ", response_expense_by_month_by_type)
                response["response_expense_by_month_by_type"] = response_expense_by_month_by_type


                # print("In Cashflow TTM 7")


                response_expense = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER
                    SELECT -- * , 
                        cf_year, cf_month, pur_due_date
                        , pur_cf_type, purchase_type
                        , pur_amount_due, total_paid, amt_remaining, payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND pur_due_date > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND pur_cf_type = 'expense'
                    ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)
                
                # print("Query: ", response_expense)
                response["response_expense"] = response_expense


                # print("In Cashflow TTM 8")


                # OTHER
                response_other = db.execute("""
                    -- ALL OTHER TRANSACTIONS AFFECTING A PARTICULAR OWNER
                    SELECT -- * , 
                        cf_year, cf_month, pur_due_date
                        , pur_cf_type, purchase_type
                        , pur_amount_due, total_paid, amt_remaining, payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE owner_uid = \'""" + owner_id + """\'
                        AND pur_due_date > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type != 'expense' AND pur_cf_type != 'revenue')
                    ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)
                
                # print("Query: ", response_other)
                response["response_other"] = response_other

                # print("In Cashflow TTM 9")

                # print(response)
                return response

