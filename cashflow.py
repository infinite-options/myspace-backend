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
        print("In Cashflow By Owner")
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
                        purchase_uid, cf_year -- , cf_month, pur_due_date
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
                        purchase_uid, cf_year, cf_month -- , pur_due_date
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
                        purchase_uid, cf_year, cf_month -- , pur_due_date
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
                        purchase_uid, cf_year, cf_month, pur_due_date
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
                        purchase_uid, cf_year -- , cf_month, pur_due_date
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
                        purchase_uid,cf_year, cf_month -- , pur_due_date
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
                        purchase_uid,cf_year, cf_month -- , pur_due_date
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
                        purchase_uid,cf_year, cf_month, pur_due_date
                        , pur_cf_type, purchase_type
                        , pur_amount_due, total_paid, amt_remaining, payment_status
                        , property_address, property_unit
			, space.bills.*
                    FROM space.pp_details
		    LEFT JOIN space.bills ON pur_bill_id = bill_uid
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
                       purchase_uid, cf_year, cf_month, pur_due_date
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
                        purchase_uid,cf_year -- , cf_month, pur_due_date
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
                        purchase_uid,cf_year, cf_month -- , pur_due_date
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
                        purchase_uid,cf_year, cf_month -- , pur_due_date
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
                        purchase_uid,cf_year, cf_month, pur_due_date
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
                        purchase_uid,cf_year -- , cf_month, pur_due_date
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
                        purchase_uid,cf_year, cf_month -- , pur_due_date
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
                        purchase_uid,cf_year, cf_month -- , pur_due_date
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
                        purchase_uid,cf_year, cf_month, pur_due_date
                        , pur_cf_type, purchase_type
                        , pur_amount_due, total_paid, amt_remaining, payment_status
                        , property_address, property_unit
			, space.bills.*
                    FROM space.pp_details
		    LEFT JOIN space.bills ON pur_bill_id = bill_uid
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
                        purchase_uid,cf_year, cf_month, pur_due_date
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


class Cashflow(Resource):
    def get(self, user_id, year):
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
                    WHERE receiver_profile_uid = \'""" + user_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'REVENUE' OR pur_cf_type = 'revenue')
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
                    WHERE receiver_profile_uid = \'""" + user_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'REVENUE' OR pur_cf_type = 'revenue')
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
                    WHERE receiver_profile_uid = \'""" + user_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'REVENUE' OR pur_cf_type = 'revenue')
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
			, space.bills.*
                    FROM space.pp_details
		    LEFT JOIN space.bills ON pur_bill_id = bill_uid AND pur_bill_id IS NOT NULL
                    WHERE receiver_profile_uid = \'""" + user_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'REVENUE' OR pur_cf_type = 'revenue')
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
                    WHERE pur_payer = \'""" + user_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
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
                    WHERE pur_payer = \'""" + user_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
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
                    WHERE pur_payer = \'""" + user_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
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
			, space.bills.*
                    FROM space.pp_details
		    LEFT JOIN space.bills ON pur_bill_id = bill_uid AND pur_bill_id IS NOT NULL
                    WHERE pur_payer = \'""" + user_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
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
                    WHERE receiver_profile_uid = \'""" + user_id + """\'
                        AND cf_year = \'""" + year + """\'
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
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
                    WHERE receiver_profile_uid = \'""" + user_id + """\'
                        AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'REVENUE' OR pur_cf_type = 'revenue')
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
                    WHERE receiver_profile_uid = \'""" + user_id + """\'
                        AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'REVENUE' OR pur_cf_type = 'revenue')
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
                    WHERE receiver_profile_uid = \'""" + user_id + """\'
                        AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'REVENUE' OR pur_cf_type = 'revenue')
                        GROUP BY property_address, property_unit, cf_month, cf_year, purchase_type
                        ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)

                # print("Query: ", response_revenue_by_month_by_type)
                response["response_revenue_by_month_by_type"] = response_revenue_by_month_by_type

                # print("In Cashflow TTM 3")

                response_revenue = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER
                    SELECT -- * , 
                        purchase_uid, pur_property_id, cf_year, cf_month, pur_due_date
                        , pur_cf_type, purchase_type
                        , pur_amount_due, pur_notes, pur_description, total_paid, amt_remaining, payment_status
                        , property_address, property_unit
			, space.bills.*
                    FROM space.pp_details
		    LEFT JOIN space.bills ON pur_bill_id = bill_uid AND pur_bill_id IS NOT NULL
                    WHERE receiver_profile_uid = \'""" + user_id + """\'
                        AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'REVENUE' OR pur_cf_type = 'revenue')
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
                    WHERE pur_payer = \'""" + user_id + """\'
                        AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
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
                    WHERE pur_payer = \'""" + user_id + """\'
                        AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
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
                    WHERE pur_payer = \'""" + user_id + """\'
                        AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
                        GROUP BY property_address, property_unit, cf_month,  purchase_type
                        ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)

                # print("Query: ", response_expense_by_month_by_type)
                response["response_expense_by_month_by_type"] = response_expense_by_month_by_type

                # print("In Cashflow TTM 7")

                response_expense = db.execute("""
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER
                    SELECT -- * , 
                        purchase_uid, pur_property_id, cf_year, cf_month, pur_due_date
                        , pur_cf_type, purchase_type
                        , pur_amount_due, pur_notes, pur_description, total_paid, amt_remaining, payment_status
                        , property_address, property_unit
			, space.bills.*
                    FROM space.pp_details
		    LEFT JOIN space.bills ON pur_bill_id = bill_uid AND pur_bill_id IS NOT NULL
                    WHERE pur_payer = \'""" + user_id + """\'
                        AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
                    ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)

                # print("Query: ", response_expense)
                response["response_expense"] = response_expense

                # print("In Cashflow TTM 8")

                # OTHER
                response_other = db.execute("""
                    -- ALL OTHER TRANSACTIONS AFFECTING A PARTICULAR OWNER
                    SELECT -- * , 
                        purchase_uid, pur_property_id, cf_year, cf_month, pur_due_date
                        , pur_cf_type, purchase_type
                        , pur_amount_due, pur_notes, pur_description, total_paid, amt_remaining, payment_status
                        , property_address, property_unit
                    FROM space.pp_details
                    WHERE receiver_profile_uid = \'""" + user_id + """\'
                        AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
                        AND purchase_status != 'DELETED'
                        AND (pur_cf_type != 'expense' AND pur_cf_type != 'revenue' AND  pur_cf_type != 'EXPENSE' AND pur_cf_type != 'REVENUE')
                    ORDER BY property_address ASC, property_unit ASC, pur_due_date ASC;
                    """)

                # print("Query: ", response_other)
                response["response_other"] = response_other

                return response

class HappinessMatrix(Resource):
    def get(self, user_id):
        response = {}
        with connect() as db:
            print("in connect loop")

            vacancy = db.execute(""" 
SELECT 
    property_owner_id as owner_uid,
    COUNT(CASE WHEN rent_status = 'VACANT' THEN 1 END) as vacancy_num, 
    COUNT(*) AS total_properties,
    cast(COUNT(CASE WHEN rent_status = 'VACANT' THEN 1 END)*-100/COUNT(*) as decimal) as vacancy_perc
FROM (
    SELECT *,
        CASE
            WHEN (lease_status = 'ACTIVE' AND payment_status IS NOT NULL) THEN payment_status
            WHEN (lease_status = 'ACTIVE' AND payment_status IS NULL) THEN 'UNPAID'
            ELSE 'VACANT'
        END AS rent_status
    FROM (
        SELECT *
        FROM space.property_owner
        LEFT JOIN space.properties ON property_uid = property_id
        LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = 'ACTIVE') AS l ON property_uid = lease_property_id
        LEFT JOIN (SELECT * FROM space.contracts WHERE contract_status = 'ACTIVE') AS c ON contract_property_id = property_uid
        WHERE contract_business_id = \'""" + user_id + """\'
    ) AS o
    LEFT JOIN (
        SELECT *
        FROM space.pp_status 
        WHERE (purchase_type = 'RENT' OR ISNULL(purchase_type))
            AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
            AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
    ) as r
    ON pur_property_id = property_id
) AS rs
GROUP BY property_owner_id;
                        """)
            response["vacancy"] = vacancy
            for i in range(0,len(response["vacancy"]["result"])):
                response["vacancy"]["result"][i]["vacancy_perc"] = float(response["vacancy"]["result"][i]["vacancy_perc"])
            delta_cashflow = db.execute("""

SELECT -- * , 
space.p_details.owner_uid AS owner_id,space.p_details.owner_first_name,space.p_details.owner_last_name,space.p_details.owner_photo_url,
cast(ifnull(-100*ABS((ifnull(sum(pur_amount_due),0)-ifnull(sum(total_paid),0))/ifnull(sum(pur_amount_due),0)), 0) as decimal(10,2)) as delta_cashflow_perc 
, cast(ifnull(sum(total_paid),0) as decimal(10.2)) as cashflow , cast(ifnull(sum(pur_amount_due),0) as decimal(10,2)) as expected_cashflow -- , payment_status
FROM space.p_details
LEFT JOIN space.pp_details ON space.p_details.owner_uid = space.pp_details.pur_payer
WHERE space.p_details.contract_business_id = \'""" + user_id + """\'
GROUP BY space.p_details.owner_uid;
	            """)

            response["delta_cashflow"] = delta_cashflow

            for i in range(0,len(response["delta_cashflow"]["result"])):
                response["delta_cashflow"]["result"][i]["delta_cashflow_perc"] = float(response["delta_cashflow"]["result"][i]["delta_cashflow_perc"])
                response["delta_cashflow"]["result"][i]["cashflow"] = float(response["delta_cashflow"]["result"][i]["cashflow"])
                response["delta_cashflow"]["result"][i]["expected_cashflow"] = float(response["delta_cashflow"]["result"][i]["expected_cashflow"])

            return response

class CashflowSimplified(Resource):
    def get(self, user_id):
        print("In Cashflow Simplified")
        response = {}

        today = date.today()

        with connect() as db:
            print("in connect loop")
            cashflow = db.execute("""                            
                    -- ALL REVENUE TRANSACTIONS AFFECTING A PARTICULAR OWNER
                    SELECT -- * , 
                        purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer
                        , pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_year
                        , property_uid
                        , property_address, property_unit, property_city, property_state, property_zip
                        , bill_uid, bill_timestamp, bill_created_by, bill_description, bill_amount, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                    FROM space.pp_details
                    LEFT JOIN space.bills ON pur_bill_id = bill_uid AND pur_bill_id IS NOT NULL
                    WHERE -- receiver_profile_uid = '110-000003'
                        receiver_profile_uid = \'""" + user_id + """\'
                        AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 21*30 DAY)
                        AND purchase_status != 'DELETED'
                    ORDER BY pur_timestamp DESC;
                    """)
            return cashflow