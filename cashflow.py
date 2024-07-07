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
from werkzeug.exceptions import BadRequest


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
                        , pur_cf_type, purchase_type, pur_payer
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
                        purchase_uid, cf_year, cf_month, pur_due_date
                        , pur_cf_type, purchase_type, pur_payer
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
            if user_id[:3] == '600':
                with connect() as db:
                    businessType = db.execute(""" 
                                -- CHECK BUSINESS TYPE
                                SELECT -- *
                                    business_uid, business_type
                                FROM space.businessProfileInfo
                                WHERE business_uid = \'""" + user_id + """\';
                                """)

                        # print(businessType)
                    print(businessType["result"])
                    # print(businessType["result"][0]["business_type"])

                    # if businessType["result"] == "()":
                    if len(businessType["result"]) == 0:
                        print("BUSINESS UID Not found")
                        response["MaintenanceStatus"] = "BUSINESS UID Not Found"
                        return response

                    elif businessType["result"][0]["business_type"] == "MAINTENANCE":
                        response["MaintenanceStatus"] = "YOU ENTERED A MAINTENANCE ID, PLEASE ENTER A VALID PM ID"
                        return response

                    elif businessType["result"][0]["business_type"] == "MANAGEMENT":
                        ownerl = []
                        selectQuery = db.execute("""
                                                SELECT property_owner_id FROM
                                                space.contracts
                                                LEFT JOIN space.properties ON contract_property_id = property_uid
                                                LEFT JOIN space.property_owner ON property_uid = property_id
                                                WHERE contract_business_id = \'""" + user_id + """\'
                                                GROUP BY property_owner_id;
                                                """)

                        # print("Query: ", response_revenue_by_year)
                        response["owners"] = selectQuery
                        print(response)
                        if len(response["owners"]["result"]) != 0:
                            for i in range(len(response["owners"]['result'])):
                                ownerl.append(response["owners"]["result"][i]["property_owner_id"])
                            print(response)
                            response = {}
                            ownerl = [value for value in ownerl if value is not None]
                            for owner in ownerl:
                                response[owner] = {}  # Initialize response dictionary for each owner

                                # Query for revenue by year
                                response_revenue_by_year = db.execute("""
                                    SELECT -- * , 
                                        cf_year -- , cf_month, pur_due_date
                                        , pur_cf_type -- , purchase_type
                                        , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                                        , property_address, property_unit, pur_property_id,
                                        cast(ifnull(-100*ABS((ifnull(sum(pur_amount_due),0)-ifnull(sum(total_paid),0))/ifnull(sum(pur_amount_due),0)), 0) as decimal(10,2)) as delta_cashflow_perc 
, cast(ifnull(sum(total_paid),0) as decimal(10.2)) as cashflow , cast(ifnull(sum(pur_amount_due),0) as decimal(10,2)) as expected_cashflow
                                    FROM space.pp_details
                                    WHERE receiver_profile_uid = \'""" + owner + """\'
                                        AND cf_year = \'""" + year + """\'
                                        AND purchase_status != 'DELETED'
                                        AND (pur_cf_type = 'REVENUE' OR pur_cf_type = 'revenue')
                                        GROUP BY property_address, property_unit
                                        ORDER BY property_address ASC, property_unit ASC;
                                """)

                                for row in response_revenue_by_year["result"]:
                                    property_id = row["pur_property_id"]
                                    response[owner][property_id] = {
                                        "revenue_by_year": {
                                            "cf_year": row["cf_year"],
                                            "sum(pur_amount_due)": row["sum(pur_amount_due)"],
                                            "sum(total_paid)": row["sum(total_paid)"],
                                            "sum(amt_remaining)": row["sum(amt_remaining)"],
                                            "property_address": row["property_address"],
                                            "property_unit": row["property_unit"],
                                            "cashflow": row["cashflow"],
                                            "expected_cashflow": row["expected_cashflow"],
                                            "delta_cashflow_perc": row["delta_cashflow_perc"]
                                        }
                                    }

                                # Query for expense by year
                                response_expense_by_year = db.execute("""
                                    SELECT -- * , 
                                        cf_year -- , cf_month, pur_due_date
                                        , pur_cf_type -- , purchase_type
                                        , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                                        , property_address, property_unit, pur_property_id,
                                        cast(ifnull(-100*ABS((ifnull(sum(pur_amount_due),0)-ifnull(sum(total_paid),0))/ifnull(sum(pur_amount_due),0)), 0) as decimal(10,2)) as delta_cashflow_perc 
, cast(ifnull(sum(total_paid),0) as decimal(10.2)) as cashflow , cast(ifnull(sum(pur_amount_due),0) as decimal(10,2)) as expected_cashflow
                                    FROM space.pp_details
                                    WHERE pur_payer = \'""" + owner + """\'
                                        AND cf_year = \'""" + year + """\'
                                        AND purchase_status != 'DELETED'
                                        AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
                                        GROUP BY property_address, property_unit
                                        ORDER BY property_address ASC, property_unit ASC;
                                """)

                                for row in response_expense_by_year["result"]:
                                    property_id = row["pur_property_id"]
                                    if property_id in response[owner]:
                                        response[owner][property_id]["expense_by_year"] = {
                                            "cf_year": row["cf_year"],
                                            "sum(pur_amount_due)": row["sum(pur_amount_due)"],
                                            "sum(total_paid)": row["sum(total_paid)"],
                                            "sum(amt_remaining)": row["sum(amt_remaining)"],
                                            "property_address": row["property_address"],
                                            "property_unit": row["property_unit"],
                                            "cashflow": row["cashflow"],
                                            "expected_cashflow": row["expected_cashflow"],
                                            "delta_cashflow_perc": row["delta_cashflow_perc"]
                                        }
                                    else:
                                        response[owner][property_id] = {
                                            "expense_by_year": {
                                                "cf_year": row["cf_year"],
                                                "sum(pur_amount_due)": row["sum(pur_amount_due)"],
                                                "sum(total_paid)": row["sum(total_paid)"],
                                                "sum(amt_remaining)": row["sum(amt_remaining)"],
                                                "property_address": row["property_address"],
                                                "property_unit": row["property_unit"],
                                                "cashflow": row["cashflow"],
                                                "expected_cashflow": row["expected_cashflow"],
                                                "delta_cashflow_perc": row["delta_cashflow_perc"]
                                            }
                                        }

                            return response
                        else:
                            response["Owners"] = "This PM ID HAS NO CONTRACTED OWNERS"

            else:
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
                            , pur_cf_type, purchase_type, pur_payer
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
            if user_id[:3] == '600':
                with connect() as db:
                    businessType = db.execute(""" 
                                -- CHECK BUSINESS TYPE
                                SELECT -- *
                                    business_uid, business_type
                                FROM space.businessProfileInfo
                                WHERE business_uid = \'""" + user_id + """\';
                                """)

                    # print(businessType)
                    print(businessType["result"])
                    # print(businessType["result"][0]["business_type"])

                    # if businessType["result"] == "()":
                    if len(businessType["result"]) == 0:
                        print("BUSINESS UID Not found")
                        response["MaintenanceStatus"] = "BUSINESS UID Not Found"
                        return response

                    elif businessType["result"][0]["business_type"] == "MAINTENANCE":
                        response["MaintenanceStatus"] = "YOU ENTERED A MAINTENANCE ID, PLEASE ENTER A VALID PM ID"
                        return response

                    elif businessType["result"][0]["business_type"] == "MANAGEMENT":
                        ownerl = []
                        selectQuery = db.execute("""
                                                SELECT property_owner_id FROM
                                                space.contracts
                                                LEFT JOIN space.properties ON contract_property_id = property_uid
                                                LEFT JOIN space.property_owner ON property_uid = property_id
                                                WHERE contract_business_id = \'""" + user_id + """\'
                                                GROUP BY property_owner_id;
                                                """)

                        # print("Query: ", response_revenue_by_year)
                        response["owners"] = selectQuery
                        if len(response["owners"]["result"]) != 0:
                            for i in range(len(response["owners"]['result'])):
                                ownerl.append(response["owners"]["result"][i]["property_owner_id"])
                            print(response)
                            response = {}
                            ownerl = [value for value in ownerl if value is not None]
                            for owner in ownerl:
                                response[owner] = {}  # Initialize response dictionary for each owner

                                # Query for revenue by year
                                response_revenue_by_year = db.execute("""
                                    SELECT 
                                        cf_year,
                                        sum(pur_amount_due),
                                        sum(total_paid),
                                        sum(amt_remaining),
                                        property_address,
                                        property_unit,
                                        pur_property_id,
                                        cast(ifnull(-100*ABS((ifnull(sum(pur_amount_due),0)-ifnull(sum(total_paid),0))/ifnull(sum(pur_amount_due),0)), 0) as decimal(10,2)) as delta_cashflow_perc 
, cast(ifnull(sum(total_paid),0) as decimal(10.2)) as cashflow , cast(ifnull(sum(pur_amount_due),0) as decimal(10,2)) as expected_cashflow
                                    FROM space.pp_details
                                    WHERE receiver_profile_uid = '""" + owner + """'
                                        AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
                                        AND purchase_status != 'DELETED'
                                        AND (pur_cf_type = 'REVENUE' OR pur_cf_type = 'revenue')
                                    GROUP BY property_address, property_unit
                                    ORDER BY property_address ASC, property_unit ASC
                                """)

                                for row in response_revenue_by_year["result"]:
                                    property_id = row["pur_property_id"]
                                    response[owner][property_id] = {
                                        "revenue_by_year": {
                                            "cf_year": row["cf_year"],
                                            "sum(pur_amount_due)": row["sum(pur_amount_due)"],
                                            "sum(total_paid)": row["sum(total_paid)"],
                                            "sum(amt_remaining)": row["sum(amt_remaining)"],
                                            "property_address": row["property_address"],
                                            "property_unit": row["property_unit"],
                                            "cashflow": row["cashflow"],
                                            "expected_cashflow": row["expected_cashflow"],
                                            "delta_cashflow_perc": row["delta_cashflow_perc"]
                                        }
                                    }

                                # Query for expense by year
                                response_expense_by_year = db.execute("""
                                    SELECT 
                                        cf_year,
                                        sum(pur_amount_due),
                                        sum(total_paid),
                                        sum(amt_remaining),
                                        property_address,
                                        property_unit,
                                        pur_property_id,
                                        cast(ifnull(-100*ABS((ifnull(sum(pur_amount_due),0)-ifnull(sum(total_paid),0))/ifnull(sum(pur_amount_due),0)), 0) as decimal(10,2)) as delta_cashflow_perc 
, cast(ifnull(sum(total_paid),0) as decimal(10.2)) as cashflow , cast(ifnull(sum(pur_amount_due),0) as decimal(10,2)) as expected_cashflow
                                    FROM space.pp_details
                                    WHERE pur_payer = '""" + owner + """'
                                        AND STR_TO_DATE(pur_due_date, '%m-%d-%Y') > DATE_SUB(NOW(), INTERVAL 365 DAY)
                                        AND purchase_status != 'DELETED'
                                        AND (pur_cf_type = 'EXPENSE' OR pur_cf_type = 'expense')
                                    GROUP BY property_address, property_unit
                                    ORDER BY property_address ASC, property_unit ASC
                                """)

                                for row in response_expense_by_year["result"]:
                                    property_id = row["pur_property_id"]
                                    if property_id in response[owner]:
                                        response[owner][property_id]["expense_by_year"] = {
                                            "cf_year": row["cf_year"],
                                            "sum(pur_amount_due)": row["sum(pur_amount_due)"],
                                            "sum(total_paid)": row["sum(total_paid)"],
                                            "sum(amt_remaining)": row["sum(amt_remaining)"],
                                            "property_address": row["property_address"],
                                            "property_unit": row["property_unit"],
                                            "cashflow": row["cashflow"],
                                            "expected_cashflow": row["expected_cashflow"],
                                            "delta_cashflow_perc": row["delta_cashflow_perc"]

                                        }
                                    else:
                                        response[owner][property_id] = {
                                            "expense_by_year": {
                                                "cf_year": row["cf_year"],
                                                "sum(pur_amount_due)": row["sum(pur_amount_due)"],
                                                "sum(total_paid)": row["sum(total_paid)"],
                                                "sum(amt_remaining)": row["sum(amt_remaining)"],
                                                "property_address": row["property_address"],
                                                "property_unit": row["property_unit"],
                                                "cashflow": row["cashflow"],
                                                "expected_cashflow": row["expected_cashflow"],
                                                "delta_cashflow_perc": row["delta_cashflow_perc"]
                                            }
                                        }

                            return response
                        else:
                            response["Owners"] = "This PM ID HAS NO CONTRACTED OWNERS"
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
                        -- WHERE receiver_profile_uid = '110-000003'
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
                        -- WHERE receiver_profile_uid = '110-000003'
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
                        -- WHERE receiver_profile_uid = '110-000003'
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
                            , pur_cf_type, purchase_type, pur_payer
                            , pur_amount_due, pur_notes, pur_description, total_paid, amt_remaining, payment_status
                            , property_address, property_unit
                            , space.bills.*
                        FROM space.pp_details
                        LEFT JOIN space.bills ON pur_bill_id = bill_uid AND pur_bill_id IS NOT NULL
                        WHERE receiver_profile_uid = \'""" + user_id + """\'
                        -- WHERE receiver_profile_uid = '110-000003'
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
                        -- ALL EXPENSE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY YEAR
                        SELECT -- * , 
                            cf_year -- , cf_month, pur_due_date
                            , pur_cf_type -- , purchase_type
                            , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                            , property_address, property_unit
                        FROM space.pp_details
                        WHERE payer_profile_uid = \'""" + user_id + """\'
                        -- WHERE payer_profile_uid = '110-000003'
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
                        -- ALL EXPENSE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH
                        SELECT -- * , 
                            cf_year, cf_month -- , pur_due_date
                            , pur_cf_type -- , purchase_type
                            , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                            , property_address, property_unit
                        FROM space.pp_details
                        WHERE payer_profile_uid = \'""" + user_id + """\'
                        -- WHERE payer_profile_uid = '110-000003'
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
                        -- ALL EXPENSE TRANSACTIONS AFFECTING A PARTICULAR OWNER BY PROPERTY BY MONTH BY PURCHASE TYPE
                        SELECT -- * , 
                            cf_year, cf_month -- , pur_due_date
                            , pur_cf_type, purchase_type
                            , sum(pur_amount_due), sum(total_paid), sum(amt_remaining) -- , payment_status
                            , property_address, property_unit
                        FROM space.pp_details
                        WHERE payer_profile_uid = \'""" + user_id + """\'
                        -- WHERE payer_profile_uid = '110-000003'
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
                        WHERE payer_profile_uid = \'""" + user_id + """\'
                        -- WHERE payer_profile_uid = '110-000003'
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
        
class CashflowSummary(Resource):
    def get(self, user_id):
        print("In Cashflow Summary for ", user_id)
        response = {}
        with connect() as db:

            if user_id.startswith("110"):
                ownerCashFlow = db.execute("""
                        -- OWNER CASHFLOW
                                           
                        -- EXPECTED REVENUE
                        SELECT pur_cf_type, pur_receiver, pur_payer
                            , SUM(pur_amount_due) AS cf
                            , "EXPECTED REVENUE" AS cf_type
                        FROM space.pp_details
                        -- WHERE pur_receiver = '110-000003'
                        WHERE pur_receiver = \'""" + user_id + """\'
                        AND pur_cf_type = 'revenue'

                        UNION  
                                           
                        -- ACTUAL REVENUE
                        SELECT pur_cf_type, pur_receiver, pur_payer
                            , SUM(pur_amount_due) AS cf
                            , "ACTUAL REVENUE" AS cf_type
                        FROM space.pp_details
                        -- WHERE pur_receiver = '110-000003'
                        WHERE pur_receiver = \'""" + user_id + """\'
                        AND pur_cf_type = 'revenue'
                        AND payment_status IN ("PAID", "PARTIALLY PAID", "PAID LATE")
                        
                        UNION  
                                           
                        -- EXPECTED EXPENSE
                        SELECT pur_cf_type, pur_receiver, pur_payer
                            , SUM(pur_amount_due) AS cf
                            , "EXPECTED EXPENSE" AS cf_type
                        FROM space.pp_details
                        -- WHERE pur_payer = '110-000003'
                        WHERE pur_payer = \'""" + user_id + """\'
                        AND pur_cf_type = 'expense'
                        
                        UNION  
                                           
                        -- ACTUAL EXPENSE 
                        SELECT pur_cf_type, pur_receiver, pur_payer
                            , SUM(pur_amount_due) AS cf
                            , "ACTUAL EXPENSE" AS cf_type
                        FROM space.pp_details
                        -- WHERE pur_payer = '110-000003'
                        WHERE pur_payer = \'""" + user_id + """\'
                        AND pur_cf_type = 'expense'
                        AND payment_status IN ("PAID", "PARTIALLY PAID", "PAID LATE")
                        ; 
                        """)
                response["profile"] = ownerCashFlow

            elif user_id.startswith("600"):
                businessCashFlow = db.execute("""
                    -- MANAGER CASHFLOW

                    -- EXPECTED REVENUE
                    SELECT pur_cf_type, pur_receiver, pur_payer
                        , SUM(pur_amount_due) AS cf
                        , "EXPECTED REVENUE" AS cf_type
                    FROM space.pp_details
                    -- WHERE pur_receiver = '600-000003'
                    WHERE pur_receiver = \'""" + user_id + """\'
                    AND pur_cf_type = 'expense'

                    UNION  
                    -- ACTUAL REVENUE
                    SELECT pur_cf_type, pur_receiver, pur_payer
                        , SUM(pur_amount_due) AS cf
                        , "ACTUAL REVENUE" AS cf_type
                    FROM space.pp_details
                    -- WHERE pur_receiver = '600-000003'
                    WHERE pur_receiver = \'""" + user_id + """\'
                    AND pur_cf_type = 'expense'
                    AND payment_status IN ("PAID", "PARTIALLY PAID", "PAID LATE")
                    
                    UNION  
                    -- PASSTHROUGH EXPENSE MAINTENANCE
                    SELECT pur_cf_type, pur_receiver, pur_payer
                        , SUM(pur_amount_due) AS cf
                        , "MAINTENANCE EXPECTED EXPENSE PAID" AS cf_type
                    FROM space.pp_details
                    -- WHERE pur_payer = '600-000003'
                    WHERE pur_payer = \'""" + user_id + """\'
                    AND pur_cf_type = 'expense'
                    AND purchase_type = 'MAINTENANCE'
                    
                    
                    UNION  
                    -- PASSTHROUGH EXPENSE MAINTENANCE
                    SELECT pur_cf_type, pur_receiver, pur_payer
                        , SUM(pur_amount_due) AS cf
                        , "RENT EXPECTED EXPENSE PAID" AS cf_type
                    FROM space.pp_details
                    -- WHERE pur_payer = '600-000003'
                    WHERE pur_payer = \'""" + user_id + """\'
                    AND pur_cf_type = 'revenue'
                    AND purchase_type = 'RENT'  
                    
                    UNION  
                    -- PASSTHROUGH EXPENSE MAINTENANCE
                    SELECT pur_cf_type, pur_receiver, pur_payer
                        , SUM(pur_amount_due) AS cf
                        , "MAINTENANCE EXPECTED REVENUE RECEIVED" AS cf_type
                    FROM space.pp_details
                    -- WHERE pur_receiver = '600-000003'
                    WHERE pur_receiver = \'""" + user_id + """\'
                    AND pur_cf_type = 'expense'
                    AND purchase_type = 'MAINTENANCE'
                    
                    
                    UNION  
                    -- PASSTHROUGH EXPENSE MAINTENANCE
                    SELECT pur_cf_type, pur_receiver, pur_payer
                        , SUM(pur_amount_due) AS cf
                        , "RENT EXPECTED REVENUE RECEIVED" AS cf_type
                    FROM space.pp_details
                    -- WHERE pur_receiver = '600-000003'
                    WHERE pur_receiver = \'""" + user_id + """\'
                    AND pur_cf_type = 'revenue'
                    AND purchase_type = 'RENT'  
                    
                    UNION  
                    -- PASSTHROUGH EXPENSE MAINTENANCE
                    SELECT pur_cf_type, pur_receiver, pur_payer
                        , SUM(pur_amount_due) AS cf
                        , "MAINTENANCE ACTUAL EXPENSE PAID" AS cf_type
                    FROM space.pp_details
                    -- WHERE pur_payer = '600-000003'
                    WHERE pur_payer = \'""" + user_id + """\'
                    AND pur_cf_type = 'expense'
                    AND purchase_type = 'MAINTENANCE'
                    AND payment_status IN ("PAID", "PARTIALLY PAID", "PAID LATE")
                    
                    
                    UNION  
                    -- PASSTHROUGH EXPENSE MAINTENANCE
                    SELECT pur_cf_type, pur_receiver, pur_payer
                        , SUM(pur_amount_due) AS cf
                        , "RENT ACTUAL EXPENSE PAID" AS cf_type
                    FROM space.pp_details
                    -- WHERE pur_payer = '600-000003'
                    WHERE pur_payer = \'""" + user_id + """\'
                    AND pur_cf_type = 'revenue'
                    AND purchase_type = 'RENT'  
                    AND payment_status IN ("PAID", "PARTIALLY PAID", "PAID LATE")
                    
                    UNION  
                    -- PASSTHROUGH EXPENSE MAINTENANCE
                    SELECT pur_cf_type, pur_receiver, pur_payer
                        , SUM(pur_amount_due) AS cf
                        , "MAINTENANCE ACTUAL REVENUE RECEIVED" AS cf_type
                    FROM space.pp_details
                    -- WHERE pur_receiver = '600-000003'
                    WHERE pur_receiver = \'""" + user_id + """\'
                    AND pur_cf_type = 'expense'
                    AND purchase_type = 'MAINTENANCE'
                    AND payment_status IN ("PAID", "PARTIALLY PAID", "PAID LATE")
                    
                    
                    UNION  
                    -- PASSTHROUGH EXPENSE MAINTENANCE
                    SELECT pur_cf_type, pur_receiver, pur_payer
                        , SUM(pur_amount_due) AS cf
                        , "RENT ACTUAL REVENUE RECEIVED" AS cf_type
                    FROM space.pp_details
                    -- WHERE pur_receiver = '600-000003'
                    WHERE pur_receiver = \'""" + user_id + """\'
                    AND pur_cf_type = 'revenue'
                    AND purchase_type = 'RENT'
                    AND payment_status IN ("PAID", "PARTIALLY PAID", "PAID LATE")
                    ;
                    """)
                response["profile"] = businessCashFlow

            else:
                raise BadRequest("Request failed, no UID in payload.")

        return response
    

class CashflowRevised(Resource):
    def get(self, user_id, type):
        print("In Cashflow Revised")
        response = {}

        today = date.today()

        with connect() as db:

            if   type == 'DETAILS':
                print("ALL DETAILS")
                cashflow = db.execute("""                            
                        -- ALL DETAILS
                        SELECT pur_receiver, pur_payer
                            , SUM(pur_amount_due) AS amount_due
                            , SUM(total_paid) AS total_paid
                            , cf_month, cf_month_num, cf_year
                            , pur_cf_type
                            , purchase_type
                            , property_uid, property_address, property_unit, property_city, property_state, property_zip
                            , purchase_uid, purchase_status, pur_amount_due
                        --     , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description
                        --     , pur_receiver, pur_initiator, pur_payer
                        --     , pur_group, pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_month_num, cf_year
                        --     , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email
                        --     , initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email
                        --     , payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email
                        --     , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image
                        --     , property_id, property_owner_id, po_owner_percent
                        --     , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_photo_url
                        --     , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                        --     , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                        --     , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_actual_rent, leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lt_lease_id, lt_tenant_id, lt_responsibility
                        --     , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                        FROM space.pp_details
                        -- WHERE (pur_receiver = '110-000003' OR pur_payer = '110-000003')
                        WHERE (pur_receiver = \'""" + user_id + """\' OR pur_payer = \'""" + user_id + """\')
                        GROUP BY cf_month, cf_year, pur_cf_type, purchase_type, property_uid, purchase_uid
                        ORDER BY cf_month_num, property_uid;
                        """)
                return cashflow
            
            elif type == 'PROPERTY':
                print("Cashflow by Year, by Month, by CF Type, by Purchase Category, by Property")
                cashflow = db.execute("""                            
                        -- GROUPED BY OWNER, BY YEAR, BY MONTH, BY CF_TYPE, BY PURCHASE CATEGORY, BY PROPERTY
                        SELECT pur_receiver, pur_payer
                            , SUM(pur_amount_due) AS pur_amount_due
                            , SUM(total_paid) AS total_paid
                            , cf_month, cf_month_num, cf_year
                            , pur_cf_type
                            , purchase_type
                            , property_uid, property_address, property_unit, property_city, property_state, property_zip
                            -- , purchase_uid, purchase_status, pur_amount_due
                            , JSON_ARRAYAGG(JSON_OBJECT(
                                'purchase_uid',  purchase_uid,
                                'purchase_status', purchase_status,
                                'pur_amount_due', pur_amount_due
                                )) AS individual_purchase
                            -- , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description
                        --     , pur_receiver, pur_initiator, pur_payer
                        --     , pur_group, pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_month_num, cf_year
                        --     , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email
                        --     , initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email
                        --     , payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email
                        --     , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image
                        --     , property_id, property_owner_id, po_owner_percent
                        --     , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_photo_url
                        --     , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                        --     , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                        --     , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_actual_rent, leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lt_lease_id, lt_tenant_id, lt_responsibility
                        --     , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                        FROM space.pp_details
                        WHERE (pur_receiver = '110-000003' OR pur_payer = '110-000003')
                        GROUP BY cf_month, cf_year, pur_cf_type, purchase_type, property_uid
                        ORDER BY cf_month_num, property_uid;
                        """)
                return cashflow
            
            elif type == 'CATEGORY':
                print("Cashflow by Year, by Month, by CF Type, by Purchase Category")
                cashflow = db.execute("""                            
                        -- GROUPED BY OWNER, BY YEAR, BY MONTH, BY CF_TYPE, BY PURCHASE CATEGORY
                        SELECT pur_receiver, pur_payer
                            , SUM(pur_amount_due) AS pur_amount_due
                            , SUM(total_paid) AS total_paid
                            , cf_month, cf_month_num, cf_year
                            , pur_cf_type
                            , purchase_type
                            -- , property_uid, property_address, property_unit, property_city, property_state, property_zip
                            , JSON_ARRAYAGG(JSON_OBJECT(
                                'property_uid',  property_uid,
                                'property_address', property_address,
                                'property_unit',  property_unit,
                                'property_city', property_city,
                                'property_state',  property_state,
                                'property_zip', property_zip,
                                'individual_purchase', individual_purchase
                                )) AS property
                        FROM (
                            SELECT pur_receiver, pur_payer
                                , SUM(pur_amount_due) AS pur_amount_due
                                , SUM(total_paid) AS total_paid
                                , cf_month, cf_month_num, cf_year
                                , pur_cf_type
                                , purchase_type
                                , property_uid, property_address, property_unit, property_city, property_state, property_zip
                                -- , purchase_uid, purchase_status, pur_amount_due
                                , JSON_ARRAYAGG(JSON_OBJECT(
                                    'purchase_uid',  purchase_uid,
                                    'purchase_status', purchase_status,
                                    'pur_amount_due', pur_amount_due
                                    )) AS individual_purchase
                                -- , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description
                            --     , pur_receiver, pur_initiator, pur_payer
                            --     , pur_group, pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_month_num, cf_year
                            --     , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email
                            --     , initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email
                            --     , payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email
                            --     , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image
                            --     , property_id, property_owner_id, po_owner_percent
                            --     , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_photo_url
                            --     , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                            --     , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                            --     , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_actual_rent, leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lt_lease_id, lt_tenant_id, lt_responsibility
                            --     , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                            FROM space.pp_details
                            WHERE (pur_receiver = '110-000003' OR pur_payer = '110-000003')
                            GROUP BY cf_month, cf_year, pur_cf_type, purchase_type, property_uid
                            ORDER BY cf_month_num, property_uid
                            ) AS p
                            GROUP BY cf_month, cf_year, pur_cf_type, purchase_type
                            ORDER BY cf_month_num
                        """)
                return cashflow
            
            elif type == 'TYPE':
                print("Cashflow by Year, by Month, by CF Type")
                cashflow = db.execute("""                            
                        -- GROUPED BY OWNER, BY YEAR, BY MONTH, BY CF_TYPE
                        SELECT pur_receiver, pur_payer
                            , SUM(pur_amount_due) AS pur_amount_due
                            , SUM(total_paid) AS total_paid
                            , cf_month, cf_month_num, cf_year
                            , pur_cf_type
                            , JSON_ARRAYAGG(JSON_OBJECT(
                                'purchase_type',  purchase_type,
                                'property', property
                                )) AS cf_type
                        FROM (
                            -- GROUPED BY OWNER, BY YEAR, BY MONTH, BY CF_TYPE, BY PURCHASE CATEGORY
                            SELECT pur_receiver, pur_payer
                                , SUM(pur_amount_due) AS pur_amount_due
                                , SUM(total_paid) AS total_paid
                                , cf_month, cf_month_num, cf_year
                                , pur_cf_type
                                , purchase_type
                                -- , property_uid, property_address, property_unit, property_city, property_state, property_zip
                                , JSON_ARRAYAGG(JSON_OBJECT(
                                    'property_uid',  property_uid,
                                    'property_address', property_address,
                                    'property_unit',  property_unit,
                                    'property_city', property_city,
                                    'property_state',  property_state,
                                    'property_zip', property_zip,
                                    'individual_purchase', individual_purchase
                                    )) AS property
                            FROM (
                                SELECT pur_receiver, pur_payer
                                    , SUM(pur_amount_due) AS pur_amount_due
                                    , SUM(total_paid) AS total_paid
                                    , cf_month, cf_month_num, cf_year
                                    , pur_cf_type
                                    , purchase_type
                                    , property_uid, property_address, property_unit, property_city, property_state, property_zip
                                    -- , purchase_uid, purchase_status, pur_amount_due
                                    , JSON_ARRAYAGG(JSON_OBJECT(
                                        'purchase_uid',  purchase_uid,
                                        'purchase_status', purchase_status,
                                        'pur_amount_due', pur_amount_due
                                        )) AS individual_purchase
                                    -- , purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description
                                --     , pur_receiver, pur_initiator, pur_payer
                                --     , pur_group, pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_month_num, cf_year
                                --     , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email
                                --     , initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email
                                --     , payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email
                                --     , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image
                                --     , property_id, property_owner_id, po_owner_percent
                                --     , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_photo_url
                                --     , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                --     , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                                --     , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_actual_rent, leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lt_lease_id, lt_tenant_id, lt_responsibility
                                --     , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                                FROM space.pp_details
                                WHERE (pur_receiver = '110-000003' OR pur_payer = '110-000003')
                                GROUP BY cf_month, cf_year, pur_cf_type, purchase_type, property_uid
                                ORDER BY cf_month_num, property_uid
                                ) AS p
                                GROUP BY cf_month, cf_year, pur_cf_type, purchase_type
                                ORDER BY cf_month_num
                            ) AS c
                        GROUP BY cf_month, cf_year, pur_cf_type
                        ORDER BY cf_month_num
                        """)
                return cashflow