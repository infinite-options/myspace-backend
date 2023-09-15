from flask import request
from flask_restful import Resource
from data_pm import connect

class QuotesStatusByBusiness(Resource): 
    def get(self):
        args = request.args
        business_id = args.get("business_id")
        filter = args.get("filter")
        group_by = args.get("group_by")
        last_30_days = ""
        if filter == "last_30_days":
            last_30_days = """ quote_created_date BETWEEN (CURDATE() - INTERVAL 1 MONTH) AND CURDATE()
                    AND"""
        with connect() as db:
            query = """ 
                SELECT maintenance_request_uid, maintenance_title, maintenance_priority, maintenance_images,
                    maintenance_quote_uid, quote_business_id, property_uid, property_address, quote_status
                FROM space.m_details
                    LEFT JOIN space.properties ON property_uid = maintenance_property_id
                WHERE""" + last_30_days + """ quote_business_id = \'""" + business_id + """\'
            """
            query_result = db.execute(query)
            response_dict = {}
            for item in query_result["result"]:
                group_by_value = item.pop(str(group_by or "quote_status"))
                if group_by_value in response_dict:
                    response_dict[group_by_value].append(item)
                else:
                    response_dict[group_by_value] = [item]
            return response_dict

class QuotesByBusiness(Resource): 
    def get(self):
        args = request.args
        business_id = args.get("business_id")
        with connect() as db:
            query = """ 
                SELECT maintenance_request_uid, maintenance_title, maintenance_desc, maintenance_images,
                    maintenance_request_type, maintenance_priority, maintenance_request_created_date, maintenance_quote_uid,
                    quote_business_id, quote_earliest_availability, quote_event_duration, quote_notes, quote_status,
                    quote_total_estimate, property_uid, property_address, CONCAT(first_name, ' ', last_name) AS manager_name
                FROM space.m_details
                    LEFT JOIN space.properties ON property_uid = maintenance_property_id
                    LEFT JOIN space.businessProfileInfo ON quote_business_id = business_uid
                    LEFT JOIN space.users ON business_user_id = user_uid
                WHERE quote_business_id = \'""" + business_id + """\'
            """
            query_result = db.execute(query)
            response_dict = {}
            for item in query_result["result"]:
                group_by_value = item.pop("quote_status")
                if group_by_value in response_dict:
                    response_dict[group_by_value].append(item)
                else:
                    response_dict[group_by_value] = [item]
            return response_dict
        

class QuotesByRequest(Resource): 
    def get(self):
        args = request.args
        request_id = args.get("maintenance_request_id")
        with connect() as db:
            query = """ 
                SELECT *
                FROM space.maintenanceQuotes
                WHERE quote_maintenance_request_id = \'""" + request_id + """\' AND quote_status = 'SENT'
            """
            response = db.execute(query)
            return response