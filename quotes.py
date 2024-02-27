from flask import request
from flask_restful import Resource
from data_pm import connect, uploadImage
from datetime import date, datetime, timedelta
import json

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


# TODO: Move (post) to /maintenanceQuotes after UI routing changes
class Quotes(Resource): 
    def post(self):
        response = []
        payload = request.form
        quote_maintenance_request_id = payload.get("quote_maintenance_request_id")
        quote_maintenance_contacts = payload.get("quote_maintenance_contacts").split(',')
        # print("Contacts: ", quote_maintenance_contacts, type(quote_maintenance_contacts))
        quote_pm_notes = payload["quote_pm_notes"]
        today = datetime.today().strftime('%m-%d-%Y %H:%M:%S')
        with connect() as db:
            for quote_business_id in quote_maintenance_contacts:
                # print("Business ID: ", quote_business_id)
                quote = {}
                quote["maintenance_quote_uid"] = db.call('space.new_quote_uid')['result'][0]['new_id']
                quote["quote_business_id"] = quote_business_id
                quote["quote_maintenance_request_id"] = quote_maintenance_request_id
                quote["quote_status"] = "REQUESTED"
                quote["quote_pm_notes"] = quote_pm_notes
                quote["quote_created_date"] = today

                # images = []
                # i = 0
                # while True:
                #     filename = f'img_{i}'
                #     file = request.files.get(filename)
                #     if file:
                #         key = f'maintenanceQuotes/{quote["maintenance_quote_uid"]}/{filename}'
                #         image = uploadImage(file, key, '')
                #         images.append(image)
                #     else:
                #         break
                #     i += 1

                images = []
                i = -1
                # WHILE WHAT IS TRUE?
                while True:
                    print("In while loop")
                    filename = f'img_{i}'
                    # print("Filename: ", filename)
                    if i == -1:
                        filename = 'img_cover'
                    file = request.files.get(filename)
                    # print("File: ", file)
                    if file:
                        key = f'maintenanceQuotes/{quote["maintenance_quote_uid"]}/{filename}'
                        image = uploadImage(file, key, '')
                        images.append(image)
                    else:
                        break
                    i += 1
                    

                quote["quote_maintenance_images"] = json.dumps(images)
                query_response = db.insert('maintenanceQuotes', quote)
                response.append(query_response)
        return response
