from flask import request
from flask_restful import Resource

from data_pm import connect
from werkzeug.exceptions import BadRequest
import json


class Utilities(Resource):
    def get(self):
        response = {}
        where = request.args.to_dict()
        with connect() as db:
            response = db.select('property_utility', where )
        return response
    

    def post(self):
        print("In Utilities")
        with connect() as db:
            data = request.form
            fields = [
                "contract_property_id"
                , 'contract_business_id'
                , "contract_start_date"
                , 'contract_end_date'
                , "contract_fees"
                , "contract_assigned_contacts"
                , "contract_documents"
                , "contract_name"
                , "contract_status"
                , "contract_early_end_date"
            ]
            newContract = {}
            for field in fields:
                if field in data:
                    newContract[field] = data.get(field)
                    # print(newContract[field])

            response = db.insert('contracts', newContract)
        return response
    

    
    def post(self):
        print("In Insert Property Utility")
        response = {}
        payload = request.form.to_dict()
        print(payload)
        if payload.get('property_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'property_uid': payload.pop('property_uid')}

        if payload.get('property_utility') is None:
            raise BadRequest("No Utilities in payload.")
        else:
            keyUtility = {'property_utility': payload.pop('property_utility')}

            print(type(key), key)
            property_uid_value = key['property_uid']
            print(property_uid_value)
            print(type(keyUtility), keyUtility)

            json_string = keyUtility['property_utility']

            # Parse the JSON string into a Python dictionary
            data = json.loads(json_string)

            # Now, you can iterate over the items in the dictionary
            for key, value in data.items():
                print(f'Key: {key}, Value: {value}')

                with connect() as db:
                    print("in connect")
                    
                    utilityQuery = ("""
                        INSERT INTO space.property_utility
                        SET utility_property_id = \'""" + property_uid_value + """\',
                        utility_type_id = \'""" + key + """\',
                        utility_payer_id = \'""" + value + """\';
                    """)

                    response["Utility_query"] = db.execute(utilityQuery, [], 'post')

            return response
        


    def put(self):
        print("In update Property Utility")
        response = {}
        payload = request.form.to_dict()
        print(payload)
        if payload.get('property_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'property_uid': payload.pop('property_uid')}

        if payload.get('property_utility') is None:
            raise BadRequest("No Utilities in payload.")
        else:
            keyUtility = {'property_utility': payload.pop('property_utility')}

            print(type(key), key)
            property_uid_value = key['property_uid']
            print(property_uid_value)
            print(type(keyUtility), keyUtility)

            json_string = keyUtility['property_utility']

            # Parse the JSON string into a Python dictionary
            data = json.loads(json_string)

            # Now, you can iterate over the items in the dictionary
            for key, value in data.items():
                print(f'Key: {key}, Value: {value}')

                with connect() as db:
                    print("in connect")
                    
                    utilityQuery = ("""
                        UPDATE space.property_utility
                        SET utility_payer_id = \'""" + value + """\'
                        WHERE utility_property_id = \'""" + property_uid_value + """\'
                            AND utility_type_id = \'""" + key + """\';
                    """)

                    response["Utility_query"] = db.execute(utilityQuery, [], 'post')

            return response
            

