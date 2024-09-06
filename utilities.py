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
        print("In Insert Property Utility")
        response = {}
        payload = request.form.to_dict()
        print(payload)
        if payload.get('property_uid') in {None, '', 'null'}:
            print("No property_uid")
            raise BadRequest("Request failed, no UID in payload.")
        key = {'property_uid': payload.pop('property_uid')}

        if payload.get('property_utility') in {None, '', 'null'}:
            print("No property_utility")
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
        print("\nIn Utility PUT")
        response = {}
        payload = request.form.to_dict()
        # print("Utility Update Payload: ", payload)

        # Verify uid has been included in the data
        if payload.get('property_uid') in {None, '', 'null'}:
            print("No property_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        key = {'property_uid': payload.pop('property_uid')}
        # print("Utility Property Key: ", key)
        # print("Utility Property Key: ", key['property_uid'])


        # Getting data from FrontEnd
        if payload.get('property_utility') in {None, '', 'null'}:
            print("No property_utility")
            raise BadRequest("No Utilities in payload.")
        
        else:

            # Get Property Utility Settings
            property_utility_dict = json.loads(payload['property_utility'])
            # print("Utility Settings from FrontEnd: ", property_utility_dict)
            # for property_utility, responsibility in property_utility_dict.items():
            #     print("Utility Settings: ", property_utility, responsibility)

            
            # Get current property utilities from the database
            with connect() as db:
                    # print("\nin connect")

                    response = db.execute("""
                    SELECT *
                    FROM space.property_utility
                    -- WHERE utility_property_id = '200-000001'
                    WHERE utility_property_id= \'""" + key['property_uid'] + """\'
                    """)
                    db_utilities = response['result']
                    # print("Current DB Values: ", db_utilities, type(db_utilities))

            # for utility in db_utilities:
            #     print("DB Utility: ", utility)


            # For each utility key, value pair
            for property_utility, responsibility in property_utility_dict.items():
                # print("\nUtility Setting: ", property_utility, responsibility)
                # print("Utility to check: ", property_utility)

                for utility in db_utilities:
                    # print("Current db utility", utility['utility_type_id'])
                    if utility['utility_type_id'] == property_utility:
                        # print("Property utility already exists in database")
                        # break
                        if utility['utility_payer_id'] == responsibility:
                            # Utility - Responsibility Match found
                            # print(f"Match found for utility_type_id {property_utility} with utility_payer_id {responsibility}")
                            break
                        else:
                            # Utility found but responsibility has changed => Do UPDATE
                            # print("In Update")
                            with connect() as db:
                                # print("in connect")
                                
                                utilityQuery = ("""
                                    UPDATE space.property_utility
                                    SET utility_payer_id = \'""" + responsibility + """\'
                                    WHERE utility_property_id = \'""" + key['property_uid'] + """\'
                                        AND utility_type_id = \'""" + property_utility + """\';
                                """)
                                # print(utilityQuery)
                                response["Utility_query"] = db.execute(utilityQuery, [], 'post')
                                # print(response)
                        break
                        
                else:
                    # Utility is NOT in database => Do INSERT
                    # print("In Insert")

                    with connect() as db:
                        # print("in connect")
                        
                        utilityQuery = ("""
                            INSERT INTO space.property_utility
                            SET utility_property_id = \'""" +  key['property_uid'] + """\',
                            utility_type_id = \'""" + property_utility + """\',
                            utility_payer_id = \'""" + responsibility + """\';
                        """)
                        # print(utilityQuery)
                        response["Utility_query"] = db.execute(utilityQuery, [], 'post')
                        # print(response)

            return response
            

