from flask import request
from flask_restful import Resource

from data_pm import connect


class Template(Resource):
    def get(self):
        response = {}
        where = request.args.to_dict()
        with connect() as db:
            response = db.select('lists', where)
        return response
    
class Listings (Resource):
    def get(self):
        print('in Listings')
        response = {}
        # conn = connect()

        with connect() as db:
            print("in connect loop")
            listingsQuery = db.execute(""" 
                        -- AVAILABLE LISTINGS
                        SELECT * FROM space.contracts
                        LEFT JOIN space.properties ON contract_property_id = property_uid
                        LEFT JOIN space.leases ON lease_property_id = property_uid
                        WHERE contract_status = "ACTIVE" AND property_available_to_rent = "1"
                            AND (lease_end < DATE_ADD(CURDATE(), INTERVAL 1 MONTH) OR ISNULL(lease_end));
                    """)
            

            # print("Query: ", listingsQuery)
            # items = execute(istingsQuery, "get", conn)
            response["Property_Dashboard"] = listingsQuery
            return response
