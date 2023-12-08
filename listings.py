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
    def get(self, tenant_id):
        print('in Listings')
        response = {}
        # conn = connect()

        with connect() as db:
            print("in connect loop")
            listingsQuery = db.execute(""" 
                     -- AVAILABLE LISTINGS
                    SELECT * FROM space.properties
                    RIGHT JOIN (SELECT * FROM space.contracts WHERE contract_status = "ACTIVE") as c 
                        ON contract_property_id = property_uid
                    LEFT JOIN (SELECT lease_property_id
                            , JSON_ARRAYAGG(JSON_OBJECT
                            ('lt_tenant_id', lt_tenant_id
                            , 'lt_responsibility', lt_responsibility
                            , 'lease_status', lease_status
                            , 'lease_end', lease_end
                            )) AS tenants
                        FROM space.leases
                        LEFT JOIN space.lease_tenant ON lt_lease_id = lease_uid
                        WHERE lease_status NOT IN ("EXPIRED","TERMINATED") AND
                            -- lt_tenant_id = "350-000084" 
                            lt_tenant_id = \'""" + tenant_id + """\'
                        GROUP BY lease_property_id) as l
                        ON lease_property_id = property_uid
                    WHERE property_available_to_rent = 1;
                    """)

            # print("Query: ", listingsQuery)
            # items = execute(istingsQuery, "get", conn)
            response["Property_Dashboard"] = listingsQuery
            return response
