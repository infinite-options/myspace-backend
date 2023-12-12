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
    def get(self,tenant_id):
        print('in Listings')
        response = {}
        # conn = connect()

        with connect() as db:
            print("in listings loop")
            listingsQuery = db.execute(""" 
                    -- AVAILABLE LISTINGS EXCLUDING ACTIVE LEASES EXPIRING MORE THAT 2 MONTH FROM NOW
                    SELECT * 
                    FROM (
                        SELECT * FROM space.properties
                        RIGHT JOIN (
                            SELECT * FROM space.contracts 
                            WHERE contract_status = "ACTIVE") AS c 
                        ON contract_property_id = property_uid
                        WHERE property_available_to_rent = 1 ) AS p
                    LEFT JOIN ( 
                        SELECT * FROM space.leases 
                        -- WHERE lease_status = "ACTIVE"
                        WHERE lease_status = "ACTIVE" AND STR_TO_DATE(lease_end, '%m-%d-%Y') > DATE_ADD(CURDATE(), INTERVAL 2 MONTH)
                        ) AS l
                    ON lease_property_id = property_uid
                    WHERE lease_status IS null;
                    """)

            # print("Query: ", listingsQuery)
            # items = execute(istingsQuery, "get", conn)
            response["Available_Listings"] = listingsQuery

        with connect() as db:
            print("in tenant loop")
            tenantsQuery = db.execute(""" 

                    -- SHOW TENANT LEASES
                    SELECT * FROM space.lease_tenant
                    LEFT JOIN space.leases ON lease_uid = lt_lease_id
                    -- WHERE lt_tenant_id = "350-000084"
                    WHERE lt_tenant_id = \'""" + tenant_id + """\'
                    """)
            # print("Query: ", tenantsQuery)
            # items = execute(istingsQuery, "get", conn)
            response["Tenant_Leases"] = tenantsQuery

            return response
                                       
                                                   
