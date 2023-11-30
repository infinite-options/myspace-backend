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
                    SELECT * FROM space.contracts
                    LEFT JOIN space.properties ON contract_property_id = property_uid
                    LEFT JOIN (SELECT JSON_ARRAYAGG(JSON_OBJECT
                                                    ('tenant_uid', tenant_uid,
                                                    'lt_responsibility', lt_responsibility,
                                                    'tenant_first_name', tenant_first_name,
                                                    'tenant_last_name', tenant_last_name,
                                                    'tenant_phone_number', tenant_phone_number,
                                                    'tenant_email', tenant_email
                                                    )) AS tenants, space.leases.*, lt_tenant_id, CASE WHEN lt_tenant_id = \'""" + tenant_id + """\' AND lease_status = "ACTIVE" THEN 1 ELSE NULL END AS is_target_tenant FROM space.leases LEFT JOIN space.t_details ON lt_lease_id = lease_uid WHERE (lease_end < DATE_ADD(CURDATE(), INTERVAL 1 MONTH) AND lease_end != "") OR (lt_tenant_id = \'""" + tenant_id + """\') GROUP BY lt_lease_id) AS l ON lease_property_id = property_uid
                    WHERE (contract_status = "ACTIVE" AND property_available_to_rent = "1") OR (lt_tenant_id = \'""" + tenant_id + """\')
                    GROUP BY contract_property_id;
                    """)

            # print("Query: ", listingsQuery)
            # items = execute(istingsQuery, "get", conn)
            response["Property_Dashboard"] = listingsQuery
            return response
