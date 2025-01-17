from flask import request
from flask_restful import Resource

from data_pm import connect


class Template(Resource):
    def get(self):
        response = {}
        where = request.args.to_dict()
        with connect() as db:
            response = db.select('space_dev.lists', where)
        return response
    
class Listings (Resource):
    def get(self,tenant_id):
        print('in Listings')
        response = {}
        # conn = connect()

        with connect() as db:
            print("in listings loop")
            # listingsQuery = db.execute(""" 
            #         -- AVAILABLE LISTINGS EXCLUDING ACTIVE LEASES EXPIRING MORE THAT 2 MONTH FROM NOW
            #         SELECT * 
            #         FROM (
            #             SELECT * FROM space_dev.properties
            #             RIGHT JOIN (
            #                 SELECT * FROM space_dev.contracts 
            #                 WHERE contract_status = "ACTIVE") AS c 
            #             ON contract_property_id = property_uid
            #             WHERE property_available_to_rent = 1 ) AS p
            #         LEFT JOIN ( 
            #             SELECT * FROM space_dev.u_details                                                 
            #             ) AS u                                       
            #         ON utility_property_id = property_uid
            #         LEFT JOIN ( 
            #             SELECT * FROM space_dev.leases 
            #             -- WHERE lease_status = "ACTIVE"
            #             WHERE (lease_status = "ACTIVE" OR lease_status = "ACTIVE M2M") AND STR_TO_DATE(lease_end, '%m-%d-%Y') > DATE_ADD(CURDATE(), INTERVAL 2 MONTH)
            #             ) AS l
            #         ON lease_property_id = property_uid
            #         WHERE lease_status IS null
            #         ORDER BY property_uid;
            #         """)

            listingsQuery = db.execute(""" 
                    -- AVAILABLE LISTINGS 
                    SELECT * 
                    FROM space_dev.properties
                    LEFT JOIN (SELECT * FROM space_dev.contracts WHERE contract_status = 'ACTIVE') c ON property_uid = contract_property_id
                    LEFT JOIN space_dev.u_details ON utility_property_id = property_uid
                    WHERE property_available_to_rent = '1'
                    """)

            # print("Query: ", listingsQuery)
            # items = execute(istingsQuery, "get", conn)
            response["Available_Listings"] = listingsQuery

        with connect() as db:
            print("in tenant loop")
            tenantsQuery = db.execute(""" 
                    -- SHOW TENANT LEASES
                    SELECT * FROM space_dev.lease_tenant
                    LEFT JOIN space_dev.leases ON lease_uid = lt_lease_id
                    LEFT JOIN (
                        SELECT fees_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                            ('leaseFees_uid', leaseFees_uid,
                            'fee_name', fee_name,
                            'fee_type', fee_type,
                            'charge', charge,
                            'due_by', due_by,
                            'late_by', late_by,
                            'late_fee', late_fee,
                            'perDay_late_fee', perDay_late_fee,
                            'frequency', frequency,
                            'available_topay', available_topay,
                            'due_by_date', due_by_date
                            )) AS lease_fees
                            FROM space_dev.leaseFees
                            GROUP BY fees_lease_id) as t ON lt_lease_id = fees_lease_id
                            WHERE lt_tenant_id = \'""" + tenant_id + """\';
                    """)
            # print("Query: ", tenantsQuery)
            # items = execute(istingsQuery, "get", conn)
            response["Tenant_Leases"] = tenantsQuery

            return response
                                       
                                                   
