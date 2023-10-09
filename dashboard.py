from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest


class ownerDashboard(Resource):
    def get(self, owner_id):
        print('in Owner Dashboard')
        response = {}

        # print("Owner UID: ", owner_id)

        with connect() as db:
            # print("in owner dashboard")
            maintenanceQuery = db.execute(""" 
                    -- MAINTENANCE STATUS BY USER
                    SELECT -- * 
                        property_owner_id
                        , maintenance_request_status
                        , COUNT(maintenance_request_status) AS num
                    FROM space.maintenanceRequests
                    LEFT JOIN space.o_details ON maintenance_property_id = property_id
                    WHERE owner_uid = \'""" + owner_id + """\'
                    GROUP BY maintenance_request_status;
                    """)

            # print("Query: ", maintenanceQuery)
            response["MaintenanceStatus"] = maintenanceQuery

            leaseQuery = db.execute(""" 
                    -- LEASE STATUS BY USER
                    SELECT o_details.property_owner_id
                        , leases.lease_end
                        , COUNT(lease_end) AS num
                    FROM space.leases
                    LEFT JOIN space.o_details ON property_id = lease_property_id
                    LEFT JOIN space.properties ON property_uid = lease_property_id
                    LEFT JOIN space.leaseFees ON lease_uid = fees_lease_id
                    LEFT JOIN space.leaseDocuments ON lease_uid = ld_lease_id
                    LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                    LEFT JOIN space.b_details ON contract_property_id = lease_property_id
                    WHERE lease_status = "ACTIVE"
                        AND contract_status = "ACTIVE"
                        AND fee_name = "RENT"
                        AND ld_type = "LEASE"
                        AND property_owner_id = \'""" + owner_id + """\'
                    GROUP BY MONTH(lease_end),
                            YEAR(lease_end);
                    """)

            # print("Query: ", leaseQuery)
            response["LeaseStatus"] = leaseQuery

            rentQuery = db.execute(""" 
                    -- RENT STATUS BY PROPERTY FOR OWNER DASHBOARD
                    SELECT -- *,
                        property_owner_id
                        , rent_status
                        , COUNT(rent_status) AS num
                    FROM (
                        SELECT property_id, property_owner_id, po_owner_percent
                            , property_address, property_unit, property_city, property_state, property_zip
                            , pp_status.*
                            , IF (ISNULL(payment_status), "VACANT", payment_status) AS rent_status
                        FROM space.property_owner
                        LEFT JOIN space.properties ON property_uid = property_id
                        LEFT JOIN space.pp_status ON pur_property_id = property_id
                        WHERE property_owner_id = \'""" + owner_id + """\'
                            AND (purchase_type = "RENT" OR ISNULL(purchase_type))
                            AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                            AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                        GROUP BY property_id
                        ) AS rs
                    GROUP BY rent_status
                    """)

            # print("Query: ", leaseQuery)
            response["RentStatus"] = rentQuery

            return response