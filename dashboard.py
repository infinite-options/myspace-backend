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
                    -- MAINTENANCE STATUS BY OWNER
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
                    -- LEASE STATUS BY OWNER
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

class managerDashboard(Resource):
    def get(self,manager_id):
        print('in Manager Dashboard')
        response = {}

        # print("Owner UID: ", owner_id)

        with connect() as db:
            print("in Manager dashboard")
            maintenanceQuery = db.execute(""" 
                    -- MAINTENANCE STATUS BY MANAGER
                    SELECT -- * 
                        contract_business_id
                        , maintenance_request_status
                        , COUNT(maintenance_request_status) AS num
                    FROM space.maintenanceRequests
                    LEFT JOIN space.b_details ON maintenance_property_id = contract_property_id
                    WHERE contract_business_id = \'""" + manager_id + """\'
                    GROUP BY maintenance_request_status;
                    """)

            # print("Query: ", maintenanceQuery)
            response["MaintenanceStatus"] = maintenanceQuery

            leaseQuery = db.execute(""" 
                    -- LEASE STATUS BY MANAGER
                    SELECT b_details.contract_business_id
                        , leases.lease_end
                        , COUNT(lease_end) AS num
                    FROM space.leases
                    LEFT JOIN space.properties ON property_uid = lease_property_id
                    LEFT JOIN space.leaseFees ON lease_uid = fees_lease_id
                    LEFT JOIN space.leaseDocuments ON lease_uid = ld_lease_id
                    LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                    LEFT JOIN space.b_details ON contract_property_id = lease_property_id
                    WHERE lease_status = "ACTIVE"
                        AND contract_status = "ACTIVE"
                        AND fee_name = "RENT"
                        AND ld_type = "LEASE"
                        AND contract_business_id = \'""" + manager_id + """\'
                    GROUP BY MONTH(lease_end),
                            YEAR(lease_end); 
                    """)

            # print("lease Query: ", leaseQuery)
            response["LeaseStatus"] = leaseQuery

            rentQuery = db.execute(""" 
                    -- RENT STATUS BY PROPERTY FOR OWNER DASHBOARD
                    SELECT -- *,
                        contract_business_id
                        , rent_status
                        , COUNT(rent_status) AS num
                    FROM (
                        SELECT b.contract_property_id, contract_business_id
                            , pp_status.*
                            , IF (ISNULL(payment_status), "VACANT", payment_status) AS rent_status
                        FROM space.b_details AS b
                        LEFT JOIN space.properties ON property_uid = b.contract_property_id
                        LEFT JOIN space.pp_status ON pur_property_id = b.contract_property_id
                        WHERE contract_business_id = \'""" + manager_id + """\'
                            AND (purchase_type = "RENT" OR ISNULL(purchase_type))
                            AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                            AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                        GROUP BY b.contract_property_id
                        ) AS rs
                    GROUP BY rent_status
                                        """)

            # print("rent Query: ", rentQuery)
            response["RentStatus"] = rentQuery
            # print(response)
            return response




class tenantDashboard(Resource):
    def get(self, tenant_id):
        response = {}
        print('in Tenant Dashboard')

        with connect() as db:
            property = db.execute("""
                    -- TENENT PROPERTY INFO
                    -- NEED TO WORK OUT THE CASE WHAT A TENANT RENTS MULITPLE PROPERTIES
                    SELECT -- *,
                        SUM(pur_amount_due) AS balance 
                        , CAST(MIN(STR_TO_DATE(pur_due_date, '%Y-%m-%d')) AS CHAR) as earliest_due_date
                        , lt_lease_id, lt_tenant_id, lt_responsibility, lease_uid, lease_property_id
                        , lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date
                        -- , lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, linked_application_id-DNU, lease_docuSign
                        -- , lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_rent, lease_actual_rent
                        , property_uid
                        -- , property_available_to_rent, property_active_date
                        , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                        -- , property_type, property_num_beds, property_num_baths, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent
                        , property_images
                        -- , property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_owner_id-DNU, property_manager_id-DNU, property_appliances-DNU, property_appliance_id-DNU, property_utilities-DNU, property_utility_id-DNU
                    FROM space.lease_tenant
                    LEFT JOIN space.leases ON lease_uid = lt_lease_id
                    LEFT JOIN space.properties l ON lease_property_id = property_uid
                    LEFT JOIN space.purchases pur ON property_uid = pur_property_id
                    WHERE lt_tenant_id = \'""" + tenant_id + """\'
                    GROUP BY property_uid;
                    """)
            response["property"] = property


            maintenance = db.execute("""
                    -- TENENT MAINTENANCE TICKETS
                    SELECT mr.maintenance_images, mr.maintenance_title,
                        mr.maintenance_request_status, mr.maintenance_priority,
                        mr.maintenance_scheduled_date, mr.maintenance_scheduled_time
                    FROM space.maintenanceRequests mr
                        INNER JOIN space.properties p ON p.property_uid = mr.maintenance_property_id
                    WHERE p.property_uid = \'""" + property['result'][0]['property_uid'] + """\';
                    """)
            response["maintenanceRequests"] = maintenance


            announcements = db.execute("""
                -- TENENT ANNOUNCEMENTS
                SELECT * FROM announcements
                WHERE announcement_receiver LIKE '%""" + tenant_id + """%'
                AND (announcement_mode = 'Tenants' OR announcement_mode = 'Properties')
                AND announcement_properties LIKE  '%""" + property['result'][0]['property_uid'] + """%' """)
            response["announcements"] = announcements

            return response


class maintenanceDashboard(Resource):
    def get(self, business_id):
        print('in Maintenance Dashboard')
        response = {}

        # print("Maintenance UID: ", business_id)

        with connect() as db:
            # print("in maintenance dashboard")
            currentActivity = db.execute(""" 
                    -- CURRENT ACTIVITY
                    SELECT *,
                        COUNT(maintenance_status) AS num
                        ,SUM(quote_total_estimate) AS total_estimate
                    FROM (
                        SELECT quote_business_id, quote_status, maintenance_request_status, quote_total_estimate
                            , CASE
                                    WHEN quote_status = "SENT" OR quote_status = "WITHDRAWN" OR quote_status = "REFUSED" OR quote_status = "REJECTED"  THEN "SUBMITTED"
                                    WHEN quote_status = "ACCEPTED" OR quote_status = "SCHEDULE"   THEN "ACCEPTED"
                                    WHEN quote_status = "SCHEDULED" OR quote_status = "RESCHEDULE"   THEN "SCHEDULED"
                                    WHEN quote_status = "COMPLETED"   THEN "PAID"
                                    ELSE quote_status
                                END AS maintenance_status
                        FROM space.m_details
                        WHERE quote_business_id = \'""" + business_id + """\'
                        ) AS ms
                    GROUP BY maintenance_status;
                    """)

            # print("Query: ", maintenanceQuery)
            response["CurrentActivities"] = currentActivity

            workOrders = db.execute(""" 
                    -- WORK ORDERS
                    SELECT *
                    FROM (
                        SELECT * -- , quote_business_id, quote_status, maintenance_request_status, quote_total_estimate
                            , CASE
                                    WHEN quote_status = "SENT" OR quote_status = "WITHDRAWN" OR quote_status = "REFUSED" OR quote_status = "REJECTED"  THEN "SUBMITTED"
                                    WHEN quote_status = "ACCEPTED" OR quote_status = "SCHEDULE"   THEN "ACCEPTED"
                                    WHEN quote_status = "SCHEDULED" OR quote_status = "RESCHEDULE"   THEN "SCHEDULED"
                                    WHEN quote_status = "COMPLETED"   THEN "PAID"
                                    ELSE quote_status
                                END AS maintenance_status
                        FROM space.m_details
                        WHERE quote_business_id = \'""" + business_id + """\'
                            ) AS ms
                    ORDER BY maintenance_status;
                    """)

            # print("Query: ", leaseQuery)
            response["WorkOrders"] = workOrders

            return response