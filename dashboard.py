from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity


from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest

from queries import DashboardCashflowQuery, AnnouncementReceiverQuery, AnnouncementSenderQuery, RentDashboardQuery, DashboardProfitQuery

class Dashboard(Resource):
    def get(self, user_id):
        print('in Dashboard ', user_id)
        response = {}

        if user_id.startswith("600"):
            business_type = ""
            print('in Get Business Contacts')
            with connect() as db:
                print("in connect loop")
                query = db.execute(""" 
                    -- FIND ALL CURRENT BUSINESS CONTACTS
                        SELECT business_type
                        FROM businessProfileInfo
                        WHERE business_uid = \'""" + user_id + """\';
                        """)

            business_type = query['result'][0]['business_type']
            print(business_type)

            if business_type == "MAINTENANCE":
                with connect() as db:
                    print("in maintenance dashboard")

                    response["currentActivities"] = db.execute(""" 
                            -- CURRENT ACTIVITY FOR GRAPH AND TABLE
                            SELECT -- *,
                                quote_business_id
                                , maintenance_status
                                , COUNT(maintenance_status) AS num
                                , SUM(quote_total_estimate) AS total_estimate
                                , JSON_ARRAYAGG(JSON_OBJECT(
                                    'maintenance_request_uid', maintenance_request_uid,
                                    'maintenance_property_id', maintenance_property_id,
                                    'maintenance_title', maintenance_title,
                                    'maintenance_desc', maintenance_desc,
                                    'maintenance_images', maintenance_images,
                                    'maintenance_request_type', maintenance_request_type,
                                    'maintenance_priority', maintenance_priority,
                                    'maintenance_request_created_date', maintenance_request_created_date,
                                    'maintenance_scheduled_date', maintenance_scheduled_date,
                                    'maintenance_scheduled_time', maintenance_scheduled_time,
                                    'maintenance_frequency', maintenance_frequency,
                                    'property_address', property_address,
                                    'property_unit', property_unit,
                                    'property_city', property_city,
                                    'property_state', property_state,
                                    'property_zip', property_zip
                                    )) AS maintenance_request_info
                            FROM (
                                SELECT *
                                , CASE
                                        WHEN quote_status IN ("REQUESTED", "MORE INFO")                 THEN "REQUESTED"
                                        WHEN quote_status = "SENT" 	                                    THEN "SUBMITTED"
                                        WHEN quote_status IN ("ACCEPTED", "SCHEDULE")                   THEN "ACCEPTED"
                                        WHEN quote_status IN ("SCHEDULED" , "RESCHEDULE")               THEN "SCHEDULED"
                                        WHEN quote_status = "FINISHED"                                  THEN "FINISHED"
                                        WHEN quote_status = "COMPLETED"                                 THEN "PAID"   
                                        WHEN quote_status IN ("CANCELLED", "ARCHIVE", "NOT ACCEPTED","WITHDRAWN" ,"WITHDRAW", "REFUSED" ,"REJECTED")      THEN "ARCHIVE"
                                        ELSE quote_status
                                    END AS maintenance_status
                                FROM m_details
                                LEFT JOIN properties ON maintenance_property_id = property_uid
                                -- WHERE quote_business_id = '600-000012' 
                                WHERE quote_business_id = \'""" + user_id + """\'
                                ) AS ms
                            GROUP BY maintenance_status
                            ORDER BY maintenance_status;
                            """)


                    response["workOrders"] = db.execute(""" 
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
                                FROM m_details
                                WHERE quote_business_id = \'""" + user_id + """\'
                                    ) AS ms
                            ORDER BY maintenance_status;
                            """)


                    response["currentQuotes"] = db.execute(""" 
                            -- CURRENT QUOTES
                            SELECT m_details.*
                                , property_uid
                                , property_address, property_unit, property_city, property_state, property_zip
                                , business_uid
                                , business_name, business_phone_number, business_email 
                                , business_photo_url
                                , CASE
                                        WHEN quote_status = "REQUESTED"                                 THEN "REQUESTED"
                                        WHEN quote_status = "SENT" 	                                    THEN "SUBMITTED"
                                        WHEN quote_status IN ("ACCEPTED", "SCHEDULE")                   THEN "ACCEPTED"
                                        WHEN quote_status IN ("SCHEDULED" , "RESCHEDULE")               THEN "SCHEDULED"
                                        WHEN quote_status = "FINISHED"                                  THEN "FINISHED"
                                        WHEN quote_status = "COMPLETED"                                 THEN "PAID"   
                                        WHEN quote_status IN ("CANCELLED", "ARCHIVE", "NOT ACCEPTED","WITHDRAWN" ,"WITHDRAW", "REFUSED" ,"REJECTED")      THEN "ARCHIVE"
                                        ELSE quote_status
                                    END AS maintenance_status 
                            FROM m_details
                            LEFT JOIN properties ON property_uid = maintenance_property_id
                            LEFT JOIN b_details ON contract_property_id = maintenance_property_id
                            -- WHERE quote_business_id = '600-000012' 
                            WHERE quote_business_id = \'""" + user_id + """\'
                                AND quote_status IN ("ACCEPTED", "SCHEDULE", "SCHEDULED" , "RESCHEDULE", "FINISHED", "COMPLETED");
                            """)


                    return response
                
            elif business_type == "MANAGEMENT":
                with connect() as db:
                    print("in Manager dashboard")


                    # CASHFLOW
                    # print("Query: ", cashFlow)
                    response["cashflowStatus"] = DashboardCashflowQuery(user_id)

                    # print("Query: ", profit)
                    response["profitStatus"] = DashboardProfitQuery(user_id)


                    # HAPPINESS MATRIX - VACANCY AND CASHFLOW
                    response["happinessMatrix"] = db.execute("""
                            -- CASHFLOW ENDPOINT REWRITE TO INCLUDE BOTH VACANCY AND CASHFLOW
                            SELECT -- *,
                                -- contracts.contract_property_id
                                -- , property_owner.*
                                property_owner_id, owner_first_name, owner_last_name, owner_photo_url
                                -- , l.*
                                , COUNT(contract_property_id) AS total_properties
                                , COUNT(contract_property_id) - COUNT(lease_status) AS vacancy_num
                                , ROUND((COUNT(contract_property_id) - COUNT(lease_status))/COUNT(contract_property_id)*100, 0) AS vacancy_perc
                                , SUM(pur_amount_due) AS expected_cashflow
                                , if(SUM(total_paid) IS NULL, 0, SUM(total_paid)) AS actual_cashflow
                                , if(SUM(pur_amount_due) != 0, SUM(pur_amount_due) - if(SUM(total_paid) IS NULL, 0, SUM(total_paid)), 0)  AS delta_cashflow
                                , if(SUM(pur_amount_due) != 0, ROUND((SUM(pur_amount_due) - if(SUM(total_paid) IS NULL, 0, SUM(total_paid)) )/SUM(pur_amount_due)*100, 0), 0) AS percent_delta_cashflow
                                -- , pp.*
                            FROM contracts
                            LEFT JOIN property_owner ON contract_property_id = property_id
                            -- LEFT JOIN ownerProfileInfo ON property_owner_id = owner_uid
                            LEFT JOIN (SELECT lease_property_id
                                , lease_status 
                                FROM leases 
                                WHERE lease_status IN ('ACTIVE', 'ACTIVE M2M')) AS l ON contract_property_id = lease_property_id
                            LEFT JOIN (SELECT pur_property_id
                                , SUM(if(pur_receiver LIKE "110%", pur_amount_due, -pur_amount_due)) AS pur_amount_due
                                , SUM(if(pur_receiver LIKE "110%", total_paid, -total_paid)) AS total_paid
                                , JSON_ARRAYAGG(purchase_uid) AS purchase_ids
                            FROM pp_status
                            -- WHERE pur_payer LIKE "110%" OR pur_receiver LIKE "110%"
                            WHERE (pur_payer LIKE "110%" OR pur_receiver LIKE "110%")
                                AND CAST(cf_year AS UNSIGNED) <= YEAR(CURDATE()) AND CAST(cf_month_num AS UNSIGNED) <= MONTH(CURDATE())
                                -- 	AND STR_TO_DATE(
                                --         pur_due_date, 
                                --         CASE 
                                --             WHEN pur_due_date LIKE '% %' THEN '%m-%d-%Y %H-%i' -- For mm-dd-yyyy hh-mm format
                                --             ELSE '%m-%d-%Y' -- For mm-dd-yyyy format
                                --         END
                                --     ) <= CURDATE()
                            GROUP BY pur_property_id) AS pp ON pur_property_id = contract_property_id
                            LEFT JOIN ownerProfileInfo ON property_owner_id = owner_uid
                            -- WHERE contract_business_id = '600-000011'
                            WHERE contract_business_id = \'""" + user_id + """\'
                                AND contract_status = 'ACTIVE'
                            GROUP BY property_owner_id
                            """)


                    # MAINTENANCE     
                    response["maintenanceStatus"] = db.execute(""" 
                            -- MAINTENANCE STATUS BY MANAGER
                            SELECT contract_business_id
                                , maintenance_status
                                , COUNT(maintenance_status) AS num
                            FROM (-- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                                SELECT *
                                , CASE  
                                        WHEN quote_status = "COMPLETED"                                           					THEN "PAID" 
                                        WHEN maintenance_request_status IN ("NEW" ,"INFO")                                      	THEN "NEW REQUEST"
                                        WHEN quote_status = "SCHEDULED"                                           			        THEN "SCHEDULED"
                                        WHEN maintenance_request_status = 'CANCELLED' or quote_status = "FINISHED"       			THEN "COMPLETED"
                                        WHEN quote_status IN ("MORE INFO", "SENT" ,"REFUSED" , "REQUESTED", "REJECTED", "WITHDRAWN") THEN "QUOTES REQUESTED"
                                        WHEN quote_status IN ("ACCEPTED" , "SCHEDULE")                                   			THEN "QUOTES ACCEPTED"
                                        ELSE maintenance_request_status -- "NEW REQUEST"
                                    END AS maintenance_status

                                FROM maintenanceRequests
                                LEFT JOIN m_quote_rank AS quote_summary ON maintenance_request_uid = qmr_id

                                LEFT JOIN bills ON maintenance_request_uid = bill_maintenance_request_id
                                LEFT JOIN (
                                    SELECT quote_maintenance_request_id, 
                                        JSON_ARRAYAGG(JSON_OBJECT
                                            ('maintenance_quote_uid', maintenance_quote_uid,
                                            'quote_status', quote_status,
                                            'quote_pm_notes', quote_pm_notes,
                                            'quote_business_id', quote_business_id,
                                            'quote_services_expenses', quote_services_expenses,
                                            'quote_event_type', quote_event_type,
                                            'quote_event_duration', quote_event_duration,
                                            'quote_notes', quote_notes,
                                            'quote_created_date', quote_created_date,
                                            'quote_total_estimate', quote_total_estimate,
                                            'quote_maintenance_images', quote_maintenance_images,
                                            'quote_adjustment_date', quote_adjustment_date,
                                            'quote_earliest_available_date', quote_earliest_available_date,
                                            'quote_earliest_available_time', quote_earliest_available_time
                                            )) AS quote_info
                                    FROM maintenanceQuotes
                                    GROUP BY quote_maintenance_request_id) as qi ON quote_maintenance_request_id = maintenance_request_uid
                                LEFT JOIN pp_status ON bill_uid = pur_bill_id AND pur_receiver = maintenance_assigned_business
                                LEFT JOIN properties ON property_uid = maintenance_property_id
                                LEFT JOIN o_details ON maintenance_property_id = property_id
                                LEFT JOIN (SELECT * FROM b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                                LEFT JOIN (SELECT * FROM leases WHERE lease_status IN ('ACTIVE', 'ACTIVE M2M')) AS l ON maintenance_property_id = lease_property_id
                                LEFT JOIN t_details ON lt_lease_id = lease_uid

                                WHERE business_uid = \'""" + user_id + """\' -- AND (pur_receiver = \'""" + user_id + """\' OR ISNULL(pur_receiver))
                                -- WHERE business_uid = '600-000043' -- AND (pur_receiver = '600-000003' OR ISNULL(pur_receiver))
                                ORDER BY maintenance_request_created_date
                            ) AS ms
                            GROUP BY maintenance_status;
                            """)


                    # LEASES
                    response["leaseStatus"] = db.execute("""
                            -- LEASE EXPRIATION BY MONTH FOR OWNER AND PM
                            SELECT lease_end_month
                                , lease_end_num
                                , COUNT(lease_end_month) AS leases_expiring
                                , COUNT(move_out_date) AS move_out
                            FROM (
                                SELECT -- *
                                    l.*
                                    , property_owner_id
                                    , contract_business_id
                                FROM (
                                    -- FIND ALL ACTIVE/ENDED LEASES WITH OR WITHOUT A MOVE OUT DATE
                                    SELECT -- *,
                                    lease_uid, lease_property_id , lease_application_date, lease_start, lease_end, lease_status 
                                    -- , lease_assigned_contacts, lease_documents, lease_early_end_date
                                    , lease_renew_status, move_out_date 
                                    -- , lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_rent_available_topay-DNU, lease_consent, lease_actual_rent, lease_move_in_date, lease_end_notice_period, lease_days_remaining, lease_end_month
                                    , DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) AS lease_days_remaining
                                    , CASE
                                            WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) > DATEDIFF(LAST_DAY(DATE_ADD(NOW(), INTERVAL 11 MONTH)), NOW()) THEN 'FUTURE' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'FUTURE'
                                            WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) < 0 THEN 'M2M' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'M2M'
                                            ELSE MONTHNAME(STR_TO_DATE(LEFT(lease_end, 2), '%m'))
                                    END AS lease_end_month
                                    , CAST(LEFT(lease_end, 2) AS UNSIGNED) AS lease_end_num
                                    FROM leases 
                                    WHERE lease_status IN ('ACTIVE', 'ACTIVE M2M') OR lease_status = "ENDED"
                                    ) AS l
                                LEFT JOIN property_owner ON property_id = lease_property_id
                                LEFT JOIN (SELECT * FROM contracts WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
                                -- WHERE property_owner_id = "110-000003"
                                -- WHERE contract_business_id = "600-000003"
                                -- WHERE property_owner_id = \'""" + user_id + """\'
                                WHERE contract_business_id = \'""" + user_id + """\'
                                ) AS le
                            GROUP BY lease_end_month
                            ORDER BY lease_end_num ASC
                            """)


                    # RENT STATUS
                    response["rentStatus"] = RentDashboardQuery(user_id)
                    
                    
                    # PROPERTY LIST
                    # print("In Property List")
                    response["properties"] = db.execute("""
                            SELECT -- *,
                                property_uid, property_address, property_unit
                            FROM contracts
                            LEFT JOIN properties ON contract_property_id = property_uid
                            -- WHERE contract_business_id = '600-000003' AND
                            WHERE contract_business_id = \'""" + user_id + """\' AND
                                contract_status = 'ACTIVE';
                        """)


                    # NEW PM REQUESTS
                    # print("In New PM Requests")
                    response["newPMRequests"] = db.execute("""
                            -- NEW PROPERTIES FOR MANAGER
                            SELECT *, CASE WHEN announcements IS NULL THEN false ELSE true END AS announcements_boolean
                            FROM o_details
                            LEFT JOIN properties ON property_id = property_uid
                            LEFT JOIN b_details ON contract_property_id = property_uid
                            LEFT JOIN (
                            SELECT announcement_properties, JSON_ARRAYAGG(JSON_OBJECT
                            ('announcement_uid', announcement_uid,
                            'announcement_title', announcement_title,
                            'announcement_msg', announcement_msg,
                            'announcement_mode', announcement_mode,
                            'announcement_date', announcement_date,
                            'announcement_receiver', announcement_receiver
                            )) AS announcements
                            FROM announcements
                            GROUP BY announcement_properties) as t ON announcement_properties = property_uid
                            WHERE contract_business_id = \'""" + user_id + """\'  AND (contract_status = "NEW" OR contract_status = "SENT" OR contract_status = "REJECTED");
                        """)


                    return response
            
            else:
                print("No business_type Match")
                return("Business Type is not a valid option")

        elif user_id.startswith("110"):
            with connect() as db:
                # print("in owner dashboard")

                response["cashflowStatus"] = DashboardCashflowQuery(user_id)


                response["announcementsReceived"] = AnnouncementSenderQuery(user_id)
                
                
                response["announcementsSent"] = AnnouncementReceiverQuery(user_id)


                response["maintenanceStatus"] = db.execute(""" 
                        -- MAINTENANCE STATUS BY OWNER
                        SELECT -- * 
                            property_owner_id
                            , CASE
                                WHEN maintenance_request_status = 'NEW' THEN 'NEW REQUEST'
                                WHEN maintenance_request_status = 'INFO' THEN 'INFO REQUESTED'
                                ELSE maintenance_request_status
                            END AS maintenance_status
                            , COUNT(maintenance_request_status) AS num
                        FROM maintenanceRequests
                        LEFT JOIN o_details ON maintenance_property_id = property_id
                        WHERE owner_uid = \'""" + user_id + """\'
                        GROUP BY maintenance_request_status;
                        """)


                response["leaseStatus"] = db.execute(""" 
                            -- LEASE EXPRIATION BY MONTH FOR OWNER AND PM
                            SELECT lease_end_month
                                , lease_end_num
                                , COUNT(lease_end_month) AS leases_expiring
                                , COUNT(move_out_date) AS move_out
                            FROM (
                                SELECT -- *
                                    l.*
                                    , property_owner_id
                                    , contract_business_id
                                FROM (
                                    -- FIND ALL ACTIVE/ENDED LEASES WITH OR WITHOUT A MOVE OUT DATE
                                    SELECT -- *,
                                    lease_uid, lease_property_id , lease_application_date, lease_start, lease_end, lease_status 
                                    -- , lease_assigned_contacts, lease_documents, lease_early_end_date
                                    , lease_renew_status, move_out_date 
                                    -- , lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_rent_available_topay-DNU, lease_consent, lease_actual_rent, lease_move_in_date, lease_end_notice_period, lease_days_remaining, lease_end_month
                                    , DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) AS lease_days_remaining
                                    , CASE
                                            WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) > DATEDIFF(LAST_DAY(DATE_ADD(NOW(), INTERVAL 11 MONTH)), NOW()) THEN 'FUTURE' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'FUTURE'
                                            WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) < 0 THEN 'M2M' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'M2M'
                                            ELSE MONTHNAME(STR_TO_DATE(LEFT(lease_end, 2), '%m'))
                                    END AS lease_end_month
                                    , CAST(LEFT(lease_end, 2) AS UNSIGNED) AS lease_end_num
                                    FROM leases 
                                    WHERE lease_status IN ('ACTIVE', 'ACTIVE M2M') OR lease_status = "ENDED"
                                    ) AS l
                                LEFT JOIN property_owner ON property_id = lease_property_id
                                LEFT JOIN (SELECT * FROM contracts WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
                                -- WHERE property_owner_id = "110-000003"
                                -- WHERE contract_business_id = "600-000003"
                                WHERE property_owner_id = \'""" + user_id + """\'
                                -- WHERE contract_business_id = \'""" + user_id + """\'
                                ) AS le
                            GROUP BY lease_end_month
                            ORDER BY lease_end_num ASC
                        """)


                response["rentStatus"] = RentDashboardQuery(user_id)
                

                response["properties"] = db.execute(""" 
                            -- PROPERTY LIST FOR DASHBOARD
                            SELECT -- *,
                                property_owner_id,
                                property_uid,
                                property_address
                            FROM property_owner
                            LEFT JOIN properties ON property_id = property_uid
                            -- WHERE property_owner_id = "110-000003"
                            WHERE property_owner_id = \'""" + user_id + """\';
                        """)


                return response

        elif user_id.startswith("350"):
            with connect() as db:

                
                response["announcementsReceived"] = AnnouncementSenderQuery(user_id)
                
                
                response["announcementsSent"] = AnnouncementReceiverQuery(user_id)


                response["tenantTransactions"] = db.execute("""
                        -- All Cashflow Transactions
                        SELECT *
                        FROM pp_status
                        -- WHERE pur_receiver = '350-000003'  OR pur_payer = '350-000003'
                        WHERE pur_receiver = \'""" + user_id + """\' OR pur_payer = \'""" + user_id + """\'
                            -- AND cf_month = DATE_FORMAT(NOW(), '%M')
                            -- AND cf_year = DATE_FORMAT(NOW(), '%Y')
                    """)


                response["tenantPayments"] = db.execute("""
                        -- Tenant Payment Details
                        SELECT -- *,
                            payment_uid, pay_purchase_id, pay_amount, payment_notes, pay_charge_id, payment_type, payment_date, payment_verify, paid_by
                            , payment_intent
                            -- , payment_method, payment_date_cleared, payment_client_secret, purchase_uid, pur_timestamp
                            , pur_property_id, purchase_type, pur_description, pur_notes 
                            -- , pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due
                            , purchase_status, pur_status_value -- , pur_receiver, pur_initiator, pur_payer, pur_late_Fee, pur_perDay_late_fee, pur_due_by, pur_late_by, pur_group, pur_leaseFees_id
                        FROM payments
                        LEFT JOIN purchases ON pay_purchase_id = purchase_uid
                        -- WHERE paid_by = "350-000007";
                        WHERE paid_by = \'""" + user_id + """\' 
                    """)


                response["leaseDetails"] = db.execute(""" 
                        -- OWNER, PROPERTY MANAGER, TENANT LEASES
                        SELECT * 
                        FROM leases
                        LEFT JOIN (SELECT fees_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                                ('leaseFees_uid', leaseFees_uid,
                                'fee_name', fee_name,
                                'fee_type', fee_type,
                                'charge', charge,
                                'due_by', due_by,
                                'late_by', late_by,
                                'late_fee',late_fee,
                                'perDay_late_fee', perDay_late_fee,
                                'frequency', frequency,
                                'available_topay', available_topay,
                                'due_by_date', due_by_date
                                )) AS lease_fees
                                FROM leaseFees
                                GROUP BY fees_lease_id) AS lf ON fees_lease_id = lease_uid
                        LEFT JOIN properties ON property_uid = lease_property_id
                        LEFT JOIN o_details ON property_id = lease_property_id
                        LEFT JOIN (
                            SELECT lt_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                                ('tenant_uid', tenant_uid,
                                'lt_responsibility', lt_responsibility,
                                'tenant_first_name', tenant_first_name,
                                'tenant_last_name', tenant_last_name,
                                'tenant_phone_number', tenant_phone_number,
                                'tenant_email', tenant_email
                                )) AS tenants
                                FROM t_details 
                                GROUP BY lt_lease_id) as t ON lease_uid = lt_lease_id
                        LEFT JOIN (SELECT * FROM b_details WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
                        -- WHERE owner_uid = \'""" + user_id + """\'
                        -- WHERE owner_uid = "110-000003"
                        -- WHERE contract_business_id = \'""" + user_id + """\'
                        -- WHERE contract_business_id = "600-000003"
                        WHERE tenants LIKE '%""" + user_id + """%'
                        -- WHERE tenants LIKE "%350-000004%"
                        -- AND lease_status IN ('ACTIVE', 'ACTIVE M2M')
                        ; """)


                response["property"] = db.execute("""
                        -- TENENT PROPERTY INFO
                        -- NEED TO WORK OUT THE CASE WHAT A TENANT RENTS MULITPLE PROPERTIES
                        SELECT *
                        FROM lease_tenant
                        LEFT JOIN leases ON lease_uid = lt_lease_id
                        LEFT JOIN properties l ON lease_property_id = property_uid
                        LEFT JOIN (
                            SELECT 
                                pur_property_id
                                , SUM(pur_amount_due) AS pur_amount_due
                                , SUM(total_paid) AS total_paid
                                , SUM( amt_remaining) AS balance
                                , MIN(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS earliest_due_date
                                , MIN(pur_status_value) AS pur_status_value
                            FROM pp_status
                            WHERE purchase_status IN ('UNPAID', 'PARTIALLY PAID') AND LEFT(pur_payer,3) = '350'
                            GROUP BY pur_property_id
                            ) AS pp ON pur_property_id = property_uid AND lease_status IN ('ACTIVE', 'ACTIVE M2M')
                        -- WHERE lt_tenant_id = '350-000007'
                        WHERE lt_tenant_id = \'""" + user_id + """\'
                        ORDER BY lease_status
                        """)


                response["maintenanceRequests"] = db.execute("""
                        -- TENENT MAINTENANCE TICKETS
                        SELECT -- *,
                            lt_tenant_id
                            , lease_property_id -- , maintenance_request_status
                            , CASE
                                WHEN maintenance_request_status = 'NEW' THEN 'NEW REQUEST'
                                WHEN maintenance_request_status = 'INFO' THEN 'INFO REQUESTED'
                                ELSE maintenance_request_status
                            END AS maintenance_status
                            , COUNT(maintenance_request_status) AS num
                        FROM maintenanceRequests
                        LEFT JOIN (
                            SELECT * 
                            FROM leases 
                            WHERE lease_status IN ('ACTIVE', 'ACTIVE M2M')
                            ) AS l ON lease_property_id = maintenance_property_id
                        LEFT JOIN lease_tenant ON lease_uid = lt_lease_id
                        LEFT JOIN businessProfileInfo ON business_uid = maintenance_assigned_business
                        -- WHERE lt_tenant_id = '350-000007'
                        WHERE lt_tenant_id = \'""" + user_id + """\'
                        GROUP BY maintenance_request_status, lease_property_id;
                        """)


                response["maintenanceStatus"] = db.execute(""" 
                        -- MAINTENANCE STATUS BY TENANT
                        SELECT * 
                            , CASE
                                WHEN maintenance_request_status = 'NEW' THEN 'NEW REQUEST'
                                WHEN maintenance_request_status = 'INFO' THEN 'INFO REQUESTED'
                                ELSE maintenance_request_status
                            END AS maintenance_status
                            -- , COUNT(maintenance_request_status) AS num
                        FROM maintenanceRequests
                        LEFT JOIN (
                            SELECT * 
                            FROM leases 
                            WHERE lease_status IN ('ACTIVE', 'ACTIVE M2M')
                            ) AS l ON lease_property_id = maintenance_property_id
                        LEFT JOIN lease_tenant ON lease_uid = lt_lease_id
                        LEFT JOIN businessProfileInfo ON business_uid = maintenance_assigned_business
                        -- WHERE lt_tenant_id = '350-000005'
                        WHERE lt_tenant_id = \'""" + user_id + """\'
                        -- GROUP BY maintenance_request_status;
                        """)


                return response
