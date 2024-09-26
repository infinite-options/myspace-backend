from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity


from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest

from queries import DashboardCashflowQuery, AnnouncementReceiverQuery, AnnouncementSenderQuery

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
                        FROM space.businessProfileInfo
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
                                FROM space.m_details
                                LEFT JOIN space.properties ON maintenance_property_id = property_uid
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
                                FROM space.m_details
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
                            FROM space.m_details
                            LEFT JOIN space.properties ON property_uid = maintenance_property_id
                            LEFT JOIN space.b_details ON contract_property_id = maintenance_property_id
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


                    # HAPPINESS MATRIX - VACANCY AND CASHFLOW
                    response["happinessMatrix"] = db.execute("""
                                    -- CASHFLOW ENDPOINT REWRITE TO INCLUDE BOTH VACANCY AND CASHFLOW
                                    SELECT -- *,
                                        contracts.contract_property_id
                                        -- , property_owner.*
                                        , property_owner_id, owner_first_name, owner_last_name
                                        -- , l.*
                                        , COUNT(contract_property_id) AS total_properties
                                        , COUNT(contract_property_id) - COUNT(lease_status) AS vacancy_num
                                        , ROUND((COUNT(contract_property_id) - COUNT(lease_status))/COUNT(contract_property_id)*100, 0) AS vacancy_perc
                                        , SUM(pur_amount_due) AS expected_cashflow
                                        , if(SUM(total_paid) IS NULL, 0, SUM(total_paid)) AS actual_cashflow
                                        , if(SUM(pur_amount_due) != 0, SUM(pur_amount_due) - if(SUM(total_paid) IS NULL, 0, SUM(total_paid)), 0)  AS delta_cashflow
                                        , if(SUM(pur_amount_due) != 0, ROUND((SUM(pur_amount_due) - if(SUM(total_paid) IS NULL, 0, SUM(total_paid)) )/SUM(pur_amount_due)*100, 0), 0) AS percent_delta_cashflow
                                        -- , pp.*
                                    FROM space.contracts
                                    LEFT JOIN property_owner ON contract_property_id = property_id
                                    -- LEFT JOIN ownerProfileInfo ON property_owner_id = owner_uid
                                    LEFT JOIN (SELECT lease_property_id
                                        , lease_status 
                                        FROM space.leases 
                                        WHERE lease_status = "ACTIVE" OR lease_status = "ACTIVE M2M") AS l ON contract_property_id = lease_property_id
                                    LEFT JOIN (SELECT pur_property_id
                                        , SUM(if(pur_receiver LIKE "110%", pur_amount_due, -pur_amount_due)) AS pur_amount_due
                                        , SUM(if(pur_receiver LIKE "110%", total_paid, -total_paid)) AS total_paid
                                        , JSON_ARRAYAGG(purchase_uid) AS purchase_ids
                                    FROM space.pp_status
                                    WHERE pur_payer LIKE "110%" OR pur_receiver LIKE "110%"
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
                            FROM (
                                SELECT *
                                    -- quote_business_id, quote_status, maintenance_request_status, quote_total_estimate
                                    , CASE  
										WHEN quote_status = "COMPLETED"                                           			THEN "PAID" 
                                        WHEN maintenance_request_status IN ("NEW" ,"INFO")                                  THEN "NEW REQUEST"
                                        WHEN maintenance_request_status = "SCHEDULED"                                       THEN "SCHEDULED"
                                        WHEN maintenance_request_status = 'CANCELLED' or quote_status = "FINISHED"          THEN "COMPLETED"
                                        WHEN quote_status IN ("SENT" ,"REFUSED" , "REQUESTED", "REJECTED", "WITHDRAWN") 	THEN "QUOTES REQUESTED"
                                        WHEN quote_status IN ("ACCEPTED" , "SCHEDULE")                                  	THEN "QUOTES ACCEPTED"
                                        ELSE quote_status
                                    END AS maintenance_status
                                FROM (
                                    SELECT * 
                                    FROM space.maintenanceRequests
                                    LEFT JOIN (
                                        SELECT *,
                                            CASE
                                                WHEN max_quote_rank = "10" THEN "REQUESTED"
                                                WHEN max_quote_rank = "11" THEN "REFUSED"
                                                WHEN max_quote_rank = "20" THEN "SENT"
                                                WHEN max_quote_rank = "21" THEN "REJECTED"
                                                WHEN max_quote_rank = "22" THEN "WITHDRAWN"
                                                WHEN max_quote_rank = "30" THEN "ACCEPTED"
                                                WHEN max_quote_rank = "40" THEN "SCHEDULE"
                                                WHEN max_quote_rank = "50" THEN "SCHEDULED"
                                                WHEN max_quote_rank = "60" THEN "RESCHEDULED"
                                                WHEN max_quote_rank = "70" THEN "FINISHED"
                                                WHEN max_quote_rank = "80" THEN "COMPLETED"
                                                ELSE "0"
                                            END AS quote_status
                                        FROM 
                                        (
                                            SELECT -- maintenance_quote_uid, 
                                                quote_maintenance_request_id AS qmr_id
                                                -- , quote_status
                                                , MAX(quote_rank) AS max_quote_rank
                                            FROM (
                                                SELECT -- *,
                                                    maintenance_quote_uid, quote_maintenance_request_id, quote_status,
                                                    -- , quote_pm_notes, quote_business_id, quote_services_expenses,quote_earliest_available_date,quote_earliest_available_time , quote_event_type, quote_event_duration, quote_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                                                CASE
                                                    WHEN quote_status = "REQUESTED" THEN "10"
                                                    WHEN quote_status = "REFUSED" THEN "11"
                                                    WHEN quote_status = "SENT" THEN "20"
                                                    WHEN quote_status = "REJECTED" THEN "21"
                                                    WHEN quote_status = "WITHDRAWN"  THEN "22"
                                                    WHEN quote_status = "ACCEPTED" THEN "30"
                                                    WHEN quote_status = "SCHEDULE" THEN "40"
                                                    WHEN quote_status = "SCHEDULED" THEN "50"
                                                    WHEN quote_status = "RESCHEDULED" THEN "60"
                                                    WHEN quote_status = "FINISHED" THEN "70"
                                                    WHEN quote_status = "COMPLETED" THEN "80"     
                                                    ELSE 0
                                                END AS quote_rank
                                                FROM space.maintenanceQuotes
                                                ) AS qr
                                            GROUP BY quote_maintenance_request_id
                                            ) AS qr_quoterank
                                    ) AS quote_summary ON maintenance_request_uid = qmr_id
                                ) AS quotes
                            LEFT JOIN ( SELECT * FROM space.contracts WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                                WHERE contract_business_id = \'""" + user_id + """\'
                                -- WHERE contract_business_id = "600-000032"
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
                                            WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) < 0 THEN 'MTM' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'MTM'
                                            ELSE MONTHNAME(STR_TO_DATE(LEFT(lease_end, 2), '%m'))
                                    END AS lease_end_month
                                    , CAST(LEFT(lease_end, 2) AS UNSIGNED) AS lease_end_num
                                    FROM space.leases 
                                    WHERE (lease_status = "ACTIVE" OR lease_status = "ENDED")
                                    ) AS l
                                LEFT JOIN space.property_owner ON property_id = lease_property_id
                                LEFT JOIN (SELECT * FROM space.contracts WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
                                -- WHERE property_owner_id = "110-000003"
                                -- WHERE contract_business_id = "600-000003"
                                -- WHERE property_owner_id = \'""" + user_id + """\'
                                WHERE contract_business_id = \'""" + user_id + """\'
                                ) AS le
                            GROUP BY lease_end_month
                            ORDER BY lease_end_num ASC
                            """)


                    # RENT STATUS
                    response["rentStatus"] = db.execute(""" 
                            -- PROPERTY RENT STATUS FOR DASHBOARD
                            SELECT 
                                rent_status
                                , COUNT(rent_status) AS num
                            FROM (
                                SELECT -- *,
                                    property_uid, owner_uid, contract_uid, contract_status, business_uid, lease_uid, lease_start, lease_end, lease_status, tenant_uid -- , rent_status
                                    , pur_property_id, purchase_type, pur_due_date, pur_amount_due
                                    , if(ISNULL(pur_status_value), "0", pur_status_value) AS pur_status_value
                                    , if(ISNULL(purchase_status), "UNPAID", purchase_status) AS purchase_status
                                    , pur_description, cf_month, cf_year
                                    , CASE
                                        WHEN ISNULL(contract_uid) THEN "NO MANAGER"
                                        WHEN ISNULL(lease_status) THEN "VACANT"
                                        WHEN ISNULL(purchase_status) THEN "UNPAID"
                                        ELSE purchase_status
                                        END AS rent_status 
                                FROM (
                                    -- Find number of properties
                                    SELECT -- *,
                                            property_uid -- , property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_utilities
                                            -- , po_owner_percent, po_start_date, po_end_date
                                            , owner_uid -- , owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
                                            , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name
                                            , contract_status -- , contract_early_end_date
                                            , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                                            , lease_uid, lease_start, lease_end
                                            , lease_status -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
                                            , tenant_uid -- , tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                                            -- , if(ISNULL(lease_status), "VACANT", lease_status) AS rent_status 
                                    FROM space.p_details
                                    -- WHERE business_uid = "600-000003"
                                    -- WHERE owner_uid = "110-000003"
                                    -- WHERE owner_uid = \'""" + user_id + """\'
                                    WHERE business_uid = \'""" + user_id + """\'
                                    -- WHERE tenant_uid = \'""" + user_id + """\'  
                                    ) AS p
                                -- Link to rent status
                                LEFT JOIN (
                                    SELECT  -- *,
                                        pur_property_id
                                        , purchase_type
                                        , pur_due_date
                                        , SUM(pur_amount_due) AS pur_amount_due
                                        , MIN(pur_status_value) AS pur_status_value
                                        , purchase_status
                                        , pur_description
                                        , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_month
                                        , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_year
                                    FROM space.purchases
                                    WHERE purchase_type = "Rent"
                                        AND LEFT(pur_payer, 3) = '350'
                                        AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = MONTH(CURRENT_DATE)
                                        AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = YEAR(CURRENT_DATE)
                                    GROUP BY pur_due_date, pur_property_id, purchase_type
                                    ) AS pp
                                    ON property_uid = pur_property_id
                                ) AS rs
                            GROUP BY rent_status;
                            """)

                    
                    # PROPERTY LIST
                    # print("In Property List")
                    response["properties"] = db.execute("""
                            SELECT -- *,
                                property_uid, property_address, property_unit
                            FROM space.contracts
                            LEFT JOIN space.properties ON contract_property_id = property_uid
                            -- WHERE contract_business_id = '600-000003' AND
                            WHERE contract_business_id = \'""" + user_id + """\' AND
                                contract_status = 'ACTIVE';
                        """)


                    # NEW PM REQUESTS
                    # print("In New PM Requests")
                    response["newPMRequests"] = db.execute("""
                            -- NEW PROPERTIES FOR MANAGER
                            SELECT *, CASE WHEN announcements IS NULL THEN false ELSE true END AS announcements_boolean
                            FROM space.o_details
                            LEFT JOIN space.properties ON property_id = property_uid
                            LEFT JOIN space.b_details ON contract_property_id = property_uid
                            LEFT JOIN (
                            SELECT announcement_properties, JSON_ARRAYAGG(JSON_OBJECT
                            ('announcement_uid', announcement_uid,
                            'announcement_title', announcement_title,
                            'announcement_msg', announcement_msg,
                            'announcement_mode', announcement_mode,
                            'announcement_date', announcement_date,
                            'announcement_receiver', announcement_receiver
                            )) AS announcements
                            FROM space.announcements
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
                        FROM space.maintenanceRequests
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
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
                                            WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) < 0 THEN 'MTM' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'MTM'
                                            ELSE MONTHNAME(STR_TO_DATE(LEFT(lease_end, 2), '%m'))
                                    END AS lease_end_month
                                    , CAST(LEFT(lease_end, 2) AS UNSIGNED) AS lease_end_num
                                    FROM space.leases 
                                    WHERE (lease_status = "ACTIVE" OR lease_status = "ENDED")
                                    ) AS l
                                LEFT JOIN space.property_owner ON property_id = lease_property_id
                                LEFT JOIN (SELECT * FROM space.contracts WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
                                -- WHERE property_owner_id = "110-000003"
                                -- WHERE contract_business_id = "600-000003"
                                WHERE property_owner_id = \'""" + user_id + """\'
                                -- WHERE contract_business_id = \'""" + user_id + """\'
                                ) AS le
                            GROUP BY lease_end_month
                            ORDER BY lease_end_num ASC
                        """)


                response["rentStatus"] = db.execute(""" 
                            -- PROPERTY RENT STATUS FOR DASHBOARD
                            SELECT 
                                rent_status
                                , COUNT(rent_status) AS num
                            FROM (
                                SELECT -- *,
                                    property_uid, owner_uid, contract_uid, contract_status, business_uid, lease_uid, lease_start, lease_end, lease_status, tenant_uid -- , rent_status
                                    , pur_property_id, purchase_type, pur_due_date, pur_amount_due
                                    , if(ISNULL(pur_status_value), "0", pur_status_value) AS pur_status_value
                                    , if(ISNULL(purchase_status), "UNPAID", purchase_status) AS purchase_status
                                    , pur_description, cf_month, cf_year
                                    , CASE
                                        WHEN ISNULL(contract_uid) THEN "NO MANAGER"
                                        WHEN ISNULL(lease_status) THEN "VACANT"
                                        WHEN ISNULL(purchase_status) THEN "UNPAID"
                                        ELSE purchase_status
                                        END AS rent_status 
                                FROM (
                                    -- Find number of properties
                                    SELECT -- *,
                                            property_uid -- , property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_utilities
                                            -- , po_owner_percent, po_start_date, po_end_date
                                            , owner_uid -- , owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
                                            , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name
                                            , contract_status -- , contract_early_end_date
                                            , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                                            , lease_uid, lease_start, lease_end
                                            , lease_status -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
                                            , tenant_uid -- , tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                                            -- , if(ISNULL(lease_status), "VACANT", lease_status) AS rent_status 
                                    FROM space.p_details
                                    -- WHERE business_uid = "600-000003"
                                    -- WHERE owner_uid = "110-000003"
                                    WHERE owner_uid = \'""" + user_id + """\'
                                    -- WHERE business_uid = \'""" + user_id + """\'
                                    -- WHERE tenant_uid = \'""" + user_id + """\'  
                                    ) AS p
                                -- Link to rent status
                                LEFT JOIN (
                                    SELECT  -- *,
                                        pur_property_id
                                        , purchase_type
                                        , pur_due_date
                                        , SUM(pur_amount_due) AS pur_amount_due
                                        , MIN(pur_status_value) AS pur_status_value
                                        , purchase_status
                                        , pur_description
                                        , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_month
                                        , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_year
                                    FROM space.purchases
                                    WHERE purchase_type = "Rent"
                                        AND LEFT(pur_payer, 3) = '350'
                                        AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = MONTH(CURRENT_DATE)
                                        AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = YEAR(CURRENT_DATE)
                                    GROUP BY pur_due_date, pur_property_id, purchase_type
                                    ) AS pp
                                    ON property_uid = pur_property_id
                                ) AS rs
                            GROUP BY rent_status;
                        """)


                response["properties"] = db.execute(""" 
                            -- PROPERTY LIST FOR DASHBOARD
                            SELECT -- *,
                                property_owner_id,
                                property_uid,
                                property_address
                            FROM space.property_owner
                            LEFT JOIN space.properties ON property_id = property_uid
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
                        FROM space.pp_status
                        -- WHERE pur_receiver = '350-000003'  OR pur_payer = '350-000003'
                        WHERE pur_receiver = \'""" + user_id + """\' OR pur_payer = \'""" + user_id + """\'
                            -- AND cf_month = DATE_FORMAT(NOW(), '%M')
                            -- AND cf_year = DATE_FORMAT(NOW(), '%Y')
                    """)


                response["leaseDetails"] = db.execute(""" 
                        -- OWNER, PROPERTY MANAGER, TENANT LEASES
                        SELECT * 
                        FROM space.leases
                        LEFT JOIN space.properties ON property_uid = lease_property_id
                        LEFT JOIN space.o_details ON property_id = lease_property_id
                        LEFT JOIN (
                            SELECT lt_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                                ('tenant_uid', tenant_uid,
                                'lt_responsibility', lt_responsibility,
                                'tenant_first_name', tenant_first_name,
                                'tenant_last_name', tenant_last_name,
                                'tenant_phone_number', tenant_phone_number,
                                'tenant_email', tenant_email
                                )) AS tenants
                                FROM space.t_details 
                                GROUP BY lt_lease_id) as t ON lease_uid = lt_lease_id
                        LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
                        -- WHERE owner_uid = \'""" + user_id + """\'
                        -- WHERE owner_uid = "110-000003"
                        -- WHERE contract_business_id = \'""" + user_id + """\'
                        -- WHERE contract_business_id = "600-000003"
                        WHERE tenants LIKE '%""" + user_id + """%'
                        -- WHERE tenants LIKE "%350-000007%"
                        ; """)


                response["property"] = db.execute("""
                        -- TENENT PROPERTY INFO
                        -- NEED TO WORK OUT THE CASE WHAT A TENANT RENTS MULITPLE PROPERTIES
                        SELECT -- *
                            -- SUM( CASE WHEN purchase_status = 'UNPAID' AND pur_payer = '350-000002' THEN pur_amount_due ELSE 0 END) as balance
                            SUM( CASE WHEN purchase_status = 'UNPAID' AND pur_payer = \'""" + user_id + """\'  THEN pur_amount_due ELSE 0 END) as balance
                            -- ,CAST(MIN(STR_TO_DATE(CASE WHEN purchase_status = 'UNPAID' AND pur_payer = '350-000002' THEN pur_due_date ELSE lease_end END, '%m-%d-%Y')) AS CHAR) AS earliest_due_date
                            ,CAST(MIN(STR_TO_DATE(CASE WHEN purchase_status = 'UNPAID' AND pur_payer = \'""" + user_id + """\' THEN pur_due_date ELSE lease_end END, '%m-%d-%Y')) AS CHAR) AS earliest_due_date
                            , lt_lease_id, lt_tenant_id, lt_responsibility, lease_uid, lease_property_id
                            , lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date
                            -- , lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, linked_application_id-DNU, lease_docuSign
                            -- , lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lease_actual_rent
                            , property_uid
                            -- , property_available_to_rent, property_active_date
                            , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude
                            , property_type, property_num_beds, property_num_baths, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent
                            , property_images, property_favorite_image
                            -- , property_taxes, property_mortgages, property_insurance, property_featured
                            , property_description, property_notes
                            , MAX(CASE WHEN lease_status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_priority
                        FROM space.lease_tenant
                        LEFT JOIN space.leases ON lease_uid = lt_lease_id
                        LEFT JOIN space.properties l ON lease_property_id = property_uid
                        -- LEFT JOIN space.purchases pur ON property_uid = pur_property_id
                        LEFT JOIN space.pp_status pur ON property_uid = pur_property_id
                        -- WHERE pur_payer = '350-000002' 
                        -- WHERE lt_tenant_id = '350-000002' 
                        WHERE lt_tenant_id = \'""" + user_id + """\' 
                            AND property_uid!=""
                        GROUP BY lease_uid
                        ORDER BY lease_status;
                        """)
                   
                    
                response["maintenanceRequests"] = db.execute("""
                        -- TENENT MAINTENANCE TICKETS
                        SELECT  property_uid, owner_first_name, owner_last_name, owner_phone_number, owner_email,
                            p.business_name, p.business_phone_number, p.business_email,
                            mr.*
                        FROM space.maintenanceRequests mr
                            LEFT JOIN space.p_details p ON property_uid = mr.maintenance_property_id
                            LEFT JOIN space.businessProfileInfo b ON b.business_uid = p.business_uid
                        -- WHERE tenant_uid = '350-000162';
                        WHERE tenant_uid = \'""" + user_id + """\';
                        """)


                response["maintenanceRequestsNew"] = db.execute("""
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
                        FROM space.maintenanceRequests
                        LEFT JOIN (
                            SELECT * 
                            FROM space.leases 
                            WHERE lease_status = 'ACTIVE'
                            ) AS l ON lease_property_id = maintenance_property_id
                        LEFT JOIN space.lease_tenant ON lease_uid = lt_lease_id
                        LEFT JOIN space.businessProfileInfo ON business_uid = maintenance_assigned_business
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
                        FROM space.maintenanceRequests
                        LEFT JOIN (
                            SELECT * 
                            FROM space.leases 
                            WHERE lease_status = 'ACTIVE'
                            ) AS l ON lease_property_id = maintenance_property_id
                        LEFT JOIN space.lease_tenant ON lease_uid = lt_lease_id
                        LEFT JOIN space.businessProfileInfo ON business_uid = maintenance_assigned_business
                        -- WHERE lt_tenant_id = '350-000005'
                        WHERE lt_tenant_id = \'""" + user_id + """\'
                        -- GROUP BY maintenance_request_status;
                        """)







                return response
