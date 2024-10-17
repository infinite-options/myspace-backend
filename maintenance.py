
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

import boto3
from data_pm import connect, uploadImage, deleteImage, s3, processImage, processDocument
from datetime import date, timedelta, datetime
from calendar import monthrange
import json
import ast
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest
from queries import MaintenanceDetails


class MaintenanceRequests(Resource):
    def get(self, uid):
        print('in Maintenance Request', uid)
        response = {}

        print("UID: ", uid)

        if uid[:3] == '110':
            print("In Owner ID")

            with connect() as db:
                print("in connect loop")
                maintenanceRequests = db.execute(""" 
                        -- MAINTENANCE REQUEST BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT   *, -- bill_property_id,  maintenance_property_id,
                        CASE 
							WHEN maintenance_request_status = "NEW" 						THEN "NEW REQUEST"
                            WHEN maintenance_request_status = "INFO"						THEN "INFO REQUESTED"
                            WHEN maintenance_request_status = "MORE-INFO"						THEN "PROCESSING"
                            WHEN maintenance_request_status IN ('PROCESSING', 'SCHEDULED', 'CANCELLED', 'COMPLETED') THEN maintenance_request_status
						END AS maintenance_status
                            -- maintenance_request_status, quote_status
                            -- maintenanceRequests.*
                            -- Properties
                            --  property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_images
                        -- 	, property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                        -- 	, business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, contract_status
                        -- 	, lease_uid, lease_status, lease_assigned_contacts,  tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                        FROM space.maintenanceRequests
                        LEFT JOIN space.properties ON property_uid = maintenance_property_id
                        -- LEFT JOIN space.bills ON bill_maintenance_quote_id = maintenance_quote_uid
                        -- LEFT JOIN space.pp_status ON pur_bill_id = bill_uid
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS b ON maintenance_property_id = contract_property_id
                        LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
                        LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                        -- WHERE owner_uid = "110-000095"
                        WHERE owner_uid = \'""" + uid + """\'
                        -- WHERE business_uid = \'""" + uid + """\'
                        -- WHERE tenant_uid = \'""" + uid + """\'
                        -- WHERE quote_business_id = \'""" + uid + """\'
                        -- WHERE maintenance_property_id = \'""" + uid + """\'
                        ORDER BY maintenance_request_created_date;
                        """)

            if maintenanceRequests.get('code') == 200:
                status_colors = {
                    'NEW REQUEST': '#A52A2A',
                    'INFO REQUESTED': '#C06A6A',
                    # 'QUOTES REQUESTED': '#D29494',  # Deprecated as per new figma
                    'PROCESSING': '#3D5CAC',
                    'SCHEDULED': '#3D5CAC',
                    'CANCELLED': '#FFFFFF',
                    'COMPLETED': '#3D5CAC',
                }
                mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in
                                status_colors.items()}
                
                # print("Mapped Items: ", mapped_items)

                response = maintenanceRequests
                # print("\nQuery response: ", response)

                for record in response['result']:
                    # print("\nRecord: ", record)
                    status = record.get('maintenance_status')
                    # print("\nStatus: ", status)
                    mapped_items[status]['maintenance_items'].append(record)
                    # print("\nMapped Item: ", mapped_items)

                response['result'] = mapped_items
                return response

            response["maintenanceRequests"] = maintenanceRequests
            return response

        elif uid[:3] == '350':
            print("In Tenant ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceRequests = db.execute(""" 
                        -- MAINTENANCE REQUEST BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT   *, -- bill_property_id,  maintenance_property_id,
                        CASE 
							WHEN maintenance_request_status = "NEW" 						THEN "NEW REQUEST"
                            WHEN maintenance_request_status = "INFO"						THEN "INFO REQUESTED"
                            WHEN maintenance_request_status IN ('PROCESSING', 'SCHEDULED', 'CANCELLED', 'COMPLETED') THEN maintenance_request_status
						END AS maintenance_status
                            -- maintenance_request_status, quote_status
                            -- maintenanceRequests.*
                            -- Properties
                            --  property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_images
                        -- 	, property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                        -- 	, business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, contract_status
                        -- 	, lease_uid, lease_status, lease_assigned_contacts,  tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                        FROM space.maintenanceRequests
                        LEFT JOIN space.properties ON property_uid = maintenance_property_id
                        -- LEFT JOIN space.bills ON bill_maintenance_quote_id = maintenance_quote_uid
                        -- LEFT JOIN space.pp_status ON pur_bill_id = bill_uid
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS b ON maintenance_property_id = contract_property_id
                        LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
                        LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                        -- WHERE owner_uid = "110-000095"
                        -- WHERE owner_uid = \'""" + uid + """\'
                        -- WHERE business_uid = \'""" + uid + """\'
                        WHERE tenant_uid = \'""" + uid + """\'
                        -- WHERE quote_business_id = \'""" + uid + """\'
                        -- WHERE maintenance_property_id = \'""" + uid + """\'
                        ORDER BY maintenance_request_created_date;
                        """)

            if maintenanceRequests.get('code') == 200:
                status_colors = {
                    'NEW REQUEST': '#A52A2A',
                    'INFO REQUESTED': '#C06A6A',
                    # 'QUOTES REQUESTED': '#D29494',  # Deprecated as per new figma
                    'PROCESSING': '#3D5CAC',
                    'SCHEDULED': '#3D5CAC',
                    'CANCELLED': '#FFFFFF',
                    'COMPLETED': '#3D5CAC',
                }
                mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in
                                status_colors.items()}

                response = maintenanceRequests

                for record in response['result']:
                    status = record.get('maintenance_status')
                    mapped_items[status]['maintenance_items'].append(record)

                response['result'] = mapped_items
                return response

            response["maintenanceRequests"] = maintenanceRequests
            return response

        elif uid[:3] == '200':
            print("In Property ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceRequests = db.execute(""" 
                        -- MAINTENANCE REQUEST BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT   *, -- bill_property_id,  maintenance_property_id,
                        CASE 
							WHEN maintenance_request_status = "NEW" 						THEN "NEW REQUEST"
                            WHEN maintenance_request_status = "INFO"						THEN "INFO REQUESTED"
                            WHEN maintenance_request_status IN ('PROCESSING', 'SCHEDULED', 'CANCELLED', 'COMPLETED') THEN maintenance_request_status
						END AS maintenance_status
                            -- maintenance_request_status, quote_status
                            -- maintenanceRequests.*
                            -- Properties
                            --  property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_images
                        -- 	, property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                        -- 	, business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, contract_status
                        -- 	, lease_uid, lease_status, lease_assigned_contacts,  tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                        FROM space.maintenanceRequests
                        LEFT JOIN space.properties ON property_uid = maintenance_property_id
                        -- LEFT JOIN space.bills ON bill_maintenance_quote_id = maintenance_quote_uid
                        -- LEFT JOIN space.pp_status ON pur_bill_id = bill_uid
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS b ON maintenance_property_id = contract_property_id
                        LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
                        LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                        -- WHERE owner_uid = "110-000095"
                        -- WHERE owner_uid = \'""" + uid + """\'
                        -- WHERE business_uid = \'""" + uid + """\'
                        -- WHERE tenant_uid = \'""" + uid + """\'
                        -- WHERE quote_business_id = \'""" + uid + """\'
                        WHERE maintenance_property_id = \'""" + uid + """\'
                        ORDER BY maintenance_request_created_date;
                        """)

            if maintenanceRequests.get('code') == 200:
                status_colors = {
                    'NEW REQUEST': '#A52A2A',
                    'INFO REQUESTED': '#C06A6A',
                    # 'QUOTES REQUESTED': '#D29494',  # Deprecated as per new figma
                    'PROCESSING': '#3D5CAC',
                    'SCHEDULED': '#3D5CAC',
                    'CANCELLED': '#FFFFFF',
                    'COMPLETED': '#3D5CAC',
                }
                mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in
                                status_colors.items()}

                response = maintenanceRequests

                for record in response['result']:
                    status = record.get('maintenance_status')
                    mapped_items[status]['maintenance_items'].append(record)

                response['result'] = mapped_items
                return response

            response["maintenanceRequests"] = maintenanceRequests
            return response

        else:
            print("UID Not found")
            response["MaintenanceStatus"] = "UID Not Found"
            return response

    def post(self):
        print("In Maintenace Requests POST")
        response = {}
        newMaintenanceReq = {}
        payload = request.form.to_dict()
        print("Maintenance Request Add Payload: ", payload)

        if payload.get('maintenance_request_uid'):
            print("maintenance_request_uid found.  Please call PUT endpoint")
            raise BadRequest("Request failed, UID found in payload.")
        
        with connect() as db:
            newMaintenanceReqUID = db.call('new_request_uid')['result'][0]['new_id']
            key = {'maintenance_request_uid': newMaintenanceReqUID}
            print("Maintenance Req Key: ", key)
           
           # --------------- PROCESS IMAGES ------------------

            processImage(key, payload)
            print("Payload after function: ", payload)
            
            # --------------- PROCESS IMAGES ------------------


            # Add Maintenance Request Info
            payload['maintenance_request_status'] = 'NEW'  
            payload['maintenance_images'] = '[]' if payload.get('maintenance_images') in {None, '', 'null'} else payload.get('maintenance_images', '[]')
            print("Add Maintenance Req Payload: ", payload)  


            payload["maintenance_request_uid"] = newMaintenanceReqUID  
            response['Add Maintenance Req'] = db.insert('maintenanceRequests', payload)
            response['maintenance_request_uid'] = newMaintenanceReqUID 
            response['Maintenance Req Images Added'] = payload.get('maintenance_images', "None")
            print("\nNew Maintenance Request Added")

        return response

    def put(self):
        print('in Maintenance Requests PUT')
        response = {}
        # response1 = {}
        payload = request.form.to_dict()
        print("Maintenance Request Update Payload: ", payload)

        # Verify uid has been included in the data
        if payload.get('maintenance_request_uid') in {None, '', 'null'}:
            print("No maintenance_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        # maintenance_request_uid = payload.get('maintenance_request_uid')
        key = {'maintenance_request_uid': payload.pop('maintenance_request_uid')}
        print("Maintenance Request Key: ", key)


        # --------------- PROCESS IMAGES ------------------

        processImage(key, payload)
        print("Payload after function: ", payload)
        
        # --------------- PROCESS IMAGES ------------------



        # Write to Database
        with connect() as db:
            print("Checking Inputs: ", key, payload)
            response["request_update"] = db.update('maintenanceRequests', key, payload)
            print("Response:" , response)

        return response


class MaintenanceQuotes(Resource):
    def get(self, uid):
        response = {}

        print("UID: ", uid)

        if uid[:3] == '110':
            print("In Owner ID")
            with connect() as db:
                ownerQuery = db.execute(""" 
                                -- MAINTENANCE QUOTES
                                SELECT -- *, 
                                maintenance_quote_uid, quote_maintenance_request_id, quote_status, quote_pm_notes, quote_business_id, quote_services_expenses, quote_earliest_available_date, quote_earliest_available_time, quote_documents, quote_event_type, quote_event_duration, quote_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                                , maintenance_request_uid, maintenance_property_id, maintenance_request_status, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date, maintenance_callback_number, maintenance_estimated_cost, maintenance_pm_notes
                                , property_uid
                                -- , property_available_to_rent, property_active_date, property_listed_date
                                , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type
                                -- , property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_id, property_owner_id, po_owner_percent
                                , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email -- , owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
                                , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                , pm_business_info.business_uid as pm_business_uid, pm_business_info.business_type as pm_business_type, pm_business_info.business_name as pm_business_name, pm_business_info.business_phone_number as pm_business_phone_number, pm_business_info.business_email as pm_business_email, pm_business_info.business_address as pm_business_address, pm_business_info.business_unit as pm_business_unit, pm_business_info.business_city as pm_business_city, pm_business_info.business_state as pm_business_state, pm_business_info.business_zip as pm_business_zip, pm_business_info.business_photo_url as pm_business_photo_url
                                , maintenance_info.business_uid as maint_business_uid, maintenance_info.business_type as maint_business_type, maintenance_info.business_name as maint_business_name, maintenance_info.business_phone_number as maint_business_phone_number, maintenance_info.business_email as maint_business_email, maintenance_info.business_address as maint_business_address, maintenance_info.business_unit as maint_business_unit, maintenance_info.business_city as maint_business_city, maintenance_info.business_state as maint_business_state, maintenance_info.business_zip as maint_business_zip, maintenance_info.business_photo_url as maint_business_photo_url
                                FROM space.maintenanceQuotes
                                LEFT JOIN space.maintenanceRequests ON quote_maintenance_request_id = maintenance_request_uid
                                LEFT JOIN space.properties ON maintenance_property_id = property_uid 
                                LEFT JOIN space.property_owner ON property_id = property_uid
                                LEFT JOIN space.ownerProfileInfo ON property_owner_id = owner_uid
                                LEFT JOIN space.contracts ON contract_property_id = property_uid 
                                LEFT JOIN space.businessProfileInfo AS pm_business_info ON contract_business_id = pm_business_info.business_uid
                                LEFT JOIN space.businessProfileInfo AS maintenance_info ON quote_business_id = maintenance_info.business_uid
                                WHERE owner_uid = \'""" + uid + """\';
                                -- WHERE space.pm_business_info.business_uid = \'""" + uid + """\';
                                -- WHERE space.pm_business_info.business_uid = '600-000003';
                                """)

            response["maintenanceQuotes"] = ownerQuery


        elif uid[:3] == '600':
            
            business_type = ""
            print('in Get Business Contacts')
            with connect() as db:
                # print("in connect loop")
                query = db.execute(""" 
                    -- FIND ALL CURRENT BUSINESS CONTACTS
                        SELECT business_type
                        FROM space.businessProfileInfo
                        WHERE business_uid = \'""" + uid + """\';
                        """)

            business_type = query['result'][0]['business_type']
            print(business_type)

            if business_type == "MANAGEMENT":
                print("in Management")




                with connect() as db:
                    businessQuery = db.execute(""" 
                                -- MANAGER QUOTES
                                SELECT -- *, 
                                maintenance_quote_uid, quote_maintenance_request_id, quote_status, quote_pm_notes, quote_business_id, quote_services_expenses, quote_earliest_available_date, quote_earliest_available_time, quote_documents, quote_event_type, quote_event_duration, quote_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                                , maintenance_request_uid, maintenance_property_id, maintenance_request_status, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date, maintenance_callback_number, maintenance_estimated_cost, maintenance_pm_notes
                                , property_uid
                                -- , property_available_to_rent, property_active_date, property_listed_date
                                , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type
                                -- , property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_id, property_owner_id, po_owner_percent
                                , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email -- , owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
                                , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                , pm_business_info.business_uid as pm_business_uid, pm_business_info.business_type as pm_business_type, pm_business_info.business_name as pm_business_name, pm_business_info.business_phone_number as pm_business_phone_number, pm_business_info.business_email as pm_business_email, pm_business_info.business_address as pm_business_address, pm_business_info.business_unit as pm_business_unit, pm_business_info.business_city as pm_business_city, pm_business_info.business_state as pm_business_state, pm_business_info.business_zip as pm_business_zip, pm_business_info.business_photo_url as pm_business_photo_url
                                , maintenance_info.business_uid as maint_business_uid, maintenance_info.business_type as maint_business_type, maintenance_info.business_name as maint_business_name, maintenance_info.business_phone_number as maint_business_phone_number, maintenance_info.business_email as maint_business_email, maintenance_info.business_address as maint_business_address, maintenance_info.business_unit as maint_business_unit, maintenance_info.business_city as maint_business_city, maintenance_info.business_state as maint_business_state, maintenance_info.business_zip as maint_business_zip, maintenance_info.business_photo_url as maint_business_photo_url
                                FROM space.maintenanceQuotes
                                LEFT JOIN space.maintenanceRequests ON quote_maintenance_request_id = maintenance_request_uid
                                LEFT JOIN space.properties ON maintenance_property_id = property_uid 
                                LEFT JOIN space.property_owner ON property_id = property_uid
                                LEFT JOIN space.ownerProfileInfo ON property_owner_id = owner_uid
                                LEFT JOIN space.contracts ON contract_property_id = property_uid 
                                LEFT JOIN space.businessProfileInfo AS pm_business_info ON contract_business_id = pm_business_info.business_uid
                                LEFT JOIN space.businessProfileInfo AS maintenance_info ON quote_business_id = maintenance_info.business_uid
                                -- WHERE owner_uid = \'""" + uid + """\';
                                WHERE space.pm_business_info.business_uid = \'""" + uid + """\';
                                -- WHERE space.pm_business_info.business_uid = '600-000008';
                                """)
                    response["maintenanceQuotes"] = businessQuery



            else:
                print("in Maintenance")
                with connect() as db:
                    
                    businessQuery = db.execute(""" 
                                -- MAINTENANCE QUOTES
                                SELECT -- *, 
                                    maintenance_quote_uid, quote_maintenance_request_id, quote_status, quote_pm_notes, quote_business_id, quote_services_expenses, quote_earliest_available_date, quote_earliest_available_time, quote_documents, quote_event_type, quote_event_duration, quote_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                                    , maintenance_request_uid, maintenance_property_id, maintenance_request_status, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date, maintenance_callback_number, maintenance_estimated_cost, maintenance_pm_notes
                                    , property_uid
                                    -- , property_available_to_rent, property_active_date, property_listed_date
                                    , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type
                                    -- , property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_id, property_owner_id, po_owner_percent
                                    , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email -- , owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
                                    , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                    , pm_business_info.business_uid as pm_business_uid, pm_business_info.business_type as pm_business_type, pm_business_info.business_name as pm_business_name, pm_business_info.business_phone_number as pm_business_phone_number, pm_business_info.business_email as pm_business_email, pm_business_info.business_address as pm_business_address, pm_business_info.business_unit as pm_business_unit, pm_business_info.business_city as pm_business_city, pm_business_info.business_state as pm_business_state, pm_business_info.business_zip as pm_business_zip, pm_business_info.business_photo_url as pm_business_photo_url
                                    , maintenance_info.business_uid as maint_business_uid, maintenance_info.business_type as maint_business_type, maintenance_info.business_name as maint_business_name, maintenance_info.business_phone_number as maint_business_phone_number, maintenance_info.business_email as maint_business_email, maintenance_info.business_address as maint_business_address, maintenance_info.business_unit as maint_business_unit, maintenance_info.business_city as maint_business_city, maintenance_info.business_state as maint_business_state, maintenance_info.business_zip as maint_business_zip, maintenance_info.business_photo_url as maint_business_photo_url
                                FROM space.maintenanceQuotes
                                LEFT JOIN space.maintenanceRequests ON quote_maintenance_request_id = maintenance_request_uid
                                LEFT JOIN space.properties ON maintenance_property_id = property_uid 
                                LEFT JOIN space.property_owner ON property_id = property_uid
                                LEFT JOIN space.ownerProfileInfo ON property_owner_id = owner_uid
                                LEFT JOIN space.contracts ON contract_property_id = property_uid 
                                LEFT JOIN space.businessProfileInfo AS pm_business_info ON contract_business_id = pm_business_info.business_uid
                                LEFT JOIN space.businessProfileInfo AS maintenance_info ON quote_business_id = maintenance_info.business_uid
                                -- WHERE owner_uid = \'""" + uid + """\';
                                -- WHERE space.pm_business_info.business_uid = \'""" + uid + """\';
                                -- WHERE space.pm_business_info.business_uid = '600-000003';
                                WHERE quote_business_id = \'""" + uid + """\';
                                -- WHERE quote_business_id = '600-000256';
                                            """)
                    response["maintenanceQuotes"] = businessQuery


        elif uid[:3] == '350':
            with connect() as db:
                tenantQuery = db.execute(""" 
                                -- MAINTENANCE QUOTES
                                SELECT -- *,
                                    maintenance_quote_uid, quote_maintenance_request_id, quote_status, quote_pm_notes, quote_business_id, quote_services_expenses, quote_earliest_available_date,quote_earliest_available_time, quote_documents, quote_event_type, quote_event_duration, quote_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                                    , maintenance_request_uid, maintenance_property_id, maintenance_request_status, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by, maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency, maintenance_notes, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date, maintenance_callback_number, maintenance_estimated_cost, maintenance_pm_notes
                                    , property_uid
                                    -- , property_available_to_rent, property_active_date, property_listed_date
                                    , property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type
                                    , property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image
                                    -- , lease_uid, lease_property_id, lease_application_date, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_rent_available_topay, lease_consent, lease_actual_rent, lease_move_in_date
                                    -- , lt_lease_id, lt_tenant_id, lt_responsibility
                                    , tenant_uid -- , tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_current_address-DNU, tenant_photo_url
                                    , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                    , pm_business_info.business_uid as pm_business_uid, pm_business_info.business_type as pm_business_type, pm_business_info.business_name as pm_business_name, pm_business_info.business_phone_number as pm_business_phone_number, pm_business_info.business_email as pm_business_email, pm_business_info.business_address as pm_business_address, pm_business_info.business_unit as pm_business_unit, pm_business_info.business_city as pm_business_city, pm_business_info.business_state as pm_business_state, pm_business_info.business_zip as pm_business_zip, pm_business_info.business_photo_url as pm_business_photo_url
                                    , maintenance_info.business_uid as maint_business_uid, maintenance_info.business_type as maint_business_type, maintenance_info.business_name as maint_business_name, maintenance_info.business_phone_number as maint_business_phone_number, maintenance_info.business_email as maint_business_email, maintenance_info.business_address as maint_business_address, maintenance_info.business_unit as maint_business_unit, maintenance_info.business_city as maint_business_city, maintenance_info.business_state as maint_business_state, maintenance_info.business_zip as maint_business_zip, maintenance_info.business_photo_url as maint_business_photo_url
                                FROM space.maintenanceQuotes
                                LEFT JOIN space.maintenanceRequests ON quote_maintenance_request_id = maintenance_request_uid
                                LEFT JOIN space.properties ON maintenance_property_id = property_uid
                                LEFT JOIN space.leases ON lease_property_id = property_uid
                                LEFT JOIN space.lease_tenant ON lt_lease_id = lease_uid
                                LEFT JOIN space.tenantProfileInfo ON tenant_uid = lt_tenant_id
                                LEFT JOIN space.contracts ON contract_property_id = property_uid
                                LEFT JOIN space.businessProfileInfo AS pm_business_info ON contract_business_id = pm_business_info.business_uid
                                LEFT JOIN space.businessProfileInfo AS maintenance_info ON quote_business_id = maintenance_info.business_uid
                                -- WHERE tenant_uid = '350-000002';
                                WHERE tenant_uid = \'""" + uid + """\';
                                """)
            response["maintenanceQuotes"] = tenantQuery
            
        return response

    def post(self):
        print("In Maintenace Quotes POST")
        response = {}
        
        payload = request.form.to_dict()
        print("Maintenance Quotes Add Payload: ", payload)

        if payload.get('maintenance_quote_uid'):
            print("maintenance_quote_uid found.  Please call PUT endpoint")
            raise BadRequest("Request failed, UID found in payload.")
        
        with connect() as db:
            print("Maintenance Businesses: ", payload.get('quote_business_id').split(','), type(payload.get('quote_business_id').split(',')))
            maint_businesses = payload.get('quote_business_id').split(',')
            maintenance_images_og = payload.get('quote_maintenance_images', None)
            print("OG Maintenance Images: ", maintenance_images_og)

            for quote_business_id in maint_businesses:
                payload["quote_business_id"] = quote_business_id
                payload["quote_maintenance_images"] = maintenance_images_og
                print('payload["maintenance_quote_uid"] before Process Images: ', payload["quote_maintenance_images"])
                print('payload["maintenance_quote_uid"] before Process Images: ', payload)
                newMaintenanceQuoteUID = db.call('space.new_quote_uid')['result'][0]['new_id']
                key = {'maintenance_quote_uid': newMaintenanceQuoteUID}
                print("Maintenance Quote Key: ", key)
            
                # --------------- PROCESS IMAGES ------------------

                processImage(key, payload)
                print("Payload after function: ", payload)
                
                # --------------- PROCESS IMAGES ------------------


                # Add Maintenance Request Info
                payload['quote_maintenance_images'] = '[]' if payload.get('quote_maintenance_images') in {None, '', 'null'} else payload.get('quote_maintenance_images', '[]')
                payload['quote_documents'] = '[]' if payload.get('quote_documents') in {None, '', 'null'} else payload.get('quote_documents', '[]')
                print("Add Maintenance Quote Payload: ", payload)  

                payload["quote_status"] = "REQUESTED"
                payload["quote_requested_date"] = datetime.today().strftime('%m-%d-%Y %H:%M:%S')
                payload["maintenance_quote_uid"] = newMaintenanceQuoteUID  
                response['Add Maintenance Quote'] = db.insert('maintenanceQuotes', payload)
                response['maintenance_quote_uid'] = newMaintenanceQuoteUID 
                response['Maintenance Quote Images Added'] = payload.get('quote_maintenance_images', "None")
                print("\nNew Maintenance Quote Added")

        return response

    def put(self):
        print('in Maintenance Quotes PUT')
        response = {}
        # response1 = {}
        payload = request.form.to_dict()
        print("Maintenance Quotes Update Payload: ", payload)

        # Verify uid has been included in the data
        if payload.get('maintenance_quote_uid') in {None, '', 'null'}:
            print("No maintenance_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        key = {'maintenance_quote_uid': payload.pop('maintenance_quote_uid')}
        print("Maintenance Quote Key: ", key)


        # --------------- PROCESS IMAGES ------------------

        processImage(key, payload)
        print("Payload after function: ", payload)
        
        # --------------- PROCESS IMAGES ------------------


        # --------------- PROCESS DOCUMENTS ------------------

        processDocument(key, payload)
        print("Payload after function: ", payload)
        
        # --------------- PROCESS DOCUMENTS ------------------


        # Write to Database
        with connect() as db:
            print("Checking Inputs: ", key, payload)
            response["request_update"] = db.update('maintenanceQuotes', key, payload)
            print("Response:" , response)

        return response


class MaintenanceQuotesByUid(Resource):
    def get(self, maintenance_quote_uid):
        print('in MaintenanceQuotesByUid')
        with connect() as db:
            response = db.select('maintenanceQuotes', {"maintenance_quote_uid": maintenance_quote_uid})
        return response


# class MaintenanceStatus(Resource): 
#     def get(self, uid):
#         print('in Maintenance Status')
#         response = {}

#         print("UID: ", uid)

#         if uid[:3] == '110':
#             print("In Owner ID")

#             with connect() as db:
#                 # print("in connect loop")
#                 maintenanceStatus = db.execute(""" 
#                         -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
#                         SELECT *,
#                         CASE 
# 							WHEN maintenance_request_status = "NEW" 						THEN "NEW REQUEST"
#                             WHEN maintenance_request_status = "INFO"						THEN "INFO REQUESTED"
#                             WHEN maintenance_request_status IN ('PROCESSING', 'SCHEDULED', 'CANCELLED', 'COMPLETED') THEN maintenance_request_status
# 						END AS maintenance_status
#                         -- bill_property_id,  maintenance_property_id,
#                         FROM space.m_details
#                         LEFT JOIN space.properties ON property_uid = maintenance_property_id
#                         LEFT JOIN (
#                             SELECT -- *
#                                 bill_uid, bill_timestamp, bill_created_by, bill_description, bill_utility_type, bill_split, bill_property_id, bill_documents, bill_maintenance_quote_id, bill_notes
#                                 , sum(bill_amount) AS bill_amount
#                             FROM space.bills
#                             GROUP BY bill_maintenance_quote_id
#                             ) as b ON bill_maintenance_quote_id = maintenance_quote_uid
#                         LEFT JOIN (SELECT SUM(pur_amount_due) as pur_amount_due ,SUM(total_paid) as pur_amount_paid, CASE WHEN SUM(total_paid) >= SUM(pur_amount_due) THEN "PAID" ELSE "UNPAID" END AS purchase_status , pur_bill_id, pur_property_id FROM space.pp_status GROUP BY pur_bill_id) as p ON pur_bill_id = bill_uid AND p.pur_property_id = maintenance_property_id
#                         LEFT JOIN space.o_details ON maintenance_property_id = property_id
#                         LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
#                         LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
#                         LEFT JOIN space.t_details ON lt_lease_id = lease_uid
#                         WHERE owner_uid = \'""" + uid + """\'
#                         -- WHERE business_uid = \'""" + uid + """\'
#                         -- WHERE tenant_uid = \'""" + uid + """\'
#                         -- WHERE quote_business_id = \'""" + uid + """\'
#                         -- WHERE maintenance_property_id = = \'""" + uid + """\'
#                         ORDER BY maintenance_request_created_date;
#                         """)

#             if maintenanceStatus.get('code') == 200:
#                 status_colors = {
#                     'NEW REQUEST': '#A52A2A',
#                     'INFO REQUESTED': '#C06A6A',
#                     # 'QUOTES REQUESTED': '#D29494',  # Deprecated as per new figma
#                     'PROCESSING': '#3D5CAC',
#                     'SCHEDULED': '#3D5CAC',
#                     'CANCELLED': '#FFFFFF',
#                     'COMPLETED': '#3D5CAC',
#                 }
#                 mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in
#                                 status_colors.items()}

#                 response = maintenanceStatus

#                 for record in response['result']:
#                     status = record.get('maintenance_status')
#                     mapped_items[status]['maintenance_items'].append(record)

#                 response['result'] = mapped_items
#                 return response

#             response["MaintenanceStatus"] = maintenanceStatus
#             return response
        
#         elif uid[:3] == '600':
#             print("In Business ID")

#             # CHECK IF MAINTENANCE OR MANAGEMENT
#             with connect() as db:
#                 # print("in check loop")
#                 businessType = db.execute(""" 
#                         -- CHECK BUSINESS TYPE
#                         SELECT -- *
#                             business_uid, business_type
#                         FROM space.businessProfileInfo
#                         WHERE business_uid = \'""" + uid + """\';
#                         """)
            
#             # print(businessType)
#             print(businessType["result"])
#             # print(businessType["result"][0]["business_type"])

#             # if businessType["result"] == "()":
#             if len(businessType["result"]) == 0:
#                 print("BUSINESS UID Not found")
#                 response["MaintenanceStatus"] = "BUSINESS UID Not Found"
#                 return response

#             elif businessType["result"][0]["business_type"] == "MANAGEMENT":

#                 with connect() as db:
#                     print("in MANAGEMENT")
#                     maintenanceStatus = db.execute(""" 
#                             -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
#                             SELECT *
#                             , CASE  
#                                     WHEN quote_status_ranked = "COMPLETED"                                           					THEN "PAID" 
#                                     WHEN maintenance_request_status IN ("NEW" ,"INFO")                                      			THEN "NEW REQUEST"
#                                     WHEN quote_status_ranked = "SCHEDULED"                                           			        THEN "SCHEDULED"
#                                     WHEN maintenance_request_status = 'CANCELLED' or quote_status_ranked = "FINISHED"       			THEN "COMPLETED"
#                                     WHEN quote_status_ranked IN ("MORE INFO", "SENT" ,"REFUSED" , "REQUESTED", "REJECTED", "WITHDRAWN") THEN "QUOTES REQUESTED"
#                                     WHEN quote_status_ranked IN ("ACCEPTED" , "SCHEDULE")                                   			THEN "QUOTES ACCEPTED"
#                                     ELSE maintenance_request_status -- "NEW REQUEST"
#                                 END AS maintenance_status
#                             FROM (
#                                     SELECT * 
#                                     FROM space.maintenanceRequests
#                                     LEFT JOIN (
#                                         SELECT *,
#                                             CASE
#                                                 WHEN max_quote_rank = "10" THEN "REQUESTED"
#                                                 WHEN max_quote_rank = "11" THEN "REFUSED"
#                                                 WHEN max_quote_rank = "15" THEN "MORE INFO"
#                                                 WHEN max_quote_rank = "20" THEN "SENT"
#                                                 WHEN max_quote_rank = "21" THEN "REJECTED"
#                                                 WHEN max_quote_rank = "22" THEN "WITHDRAWN"
#                                                 WHEN max_quote_rank = "30" THEN "ACCEPTED"
#                                                 WHEN max_quote_rank = "40" THEN "SCHEDULE"
#                                                 WHEN max_quote_rank = "50" THEN "SCHEDULED"
#                                                 WHEN max_quote_rank = "60" THEN "RESCHEDULED"
#                                                 WHEN max_quote_rank = "70" THEN "FINISHED"
#                                                 WHEN max_quote_rank = "80" THEN "COMPLETED"
#                                                 WHEN max_quote_rank = "90" THEN "CANCELLED"
#                                                 ELSE "0"
#                                             END AS quote_status_ranked
#                                         FROM 
#                                         (
#                                             SELECT -- maintenance_quote_uid, 
#                                                 quote_maintenance_request_id AS qmr_id
#                                                 -- , quote_status
#                                                 , MAX(quote_rank) AS max_quote_rank
#                                             FROM (
#                                                 SELECT -- *,
#                                                     maintenance_quote_uid, 
#                                                     quote_maintenance_request_id, quote_status,
#                                                     -- , quote_pm_notes, quote_business_id, quote_services_expenses, quote_earliest_available_date,quote_earliest_available_time, quote_event_type, quote_event_duration, quote_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
#                                                 CASE
#                                                     WHEN quote_status = "REQUESTED" THEN "10"
#                                                     WHEN quote_status = "REFUSED" THEN "11"
#                                                     WHEN quote_status = "MORE INFO" THEN "15"
#                                                     WHEN quote_status = "SENT" THEN "20"
#                                                     WHEN quote_status = "REJECTED" THEN "21"
#                                                     WHEN quote_status = "WITHDRAWN"  THEN "22"
#                                                     WHEN quote_status = "ACCEPTED" THEN "30"
#                                                     WHEN quote_status = "SCHEDULE" THEN "40"
#                                                     WHEN quote_status = "SCHEDULED" THEN "50"
#                                                     WHEN quote_status = "RESCHEDULED" THEN "60"
#                                                     WHEN quote_status = "FINISHED" THEN "70"
#                                                     WHEN quote_status = "COMPLETED" THEN "80"
#                                                     WHEN quote_status = "CANCELLED" THEN "90"
#                                                     ELSE 0
#                                                 END AS quote_rank
#                                                 FROM space.maintenanceQuotes
#                                                 ) AS qr
#                                             GROUP BY quote_maintenance_request_id
#                                         ) AS qr_quoterank
#                                     ) AS quote_summary ON maintenance_request_uid = qmr_id
#                                 ) AS quotes
#                             LEFT JOIN space.bills ON maintenance_request_uid = bill_maintenance_request_id
#                             LEFT JOIN (
#                                 SELECT quote_maintenance_request_id, 
#                                     JSON_ARRAYAGG(JSON_OBJECT
#                                         ('maintenance_quote_uid', maintenance_quote_uid,
#                                         'quote_status', quote_status,
#                                         'quote_pm_notes', quote_pm_notes,
#                                         'quote_business_id', quote_business_id,
#                                         'quote_services_expenses', quote_services_expenses,
#                                         'quote_event_type', quote_event_type,
#                                         'quote_event_duration', quote_event_duration,
#                                         'quote_notes', quote_notes,
#                                         'quote_created_date', quote_created_date,
#                                         'quote_total_estimate', quote_total_estimate,
#                                         'quote_maintenance_images', quote_maintenance_images,
#                                         'quote_adjustment_date', quote_adjustment_date,
#                                         'quote_earliest_available_date', quote_earliest_available_date,
#                                         'quote_earliest_available_time', quote_earliest_available_time
#                                         )) AS quote_info
#                                 FROM space.maintenanceQuotes
#                                 GROUP BY quote_maintenance_request_id) as qi ON quote_maintenance_request_id = maintenance_request_uid
#                             LEFT JOIN space.pp_status ON bill_uid = pur_bill_id AND pur_receiver = maintenance_assigned_business
#                             LEFT JOIN space.properties ON property_uid = maintenance_property_id
#                             LEFT JOIN space.o_details ON maintenance_property_id = property_id
#                             LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
#                             LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
#                             LEFT JOIN space.t_details ON lt_lease_id = lease_uid

#                             WHERE business_uid = \'""" + uid + """\' -- AND (pur_receiver = \'""" + uid + """\' OR ISNULL(pur_receiver))
#                             -- WHERE business_uid = '600-000010' -- AND (pur_receiver = '600-000003' OR ISNULL(pur_receiver))
#                             ORDER BY maintenance_request_created_date;
#                             """)

#                 # print("Completed Query")
#                 if maintenanceStatus.get('code') == 200:
#                     status_colors = {
#                         'NEW REQUEST': '#A52A2A',
#                         'QUOTES REQUESTED': '#C06A6A',
#                         'QUOTES ACCEPTED': '#D29494',
#                         'SCHEDULED': '#9EAED6',
#                         'COMPLETED': '#778DC5',
#                         'PAID': '#3D5CAC',
#                     }

#                     # print("After Colors")
#                     mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in
#                                     status_colors.items()}
                    
#                     # print("After Map")
#                     response = maintenanceStatus
#                     # print("Response: ", response)

#                     for record in response['result']:
#                         # print("Record: ", record)
#                         status = record.get('maintenance_status')
#                         # print('Status: ', status)
#                         mapped_items[status]['maintenance_items'].append(record)
#                         # print('Mapped_items: ', mapped_items)

#                     response['result'] = mapped_items
#                     # print("Final Response: ", response)
#                     return response

#                 response["MaintenanceStatus"] = maintenanceStatus
#                 return response
            
#             elif businessType["result"][0]["business_type"] == "MAINTENANCE":

#                 with connect() as db:
#                     print("in MAINTENANCE")
#                     maintenanceStatus = db.execute(""" 
#                             -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
#                             SELECT *
#                                 , CASE
#                                     WHEN space.m_details.quote_status IN ("REQUESTED", "MORE INFO")                                        		THEN "REQUESTED"
#                                     WHEN space.m_details.quote_status IN ("SENT", "REFUSED" ,"REJECTED" ) 	                                    THEN "SUBMITTED"
#                                     WHEN space.m_details.quote_status IN ("ACCEPTED", "SCHEDULE")                          						THEN "ACCEPTED"
#                                     WHEN space.m_details.quote_status IN ("SCHEDULED" , "RESCHEDULE")                       					THEN "SCHEDULED"
#                                     WHEN space.m_details.quote_status = "FINISHED"                                                       		THEN "FINISHED"
#                                     WHEN space.m_details.quote_status = "COMPLETED"                                                      		THEN "PAID"   
#                                     WHEN space.m_details.quote_status IN ("CANCELLED" , "ARCHIVE", "NOT ACCEPTED","WITHDRAWN" ,"WITHDRAW")      THEN "ARCHIVE"
#                                     ELSE space.m_details.quote_status
#                                 END AS maintenance_status
#                             FROM space.m_details
#                             LEFT JOIN space.bills ON space.m_details.maintenance_request_uid = bill_maintenance_request_id
#                             -- LEFT JOIN space.maintenanceQuotes ON bill_maintenance_quote_id = space.maintenanceQuotes.maintenance_quote_uid
#                             LEFT JOIN (SELECT * FROM space.purchases WHERE pur_receiver = '600-000012' OR ISNULL(pur_receiver)) AS pp ON bill_uid = pur_bill_id
#                             LEFT JOIN space.properties ON property_uid = maintenance_property_id
#                             LEFT JOIN space.o_details ON maintenance_property_id = property_id
#                             LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
#                             LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
#                             LEFT JOIN space.t_details ON lt_lease_id = lease_uid
#                             WHERE quote_business_id = \'""" + uid + """\'
#                             -- WHERE quote_business_id = '600-000012'
#                             """)

#                 if maintenanceStatus.get('code') == 200:
#                     status_colors = {
#                         'REQUESTED': '#DB9687',
#                         'SUBMITTED': '#D4A387',
#                         'ACCEPTED': '#BAAC7A',
#                         'SCHEDULED': '#959A76',
#                         'FINISHED': '#598A96',
#                         'PAID': '#497290',
#                         'ARCHIVE': '#497290',
#                     }

#                     mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in
#                                     status_colors.items()}

#                     response = maintenanceStatus

#                     for record in response['result']:
#                         status = record.get('maintenance_status')
#                         mapped_items[status]['maintenance_items'].append(record)

#                     response['result'] = mapped_items
#                     return response

#                 response["MaintenanceStatus"] = maintenanceStatus
#                 return response
            
#             else:
#                 print("BUSINESS TYPE Not found")
#                 response["MaintenanceStatus"] = "BUSINESS TYPE Not Found"
#                 return response
        
#         elif uid[:3] == '350':
#             print("In Tenant ID")
            
#             with connect() as db:
#                 print("in connect loop")
#                 maintenanceStatus = db.execute(""" 
#                         -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
#                         SELECT *, -- bill_property_id,  maintenance_property_id,
#                         CASE 
# 							WHEN maintenance_request_status = "NEW" 						THEN "NEW REQUEST"
#                             WHEN maintenance_request_status = "INFO"						THEN "INFO REQUESTED"
#                             WHEN maintenance_request_status IN ('PROCESSING', 'SCHEDULED', 'CANCELLED', 'COMPLETED') THEN maintenance_request_status
# 						END AS maintenance_status
#                         FROM space.m_details
#                         LEFT JOIN space.properties ON property_uid = maintenance_property_id
#                         LEFT JOIN (
#                             SELECT -- *
#                                 bill_uid, bill_timestamp, bill_created_by, bill_description, bill_utility_type, bill_split, bill_property_id, bill_documents, bill_maintenance_quote_id, bill_notes
#                                 , sum(bill_amount) AS bill_amount
#                             FROM space.bills
#                             GROUP BY bill_maintenance_quote_id
#                             ) as b ON bill_maintenance_quote_id = maintenance_quote_uid
#                         LEFT JOIN (SELECT SUM(pur_amount_due) as pur_amount_due ,SUM(total_paid) as pur_amount_paid, CASE WHEN SUM(total_paid) >= SUM(pur_amount_due) THEN "PAID" ELSE "UNPAID" END AS purchase_status , pur_bill_id, pur_property_id FROM space.pp_status GROUP BY pur_bill_id) as p ON pur_bill_id = bill_uid AND p.pur_property_id = maintenance_property_id
#                         LEFT JOIN space.o_details ON maintenance_property_id = property_id
#                         LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
#                         LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
#                         LEFT JOIN space.t_details ON lt_lease_id = lease_uid
#                             LEFT JOIN (
#                             SELECT quote_maintenance_request_id, JSON_ARRAYAGG(JSON_OBJECT
#                                 ('maintenance_quote_uid', maintenance_quote_uid,
#                                 'quote_status', quote_status,
#                                 'quote_pm_notes', quote_pm_notes,
#                                 'quote_business_id', quote_business_id,
#                                 'quote_services_expenses', quote_services_expenses,
#                                 'quote_event_type', quote_event_type,
#                                 'quote_event_duration', quote_event_duration,
#                                 'quote_notes', quote_notes,
#                                 'quote_created_date', quote_created_date,
#                                 'quote_total_estimate', quote_total_estimate,
#                                 'quote_maintenance_images', quote_maintenance_images,
#                                 'quote_adjustment_date', quote_adjustment_date,
#                                 'quote_earliest_available_date', quote_earliest_available_date,
#                                 'quote_earliest_available_time', quote_earliest_available_time
#                                 )) AS quote_info
#                                 FROM space.maintenanceQuotes
#                                 GROUP BY quote_maintenance_request_id) as qi ON qi.quote_maintenance_request_id = maintenance_request_uid
#                         -- WHERE owner_uid = \'""" + uid + """\'
#                         -- WHERE business_uid = \'""" + uid + """\'
#                         WHERE tenant_uid = \'""" + uid + """\'
#                         -- WHERE quote_business_id = \'""" + uid + """\'
#                         -- WHERE maintenance_property_id = \'""" + uid + """\'
#                         ORDER BY maintenance_request_created_date;
#                         """)

#             if maintenanceStatus.get('code') == 200:
#                 status_colors = {
#                     'NEW REQUEST': '#A52A2A',
#                     'INFO REQUESTED': '#C06A6A',
#                     # 'QUOTES REQUESTED': '#D29494',  # Deprecated as per new figma
#                     'PROCESSING': '#3D5CAC',
#                     'SCHEDULED': '#3D5CAC',
#                     'CANCELLED': '#FFFFFF',
#                     'COMPLETED': '#3D5CAC',
#                 }
#                 mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in
#                                 status_colors.items()}

#                 response = maintenanceStatus

#                 for record in response['result']:
#                     status = record.get('maintenance_status')
#                     mapped_items[status]['maintenance_items'].append(record)

#                 response['result'] = mapped_items
#                 return response

#             response["MaintenanceStatus"] = maintenanceStatus
#             return response

#         elif uid[:3] == '200':
#             print("In Property ID")
            
#             with connect() as db:
#                 print("in connect loop")
#                 maintenanceStatus = db.execute(""" 
#                         -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
#                         SELECT *, -- bill_property_id,  maintenance_property_id,
#                         CASE 
# 							WHEN maintenance_request_status = "NEW" 						THEN "NEW REQUEST"
#                             WHEN maintenance_request_status = "INFO"						THEN "INFO REQUESTED"
#                             WHEN maintenance_request_status IN ('PROCESSING', 'SCHEDULED', 'CANCELLED', 'COMPLETED') THEN maintenance_request_status
# 						END AS maintenance_status
#                         FROM space.m_details
#                         LEFT JOIN space.properties ON property_uid = maintenance_property_id
#                         LEFT JOIN (
#                             SELECT -- *
#                                 bill_uid, bill_timestamp, bill_created_by, bill_description, bill_utility_type, bill_split, bill_property_id, bill_documents, bill_maintenance_quote_id, bill_notes
#                                 , sum(bill_amount) AS bill_amount
#                             FROM space.bills
#                             GROUP BY bill_maintenance_quote_id
#                             ) as b ON bill_maintenance_quote_id = maintenance_quote_uid
#                         LEFT JOIN (SELECT SUM(pur_amount_due) as pur_amount_due ,SUM(total_paid) as pur_amount_paid, CASE WHEN SUM(total_paid) >= SUM(pur_amount_due) THEN "PAID" ELSE "UNPAID" END AS purchase_status , pur_bill_id, pur_property_id FROM space.pp_status GROUP BY pur_bill_id) as p ON pur_bill_id = bill_uid AND p.pur_property_id = maintenance_property_id
#                         LEFT JOIN space.o_details ON maintenance_property_id = property_id
#                         LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
#                         LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
#                         LEFT JOIN space.t_details ON lt_lease_id = lease_uid
#                             LEFT JOIN (
#                             SELECT quote_maintenance_request_id, JSON_ARRAYAGG(JSON_OBJECT
#                                 ('maintenance_quote_uid', maintenance_quote_uid,
#                                 'quote_status', quote_status,
#                                 'quote_pm_notes', quote_pm_notes,
#                                 'quote_business_id', quote_business_id,
#                                 'quote_services_expenses', quote_services_expenses,
#                                 'quote_event_type', quote_event_type,
#                                 'quote_event_duration', quote_event_duration,
#                                 'quote_notes', quote_notes,
#                                 'quote_created_date', quote_created_date,
#                                 'quote_total_estimate', quote_total_estimate,
#                                 'quote_maintenance_images', quote_maintenance_images,
#                                 'quote_adjustment_date', quote_adjustment_date,
#                                 'quote_earliest_available_date', quote_earliest_available_date,
#                                 'quote_earliest_available_time', quote_earliest_available_time
#                                 )) AS quote_info
#                                 FROM space.maintenanceQuotes
#                                 GROUP BY quote_maintenance_request_id) as qi ON qi.quote_maintenance_request_id = maintenance_request_uid
#                         -- WHERE owner_uid = \'""" + uid + """\'
#                         -- WHERE business_uid = \'""" + uid + """\'
#                         -- WHERE tenant_uid = \'""" + uid + """\'
#                         -- WHERE quote_business_id = \'""" + uid + """\'
#                         WHERE maintenance_property_id = \'""" + uid + """\'
#                         ORDER BY maintenance_request_created_date;
#                         """)

#             if maintenanceStatus.get('code') == 200:
#                 status_colors = {
#                     'NEW REQUEST': '#A52A2A',
#                     'INFO REQUESTED': '#C06A6A',
#                     # 'QUOTES REQUESTED': '#D29494',  # Deprecated as per new figma
#                     'PROCESSING': '#3D5CAC',
#                     'SCHEDULED': '#3D5CAC',
#                     'CANCELLED': '#FFFFFF',
#                     'COMPLETED': '#3D5CAC',
#                 }
#                 mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in
#                                 status_colors.items()}

#                 response = maintenanceStatus

#                 for record in response['result']:
#                     status = record.get('maintenance_status')
#                     mapped_items[status]['maintenance_items'].append(record)

#                 response['result'] = mapped_items
#                 return response

#             response["MaintenanceStatus"] = maintenanceStatus
#             return response

#         else:
#             print("UID Not found")
#             response["MaintenanceStatus"] = "UID Not Found"
#             return response


class MaintenanceStatus(Resource): 
    def get(self, user_id):
        print('in Maintenance Status')
        response = {}

        print("User ID: ", user_id)

        if user_id[:3] == '110':
            print("In Owner ID")

            with connect() as db:
                # print("in connect loop")
                maintenanceStatus = db.execute(""" 
                        SELECT mrs.*,
                            list_item AS maintenance_color
                        FROM (
                            SELECT *,
                                CONCAT(SUBSTRING(property_owner_id, 1, 3), "-", maintenance_status) AS concatenated_value
                            FROM(
                                SELECT *,
                                    CASE 
                                        WHEN maintenance_request_status = "NEW" 						THEN "NEW REQUEST"
                                        WHEN maintenance_request_status = "INFO"						THEN "INFO REQUESTED"
                                        WHEN maintenance_request_status IN ('PROCESSING', 'SCHEDULED', 'CANCELLED', 'COMPLETED') THEN maintenance_request_status
                                    END AS maintenance_status
                                FROM space.maintenanceRequests
                                LEFT JOIN m_quote_rank ON maintenance_request_uid = qmr_id
                                LEFT JOIN space.property_owner ON maintenance_property_id = property_id
                                LEFT JOIN ( SELECT * FROM space.contracts WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                                -- WHERE property_owner_id = '110-000007'
                                -- WHERE maintenance_property_id = '200-000084'
                                -- WHERE maintenance_assigned_business = '600-000010'
                                -- WHERE contract_business_id = '600-000043'
                                WHERE property_owner_id = \'""" + user_id + """\' 
                                -- WHERE contract_business_id = \'""" + user_id + """\' 
                                -- WHERE {column} =  \'""" + user_id + """\' 
                            ) AS mr
                        ) AS mrs
                        LEFT JOIN space.lists ON concatenated_value = list_category
                        """)

            if maintenanceStatus.get('code') == 200:
                status_colors = {
                    'NEW REQUEST': '#A52A2A',
                    'INFO REQUESTED': '#C06A6A',
                    # 'QUOTES REQUESTED': '#D29494',  # Deprecated as per new figma
                    'PROCESSING': '#3D5CAC',
                    'SCHEDULED': '#3D5CAC',
                    'CANCELLED': '#FFFFFF',
                    'COMPLETED': '#3D5CAC',
                }
                mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in
                                status_colors.items()}

                response = maintenanceStatus

                for record in response['result']:
                    status = record.get('maintenance_status')
                    mapped_items[status]['maintenance_items'].append(record)

                response['result'] = mapped_items
                return response

            response["MaintenanceStatus"] = maintenanceStatus
            return response
        
        elif user_id[:3] == '600':
            print("In Business ID")

            # CHECK IF MAINTENANCE OR MANAGEMENT
            with connect() as db:
                # print("in check loop")
                businessType = db.execute(""" 
                        -- CHECK BUSINESS TYPE
                        SELECT -- *
                            business_uid, business_type
                        FROM space.businessProfileInfo
                        WHERE business_uid = \'""" + user_id + """\';
                        """)
            
            # print(businessType)
            print(businessType["result"])
            # print(businessType["result"][0]["business_type"])

            if len(businessType["result"]) == 0:
                print("BUSINESS UID Not found")
                response["MaintenanceStatus"] = "BUSINESS UID Not Found"
                return response

            elif businessType["result"][0]["business_type"] == "MANAGEMENT":

                with connect() as db:
                    print("in MANAGEMENT")
                    maintenanceStatus = db.execute(""" 
                            -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
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

                                FROM space.maintenanceRequests
                                LEFT JOIN m_quote_rank AS quote_summary ON maintenance_request_uid = qmr_id

                                LEFT JOIN space.bills ON maintenance_request_uid = bill_maintenance_request_id
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
                                    FROM space.maintenanceQuotes
                                    GROUP BY quote_maintenance_request_id) as qi ON quote_maintenance_request_id = maintenance_request_uid
                                LEFT JOIN space.pp_status ON bill_uid = pur_bill_id AND pur_receiver = maintenance_assigned_business
                                LEFT JOIN space.properties ON property_uid = maintenance_property_id
                                LEFT JOIN space.o_details ON maintenance_property_id = property_id
                                LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                                LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
                                LEFT JOIN space.t_details ON lt_lease_id = lease_uid

                                -- WHERE business_uid = \'""" + user_id + """\' -- AND (pur_receiver = \'""" + user_id + """\' OR ISNULL(pur_receiver))
                                WHERE business_uid = '600-000003' -- AND (pur_receiver = '600-000003' OR ISNULL(pur_receiver))
                                ORDER BY maintenance_request_created_date;
                            """)

                # print("Completed Query")
                if maintenanceStatus.get('code') == 200:
                    status_colors = {
                        'NEW REQUEST': '#A52A2A',
                        'QUOTES REQUESTED': '#C06A6A',
                        'QUOTES ACCEPTED': '#D29494',
                        'SCHEDULED': '#9EAED6',
                        'COMPLETED': '#778DC5',
                        'PAID': '#3D5CAC',
                    }

                    # print("After Colors")
                    mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in
                                    status_colors.items()}
                    
                    # print("After Map")
                    response = maintenanceStatus
                    # print("Response: ", response)

                    for record in response['result']:
                        # print("Record: ", record)
                        status = record.get('maintenance_status')
                        # print('Status: ', status)
                        mapped_items[status]['maintenance_items'].append(record)
                        # print('Mapped_items: ', mapped_items)

                    response['result'] = mapped_items
                    # print("Final Response: ", response)
                    return response

                response["MaintenanceStatus"] = maintenanceStatus
                return response
            
            elif businessType["result"][0]["business_type"] == "MAINTENANCE":

                with connect() as db:
                    print("in MAINTENANCE")
                    maintenanceStatus = db.execute(""" 
                            -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                            SELECT *
                                , CASE
                                    WHEN space.m_details.quote_status IN ("REQUESTED", "MORE INFO")                                        		THEN "REQUESTED"
                                    WHEN space.m_details.quote_status IN ("SENT", "REFUSED" ,"REJECTED" ) 	                                    THEN "SUBMITTED"
                                    WHEN space.m_details.quote_status IN ("ACCEPTED", "SCHEDULE")                          						THEN "ACCEPTED"
                                    WHEN space.m_details.quote_status IN ("SCHEDULED" , "RESCHEDULE")                       					THEN "SCHEDULED"
                                    WHEN space.m_details.quote_status = "FINISHED"                                                       		THEN "FINISHED"
                                    WHEN space.m_details.quote_status = "COMPLETED"                                                      		THEN "PAID"   
                                    WHEN space.m_details.quote_status IN ("CANCELLED" , "ARCHIVE", "NOT ACCEPTED","WITHDRAWN" ,"WITHDRAW")      THEN "ARCHIVE"
                                    ELSE space.m_details.quote_status
                                END AS maintenance_status
                            FROM space.m_details
                            LEFT JOIN space.bills ON space.m_details.maintenance_request_uid = bill_maintenance_request_id
                            -- LEFT JOIN space.maintenanceQuotes ON bill_maintenance_quote_id = space.maintenanceQuotes.maintenance_quote_uid
                            LEFT JOIN (SELECT * FROM space.purchases WHERE pur_receiver = '600-000012' OR ISNULL(pur_receiver)) AS pp ON bill_uid = pur_bill_id
                            LEFT JOIN space.properties ON property_uid = maintenance_property_id
                            LEFT JOIN space.o_details ON maintenance_property_id = property_id
                            LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                            LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
                            LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                            WHERE quote_business_id = \'""" + user_id + """\'
                            -- WHERE quote_business_id = '600-000012'
                            """)

                if maintenanceStatus.get('code') == 200:
                    status_colors = {
                        'REQUESTED': '#DB9687',
                        'SUBMITTED': '#D4A387',
                        'ACCEPTED': '#BAAC7A',
                        'SCHEDULED': '#959A76',
                        'FINISHED': '#598A96',
                        'PAID': '#497290',
                        'ARCHIVE': '#497290',
                    }

                    mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in
                                    status_colors.items()}

                    response = maintenanceStatus

                    for record in response['result']:
                        status = record.get('maintenance_status')
                        mapped_items[status]['maintenance_items'].append(record)

                    response['result'] = mapped_items
                    return response

                response["MaintenanceStatus"] = maintenanceStatus
                return response
            
            else:
                print("BUSINESS TYPE Not found")
                response["MaintenanceStatus"] = "BUSINESS TYPE Not Found"
                return response
        
        elif user_id[:3] == '350':
            print("In Tenant ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceStatus = db.execute(""" 
                        -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT *, -- bill_property_id,  maintenance_property_id,
                        CASE 
							WHEN maintenance_request_status = "NEW" 						THEN "NEW REQUEST"
                            WHEN maintenance_request_status = "INFO"						THEN "INFO REQUESTED"
                            WHEN maintenance_request_status IN ('PROCESSING', 'SCHEDULED', 'CANCELLED', 'COMPLETED') THEN maintenance_request_status
						END AS maintenance_status
                        FROM space.m_details
                        LEFT JOIN space.properties ON property_uid = maintenance_property_id
                        LEFT JOIN (
                            SELECT -- *
                                bill_uid, bill_timestamp, bill_created_by, bill_description, bill_utility_type, bill_split, bill_property_id, bill_documents, bill_maintenance_quote_id, bill_notes
                                , sum(bill_amount) AS bill_amount
                            FROM space.bills
                            GROUP BY bill_maintenance_quote_id
                            ) as b ON bill_maintenance_quote_id = maintenance_quote_uid
                        LEFT JOIN (SELECT SUM(pur_amount_due) as pur_amount_due ,SUM(total_paid) as pur_amount_paid, CASE WHEN SUM(total_paid) >= SUM(pur_amount_due) THEN "PAID" ELSE "UNPAID" END AS purchase_status , pur_bill_id, pur_property_id FROM space.pp_status GROUP BY pur_bill_id) as p ON pur_bill_id = bill_uid AND p.pur_property_id = maintenance_property_id
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                        LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
                        LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                            LEFT JOIN (
                            SELECT quote_maintenance_request_id, JSON_ARRAYAGG(JSON_OBJECT
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
                                FROM space.maintenanceQuotes
                                GROUP BY quote_maintenance_request_id) as qi ON qi.quote_maintenance_request_id = maintenance_request_uid
                        -- WHERE owner_uid = \'""" + user_id + """\'
                        -- WHERE business_uid = \'""" + user_id + """\'
                        WHERE tenant_uid = \'""" + user_id + """\'
                        -- WHERE quote_business_id = \'""" + user_id + """\'
                        -- WHERE maintenance_property_id = \'""" + user_id + """\'
                        ORDER BY maintenance_request_created_date;
                        """)

            if maintenanceStatus.get('code') == 200:
                status_colors = {
                    'NEW REQUEST': '#A52A2A',
                    'INFO REQUESTED': '#C06A6A',
                    # 'QUOTES REQUESTED': '#D29494',  # Deprecated as per new figma
                    'PROCESSING': '#3D5CAC',
                    'SCHEDULED': '#3D5CAC',
                    'CANCELLED': '#FFFFFF',
                    'COMPLETED': '#3D5CAC',
                }
                mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in
                                status_colors.items()}

                response = maintenanceStatus

                for record in response['result']:
                    status = record.get('maintenance_status')
                    mapped_items[status]['maintenance_items'].append(record)

                response['result'] = mapped_items
                return response

            response["MaintenanceStatus"] = maintenanceStatus
            return response

        elif user_id[:3] == '200':
            print("In Property ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceStatus = db.execute(""" 
                        -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT *, -- bill_property_id,  maintenance_property_id,
                        CASE 
							WHEN maintenance_request_status = "NEW" 						THEN "NEW REQUEST"
                            WHEN maintenance_request_status = "INFO"						THEN "INFO REQUESTED"
                            WHEN maintenance_request_status IN ('PROCESSING', 'SCHEDULED', 'CANCELLED', 'COMPLETED') THEN maintenance_request_status
						END AS maintenance_status
                        FROM space.m_details
                        LEFT JOIN space.properties ON property_uid = maintenance_property_id
                        LEFT JOIN (
                            SELECT -- *
                                bill_uid, bill_timestamp, bill_created_by, bill_description, bill_utility_type, bill_split, bill_property_id, bill_documents, bill_maintenance_quote_id, bill_notes
                                , sum(bill_amount) AS bill_amount
                            FROM space.bills
                            GROUP BY bill_maintenance_quote_id
                            ) as b ON bill_maintenance_quote_id = maintenance_quote_uid
                        LEFT JOIN (SELECT SUM(pur_amount_due) as pur_amount_due ,SUM(total_paid) as pur_amount_paid, CASE WHEN SUM(total_paid) >= SUM(pur_amount_due) THEN "PAID" ELSE "UNPAID" END AS purchase_status , pur_bill_id, pur_property_id FROM space.pp_status GROUP BY pur_bill_id) as p ON pur_bill_id = bill_uid AND p.pur_property_id = maintenance_property_id
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                        LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
                        LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                            LEFT JOIN (
                            SELECT quote_maintenance_request_id, JSON_ARRAYAGG(JSON_OBJECT
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
                                FROM space.maintenanceQuotes
                                GROUP BY quote_maintenance_request_id) as qi ON qi.quote_maintenance_request_id = maintenance_request_uid
                        -- WHERE owner_uid = \'""" + user_id + """\'
                        -- WHERE business_uid = \'""" + user_id + """\'
                        -- WHERE tenant_uid = \'""" + user_id + """\'
                        -- WHERE quote_business_id = \'""" + user_id + """\'
                        WHERE maintenance_property_id = \'""" + user_id + """\'
                        ORDER BY maintenance_request_created_date;
                        """)

            if maintenanceStatus.get('code') == 200:
                status_colors = {
                    'NEW REQUEST': '#A52A2A',
                    'INFO REQUESTED': '#C06A6A',
                    # 'QUOTES REQUESTED': '#D29494',  # Deprecated as per new figma
                    'PROCESSING': '#3D5CAC',
                    'SCHEDULED': '#3D5CAC',
                    'CANCELLED': '#FFFFFF',
                    'COMPLETED': '#3D5CAC',
                }
                mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in
                                status_colors.items()}

                response = maintenanceStatus

                for record in response['result']:
                    status = record.get('maintenance_status')
                    mapped_items[status]['maintenance_items'].append(record)

                response['result'] = mapped_items
                return response

            response["MaintenanceStatus"] = maintenanceStatus
            return response

        else:
            print("UID Not found")
            response["MaintenanceStatus"] = "UID Not Found"
            return response
