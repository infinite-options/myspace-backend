
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img, uploadImage
from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest
import ast

from maintenance_mapper import mapMaintenanceForOwner, mapMaintenanceForTenant, mapMaintenanceForProperty, \
    mapMaintenanceForMaintenance, mapMaintenanceForPropertyManager


# MAINTENANCE BY STATUS
#                           TENANT      OWNER     PROPERTY MANAGER     
# MAINTENANCE BY STATUS        Y           Y               Y
# BY MONTH    X           X               X
# BY YEAR     X           X               X


# def get_new_maintenanceUID(conn):
#     print("In new UID request")
    
#     with connect() as db:
#         newMaintenanceQuery = db.execute("CALL space.new_request_uid;", "get", conn)
#         if newMaintenanceQuery["code"] == 280:
#             return newMaintenanceQuery["result"][0]["new_id"]
#     return "Could not generate new property UID", 500


def updateImages(imageFiles, maintenance_request_uid):
    content = []

    for filename in imageFiles:

        if type(imageFiles[filename]) == str:

            bucket = 'io-pm'
            key = imageFiles[filename].split('/io-pm/')[1]
            data = s3.get_object(
                Bucket=bucket,
                Key=key
            )
            imageFiles[filename] = data['Body']
            content.append(data['ContentType'])
        else:
            content.append('')

    s3Resource = boto3.resource('s3')
    bucket = s3Resource.Bucket('io-pm')
    bucket.objects.filter(
        Prefix=f'maintenanceRequests/{maintenance_request_uid}/').delete()
    images = []
    for i in range(len(imageFiles.keys())):

        filename = f'img_{i-1}'
        if i == 0:
            filename = 'img_cover'
        key = f'maintenanceRequests/{maintenance_request_uid}/{filename}'
        image = uploadImage(
            imageFiles[filename], key, content[i])

        images.append(image)
    return images


class MaintenanceByProperty(Resource):
    def get(self, property_id):
        print('in Maintenance By Property')
        response = {}
        
        # print("Property UID: ", property_id)

        with connect() as db:
            print("in connect loop")
            maintenanceQuery = db.execute(""" 
                    -- MAINTENANCE PROJECTS BY PROPERTY        
                    SELECT -- *
                        property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type, property_num_beds, property_num_baths, property_area
                        , maintenance_request_uid, maintenance_property_id, maintenance_request_status, maintenance_title, maintenance_desc, maintenance_images, maintenance_request_type, maintenance_request_created_by
                        , maintenance_priority, maintenance_can_reschedule, maintenance_assigned_business, maintenance_assigned_worker, maintenance_scheduled_date, maintenance_scheduled_time, maintenance_frequency
                        , maintenance_notes, maintenance_request_created_date, maintenance_request_closed_date, maintenance_request_adjustment_date, maintenance_callback_number, maintenance_estimated_cost
                    FROM space.properties
                    LEFT JOIN space.maintenanceRequests ON maintenance_property_id = property_uid
                    WHERE property_uid = \'""" + property_id + """\';
                    """)
            

            # print("Query: ", maintenanceQuery)
            response["MaintenanceProjects"] = maintenanceQuery
            return response


class MaintenanceRequests(Resource):
    def get(self, uid):
        print('in Maintenance Request')
        response = {}

        print("UID: ", uid)

        if uid[:3] == '110':
            print("In Owner ID")

            with connect() as db:
                print("in connect loop")
                maintenanceRequests = db.execute(""" 
                        -- MAINTENANCE REQUEST BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT   * -- bill_property_id,  maintenance_property_id,
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
                return mapMaintenanceForOwner(maintenanceRequests)

            response["maintenanceRequests"] = maintenanceRequests
            return response


        elif uid[:3] == '350':
            print("In Tenant ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceRequests = db.execute(""" 
                        -- MAINTENANCE REQUEST BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT   * -- bill_property_id,  maintenance_property_id,
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
                return mapMaintenanceForTenant(maintenanceRequests)

            response["maintenanceRequests"] = maintenanceRequests
            return response

        elif uid[:3] == '200':
            print("In Property ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceRequests = db.execute(""" 
                        -- MAINTENANCE REQUEST BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT   * -- bill_property_id,  maintenance_property_id,
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
                return mapMaintenanceForProperty(maintenanceRequests)

            response["maintenanceRequests"] = maintenanceRequests
            return response

        else:
            print("UID Not found")
            response["MaintenanceStatus"] = "UID Not Found"
            return response

    def post(self):
        print("In Maintenace Requests")
        response = {}
        with connect() as db:
            data = request.form
            fields = [
                'maintenance_property_id'
                , 'maintenance_title'
                , 'maintenance_desc'
                , 'maintenance_request_type'
                , 'maintenance_request_created_by'
                , 'maintenance_priority'
                , 'maintenance_can_reschedule'
                , 'maintenance_assigned_business'
                , 'maintenance_assigned_worker'
                , 'maintenance_scheduled_date'
                , 'maintenance_scheduled_time'
                , 'maintenance_frequency'
                , 'maintenance_notes'
                , 'maintenance_request_created_date'
                , 'maintenance_request_closed_date'
                , 'maintenance_request_adjustment_date'
                , 'maintenance_callback_number'
                , 'maintenance_estimated_cost'
            ]

            newRequest = {}
            for field in fields:
                newRequest[field] = data.get(field, None)
                # print(field, " = ", newRequest[field])


            # # GET NEW UID
            print("Get New Request UID")
            newRequestID = db.call('new_request_uid')['result'][0]['new_id']
            newRequest['maintenance_request_uid'] = newRequestID
            print(newRequestID)

            images = []
            i = -1
            # WHILE WHAT IS TRUE?
            while True:
                print("In while loop")
                filename = f'img_{i}'
                # print("Filename: ", filename)
                if i == -1:
                    filename = 'img_cover'
                file = request.files.get(filename)
                # print("File: ", file)
                if file:
                    key = f'maintenanceRequests/{newRequestID}/{filename}'
                    image = uploadImage(file, key, '')
                    images.append(image)
                else:
                    break
                i += 1
            newRequest['maintenance_images'] = json.dumps(images)
            # print("Images to be uploaded: ", newRequest['maintenance_images'])

            newRequest['maintenance_request_status'] = 'NEW'
            # newRequest['frequency'] = 'One time'
            # newRequest['can_reschedule'] = False

            # print(newRequest)

            response = db.insert('maintenanceRequests', newRequest)
            response['Maintenance_UID'] = newRequestID
            response['images'] = newRequest['maintenance_images']

        return response
    
    def put(self):
        response = {}
        payload = request.form
        print(payload)
        if payload.get('maintenance_request_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'maintenance_request_uid': payload['maintenance_request_uid']}
        maintenanceRequests = {k: v for k, v in payload.items()}
        with connect() as db:
            print(key, payload)
            response = db.update('maintenanceRequests', key, maintenanceRequests)
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
                                        maintenance_quote_uid, quote_maintenance_request_id, quote_status, quote_pm_notes, quote_business_id, quote_services_expenses, quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date, quote_docs
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
                                        LEFT JOIN (
                                            SELECT qd_quote_id, JSON_ARRAYAGG(JSON_OBJECT
                                                ('qd_uid', qd_uid,
                                                'qd_type', qd_type,
                                                'qd_name', qd_name,
                                                'qd_description', qd_description,
                                                'qd_created_date', qd_created_date,
                                                'qd_shared', qd_shared,
                                                'qd_link', qd_link
                                                )) AS quote_docs
                                                FROM space.quoteDocuments
                                                GROUP BY qd_quote_id) as q ON qd_quote_id = maintenance_quote_uid
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
            with connect() as db:
                businessQuery = db.execute(""" 
                                        -- MAINTENANCE QUOTES
                                        SELECT -- *, 
                                        maintenance_quote_uid, quote_maintenance_request_id, quote_status, quote_pm_notes, quote_business_id, quote_services_expenses, quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date, quote_docs
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
                                        LEFT JOIN (
                                            SELECT qd_quote_id, JSON_ARRAYAGG(JSON_OBJECT
                                                ('qd_uid', qd_uid,
                                                'qd_type', qd_type,
                                                'qd_name', qd_name,
                                                'qd_description', qd_description,
                                                'qd_created_date', qd_created_date,
                                                'qd_shared', qd_shared,
                                                'qd_link', qd_link
                                                )) AS quote_docs
                                                FROM space.quoteDocuments
                                                GROUP BY qd_quote_id) as q ON qd_quote_id = maintenance_quote_uid
                                        LEFT JOIN space.properties ON maintenance_property_id = property_uid 
                                        LEFT JOIN space.property_owner ON property_id = property_uid
                                        LEFT JOIN space.ownerProfileInfo ON property_owner_id = owner_uid
                                        LEFT JOIN space.contracts ON contract_property_id = property_uid 
                                        LEFT JOIN space.businessProfileInfo AS pm_business_info ON contract_business_id = pm_business_info.business_uid
                                        LEFT JOIN space.businessProfileInfo AS maintenance_info ON quote_business_id = maintenance_info.business_uid
                                        -- WHERE owner_uid = \'""" + uid + """\';
                                        WHERE space.pm_business_info.business_uid = \'""" + uid + """\';
                                        -- WHERE space.pm_business_info.business_uid = '600-000003';
                                        """)
            response["maintenanceQuotes"] = businessQuery

        elif uid[:3] == '350':
            with connect() as db:
                tenantQuery = db.execute(""" 
                                        -- MAINTENANCE QUOTES
                                        SELECT -- *,
											maintenance_quote_uid, quote_maintenance_request_id, quote_status, quote_pm_notes, quote_business_id, quote_services_expenses, quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date, quote_docs
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
                                        LEFT JOIN (
                                            SELECT qd_quote_id, JSON_ARRAYAGG(JSON_OBJECT
                                                ('qd_uid', qd_uid,
                                                'qd_type', qd_type,
                                                'qd_name', qd_name,
                                                'qd_description', qd_description,
                                                'qd_created_date', qd_created_date,
                                                'qd_shared', qd_shared,
                                                'qd_link', qd_link
                                                )) AS quote_docs
                                                FROM space.quoteDocuments
                                                GROUP BY qd_quote_id) as q ON qd_quote_id = maintenance_quote_uid
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
        response = []
        payload = request.form
        quote_maintenance_request_id = payload.get("quote_maintenance_request_id")
        quote_maintenance_contacts = payload.get("quote_maintenance_contacts").split(',')
        # print("Contacts: ", quote_maintenance_contacts, type(quote_maintenance_contacts))
        quote_pm_notes = payload["quote_pm_notes"]
        today = datetime.today().strftime('%m-%d-%Y %H:%M:%S')
        with connect() as db:
            for quote_business_id in quote_maintenance_contacts:
                # print("Business ID: ", quote_business_id)
                quote = {}
                quote["maintenance_quote_uid"] = db.call('space.new_quote_uid')['result'][0]['new_id']
                quote["quote_business_id"] = quote_business_id
                quote["quote_maintenance_request_id"] = quote_maintenance_request_id
                quote["quote_status"] = "REQUESTED"
                quote["quote_pm_notes"] = quote_pm_notes
                quote["quote_created_date"] = today

                # images = []
                # i = 0
                # while True:
                #     filename = f'img_{i}'
                #     file = request.files.get(filename)
                #     if file:
                #         key = f'maintenanceQuotes/{quote["maintenance_quote_uid"]}/{filename}'
                #         image = uploadImage(file, key, '')
                #         images.append(image)
                #     else:
                #         break
                #     i += 1

                images = []
                i = 0  # Start index from 0 for img_0
                while True:
                    print("In while loop")
                    if i == 0:
                        filename = 'img_cover'
                    else:
                        filename = f'img_{i - 1}'  # Adjust the filename for img_0, img_1, ...
                    file = request.files.get(filename)
                    if file:
                        key = f'maintenanceQuotes/{quote["maintenance_quote_uid"]}/{filename}'
                        image = uploadImage(file, key, '')
                        images.append(image)
                    else:
                        break
                    i += 1

                quote["quote_maintenance_images"] = json.dumps(images)
                query_response = db.insert('maintenanceQuotes', quote)
                response.append(query_response)

        return response

    def put(self):
        print('in MaintenanceQuotes')
        payload = request.form
        if payload.get('maintenance_quote_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'maintenance_quote_uid': payload['maintenance_quote_uid']}
        print("Key: ", key)
        quote = {k: v for k, v in payload.items()}
        print("KV Pairs: ", quote)

        with connect() as db:
            query = db.select('maintenanceQuotes', key)
        try:
            quote_from_db = query.get('result')[0]
            images = quote_from_db.get("quote_maintenance_images")
            images = ast.literal_eval(images) if images else []  # convert to list of URLs
            print('type: ', type(images))
            print(f'previously saved images: {images}')
        except IndexError as e:
            print(e)
            raise BadRequest("Request failed, no such Maintenance Quote in the database.")

        i = -1
        s3_i = len(images) if len(images) else -1
        # WHILE WHAT IS TRUE?
        while True:
            print("In while loop")
            filename = f'img_{i}'
            s3_filename = f'img_{s3_i}'
            # print("Filename: ", filename)
            if i == -1:
                filename = 'img_cover'
                s3_filename = f'img_cover'
            file = request.files.get(filename)
            # print("File: ", file)
            if file:
                S3key = f'maintenanceQuotes/{quote["maintenance_quote_uid"]}/{s3_filename}'
                print("S3 Key: ", S3key)
                image = uploadImage(file, S3key, '')
                images.append(image)
            else:
                break
            i += 1
            s3_i += 1

        print("Images: ",images)    
        quote["quote_maintenance_images"] = json.dumps(images)

        with connect() as db:
            print("In actual PUT")
            response = db.update('maintenanceQuotes', key, quote)
        return response


class MaintenanceQuotesByUid(Resource):
    def get(self, maintenance_quote_uid):
        print('in MaintenanceQuotesByUid')
        with connect() as db:
            response = db.select('maintenanceQuotes', {"maintenance_quote_uid": maintenance_quote_uid})
        return response


class MaintenanceStatus(Resource): 
    def get(self, uid):
        print('in Maintenance Status')
        response = {}

        print("UID: ", uid)

        if uid[:3] == '110':
            print("In Owner ID")

            with connect() as db:
                # print("in connect loop")
                maintenanceStatus = db.execute(""" 
                        -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT * -- bill_property_id,  maintenance_property_id,
                        FROM space.m_details
                        LEFT JOIN space.properties ON property_uid = maintenance_property_id
                        LEFT JOIN (
                            SELECT -- *
                                bill_uid, bill_timestamp, bill_created_by, bill_description, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                                , sum(bill_amount) AS bill_amount
                            FROM space.bills
                            GROUP BY bill_maintenance_quote_id
                            ) as b ON bill_maintenance_quote_id = maintenance_quote_uid
                        LEFT JOIN space.pp_status ON pur_bill_id = bill_uid AND pur_property_id = maintenance_property_id
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                        LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
                        LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                        WHERE owner_uid = \'""" + uid + """\'
                        -- WHERE business_uid = \'""" + uid + """\'
                        -- WHERE tenant_uid = \'""" + uid + """\'
                        -- WHERE quote_business_id = \'""" + uid + """\'
                        -- WHERE maintenance_property_id = = \'""" + uid + """\'
                        ORDER BY maintenance_request_created_date;
                        """)

            if maintenanceStatus.get('code') == 200:
                return mapMaintenanceForOwner(maintenanceStatus)

            response["MaintenanceStatus"] = maintenanceStatus
            return response
        
        elif uid[:3] == '600':
            print("In Business ID")

            # CHECK IF MAINTENANCE OR MANAGEMENT
            with connect() as db:
                # print("in check loop")
                businessType = db.execute(""" 
                        -- CHECK BUSINESS TYPE
                        SELECT -- *
                            business_uid, business_type
                        FROM space.businessProfileInfo
                        WHERE business_uid = \'""" + uid + """\';
                        """)
            
            # print(businessType)
            print(businessType["result"])
            # print(businessType["result"][0]["business_type"])

            # if businessType["result"] == "()":
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
                                -- quote_business_id, quote_status, maintenance_request_status, quote_total_estimate
                                , CASE
                                        WHEN maintenance_request_status = 'NEW' OR maintenance_request_status = 'INFO'       THEN "NEW REQUEST"
                                        WHEN maintenance_request_status = "SCHEDULED"                                        THEN "SCHEDULED"
                                        WHEN maintenance_request_status = 'CANCELLED' or quote_status = "FINISHED"           THEN "COMPLETED"
                                        WHEN quote_status = "SENT" OR quote_status = "REFUSED" OR quote_status = "REQUESTED"
                                        OR quote_status = "REJECTED" OR quote_status = "WITHDRAWN"                         THEN "QUOTES REQUESTED"
                                        WHEN quote_status = "ACCEPTED" OR quote_status = "SCHEDULE"                          THEN "QUOTES ACCEPTED"
                                        WHEN quote_status = "COMPLETED"                                                      THEN "PAID"     
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
                                        SELECT maintenance_quote_uid, 
                                            quote_maintenance_request_id AS qmr_id
                                            -- , quote_status
                                            , MAX(quote_rank) AS max_quote_rank
                                        FROM (
                                            SELECT -- *,
                                                maintenance_quote_uid, quote_maintenance_request_id, quote_status,
                                                -- , quote_pm_notes, quote_business_id, quote_services_expenses, quote_earliest_availability, quote_event_type, quote_event_duration, quote_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
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
                            LEFT JOIN (
                            SELECT -- *
                                bill_uid, bill_timestamp, bill_created_by, bill_description, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                                , sum(bill_amount) AS bill_amount
                            FROM space.bills
                            GROUP BY bill_maintenance_quote_id
                            ) as b ON bill_maintenance_quote_id = quotes.maintenance_quote_uid
                            LEFT JOIN space.pp_status ON pur_bill_id = bill_uid AND pur_property_id = maintenance_property_id
                            LEFT JOIN space.properties ON property_uid = maintenance_property_id
                            LEFT JOIN space.o_details ON maintenance_property_id = property_id
                            LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                            LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
                            LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                            WHERE business_uid = \'""" + uid + """\'
                            -- WHERE business_uid = '600-000032'
                            ORDER BY maintenance_request_created_date;
                            """)

                if maintenanceStatus.get('code') == 200:
                    return mapMaintenanceForPropertyManager(maintenanceStatus)

                response["MaintenanceStatus"] = maintenanceStatus
                return response
            
            elif businessType["result"][0]["business_type"] == "MAINTENANCE":

                with connect() as db:
                    print("in MAINTENANCE")
                    maintenanceStatus = db.execute(""" 
                             -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                            SELECT * -- bill_property_id,  maintenance_property_id,
                            , CASE
                                        WHEN quote_status = "REQUESTED"                                                      								THEN "REQUESTED"
                                        WHEN quote_status = "SENT" OR quote_status = "REFUSED" OR quote_status = "REJECTED" OR quote_status = "WITHDRAWN"  	THEN "SUBMITTED"
                                        WHEN quote_status = "ACCEPTED" OR quote_status = "SCHEDULE"                          								THEN "ACCEPTED"
                                        WHEN quote_status = "SCHEDULED" OR quote_status = "RESCHEDULE"                       								THEN "SCHEDULED"
                                        WHEN quote_status = "FINISHED"                                                       								THEN "FINISHED"
                                        WHEN quote_status = "COMPLETED"                                                      								THEN "PAID"   
                                        WHEN quote_status = "CANCELLED" OR quote_status = "ARCHIVE"                       									THEN "ARCHIVE"
                                        ELSE quote_status
                                    END AS maintenance_status
                            FROM space.m_details
                            LEFT JOIN space.properties ON property_uid = maintenance_property_id
                            LEFT JOIN (
                                SELECT -- *
                                    bill_uid, bill_timestamp, bill_created_by, bill_description, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                                    , sum(bill_amount) AS bill_amount
                                FROM space.bills
                                GROUP BY bill_maintenance_quote_id
                                ) as b ON bill_maintenance_quote_id = maintenance_quote_uid
                            LEFT JOIN space.pp_status ON pur_bill_id = bill_uid AND pur_property_id = maintenance_property_id
                            LEFT JOIN space.o_details ON maintenance_property_id = property_id
                            LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                            LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
                            LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                            -- WHERE owner_uid = \'""" + uid + """\'
                            -- WHERE business_uid = \'""" + uid + """\'
                            -- WHERE tenant_uid = \'""" + uid + """\'
                            WHERE quote_business_id = \'""" + uid + """\'
                            -- WHERE quote_business_id = "600-000033"
                            -- WHERE maintenance_property_id = \'""" + uid + """\'
                            ORDER BY maintenance_request_created_date;
                            """)

                if maintenanceStatus.get('code') == 200:
                    return mapMaintenanceForMaintenance(maintenanceStatus)

                response["MaintenanceStatus"] = maintenanceStatus
                return response
            
            else:
                print("BUSINESS TYPE Not found")
                response["MaintenanceStatus"] = "BUSINESS TYPE Not Found"
                return response
        
        elif uid[:3] == '350':
            print("In Tenant ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceStatus = db.execute(""" 
                        -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT * -- bill_property_id,  maintenance_property_id,
                        FROM space.m_details
                        LEFT JOIN space.properties ON property_uid = maintenance_property_id
                        LEFT JOIN (
                            SELECT -- *
                                bill_uid, bill_timestamp, bill_created_by, bill_description, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                                , sum(bill_amount) AS bill_amount
                            FROM space.bills
                            GROUP BY bill_maintenance_quote_id
                            ) as b ON bill_maintenance_quote_id = maintenance_quote_uid
                        LEFT JOIN space.pp_status ON pur_bill_id = bill_uid AND pur_property_id = maintenance_property_id
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                        LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
                        LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                        -- WHERE owner_uid = \'""" + uid + """\'
                        -- WHERE business_uid = \'""" + uid + """\'
                        WHERE tenant_uid = \'""" + uid + """\'
                        -- WHERE quote_business_id = \'""" + uid + """\'
                        -- WHERE maintenance_property_id = \'""" + uid + """\'
                        ORDER BY maintenance_request_created_date;
                        """)

            if maintenanceStatus.get('code') == 200:
                return mapMaintenanceForTenant(maintenanceStatus)

            response["MaintenanceStatus"] = maintenanceStatus
            return response

        elif uid[:3] == '200':
            print("In Property ID")
            
            with connect() as db:
                print("in connect loop")
                maintenanceStatus = db.execute(""" 
                        -- MAINTENANCE STATUS BY OWNER, BUSINESS, TENENT OR PROPERTY
                        SELECT * -- bill_property_id,  maintenance_property_id,
                        FROM space.m_details
                        LEFT JOIN space.properties ON property_uid = maintenance_property_id
                        LEFT JOIN (
                            SELECT -- *
                                bill_uid, bill_timestamp, bill_created_by, bill_description, bill_utility_type, bill_split, bill_property_id, bill_docs, bill_maintenance_quote_id, bill_notes
                                , sum(bill_amount) AS bill_amount
                            FROM space.bills
                            GROUP BY bill_maintenance_quote_id
                            ) as b ON bill_maintenance_quote_id = maintenance_quote_uid
                        LEFT JOIN space.pp_status ON pur_bill_id = bill_uid AND pur_property_id = maintenance_property_id
                        LEFT JOIN space.o_details ON maintenance_property_id = property_id
                        LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                        LEFT JOIN (SELECT * FROM space.leases WHERE lease_status = "ACTIVE") AS l ON maintenance_property_id = lease_property_id
                        LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                        -- WHERE owner_uid = \'""" + uid + """\'
                        -- WHERE business_uid = \'""" + uid + """\'
                        -- WHERE tenant_uid = \'""" + uid + """\'
                        -- WHERE quote_business_id = \'""" + uid + """\'
                        WHERE maintenance_property_id = \'""" + uid + """\'
                        ORDER BY maintenance_request_created_date;
                        """)

            if maintenanceStatus.get('code') == 200:
                return mapMaintenanceForProperty(maintenanceStatus)

            response["MaintenanceStatus"] = maintenanceStatus
            return response

        else:
            print("UID Not found")
            response["MaintenanceStatus"] = "UID Not Found"
            return response
