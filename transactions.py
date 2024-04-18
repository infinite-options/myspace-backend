
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar


# OVERVIEW
#           TENANT      OWNER     PROPERTY MANAGER     
# ALL         Y           Y               Y
# BY MONTH    X           X               X
# BY YEAR     X           X               X


class AllTransactions(Resource):
    # decorators = [jwt_required()]

    def get(self, uid):
        print('in Maintenance Request')
        response = {}

        print("UID: ", uid)

        if uid[:3] == '110':
            print("In Owner ID")
            with connect() as db:
                queryResponse = (""" 
                            -- ALL TRANSACTIONS
                            SELECT -- *,
                                purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description
                                , pur_receiver, pur_initiator, pur_payer
                                , pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_year
                                , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email
                                , initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email
                                , payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email
                                , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type -- , property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image
                                , property_id, property_owner_id, po_owner_percent
                                , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email -- , owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip
                                , owner_photo_url
                                , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date -- , contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email -- , business_ein_number, business_services_fees, business_locations, business_paypal, business_apple_pay, business_zelle, business_venmo, business_account_number, business_routing_number, business_documents, business_address, business_unit, business_city, business_state, business_zip
                                , business_photo_url
                                , lease_uid, lease_property_id, lease_start, lease_end, lease_status -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_actual_rent
                                -- , leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees
                                , lt_lease_id, lt_tenant_id, lt_responsibility
                                , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_current_address-DNU
                                , tenant_photo_url
                            FROM space.pp_details
                            WHERE pur_receiver=\'""" + uid + """\' 
                                OR pur_payer=\'""" + uid + """\'
                                OR property_owner_id =\'""" + uid + """\'
                            """)
                response = db.execute(queryResponse)
                
            return response

        elif uid[:3] == '600':
            print("In Business ID")
            with connect() as db:
                queryResponse = (""" 
                            -- ALL TRANSACTIONS
                            SELECT -- *,
                                purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description
                                , pur_receiver, pur_initiator, pur_payer
                                , pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_year
                                , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email
                                , initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email
                                , payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email
                                , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type -- , property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image
                                , property_id, property_owner_id, po_owner_percent
                                , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email -- , owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip
                                , owner_photo_url
                                , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date -- , contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email -- , business_ein_number, business_services_fees, business_locations, business_paypal, business_apple_pay, business_zelle, business_venmo, business_account_number, business_routing_number, business_documents, business_address, business_unit, business_city, business_state, business_zip
                                , business_photo_url
                                , lease_uid, lease_property_id, lease_start, lease_end, lease_status -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_actual_rent
                                -- , leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees
                                , lt_lease_id, lt_tenant_id, lt_responsibility
                                , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_current_address-DNU
                                , tenant_photo_url
                            FROM space.pp_details
                            WHERE pur_receiver=\'""" + uid + """\' 
                                OR pur_payer=\'""" + uid + """\'
                                OR business_uid =\'""" + uid + """\'
                            """)
                response = db.execute(queryResponse)

            return response

