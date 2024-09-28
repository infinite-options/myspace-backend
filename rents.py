
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity


from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar

from queries import testQuery, RentStatusQuery, RentDetailsQuery



# NOT SURE I NEED THIS
# def get_new_paymentUID(conn):
#     print("In new UID request")
#     with connect() as db:
#         newPaymentQuery = db.execute("CALL space.new_payment_uid;", "get", conn)
#         if newPaymentQuery["code"] == 280:
#             return newPaymentQuery["result"][0]["new_id"]
#     return "Could not generate new payment UID", 500


class RentTest(Resource):
    def get(self, user_id):
        print("in Get Rent TEST Status")
        response = {}

        response["testStatus"] = testQuery(user_id)
        return response
 

class Rents(Resource):
    def get(self, user_id):
        print("in Get Rent Status")
        response = {}

        response["RentStatus"] = RentStatusQuery(user_id)
        return response



# class Rents2(Resource):
#     def get(self, uid):
#         print("in Get Rent Status")

#         response = {}

#         # print("Property UID: ", property_id)

#         if uid[:3] == '110':
#             print("In Owner ID")
#             with connect() as db:
#                 # print("in connect loop")
#                 rentsQuery = db.execute("""  
#                     -- PROPERTY RENT STATUS FOR RENTS PAGE
#                     SELECT -- *,
#                         IF(ISNULL(cf_month), CONCAT(property_uid, "-VACANT"), CONCAT(property_uid, "-", cf_month, "-", cf_year)) AS rent_detail_index
#                         , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_favorite_image
#                         , owner_uid, contract_uid, contract_status, business_uid, lease_uid, lease_start, lease_end, lease_status, tenant_uid, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , rent_status
#                         , pur_property_id, purchase_type, pur_due_date, pur_amount_due
#                         , if(ISNULL(pur_status_value), "0", pur_status_value) AS pur_status_value
#                         , if(ISNULL(purchase_status), "UNPAID", purchase_status) AS purchase_status
#                         , pur_description, cf_month, cf_year
#                         , CASE
#                             WHEN ISNULL(contract_uid) THEN "NO MANAGER"
#                             WHEN ISNULL(lease_status) THEN "VACANT"
#                             WHEN ISNULL(purchase_status) THEN "UNPAID"
#                             ELSE purchase_status
#                             END AS rent_status 
#                     FROM (
#                         -- Find number of properties
#                         SELECT -- *,
#                                 property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip -- , property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby
#                                 , property_favorite_image
#                                 -- , po_owner_percent, po_start_date, po_end_date
#                                 , owner_uid -- , owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
#                                 , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name
#                                 , contract_status -- , contract_early_end_date
#                                 , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
#                                 , lease_uid, lease_start, lease_end
#                                 , lease_status -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
#                                 , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
#                                 -- , if(ISNULL(lease_status), "VACANT", lease_status) AS rent_status 
#                         FROM space.p_details
#                         -- WHERE tenant_uid = '350-000002'
#                         -- WHERE business_uid = "600-000003"
#                         -- WHERE owner_uid = "110-000003"
#                         WHERE owner_uid = \'""" + uid + """\'
#                         -- WHERE business_uid = \'""" + uid + """\'
#                         -- WHERE tenant_uid = \'""" + uid + """\'  
#                         ) AS p
#                     -- Link to rent status
#                     LEFT JOIN (
#                         SELECT  -- *,
#                             pur_property_id
#                             , purchase_type
#                             , pur_due_date
#                             , SUM(pur_amount_due) AS pur_amount_due
#                             , MIN(pur_status_value) AS pur_status_value
#                             , CASE
#                                     WHEN MIN(pur_status_value) = 0 THEN "UNPAID"
#                                     WHEN MIN(pur_status_value) = 1 THEN "PARTIALLY PAID"
#                                     WHEN MIN(pur_status_value) = 4 THEN "PAID LATE"
#                                     WHEN MIN(pur_status_value) = 5 THEN "PAID"
#                                     ELSE purchase_status
#                                 END AS purchase_status
#                             , pur_description
#                             , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_month
#                             , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_year
#                         FROM space.purchases
#                         WHERE purchase_type = "Rent"
#                             AND LEFT(pur_payer, 3) = '350'
#                             AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = MONTH(CURRENT_DATE)
#                             AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = YEAR(CURRENT_DATE)
#                         GROUP BY pur_property_id, purchase_type
#                         ) AS pp
#                         ON property_uid = pur_property_id;
#                     """)

#             # print("Query: ", propertiesQuery)
#             response["RentStatus"] = rentsQuery
#             return response
        
#         elif uid[:3] == '600':
#             print("In Business ID")
#             with connect() as db:
#                 # print("in connect loop")
#                 rentsQuery = db.execute(""" 
#                     -- PROPERTY RENT STATUS FOR RENTS PAGE
#                     SELECT -- *,
#                         IF(ISNULL(cf_month), CONCAT(property_uid, "-VACANT"), CONCAT(property_uid, "-", cf_month, "-", cf_year)) AS rent_detail_index
#                         , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_favorite_image
#                         , owner_uid, contract_uid, contract_status, business_uid, lease_uid, lease_start, lease_end, lease_status, tenant_uid, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , rent_status
#                         , pur_property_id, purchase_type, pur_due_date, pur_amount_due
#                         , if(ISNULL(pur_status_value), "0", pur_status_value) AS pur_status_value
#                         , if(ISNULL(purchase_status), "UNPAID", purchase_status) AS purchase_status
#                         , pur_description, cf_month, cf_year
#                         , CASE
#                             WHEN ISNULL(contract_uid) THEN "NO MANAGER"
#                             WHEN ISNULL(lease_status) THEN "VACANT"
#                             WHEN ISNULL(purchase_status) THEN "UNPAID"
#                             ELSE purchase_status
#                             END AS rent_status 
#                     FROM (
#                         -- Find number of properties
#                         SELECT -- *,
#                                 property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip -- , property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby
#                                 , property_favorite_image
#                                 -- , po_owner_percent, po_start_date, po_end_date
#                                 , owner_uid -- , owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
#                                 , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name
#                                 , contract_status -- , contract_early_end_date
#                                 , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
#                                 , lease_uid, lease_start, lease_end
#                                 , lease_status -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
#                                 , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
#                                 -- , if(ISNULL(lease_status), "VACANT", lease_status) AS rent_status 
#                         FROM space.p_details
#                         -- WHERE tenant_uid = '350-000002'
#                         -- WHERE business_uid = "600-000003"
#                         -- WHERE owner_uid = "110-000003"
#                         -- WHERE owner_uid = \'""" + uid + """\'
#                         WHERE business_uid = \'""" + uid + """\'
#                         -- WHERE tenant_uid = \'""" + uid + """\'  
#                         ) AS p
#                     -- Link to rent status
#                     LEFT JOIN (
#                         SELECT  -- *,
#                             pur_property_id
#                             , purchase_type
#                             , pur_due_date
#                             , SUM(pur_amount_due) AS pur_amount_due
#                             , MIN(pur_status_value) AS pur_status_value
#                             , CASE
#                                     WHEN MIN(pur_status_value) = 0 THEN "UNPAID"
#                                     WHEN MIN(pur_status_value) = 1 THEN "PARTIALLY PAID"
#                                     WHEN MIN(pur_status_value) = 4 THEN "PAID LATE"
#                                     WHEN MIN(pur_status_value) = 5 THEN "PAID"
#                                     ELSE purchase_status
#                                 END AS purchase_status
#                             , pur_description
#                             , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_month
#                             , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_year
#                         FROM space.purchases
#                         WHERE purchase_type = "Rent"
#                             AND LEFT(pur_payer, 3) = '350'
#                             AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = MONTH(CURRENT_DATE)
#                             AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = YEAR(CURRENT_DATE)
#                         GROUP BY pur_property_id, purchase_type
#                         ) AS pp
#                         ON property_uid = pur_property_id;
#                     """) 

#             # print("Query: ", propertiesQuery)
#             response["RentStatus"] = rentsQuery
#             return response
        
#         # elif uid[:3] == '350':
#         #     print("In Tenant ID")
            

#         #     # print("Query: ", propertiesQuery)
#         #     response["Property"] = propertiesQuery
#         #     return response
        
#         else:
#             print("UID Not found")
#             response["RentStatus"] = "UID Not Found"
#             return response


class RentDetails(Resource):
    def get(self, user_id):
        print("in Get Rent Details")
        response = {}

        response["RentStatus"] = RentDetailsQuery(user_id)
        return response


# class RentDetails2(Resource):
#     def get(self, uid):
#         print("in Get Rent Details")

#         response = {}

#         # print("Property UID: ", property_id)

#         with connect() as db:
#             print("in connect loop")
#             if uid[:3] == '110':
#                 rentQuery = db.execute(""" 
#                             -- PROPERTY RENT DETAILS FOR RENT DETAILS PAGE
#                             SELECT -- *,
#                                 IF(ISNULL(cf_month), CONCAT(property_uid, "-VACANT"), CONCAT(property_uid, "-", cf_month, "-", cf_year, "-", purchase_uid)) AS rent_detail_index
#                                 , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_favorite_image
#                                 , owner_uid, contract_uid, contract_status, business_uid, lease_uid, lease_start, lease_end, lease_status, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , rent_status
#                                 , pur_property_id, purchase_type, pur_notes, pur_due_date, pur_amount_due
#                                 , latest_date, total_paid, amt_remaining
#                                 , if(ISNULL(pur_status_value), "0", pur_status_value) AS pur_status_value
#                                 , if(ISNULL(purchase_status), "UNPAID", purchase_status) AS purchase_status
#                                 , pur_description, cf_month, cf_year
#                                 , CASE
#                                     WHEN ISNULL(contract_uid) THEN "NO MANAGER"
#                                     WHEN ISNULL(lease_status) THEN "VACANT"
#                                     WHEN ISNULL(purchase_status) THEN "UNPAID"
#                                     ELSE purchase_status
#                                     END AS rent_status
#                                 , lf_purchase_uid
#                                 , lf_purchase_type
#                                 , lf_pur_due_date
#                                 , lf_pur_amount_due
#                                 , lf_pur_status_value
#                                 , lf_purchase_status
#                                 , lf_latest_date
#                                 , lf_total_paid
#                                 , lf_amt_remaining
#                                 , lf_cf_month
#                                 , lf_cf_year
#                             FROM (
#                                 -- Find number of properties
#                                 SELECT -- *,
#                                         property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip -- , property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby
#                                         , property_favorite_image
#                                         -- , po_owner_percent, po_start_date, po_end_date
#                                         , owner_uid -- , owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
#                                         , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name
#                                         , contract_status -- , contract_early_end_date
#                                         , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
#                                         , lease_uid, lease_start, lease_end
#                                         , lease_status -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
#                                         , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
#                                         -- , if(ISNULL(lease_status), "VACANT", lease_status) AS rent_status 
#                                 FROM space.p_details
#                                 -- WHERE business_uid = "600-000003"
#                                 -- WHERE owner_uid = "110-000003"
#                                 WHERE owner_uid = \'""" + uid + """\'
#                                 -- WHERE business_uid = \'""" + uid + """\'
#                                 -- WHERE tenant_uid = \'""" + uid + """\'  
#                                 ) AS p
#                             -- Link to rent status
#                             LEFT JOIN (
#                                 SELECT  -- *,
#                                     pur_property_id
#                                     , purchase_uid
#                                     , purchase_type
#                                     , pur_notes
#                                     , pur_due_date
#                                     , SUM(pur_amount_due) AS pur_amount_due
#                                     , MIN(pur_status_value) AS pur_status_value
#                                     , CASE
#                                             WHEN MIN(pur_status_value) = 0 THEN "UNPAID"
#                                             WHEN MIN(pur_status_value) = 1 THEN "PARTIALLY PAID"
#                                             WHEN MIN(pur_status_value) = 4 THEN "PAID LATE"
#                                             WHEN MIN(pur_status_value) = 5 THEN "PAID"
#                                             ELSE purchase_status
#                                         END AS purchase_status
#                                     , pur_description
#                                     , latest_date, total_paid, amt_remaining
#                                     , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_month
#                                     , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_year
#                                     , lf_purchase_uid
#                                     , lf_purchase_type
#                                     , lf_pur_due_date
#                                     , lf_pur_amount_due
#                                     , lf_pur_status_value
#                                     , lf_purchase_status
#                                     , lf_latest_date
#                                     , lf_total_paid
#                                     , lf_amt_remaining
#                                     , lf_cf_month
#                                     , lf_cf_year

#                                 FROM (
#                                     SELECT  *
#                                     FROM space.pp_status -- space.purchases
#                                     WHERE LEFT(pur_payer, 3) = '350'
#                                         -- AND purchase_type = "Rent"
#                                 ) AS ppr
#                                 LEFT JOIN (
#                                     SELECT  pur_description AS lf_purchase_uid
#                                         , purchase_type AS lf_purchase_type
#                                         , pur_due_date AS lf_pur_due_date
#                                         , pur_amount_due AS lf_pur_amount_due
#                                         , pur_status_value AS lf_pur_status_value
#                                         , purchase_status AS lf_purchase_status
#                                         , latest_date AS lf_latest_date
#                                         , total_paid AS lf_total_paid
#                                         , amt_remaining AS lf_amt_remaining
#                                         , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS lf_cf_month
#                                         , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS lf_cf_year
#                                     FROM space.pp_status -- space.purchases
#                                     WHERE LEFT(pur_payer, 3) = '350'
#                                         AND purchase_type = "Late Fee"
#                             ) AS lf ON purchase_uid = lf_purchase_uid
#                             -- 		AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = MONTH(CURRENT_DATE)
#                             -- 		AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = YEAR(CURRENT_DATE)
#                                 GROUP BY pur_property_id, purchase_type, cf_month
#                                 ) AS pp
#                                 ON property_uid = pur_property_id
#                                 ORDER BY cf_month DESC, cf_year DESC;
#                         """)

#             elif uid[:3] == '600':
#                 rentQuery = db.execute("""
#                             -- PROPERTY RENT DETAILS FOR RENT DETAILS PAGE
#                             SELECT -- *,
#                                 IF(ISNULL(cf_month), CONCAT(property_uid, "-VACANT"), CONCAT(property_uid, "-", cf_month, "-", cf_year, "-", purchase_uid)) AS rent_detail_index
#                                 , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_favorite_image
#                                 , owner_uid, contract_uid, contract_status, business_uid, lease_uid, lease_start, lease_end, lease_status, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , rent_status
#                                 , pur_property_id, purchase_type, pur_notes, pur_due_date, pur_amount_due
#                                 , latest_date, total_paid, amt_remaining
#                                 , if(ISNULL(pur_status_value), "0", pur_status_value) AS pur_status_value
#                                 , if(ISNULL(purchase_status), "UNPAID", purchase_status) AS purchase_status
#                                 , pur_description, cf_month, cf_year
#                                 , CASE
#                                     WHEN ISNULL(contract_uid) THEN "NO MANAGER"
#                                     WHEN ISNULL(lease_status) THEN "VACANT"
#                                     WHEN ISNULL(purchase_status) THEN "UNPAID"
#                                     ELSE purchase_status
#                                     END AS rent_status
#                                 , lf_purchase_uid
#                                 , lf_purchase_type
#                                 , lf_pur_due_date
#                                 , lf_pur_amount_due
#                                 , lf_pur_status_value
#                                 , lf_purchase_status
#                                 , lf_latest_date
#                                 , lf_total_paid
#                                 , lf_amt_remaining
#                                 , lf_cf_month
#                                 , lf_cf_year
#                             FROM (
#                                 -- Find number of properties
#                                 SELECT -- *,
#                                         property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip -- , property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby
#                                         , property_favorite_image
#                                         -- , po_owner_percent, po_start_date, po_end_date
#                                         , owner_uid -- , owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
#                                         , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name
#                                         , contract_status -- , contract_early_end_date
#                                         , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
#                                         , lease_uid, lease_start, lease_end
#                                         , lease_status -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
#                                         , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
#                                         -- , if(ISNULL(lease_status), "VACANT", lease_status) AS rent_status 
#                                 FROM space.p_details
#                                 -- WHERE business_uid = "600-000003"
#                                 -- WHERE owner_uid = "110-000003"
#                                 -- WHERE owner_uid = \'""" + uid + """\'
#                                 WHERE business_uid = \'""" + uid + """\'
#                                 -- WHERE tenant_uid = \'""" + uid + """\'  
#                                 ) AS p
#                             -- Link to rent status
#                             LEFT JOIN (
#                                 SELECT  -- *,
#                                     pur_property_id
#                                     , purchase_uid
#                                     , purchase_type
#                                     , pur_notes
#                                     , pur_due_date
#                                     , SUM(pur_amount_due) AS pur_amount_due
#                                     , MIN(pur_status_value) AS pur_status_value
#                                     , CASE
#                                             WHEN MIN(pur_status_value) = 0 THEN "UNPAID"
#                                             WHEN MIN(pur_status_value) = 1 THEN "PARTIALLY PAID"
#                                             WHEN MIN(pur_status_value) = 4 THEN "PAID LATE"
#                                             WHEN MIN(pur_status_value) = 5 THEN "PAID"
#                                             ELSE purchase_status
#                                         END AS purchase_status
#                                     , pur_description
#                                     , latest_date, total_paid, amt_remaining
#                                     , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_month
#                                     , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_year
#                                     , lf_purchase_uid
#                                     , lf_purchase_type
#                                     , lf_pur_due_date
#                                     , lf_pur_amount_due
#                                     , lf_pur_status_value
#                                     , lf_purchase_status
#                                     , lf_latest_date
#                                     , lf_total_paid
#                                     , lf_amt_remaining
#                                     , lf_cf_month
#                                     , lf_cf_year

#                                 FROM (
#                                     SELECT  *
#                                     FROM space.pp_status -- space.purchases
#                                     WHERE LEFT(pur_payer, 3) = '350'
#                                         -- AND purchase_type = "Rent"
#                                 ) AS ppr
#                                 LEFT JOIN (
#                                     SELECT  pur_description AS lf_purchase_uid
#                                         , purchase_type AS lf_purchase_type
#                                         , pur_due_date AS lf_pur_due_date
#                                         , pur_amount_due AS lf_pur_amount_due
#                                         , pur_status_value AS lf_pur_status_value
#                                         , purchase_status AS lf_purchase_status
#                                         , latest_date AS lf_latest_date
#                                         , total_paid AS lf_total_paid
#                                         , amt_remaining AS lf_amt_remaining
#                                         , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS lf_cf_month
#                                         , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS lf_cf_year
#                                     FROM space.pp_status -- space.purchases
#                                     WHERE LEFT(pur_payer, 3) = '350'
#                                         AND purchase_type = "Late Fee"
#                                 ) AS lf ON purchase_uid = lf_purchase_uid
#                             -- 		AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = MONTH(CURRENT_DATE)
#                             -- 		AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = YEAR(CURRENT_DATE)
#                                 GROUP BY pur_property_id, purchase_type, cf_month
#                             ) AS pp 
#                             ON property_uid = pur_property_id
#                             ORDER BY cf_month DESC, cf_year DESC
#                             """)

#             elif uid[:3] == '350':
#                 rentQuery = db.execute("""
#                             -- PROPERTY RENT DETAILS FOR RENT DETAILS PAGE
#                             SELECT -- *,
#                                 IF(ISNULL(cf_month), CONCAT(property_uid, "-VACANT"), CONCAT(property_uid, "-", cf_month, "-", cf_year, "-", purchase_uid)) AS rent_detail_index
#                                 , property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_favorite_image
#                                 , owner_uid, contract_uid, contract_status, business_uid, lease_uid, lease_start, lease_end, lease_status, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , rent_status
#                                 , pur_property_id, purchase_type, pur_notes, pur_due_date, pur_amount_due
#                                 , latest_date, total_paid, amt_remaining
#                                 , if(ISNULL(pur_status_value), "0", pur_status_value) AS pur_status_value
#                                 , if(ISNULL(purchase_status), "UNPAID", purchase_status) AS purchase_status
#                                 , pur_description, cf_month, cf_year
#                                 , CASE
#                                     WHEN ISNULL(contract_uid) THEN "NO MANAGER"
#                                     WHEN ISNULL(lease_status) THEN "VACANT"
#                                     WHEN ISNULL(purchase_status) THEN "UNPAID"
#                                     ELSE purchase_status
#                                     END AS rent_status
#                                 , lf_purchase_uid
#                                 , lf_purchase_type
#                                 , lf_pur_due_date
#                                 , lf_pur_amount_due
#                                 , lf_pur_status_value
#                                 , lf_purchase_status
#                                 , lf_latest_date
#                                 , lf_total_paid
#                                 , lf_amt_remaining
#                                 , lf_cf_month
#                                 , lf_cf_year
#                             FROM (
#                                 -- Find number of properties
#                                 SELECT -- *,
#                                         property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip -- , property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_value_year, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby
#                                         , property_favorite_image
#                                         -- , po_owner_percent, po_start_date, po_end_date
#                                         , owner_uid -- , owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents, owner_photo_url
#                                         , contract_uid -- , contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name
#                                         , contract_status -- , contract_early_end_date
#                                         , business_uid -- , business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
#                                         , lease_uid, lease_start, lease_end
#                                         , lease_status -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, leaseFees, lt_lease_id, lt_tenant_id, lt_responsibility
#                                         , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
#                                         -- , if(ISNULL(lease_status), "VACANT", lease_status) AS rent_status 
#                                 FROM space.p_details
#                                 -- WHERE business_uid = "600-000003"
#                                 -- WHERE owner_uid = "110-000003"
#                                 -- WHERE tenant_uid = "350-000017"
#                                 -- WHERE owner_uid = \'""" + uid + """\'
#                                 -- WHERE business_uid = \'""" + uid + """\'
#                                 WHERE tenant_uid = \'""" + uid + """\'  
#                                 ) AS p
#                             -- Link to rent status
#                             LEFT JOIN (
#                                 SELECT  -- *,
#                                     pur_property_id
#                                     , purchase_uid
#                                     , purchase_type
#                                     , pur_due_date
#                                     , SUM(pur_amount_due) AS pur_amount_due
#                                     , MIN(pur_status_value) AS pur_status_value
#                                     , CASE
#                                             WHEN MIN(pur_status_value) = 0 THEN "UNPAID"
#                                             WHEN MIN(pur_status_value) = 1 THEN "PARTIALLY PAID"
#                                             WHEN MIN(pur_status_value) = 4 THEN "PAID LATE"
#                                             WHEN MIN(pur_status_value) = 5 THEN "PAID"
#                                             ELSE purchase_status
#                                         END AS purchase_status
#                                     , pur_description
#                                     , pur_notes
#                                     , latest_date, total_paid, amt_remaining
#                                     , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_month
#                                     , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS cf_year
#                                     , lf_purchase_uid
#                                     , lf_purchase_type
#                                     , lf_pur_due_date
#                                     , lf_pur_amount_due
#                                     , lf_pur_status_value
#                                     , lf_purchase_status
#                                     , lf_latest_date
#                                     , lf_total_paid
#                                     , lf_amt_remaining
#                                     , lf_cf_month
#                                     , lf_cf_year

#                                 FROM (
#                                     SELECT  *
#                                     FROM space.pp_status -- space.purchases
#                                     WHERE LEFT(pur_payer, 3) = '350'
#                                         -- AND purchase_type = "Rent"
#                                 ) AS ppr
#                                 LEFT JOIN (
#                                     SELECT  pur_description AS lf_purchase_uid
#                                         , purchase_type AS lf_purchase_type
#                                         , pur_due_date AS lf_pur_due_date
#                                         , pur_amount_due AS lf_pur_amount_due
#                                         , pur_status_value AS lf_pur_status_value
#                                         , purchase_status AS lf_purchase_status
#                                         , latest_date AS lf_latest_date
#                                         , total_paid AS lf_total_paid
#                                         , amt_remaining AS lf_amt_remaining
#                                         , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS lf_cf_month
#                                         , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) AS lf_cf_year
#                                     FROM space.pp_status -- space.purchases
#                                     WHERE LEFT(pur_payer, 3) = '350'
#                                         AND purchase_type = "Late Fee"
#                             ) AS lf ON purchase_uid = lf_purchase_uid
#                             -- 		AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = MONTH(CURRENT_DATE)
#                             -- 		AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y %H:%i')) = YEAR(CURRENT_DATE)
#                                 GROUP BY pur_property_id, purchase_type, cf_month
#                                 ) AS pp
#                                 ON property_uid = pur_property_id
#                                 ORDER BY cf_month DESC, cf_year DESC;          
#                          """)
            
#             # print("Query: ", maintenanceQuery)
#             response["RentStatus"] = rentQuery
#             return response