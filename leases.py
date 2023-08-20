
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


class LeaseDetails(Resource):
    # decorators = [jwt_required()]

    def get(self, filter_id):
        print('in Lease Details')
        response = {}

        with connect() as db:
            if filter_id[:3] == "110":

                leaseQuery = db.execute(""" 
                        -- PROPERTY MANAGEMENT AND OWNER LEASES
                        SELECT -- *,
                            lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_early_end_date, lease_renew_status, move_out_date
                            , property_owner_id, po_owner_percent, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_address, owner_unit, owner_city, owner_state, owner_zip
                            , property_address, property_unit, property_city, property_state, property_zip, property_type
                            , leaseFees_uid, fee_name, fee_type, charge, due_by, late_by, late_fee, perDay_late_fee, frequency, available_topay
                            , ld_created_date, ld_type, ld_name, ld_description, ld_shared, ld_link
                            , lt_tenant_id, lt_responsibility, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                            , contract_uid, contract_status, business_uid
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
                            AND property_owner_id = \'""" + filter_id + """\';
                        """)
                
            elif filter_id[:3] == "350":

                leaseQuery = db.execute(""" 
                        -- PROPERTY MANAGEMENT AND OWNER LEASES
                        SELECT -- *,
                            lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_early_end_date, lease_renew_status, move_out_date
                            , property_owner_id, po_owner_percent, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_address, owner_unit, owner_city, owner_state, owner_zip
                            , property_address, property_unit, property_city, property_state, property_zip, property_type
                            , leaseFees_uid, fee_name, fee_type, charge, due_by, late_by, late_fee, perDay_late_fee, frequency, available_topay
                            , ld_created_date, ld_type, ld_name, ld_description, ld_shared, ld_link
                            , lt_tenant_id, lt_responsibility, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                            , contract_uid, contract_status, business_uid
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
                            AND lt_tenant_id = \'""" + filter_id + """\';
                        """)

            else:

                leaseQuery = db.execute(""" 
                        -- PROPERTY MANAGEMENT AND OWNER LEASES
                        SELECT -- *,
                            lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_early_end_date, lease_renew_status, move_out_date
                            , property_owner_id, po_owner_percent, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_address, owner_unit, owner_city, owner_state, owner_zip
                            , property_address, property_unit, property_city, property_state, property_zip, property_type
                            , leaseFees_uid, fee_name, fee_type, charge, due_by, late_by, late_fee, perDay_late_fee, frequency, available_topay
                            , ld_created_date, ld_type, ld_name, ld_description, ld_shared, ld_link
                            , lt_tenant_id, lt_responsibility, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                            , contract_uid, contract_status, business_uid
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
                            AND business_uid = \'""" + filter_id + """\';
                        """)

            # print("Lease Query: ", leaseQuery)
            # items = execute(leaseQuery, "get", conn)
            # print(items)
            response["Lease Details"] = leaseQuery


            return response
