
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity


from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
from werkzeug.exceptions import BadRequest
from werkzeug.utils import secure_filename



# OVERVIEW
#           TENANT      OWNER     PROPERTY MANAGER     
#              X           X               X

ALLOWED_EXTENSIONS = {'txt', 'pdf'}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS



class Documents(Resource):
    # decorators = [jwt_required()]

    def get(self, user_id):
        print('in Owner Documents')
        response = {}

        with connect() as db:
            print("in connect loop")
            if user_id.startswith("110-"):
                documentQuery = db.execute(""" 
                        -- OWNER DOCUMENTS
                        SELECT property_owner_id, po_owner_percent
                            , property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
                            , contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                            , lease_uid, lease_start, lease_end, lease_status, lease_documents, lease_early_end_date, lease_renew_status
                        FROM space_prod.property_owner
                        LEFT JOIN space_prod.properties ON property_uid = property_id
                        LEFT JOIN space_prod.contracts ON property_uid = contract_property_id
                        LEFT JOIN space_prod.leases ON property_uid = lease_property_id
                        WHERE property_owner_id = \'""" + user_id + """\';
                        """)
                response["Documents"] = documentQuery

            elif user_id.startswith("350-"):
                documentQuery = db.execute(""" 
                        -- TENANT DOCUMENTS
                        SELECT tenant_uid, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                            ,lease_uid, lease_property_id, lease_end, lease_status
                            ,tenant_documents
                            ,lease_documents
                        FROM space_prod.tenantProfileInfo
                        LEFT JOIN space_prod.lease_tenant ON tenant_uid = lt_tenant_id
                        LEFT JOIN space_prod.leases ON lease_uid = lt_lease_id
                        WHERE tenant_uid = \'""" + user_id + """\';
                        """)
                response["Documents"] = documentQuery

            elif user_id.startswith("600-"):
                documents = {}
                contractsQuery = db.execute(""" 
                        -- MANAGER CONTRACTS
                        SELECT property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
                            , contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date                            
                            , owner_first_name, owner_last_name, owner_phone_number, owner_email                
                            FROM space_prod.b_details
                            LEFT JOIN space_prod.properties ON property_uid = contract_property_id
                            LEFT JOIN space_prod.o_details ON property_id = contract_property_id                                                        
                            WHERE business_uid = \'""" + user_id + """\'  AND contract_status = "ACTIVE";
                        """)
                documents["Contracts"] = contractsQuery["result"]

                applicationsQuery = db.execute(""" 
                        -- TENANT APPLICATIONS
                        SELECT property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type                            
                            , lease_uid, lease_start, lease_end, lease_status, lease_documents, lease_early_end_date, lease_renew_status, lease_adults, lease_children, lease_pets, lease_application_date
                            , tenant_first_name, tenant_last_name
                            FROM space_prod.b_details
                            LEFT JOIN space_prod.properties ON property_uid = contract_property_id
                            LEFT JOIN space_prod.leases ON lease_property_id = contract_property_id
                            LEFT JOIN space_prod.t_details ON lt_lease_id = lease_uid
                            WHERE business_uid = \'""" + user_id + """\' AND lease_status = "NEW" AND contract_status = "ACTIVE";
                        """)
                documents["Applications"] = applicationsQuery["result"]

                leasesQuery = db.execute(""" 
                        -- LEASE DOCUMENTS
                        SELECT property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
                            , lease_uid, lease_start, lease_end, lease_status, lease_documents, lease_early_end_date, lease_renew_status, lease_adults, lease_children, lease_pets, lease_application_date, lease_actual_rent
                            , tenant_first_name, tenant_last_name
                            FROM space_prod.b_details
                            LEFT JOIN space_prod.properties ON property_uid = contract_property_id
                            LEFT JOIN space_prod.leases ON lease_property_id = contract_property_id
                            LEFT JOIN space_prod.t_details ON lt_lease_id = lease_uid
                            WHERE business_uid = \'""" + user_id + """\' AND lease_status <> "NEW" AND contract_status = "ACTIVE";
                        """)
                documents["Leases"] = leasesQuery["result"]

                response["Documents"] = documents
            else:
                raise BadRequest("Request failed. Invalid User ID")

            # print("Query: ", documentQuery)
            # items = execute(documentQuery, "get", conn)
            # print(items)

            return response

    def post(self, user_id):
        print('in OwnerDocuments')
        newDocument = {}
        response = {}
        data = request.form

        if 'document_file' not in request.files:
            raise BadRequest("Request failed, no 'document_file' in payload.")

        file = request.files['document_file']

        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            raise BadRequest("Request failed, no selected file.")

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)

            # upload to s3
            # file = request.files.get(filename)
            # print("File: ", file)

            key = f'{user_id}/{filename}'
            # link = uploadImage(file, key, '')
            # print(link)
            newDocument['document_link'] = "link"

        with connect() as db:
            data = request.form
            fields = [
                'document_type',
                'document_title',
                'document_date_created',
                'document_property',
                'document_owner',
                'document_tenant',
                'document_description'
            ]
            # print("Document Type: ", data.get("document_type"))

            # newDocument['owner_uid']
            for field in fields:
                if data.get(field) is not None:
                    newDocument[field] = data.get(field)
            new_doc_id = db.call('space_prod.new_document_uid')['result'][0]['new_id']
            newDocument['document_uid'] = new_doc_id
            newDocument['document_profile_id'] = user_id

            # sql = f"""UPDATE space_prod.ownerProfileInfo
            #             SET owner_documents = JSON_ARRAY_APPEND(
            #                 IFNULL(owner_documents, JSON_ARRAY()),
            #                 '$',
            #                 "{newDocument}"
            #             )
            #             WHERE owner_uid = \'""" + owner_id + """\';"""
            # print(sql)
            # response = db.execute(sql, cmd='post')
            response = db.insert('space_prod.documents', newDocument)

        return response
