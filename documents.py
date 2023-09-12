
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
from werkzeug.exceptions import BadRequest
from werkzeug.utils import secure_filename



# OVERVIEW
#           TENANT      OWNER     PROPERTY MANAGER     
#              X           X               X

ALLOWED_EXTENSIONS = {'txt', 'pdf'}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


class OwnerDocuments(Resource):
    # decorators = [jwt_required()]

    def get(self, owner_id):
        print('in Owner Documents')
        response = {}

        with connect() as db:
            print("in connect loop")
            documentQuery = db.execute(""" 
                    -- OWNER DOCUMENTS
                    SELECT property_owner_id, po_owner_percent
                        , property_uid, property_address, property_unit, property_city, property_state, property_zip, property_type
                        , contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                        , lease_uid, lease_start, lease_end, lease_status, lease_documents, lease_early_end_date, lease_renew_status
                    FROM space.property_owner
                    LEFT JOIN space.properties ON property_uid = property_id
                    LEFT JOIN space.contracts ON property_uid = contract_property_id
                    LEFT JOIN space.leases ON property_uid = lease_property_id
                    WHERE property_owner_id = \'""" + owner_id + """\';
                    """)
            

            # print("Query: ", documentQuery)
            # items = execute(documentQuery, "get", conn)
            # print(items)
            response["Documents"] = documentQuery
            return response
        
    def post(self, owner_id):
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
            
            #upload to s3
            # file = request.files.get(filename)
            # print("File: ", file)
            
            key = f'owners/{owner_id}/{filename}'
            link = uploadImage(file, key, '')
            print(link)
            newDocument['link'] = link
            
        with connect() as db:
            data = request.form
            fields = [
                # "property_owner_id",
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

            
            sql = f"""UPDATE space.ownerProfileInfo
                        SET owner_documents = JSON_ARRAY_APPEND(
                            IFNULL(owner_documents, JSON_ARRAY()),
                            '$',
                            "{newDocument}"
                        )
                        WHERE owner_uid = \'""" + owner_id + """\';"""
            print(sql)
            response = db.execute(sql, cmd='post')

        return response


class TenantDocuments(Resource):
    # decorators = [jwt_required()]

    def get(self, tenant_id):
        print('in Tenant Documents')
        response = {}

        with connect() as db:
            print("in connect loop")
            documentQuery = db.execute(""" 
                    -- TENANT DOCUMENTS
                    SELECT tenant_uid, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                        ,lease_uid, lease_property_id, lease_end, lease_status
                        ,tenant_documents
                        ,lease_documents
                    FROM space.tenantProfileInfo
                    LEFT JOIN space.lease_tenant ON tenant_uid = lt_tenant_id
                    LEFT JOIN space.leases ON lease_uid = lt_lease_id
                    WHERE tenant_uid = \'""" + tenant_id + """\';
                    """)
            

            # print("Query: ", documentQuery)
            # items = execute(documentQuery, "get", conn)
            # print(items)
            response["Documents"] = documentQuery
            return response
