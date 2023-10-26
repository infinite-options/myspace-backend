
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, uploadImage, s3
import boto3
import json
import datetime
# from datetime import date, datetime, timedelta
# from dateutil.relativedelta import relativedelta
# import calendar
from werkzeug.exceptions import BadRequest


class LeaseDetails(Resource):
    # decorators = [jwt_required()]

    def get(self, filter_id):
        print('in Lease Details')
        response = {}

        with connect() as db:
            if filter_id[:3] == "110":

                leaseQuery = db.execute(""" 
                        -- OWNER LEASES
                        SELECT *
                        FROM space.leases
                        LEFT JOIN space.properties ON property_uid = lease_property_id
                        LEFT JOIN space.o_details ON property_id = lease_property_id
                        LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                        LEFT JOIN space.b_details ON contract_property_id = lease_property_id
                        WHERE owner_uid = \'""" + filter_id + """\'
                            AND (lease_status = "NEW" OR lease_status = "REJECTED" OR lease_status = "PROCESSING" OR lease_status = "ACTIVE" OR lease_status = "REFUSED")

                        """)
                
            elif filter_id[:3] == "350":

                leaseQuery = db.execute(""" 
                        -- TENANT LEASES
                        SELECT *
                        FROM space.leases
                        LEFT JOIN space.properties ON property_uid = lease_property_id
                        LEFT JOIN space.o_details ON property_id = lease_property_id
                        LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                        LEFT JOIN space.b_details ON contract_property_id = lease_property_id
                        WHERE lt_tenant_id = \'""" + filter_id + """\'
                            AND (lease_status = "NEW" OR lease_status = "REJECTED" OR lease_status = "PROCESSING" OR lease_status = "ACTIVE" OR lease_status = "REFUSED")
                        """)

            elif filter_id[:3] == "600":

                leaseQuery = db.execute(""" 
                        -- PROPERTY MANAGEMENT LEASES
                        SELECT *
                        FROM space.leases
                        LEFT JOIN space.properties ON property_uid = lease_property_id
                        LEFT JOIN space.o_details ON property_id = lease_property_id
                        LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                        LEFT JOIN space.b_details ON contract_property_id = lease_property_id
                        WHERE contract_business_id = \'""" + filter_id + """\'
                            AND (lease_status = "NEW" OR lease_status = "REJECTED" OR lease_status = "PROCESSING" OR lease_status = "ACTIVE" OR lease_status = "REFUSED")
                        """)
                
            else:
                leaseQuery = "UID Not Found"

            # print("Lease Query: ", leaseQuery)
            # items = execute(leaseQuery, "get", conn)
            # print(items)
            response["Lease_Details"] = leaseQuery


            return response





class LeaseApplication(Resource):

    def __call__(self):
        print("In Lease Application")

# CHECK IF TENANT HAS APPLIED TO THIS PROPERTY BEFORE
    def get(self, tenant_id, property_id):
        print("In Lease Application GET")
        print(tenant_id, property_id)
        response = {}

        with connect() as db:
            # print("in get lease applications")
            leaseQuery = db.execute(""" 
                    -- FIND LEASE-TENANT APPLICATION
                    SELECT *
                    FROM space.lease_tenant
                    LEFT JOIN space.leases ON lt_lease_id = lease_uid
                    WHERE lt_tenant_id = \'""" + tenant_id + """\' AND lease_property_id = \'""" + property_id + """\';
                    """)
            print(leaseQuery)
            print(leaseQuery['code'])
            print(len(leaseQuery['result']))
            

            if leaseQuery['code'] == 200 and int(len(leaseQuery['result']) > 0):
                # return("Tenant has already applied")
                print(leaseQuery['result'][0]['lt_lease_id'])
                return(leaseQuery['result'][0]['lt_lease_id'])

        return ("New Application")


    # decorators = [jwt_required()]

    # Trigger to generate UIDs created using Alter Table in MySQL

    def post(self):
        print("In Lease Application POST")
        response = {}

        data = request.get_json(force=True)
        print(data)

        # Remove tenenat_uid from data and store as an independant variable 
        if 'tenant_uid' in data:
            print(data['tenant_uid'])
            tenant_uid = data.get('tenant_uid')
            print(tenant_uid)
            del data['tenant_uid']


        ApplicationStatus = LeaseApplication.get(self, tenant_uid, data.get('lease_property_id'))
        print(ApplicationStatus)
        if ApplicationStatus != "New Application":
            print(ApplicationStatus)

            # key = {'lease_uid': data.pop('lease_uid')}
            # key = ApplicationStatus
            # print(key)
            key = {'lease_uid': ApplicationStatus}
            print(key)
        
            with connect() as db:
                response = db.update('leases', key, data)

            print(response)

        else:
            ApplicationStatus = "New"
            print(ApplicationStatus)

            with connect() as db:
                response = db.insert('leases',data)


            with connect() as db:
                print("Need to find lease_uid")
                leaseQuery = db.execute(""" 
                        -- FIND lease_uid
                        SELECT * FROM space.leases
                        ORDER BY lease_uid DESC
                        LIMIT 1;
                        """)
            
            print(leaseQuery)
            print(leaseQuery['result'][0]['lease_uid'])
            lease_id = leaseQuery['result'][0]['lease_uid']

            tenant_responsibiity = str(1)
            print(tenant_responsibiity, type(tenant_responsibiity))

            print(lease_id, tenant_uid, tenant_responsibiity )

            with connect() as db:
                print("Add record in lease_tenant table")
                ltQuery = (""" 
                        INSERT INTO space.lease_tenant
                        SET lt_lease_id = \'""" + lease_id + """\'
                           , lt_tenant_id = \'""" + tenant_uid + """\'
                           , lt_responsibility = \'""" + tenant_responsibiity + """\';
                        """)
                
                response = db.execute(ltQuery, [], 'post')
            
                print(ltQuery)


        # key = {'lease_uid': data.pop('lease_uid')}
        # print(key)

        response["UID"] = ApplicationStatus
        print(response["UID"])
        
        return response
        

        

    def put(self):
        print("In Lease Application PUT")
        response = {}
        
        data = request.get_json(force=True)
        # data = request.form.to_dict()  <== IF data came in as Form Data
        print(data)
        
        if data.get('lease_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
       
        key = {'lease_uid': data.pop('lease_uid')}
        print(key)
        
        with connect() as db:
            response = db.update('leases', key, data)
        return response