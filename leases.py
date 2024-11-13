
from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity


from data_pm import connect, deleteImage, uploadImage, s3, processDocument, pmDueDate
import boto3
import json
# import datetime
from datetime import datetime, date, timedelta
from decimal import Decimal
# from datetime import date, datetime, timedelta
# from dateutil.relativedelta import relativedelta
import calendar
from werkzeug.exceptions import BadRequest
import ast

from queries import LeaseDetailsQuery

ALLOWED_EXTENSIONS = {'txt', 'pdf', 'doc', 'docx'}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


class LeaseDetails(Resource):
    def get(self, user_id):
        print("in Get Lease Details")
        response = {}

        response["Lease_Detailss"] = LeaseDetailsQuery(user_id)
        return response


    def put(self):
        print("in Lease Details PUT!  THIS IS FOR TEST ONLY.  NOT AN ACTUAL ENDPOINT")
        data = request.form.to_dict()
        print("Data Reeived: ", data)
        key1 = data['documentID']
        print("Key: ", key1, type(key1))

        try:
            # Calls deleteImage function with S3 key
            response = deleteImage(key1)
            print("Response: ", response)
            print(f"Deleted existing file {key1}")
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"File {key1} does not exist")
            else:
                print(f"Error deleting file {key1}: {e}")

        return

# class LeaseDetails(Resource):
#     # decorators = [jwt_required()]

#     def get(self, filter_id):
#         print('in Lease Details', filter_id)
#         response = {}

#         with connect() as db:
#             if filter_id[:3] == "110":
#                 print('in Owner Lease Details')

#                 leaseQuery = db.execute(""" 
#                         -- OWNER, PROPERTY MANAGER, TENANT LEASES
#                         SELECT * 
#                         FROM (
#                             -- FIND ALL ACTIVE/ENDED LEASES WITH OR WITHOUT A MOVE OUT DATE
#                             SELECT *,
#                             DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) AS lease_days_remaining,
#                             CASE
#                                     WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) > DATEDIFF(LAST_DAY(DATE_ADD(NOW(), INTERVAL 11 MONTH)), NOW()) THEN 'FUTURE' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'FUTURE'
#                                     WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) < 0 THEN 'M2M' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'M2M'
#                                     ELSE MONTHNAME(STR_TO_DATE(LEFT(lease_end, 2), '%m'))
#                             END AS lease_end_month
#                             FROM space.leases 
#                             WHERE lease_status = "ACTIVE" OR lease_status = "ACTIVE M2M" OR lease_status = "ENDED"
#                             ) AS l
#                         LEFT JOIN (
#                             SELECT fees_lease_id, JSON_ARRAYAGG(JSON_OBJECT
#                                 ('leaseFees_uid', leaseFees_uid,
#                                 'fee_name', fee_name,
#                                 'fee_type', fee_type,
#                                 'charge', charge,
#                                 'due_by', due_by,
#                                 'late_by', late_by,
#                                 'late_fee', late_fee,
#                                 'perDay_late_fee', perDay_late_fee,
#                                 'frequency', frequency,
#                                 'available_topay', available_topay,
#                                 'due_by_date', due_by_date
#                                 )) AS lease_fees
#                                 FROM space.leaseFees
#                                 GROUP BY fees_lease_id) as f ON lease_uid = fees_lease_id
#                         LEFT JOIN space.properties ON property_uid = lease_property_id
#                         LEFT JOIN space.o_details ON property_id = lease_property_id
#                         LEFT JOIN (
#                             SELECT lt_lease_id, JSON_ARRAYAGG(JSON_OBJECT
#                                 ('tenant_uid', tenant_uid,
#                                 'lt_responsibility', if(lt_responsibility IS NOT NULL, lt_responsibility, "1"),
#                                 'tenant_first_name', tenant_first_name,
#                                 'tenant_last_name', tenant_last_name,
#                                 'tenant_phone_number', tenant_phone_number,
#                                 'tenant_email', tenant_email,
#                                 'tenant_drivers_license_number', tenant_drivers_license_number,
#                                 'tenant_drivers_license_state', tenant_drivers_license_state,
#                                 'tenant_ssn', tenant_ssn,
#                                 'tenant_current_salary', tenant_current_salary,
#                                 'tenant_salary_frequency', tenant_salary_frequency,
#                                 'tenant_current_job_title', tenant_current_job_title,
#                                 'tenant_current_job_company', tenant_current_job_company
#                                 )) AS tenants
#                                 FROM space.t_details 
#                                 GROUP BY lt_lease_id) as t ON lease_uid = lt_lease_id
#                         LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
#                         LEFT JOIN space.u_details ON utility_property_id = lease_property_id
#                         -- WHERE owner_uid = "110-000003"
#                         -- WHERE contract_business_id = "600-000003"
#                         -- WHERE tenants LIKE "%350-000040%"
#                         WHERE owner_uid = \'""" + filter_id + """\'
#                         -- WHERE contract_business_id = \'""" + filter_id + """\'
#                         -- WHERE tenants LIKE '%""" + filter_id + """%'
#                         ;
#                         """)
                
#             elif filter_id[:3] == "600":
#                 print('in PM Lease Details')

#                 leaseQuery = db.execute(""" 
#                         -- OWNER, PROPERTY MANAGER, TENANT LEASES
#                         SELECT * 
#                         FROM (
#                             -- FIND ALL ACTIVE/ENDED LEASES WITH OR WITHOUT A MOVE OUT DATE
#                             SELECT *,
#                             DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) AS lease_days_remaining,
#                             CASE
#                                     WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) > DATEDIFF(LAST_DAY(DATE_ADD(NOW(), INTERVAL 11 MONTH)), NOW()) THEN 'FUTURE' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'FUTURE'
#                                     WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) < 0 THEN 'M2M' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'M2M'
#                                     ELSE MONTHNAME(STR_TO_DATE(LEFT(lease_end, 2), '%m'))
#                             END AS lease_end_month
#                             FROM space.leases 
#                             WHERE lease_status = "ACTIVE" OR lease_status = "ACTIVE M2M" OR lease_status = "ENDED"
#                             ) AS l
#                         LEFT JOIN (
#                             SELECT fees_lease_id, JSON_ARRAYAGG(JSON_OBJECT
#                                 ('leaseFees_uid', leaseFees_uid,
#                                 'fee_name', fee_name,
#                                 'fee_type', fee_type,
#                                 'charge', charge,
#                                 'due_by', due_by,
#                                 'late_by', late_by,
#                                 'late_fee', late_fee,
#                                 'perDay_late_fee', perDay_late_fee,
#                                 'frequency', frequency,
#                                 'available_topay', available_topay,
#                                 'due_by_date', due_by_date
#                                 )) AS lease_fees
#                                 FROM space.leaseFees
#                                 GROUP BY fees_lease_id) as f ON lease_uid = fees_lease_id
#                         LEFT JOIN space.properties ON property_uid = lease_property_id
#                         LEFT JOIN space.o_details ON property_id = lease_property_id
#                         LEFT JOIN (
#                             SELECT lt_lease_id, JSON_ARRAYAGG(JSON_OBJECT
#                                 ('tenant_uid', tenant_uid,
#                                 'lt_responsibility', if(lt_responsibility IS NOT NULL, lt_responsibility, "1"),
#                                 'tenant_first_name', tenant_first_name,
#                                 'tenant_last_name', tenant_last_name,
#                                 'tenant_phone_number', tenant_phone_number,
#                                 'tenant_email', tenant_email,
#                                 'tenant_drivers_license_number', tenant_drivers_license_number,
#                                 'tenant_drivers_license_state', tenant_drivers_license_state,
#                                 'tenant_ssn', tenant_ssn
#                                 )) AS tenants
#                                 FROM space.t_details 
#                                 GROUP BY lt_lease_id) as t ON lease_uid = lt_lease_id
#                         LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
#                         LEFT JOIN space.u_details ON utility_property_id = lease_property_id
#                         -- WHERE owner_uid = "110-000003"
#                         -- WHERE contract_business_id = "600-000003"
#                         -- WHERE tenants LIKE "%350-000040%"
#                         -- WHERE owner_uid = \'""" + filter_id + """\'
#                         WHERE contract_business_id = \'""" + filter_id + """\'
#                         -- WHERE tenants LIKE '%""" + filter_id + """%'
#                         ;
#                         """)
                
#                 # print(leaseQuery)

#             elif filter_id[:3] == "350":
#                 print('in Tenant Lease Details')

#                 leaseQuery = db.execute(""" 
#                         -- OWNER, PROPERTY MANAGER, TENANT LEASES
#                         SELECT * 
#                         FROM (
#                             -- FIND ALL ACTIVE/ENDED LEASES WITH OR WITHOUT A MOVE OUT DATE
#                             SELECT *,
#                             DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) AS lease_days_remaining,
#                             CASE
#                                     WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) > DATEDIFF(LAST_DAY(DATE_ADD(NOW(), INTERVAL 11 MONTH)), NOW()) THEN 'FUTURE' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'FUTURE'
#                                     WHEN DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) < 0 THEN 'M2M' -- DATEDIFF(STR_TO_DATE(lease_end, '%m-%d-%Y'), NOW()) -- 'M2M'
#                                     ELSE MONTHNAME(STR_TO_DATE(LEFT(lease_end, 2), '%m'))
#                             END AS lease_end_month
#                             FROM space.leases 
#                             -- WHERE lease_status = "ACTIVE" OR lease_status = "ACTIVE M2M" OR lease_status = "ENDED"
#                             ) AS l
#                         LEFT JOIN (
#                             SELECT fees_lease_id, JSON_ARRAYAGG(JSON_OBJECT
#                                 ('leaseFees_uid', leaseFees_uid,
#                                 'fee_name', fee_name,
#                                 'fee_type', fee_type,
#                                 'charge', charge,
#                                 'due_by', due_by,
#                                 'late_by', late_by,
#                                 'late_fee', late_fee,
#                                 'perDay_late_fee', perDay_late_fee,
#                                 'frequency', frequency,
#                                 'available_topay', available_topay,
#                                 'due_by_date', due_by_date
#                                 )) AS lease_fees
#                                 FROM space.leaseFees
#                                 GROUP BY fees_lease_id) as f ON lease_uid = fees_lease_id
#                         LEFT JOIN space.properties ON property_uid = lease_property_id
#                         LEFT JOIN space.o_details ON property_id = lease_property_id
#                         LEFT JOIN (
#                             SELECT lt_lease_id, JSON_ARRAYAGG(JSON_OBJECT
#                                 ('tenant_uid', tenant_uid,
#                                 'lt_responsibility', if(lt_responsibility IS NOT NULL, lt_responsibility, "1"),
#                                 'tenant_first_name', tenant_first_name,
#                                 'tenant_last_name', tenant_last_name,
#                                 'tenant_phone_number', tenant_phone_number,
#                                 'tenant_email', tenant_email,
#                                 'tenant_drivers_license_number', tenant_drivers_license_number,
#                                 'tenant_drivers_license_state', tenant_drivers_license_state,
#                                 'tenant_ssn', tenant_ssn
#                                 )) AS tenants
#                                 FROM space.t_details 
#                                 GROUP BY lt_lease_id) as t ON lease_uid = lt_lease_id
#                         LEFT JOIN (SELECT * FROM space.b_details WHERE contract_status = "ACTIVE") b ON contract_property_id = lease_property_id
#                         LEFT JOIN space.u_details ON utility_property_id = lease_property_id
#                         -- WHERE owner_uid = "110-000003"
#                         -- WHERE contract_business_id = "600-000003"
#                         -- WHERE tenants LIKE "%350-000040%"
#                         -- WHERE owner_uid = \'""" + filter_id + """\'
#                         -- WHERE contract_business_id = \'""" + filter_id + """\'
#                         WHERE tenants LIKE '%""" + filter_id + """%'
#                         ;
#                         """)
            
#             else:
#                 leaseQuery = "UID Not Found"

#             # print("Lease Query: ", leaseQuery)
#             # items = execute(leaseQuery, "get", conn)
#             # print(items)

#             response["Lease_Details"] = leaseQuery

#             return response


class LeaseApplication(Resource):

    def __call__(self):
        print("In Lease Application")

    # CHECK IF TENANT HAS APPLIED TO THIS PROPERTY BEFORE
    def get(self, tenant_id, property_id):
        print("In Lease Application GET")
        # print(tenant_id, property_id)
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
            # print(leaseQuery)
            # print(leaseQuery['code'])
            # print(len(leaseQuery['result']))

            if leaseQuery['code'] == 200 and int(len(leaseQuery['result']) > 0):
                # return("Tenant has already applied")
                # print(leaseQuery['result'][0]['lt_lease_id'])
                return(leaseQuery['result'][0]['lt_lease_id'])

        return ("New Application")

    # decorators = [jwt_required()]

    # Trigger to generate UIDs created using Alter Table in MySQL

    def post(self):
        print("In Lease Application POST")
        response = {}

        payload = request.form.to_dict()
        print("Lease Add Payload: ", payload)

        if payload.get('lease_uid'):
            print("lease_uid found.  Please call PUT endpoint")
            raise BadRequest("Request failed, UID found in payload.")
        
        if payload.get('tenant_uid') in {None, '', 'null'}:
            print("No tenant_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        with connect() as db: 

            # Get New Lease UID    
            lease_uid = db.call('new_lease_uid')['result'][0]['new_id']
            key = {'lease_uid': lease_uid}
            response['lease_uid'] = lease_uid
            print("Contract Key: ", key)


            # --------------- PROCESS DOCUMENTS ------------------

            processDocument(key, payload)
            print("Payload after function: ", payload)
            
            # --------------- PROCESS DOCUMENTS ------------------


            # Insert data into leaseFees table
            lease_fees = payload.pop('lease_fees', None)
            # print("lease_fees in data: ", lease_fees)
            if lease_fees != None:
                json_object = json.loads(lease_fees)
                # print("lease fees json_object", json_object)
                for fees in json_object:
                    # print("fees",fees)
                    new_leaseFees = {}
                    # Get new leaseFees_uid
                    new_leaseFees["leaseFees_uid"] = db.call('new_leaseFee_uid')['result'][0]['new_id']  
                    new_leaseFees["fees_lease_id"] = lease_uid
                    for item in fees:
                        # print("Item: ", item)
                        new_leaseFees[item] = fees[item]
                        # print(new_leaseFees[item])
                    # print("Payload: ", new_leaseFees)
                    response["lease_fees"] = db.insert('leaseFees', new_leaseFees)
                    # print("response: ", response["lease_fees"])


        
            # Insert data into lease-Tenants table
            # print("Tenants: ", payload.get('tenant_uid').split(','), type(payload.get('tenant_uid').split(',')))
            tenants = payload.pop('tenant_uid').split(',')

            tenant_responsibiity = str(1/len(tenants))
            for tenant_uid in tenants:
                # print("Add record in lease_tenant table", lease_uid, tenant_uid, tenant_responsibiity)
                # lt_employment = tenant_employment if tenant_employment else []

                ltQuery = (""" 
                    INSERT INTO space.lease_tenant
                    SET lt_lease_id = \'""" + lease_uid + """\'
                        , lt_tenant_id = \'""" + tenant_uid + """\'
                        , lt_responsibility = \'""" + tenant_responsibiity + """\';
                    """)
                # print("Made it to here")
                response = db.execute(ltQuery, [], 'post')
                # print("Added tenant: ", response)
            # print("Data inserted into space.lease_tenant")
            

            # Verify LIST variables are not empty
            fields_with_lists = ["lease_adults", "lease_children", "lease_pets", "lease_vehicles", "lease_referred", "lease_assigned_contacts" , "lease_documents", "lease_income", "lease_utilities"]
            for field in fields_with_lists:
                print("field list", field)
                if payload.get(field) in [None, '', 'undefined']:
                    print(field,"Is None")
                    payload[field] = '[]' 

            # Actual Insert Statement
            print("About to insert: ", payload)
            response["lease"] = db.insert('leases', payload)
            print("Data inserted into space.leases", response)

        return response


    def put(self):
        print("\nIn Lease Application PUT")
        response = {}
        payload = request.form.to_dict()
        # print("Lease Application Payload: ", payload)

        # Verify lease_uid has been included in the data
        if payload.get('lease_uid') in {None, '', 'null'}:
            # print("No lease_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        lease_uid = payload.get('lease_uid')
        key = {'lease_uid': payload.pop('lease_uid')}
        # print("Lease Key: ", key)


        # --------------- PROCESS DOCUMENTS ------------------

        processDocument(key, payload)
        print("Payload after function: ", payload)
        
        # --------------- PROCESS DOCUMENTS ------------------

        with connect() as db: 
            print("Connection Established")

            # DEL FEES  Typically if PM is modifying the lease for Tenant
            print("delete fees: ", payload.get('delete_fees'))
            if "delete_fees" in payload:
                print("In delete fees")
                json_object = json.loads(payload.pop('delete_fees'))
                print("delete lease fees json_object", json_object)
                for delete_uid in json_object:
                    print("Fee to delete: ", delete_uid)

                    response["delete_fees"] = db.delete(""" 
                            DELETE FROM leaseFees
                            WHERE leaseFees_uid = \'""" + delete_uid + """\'
                            """)

            # POST TO LEASE FEES.  Need to post Lease Fees into leaseFees Table.  Typically when PM sends lease to Tenant
            if "lease_fees" in payload:
                # print("lease_fees in data", payload['lease_fees'])
                fields_leaseFees = ["charge", "due_by", "due_by_date", "late_by", "fee_name", "fee_type", "frequency", "available_topay",
                                            "perDay_late_fee", "late_fee"]
                temp_fees = {'lease_fees': payload.pop('lease_fees')}
                # print("Temp: ", temp_fees)

                json_string = temp_fees['lease_fees']
                json_object = json.loads(json_string)
                # print("lease fees json_object", json_object)
                for fees in json_object:
                    # print("Fees: ", fees)
                    new_leaseFees = {}
                    new_leaseFees["fees_lease_id"] = key['lease_uid']

                    # print("here 1")
                    for item in fields_leaseFees:
                        if item in fees:
                            # print("here 2", item)
                            new_leaseFees[item] = fees[item]
                    # print("Lease Fee to input: ", new_leaseFees)
 
                    # FOR FEE UPDATE
                    if 'leaseFees_uid' in fees:  
                        # print("In IF Statment ", fees['leaseFees_uid']) 
                        fee_key = {'leaseFees_uid': fees['leaseFees_uid']}
                        # print("fee_key",fee_key, type(fee_key))
                        # print("new_leaseFees: ", new_leaseFees)
                        response['lease_fees'] = db.update('leaseFees', fee_key, new_leaseFees)
                    # FOR FIRST TIME FEES ARE BEING ADDED
                    else:
                        # print("In ELSE Statment ") 
                        new_leaseFees["leaseFees_uid"] = db.call('new_leaseFee_uid')['result'][0]['new_id']    
                        # print('New Lease Fees Payload: ', new_leaseFees)
                        response["lease_fees"] = db.insert('leaseFees', new_leaseFees)

            if "lease_end" in payload:
                print("In lease_end")
                payload["lease_early_end_date"] = payload["lease_end"]

            print("Leases Payload: ", payload)
            response['lease_docs'] = db.update('leases', key, payload)
            # print("Response:" , response)
       

            # ==> ONLY POST TO PURCHASES IF THE LEASE HAS BEEN ACCEPTED AND NOT BEING RENEWED
            if payload.get('lease_status') == 'ACTIVE':
                print("\nLease Status Changed to: ", payload.get('lease_status'), lease_uid)


                # ADD DEPOSIT AND FIRST RENT PAYMENT HERE
                # CONSIDER ADDING FULL FIRST MONTHS RENT AND NEXT MONTHS PARTIAL RENT IF THERE IS A WAY TO AVOID DUPLICATE CHARGES

                # READ THE LEASE FEES
                # INSERT EACH LEASE FEE INTO PURCHASES TABLE
                fees = db.execute(""" 
                        SELECT leaseFees.* -- , leases.*
                            ,lease_property_id, lease_start, lease_status
                            , contract_uid, contract_business_id, property_owner_id, lt_tenant_id
                        FROM space.leaseFees
                        LEFT JOIN space.leases ON fees_lease_id = lease_uid
                        LEFT JOIN space.lease_tenant ON fees_lease_id = lt_lease_id
                        LEFT JOIN space.contracts ON lease_property_id = contract_property_id
                        LEFT JOIN space.property_owner ON lease_property_id = property_id
                        -- WHERE fees_lease_id = '300-000005'
                        WHERE fees_lease_id = \'""" + lease_uid + """\'
                            AND contract_status = 'ACTIVE';
                        """)
                print("Lease Fees: ", fees) 


                # PROCESS EACH LEASE FEE
                for fee in fees['result']:
                    print("\nNow calculating for: ", fee['fee_name'])


                    # SET THE COMMON VARIABLES
                    manager = fee['contract_business_id']
                    owner = fee['property_owner_id']
                    tenant = fee['lt_tenant_id']


                    # Common JSON Object Attributes
                    newRequest = {}
                    
                    newRequest['pur_timestamp'] = datetime.now().strftime("%m-%d-%Y %H:%M")
                    newRequest['pur_property_id'] = fee['lease_property_id']
                    newRequest['pur_leaseFees_id'] = fee['leaseFees_uid']

                    newRequest['purchase_type'] = fee['fee_type']
                    newRequest['pur_cf_type'] = "revenue"
                    # newRequest['pur_amount_due'] = fee['charge']
                    newRequest['purchase_status'] = "UNPAID"
                    newRequest['pur_status_value'] = "0"
                    newRequest['pur_notes'] = fee['fee_name']

                    # newRequest['pur_due_by'] = fee['due_by']
                    newRequest['pur_late_by'] = fee['late_by']
                    newRequest['pur_late_fee'] = fee['late_fee']
                    newRequest['pur_perDay_late_fee'] = fee['perDay_late_fee']

                    newRequest['purchase_date'] = datetime.today().date().strftime('%m-%d-%Y %H:%M')
                    newRequest['pur_description'] = f"Initial {fee['fee_name']}"



                    
                    
                    # Calculate Pro-Rated Rent
                    if fee['fee_name'] == 'Rent':
                        avail_to_pay = fee.get('available_topay') or 10
                        due_by = fee['due_by']  #May need to adjust this parameter for fixed day due_by dates

                        today = datetime.today()
                        # today = datetime(2024, 8, 25)  # For testing purposes
                        print("Today: ", today, type(today))

                        due_date = datetime(today.year, today.month, due_by)
                        print("Due Date: ", due_date, type(due_date))

                        # Calculate Actual due date taking December into account
                        print("Today: ", today , type(today), "vs Due Date: ", due_date, type(due_date))
                        if today < due_date:
                            print("Before due date")
                            
                        else:
                            print("After due date")
                            # Calculate the next month
                            if today.month == 12:
                                # If the current month is December, set the due date to January of the next year
                                due_date = datetime(today.year + 1, 1, due_by)
                            else:
                                # For any other month, increment the month and keep the year the same
                                due_date = datetime(today.year, today.month + 1, due_by)
                            print("Modified Due Date: ", due_date, type(due_date))

                        print("Final Due Date: ", due_date, type(due_date))

                        # Calculate PM Due Date
                        # pm_due_date = due_date + timedelta(days=32)
                        last_day_of_month = calendar.monthrange(due_date.year, due_date.month)[1]
                        # Create a new datetime object for the last day of the month
                        pm_due_date = due_date.replace(day=last_day_of_month)
                        # print("PM Due Date: ", pm_due_date, type(pm_due_date))
                        print("PM Due Date: ", pm_due_date, type(pm_due_date))



                        if fee['lease_start'] not in (None, '', 'None'):
                            try:
                                # Parse the date and time, handle cases where time may not be included
                                if ' ' in fee['lease_start']:
                                    lease_start = datetime.strptime(fee['lease_start'], '%m-%d-%Y %H:%M')
                                else:
                                    lease_start = datetime.strptime(fee['lease_start'], '%m-%d-%Y')  # No time included, defaults to 00:00
                            except ValueError as e:
                                print("Error:", e)
                        else:
                            lease_start = datetime.today().strftime('%m-%d-%Y %H:%M')

                        print("Lease Start: ", lease_start, type(lease_start))
                      
                            


                        pro_rate_days = (due_date - lease_start).days
                        print("ProRate days: ", pro_rate_days)

                        # Get the current year and month
                        current_year = today.year
                        current_month = today.month

                        # Get the number of days in the current month
                        days_in_month = calendar.monthrange(current_year, current_month)[1]
                        pro_rate_amt = round((float(fee['charge'])/days_in_month * pro_rate_days),2)
                        print("Pro-Rated rent is: ", pro_rate_amt)

                        newRequest['pur_amount_due'] = pro_rate_amt



                        # Create JSON Object for Rent Purchase for Tenant-PM Purchase
                        newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                        grouping = newRequestID
                        newRequest['purchase_uid'] = newRequestID
                        newRequest['pur_group'] = grouping
                        # print(newRequestID)
                        newRequest['pur_receiver'] = manager
                        newRequest['pur_payer'] = tenant
                        newRequest['pur_initiator'] = manager
                        

                        newRequest['pur_due_date'] = today.strftime("%m-%d-%Y %H:%M")
                        print("Unmodified: ", newRequest['pur_due_date'], type(newRequest['pur_due_date']))


                        print("Input to purchases Tenant-PM: ", newRequest)
                        # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
                        db.insert('purchases', newRequest)
                        print("Tenant-PM Fee Inserted")


                        # Create PM-Owner Purchase
                        # Create JSON Object for Rent Purchase for PM-Owner Payment
                        newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                        newRequest['purchase_uid'] = newRequestID
                        # print(newRequestID)
                        newRequest['pur_receiver'] = owner
                        newRequest['pur_payer'] = manager
                        newRequest['pur_initiator'] = manager

                        
                        # Recalculate pm_due_date given error above
                        print("lease_start: ", lease_start, type(lease_start))
                        # lease_start = lease_start.strftime("%m-%d-%Y %H:%M")
                        # print("lease_start: ", lease_start, type(lease_start))
                        pm_due_date = pmDueDate(lease_start)
                        print("PM Due Date from Function: ", pm_due_date)
                        newRequest['pur_due_date'] = pm_due_date.strftime("%m-%d-%Y %H:%M")
                        print("PM Due Date: ", newRequest['pur_due_date'], type(newRequest['pur_due_date']))                            

                        newRequest['purchase_type'] = f"{fee['fee_type']} due Owner"
                        newRequest['pur_group'] = grouping
                        newRequest['pur_late_fee'] = 0
                        newRequest['pur_perDay_late_fee'] = 0

                        print("Input to purchases PM-Owner: ", newRequest)
                        # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
                        db.insert('purchases', newRequest)
                        print("PM-Owner Fee Inserted")



                        # Create Owner-PM Payment
                        # Find contract fees based on rent
                        manager_fees = db.execute("""
                                SELECT -- *
                                    contract_uid, contract_property_id, contract_business_id
                                    -- , contract_start_date, contract_end_date
                                    , contract_fees
                                    -- , contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                    , jt.*
                                FROM 
                                    space.contracts,
                                    JSON_TABLE(
                                        contract_fees,
                                        "$[*]" COLUMNS (
                                            of_column VARCHAR(50) PATH "$.of",
                                            charge_column VARCHAR(50) PATH "$.charge",
                                            fee_name_column VARCHAR(50) PATH "$.fee_name",
                                            fee_type_column VARCHAR(10) PATH "$.fee_type",
                                            frequency_column VARCHAR(20) PATH "$.frequency"
                                        )
                                    ) AS jt
                                -- WHERE contract_uid = '010-000003' AND of_column LIKE '%rent%';
                                WHERE contract_uid = \'""" + fee['contract_uid'] + """\' AND of_column LIKE '%rent%';
                            """)
                        # print("\n Manager Fees: ", manager_fees)
                    

                        for j in range(len(manager_fees['result'])):

                            # Check if fees is monthly 
                            if manager_fees['result'][j]['frequency_column'] == 'Monthly' or manager_fees['result'][j]['frequency_column'] == 'monthly':
                                # print("Mon        thly Charge")

                                # Check if charge is a % or Fixed $ Amount
                                if manager_fees['result'][j]['fee_type_column'] == '%' or manager_fees['result'][j]['fee_type_column'] == 'PERCENT':
                                    charge_amt = Decimal(manager_fees['result'][j]['charge_column']) * Decimal(pro_rate_amt) / 100
                                else:
                                    charge_amt = Decimal(manager_fees['result'][j]['charge_column'])
                                # print("Charge Amount: ", charge_amt, property, fee['contract_uid'], manager_fees['result'][j]['charge_column'], response['result'][i]['charge'] )

                                # Create JSON Object for Fee Purchase
                                newPMRequest = {}
                                newPMRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                                newPMRequest['purchase_uid'] = newPMRequestID
                                # print(newPMRequestID)
                                
                                
                                newPMRequest['pur_timestamp'] = newRequest['pur_timestamp']
                                newPMRequest['pur_property_id'] = newRequest['pur_property_id']
                                newPMRequest['pur_leaseFees_id'] = newRequest['pur_leaseFees_id']

                                newPMRequest['purchase_status'] = newRequest['purchase_status']
                                newPMRequest['pur_status_value'] = newRequest['pur_status_value']
                                newPMRequest['purchase_date'] = newRequest['purchase_date']
                                newPMRequest['pur_group'] = newRequest['pur_group']
                                newPMRequest['pur_late_by'] = newRequest['pur_late_by']
                                newPMRequest['pur_late_fee'] = newRequest['pur_late_fee']
                                newPMRequest['pur_perDay_late_fee'] = newRequest['pur_perDay_late_fee']
                                

                                newPMRequest['purchase_type'] = "Management"
                                newPMRequest['pur_cf_type'] = "expense"
                                newPMRequest['pur_amount_due'] = charge_amt
                                
                                newPMRequest['pur_notes'] = manager_fees['result'][j]['fee_name_column']
                                newPMRequest['pur_description'] =  f"{manager_fees['result'][j]['fee_name_column']} for Initial Rent "
                                # newPMRequest['pur_description'] =  newRequestID # Original Rent Purchase ID  
                                # newPMRequest['pur_description'] = f"Fees for MARCH {nextMonth.year} CRON"
                                
                                newPMRequest['pur_receiver'] = manager
                                newPMRequest['pur_payer'] = owner
                                newPMRequest['pur_initiator'] = manager
                                

                                newPMRequest['pur_due_date'] =  newRequest['pur_due_date']
                                
                                # print(newPMRequest)
                                db.insert('purchases', newPMRequest)



                        # POST NEXT MONTHS RENT.  Is this necessary if the CRON job checks if next month rent is due?
                        if pro_rate_days < avail_to_pay:
                            print("Also post regular cycle rent (ie 2 cycles)")

                            newRequest['pur_amount_due'] = fee['charge']
                            newRequest['pur_due_date'] = due_date.strftime('%m-%d-%Y %H:%M')
                            print("Next Months Rent Due: ", newRequest['pur_due_date'], type(newRequest['pur_due_date']))   


                            newRequest['pur_description'] = f"First Month {fee['fee_name']}"

                            # Create JSON Object for Rent Purchase for Tenant-PM Payment
                            newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                            grouping = newRequestID
                            newRequest['purchase_uid'] = newRequestID
                            newRequest['pur_group'] = grouping
                            # print(newRequestID)
                            newRequest['pur_receiver'] = manager
                            newRequest['pur_payer'] = tenant
                            newRequest['pur_initiator'] = manager
                            
                            
                            # print(newRequest)
                            # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
                            db.insert('purchases', newRequest)



                            # Create PM-Owner Payment
                            # Create JSON Object for Rent Purchase for PM-Owner Payment
                            newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                            newRequest['purchase_uid'] = newRequestID
                            # print(newRequestID)
                            newRequest['pur_receiver'] = owner
                            newRequest['pur_payer'] = manager
                            newRequest['pur_initiator'] = manager
                            
                            newRequest['pur_due_date'] = (due_date + timedelta(days=15)).strftime('%m-%d-%Y %H:%M')
                            print("Next Months PM-Owner Due: ", newRequest['pur_due_date'], type(newRequest['pur_due_date'])) 

                            newRequest['pur_group'] = grouping
                            newRequest['pur_late_fee'] = 0
                            newRequest['pur_perDay_late_fee'] = 0

                            # print(newRequest)
                            # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
                            db.insert('purchases', newRequest)



                            # Create Owner-PM Payment
                            # Find contract fees based on rent
                            manager_fees = db.execute("""
                                    SELECT -- *
                                        contract_uid, contract_property_id, contract_business_id
                                        -- , contract_start_date, contract_end_date
                                        , contract_fees
                                        -- , contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                                        , jt.*
                                    FROM 
                                        space.contracts,
                                        JSON_TABLE(
                                            contract_fees,
                                            "$[*]" COLUMNS (
                                                of_column VARCHAR(50) PATH "$.of",
                                                charge_column VARCHAR(50) PATH "$.charge",
                                                fee_name_column VARCHAR(50) PATH "$.fee_name",
                                                fee_type_column VARCHAR(10) PATH "$.fee_type",
                                                frequency_column VARCHAR(20) PATH "$.frequency"
                                            )
                                        ) AS jt
                                    -- WHERE contract_uid = '010-000003' AND of_column LIKE '%rent%';
                                    WHERE contract_uid = \'""" + fee['contract_uid'] + """\' AND of_column LIKE '%rent%';
                                """)
                            # print("\n Manager Fees: ", manager_fees)
                        

                            for j in range(len(manager_fees['result'])):

                                # Check if fees is monthly 
                                if manager_fees['result'][j]['frequency_column'] == 'Monthly' or manager_fees['result'][j]['frequency_column'] == 'monthly':
                                    # print("Mon        thly Charge")

                                    # Check if charge is a % or Fixed $ Amount
                                    if manager_fees['result'][j]['fee_type_column'] == '%' or manager_fees['result'][j]['fee_type_column'] == 'PERCENT':
                                        charge_amt = Decimal(manager_fees['result'][j]['charge_column']) * Decimal(fee['charge']) / 100
                                    else:
                                        charge_amt = Decimal(manager_fees['result'][j]['charge_column'])
                                    # print("Charge Amount: ", charge_amt, property, fee['contract_uid'], manager_fees['result'][j]['charge_column'], response['result'][i]['charge'] )

                                    # Create JSON Object for Fee Purchase
                                    newPMRequest = {}
                                    newPMRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                                    newPMRequest['purchase_uid'] = newPMRequestID
                                    # print(newPMRequestID)
                                    
                                    
                                    newPMRequest['pur_timestamp'] = newRequest['pur_timestamp']
                                    newPMRequest['pur_property_id'] = newRequest['pur_property_id']
                                    newPMRequest['purchase_status'] = newRequest['purchase_status']
                                    newPMRequest['pur_status_value'] = newRequest['pur_status_value']
                                    newPMRequest['purchase_date'] = newRequest['purchase_date']
                                    newPMRequest['pur_group'] = newRequest['pur_group']
                                    newPMRequest['pur_late_by'] = newRequest['pur_late_by']
                                    newPMRequest['pur_late_fee'] = newRequest['pur_late_fee']
                                    newPMRequest['pur_perDay_late_fee'] = newRequest['pur_perDay_late_fee']

                                    newPMRequest['purchase_type'] = "Management"
                                    newPMRequest['pur_cf_type'] = "expense"
                                    newPMRequest['pur_amount_due'] = charge_amt
                                    
                                    newPMRequest['pur_notes'] = manager_fees['result'][j]['fee_name_column']
                                    newPMRequest['pur_description'] =  f"{manager_fees['result'][j]['fee_name_column']} for First Month Rent "
                                    # newPMRequest['pur_description'] =  newRequestID # Original Rent Purchase ID  
                                    # newPMRequest['pur_description'] = f"Fees for MARCH {nextMonth.year} CRON"
                                    
                                    newPMRequest['pur_receiver'] = manager
                                    newPMRequest['pur_payer'] = owner
                                    newPMRequest['pur_initiator'] = manager
                                    

                                    newPMRequest['pur_due_date'] = (due_date + timedelta(days=15)).strftime('%m-%d-%Y %H:%M')
                                    print("Next Months PM-Owner Due: ", newPMRequest['pur_due_date'], type(newPMRequest['pur_due_date'])) 

                                    # print(newPMRequest)
                                    db.insert('purchases', newPMRequest)
                            
                            
                        else: 
                            print("Only post pro-rate rent")

                            

                    else: 
                        print("Not Rent")
                        # Create JSON Object for Rent Purchase for Tenant-PM Payment
                        newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                        grouping = newRequestID
                        newRequest['purchase_uid'] = newRequestID
                        newRequest['pur_group'] = grouping
                        # print(newRequestID)
                        newRequest['pur_receiver'] = manager
                        newRequest['pur_payer'] = tenant
                        newRequest['pur_initiator'] = manager
                        # newRequest['pur_due_date'] = fee['lease_start'] if fee['lease_start'] != 'None' else datetime.today().date().strftime('%m-%d-%Y %H:%M')
                        newRequest['pur_due_date'] = today.strftime("%m-%d-%Y %H:%M")
                        print("Non Rent Due Date: ", newRequest['pur_due_date'], type(newRequest['pur_due_date']))
                        
                        newRequest['pur_amount_due'] = fee['charge']
                        # print(newRequest)
                        # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
                        db.insert('purchases', newRequest)

        return response


class LeaseReferal(Resource):

    def __call__(self):
        print("In Lease Referal")


    def post(self):
        print("In Lease Referal POST")
        response = {}
        lease_assigned_contacts = []
        

        payload = request.get_json()
        print("Lease Referal Add Payload: ", payload)
        tenants = payload["tenants"]
        print("tenants: ", tenants)
        lease_property_id = payload["property_uid"]

        with connect() as db: 

            lease_uid = db.call('space.new_lease_uid')['result'][0]['new_id']
            print("New Lease UID: ", lease_uid)

            # IF tenant_uid ==> POST
            # IF no tenant_uid ==> Check if user_uid
            #    If user_uid ==> Create Tenant Role
            #    If no user_uid ==> create user_uid and tenant UID


            # Need to figure out if the refered tenant has an account
            for tenant in tenants:
                print("\n",tenant["email"])
                tenant_responsibiity = str(float(tenant["lease_perc"])/100)
                print(tenant_responsibiity)

                # Check if Tenant ID exists
                try:
                    tenantID = (""" 
                        SELECT * 
                        FROM space.tenantProfileInfo
                        WHERE tenant_email = \'""" + tenant["email"] + """\'
                    """)

                    # print(tenantID)
                    tenant_response = db.execute(tenantID, [], 'get')
                    # print(response1['result'][0]['tenant_uid'])
                    tenant_uid = tenant_response['result'][0]['tenant_uid']
                    print("Found tenant_uid: ", tenant_uid)
                    

                    # IF Tenant ID has been found ==> 
                        # Write Property - Tenant ID - % to lease_tenant
                        # Add tenants to lease_assigned_contacts JSON object
                
                except:
                    print("tenantID not found")

                    try:
                        user_uid = 0

                        userID = (""" 
                            SELECT *
                            FROM space.users
                            WHERE email = \'""" + tenant["email"] + """\'
                        """)

                        # print(userID)
                        user_response = db.execute(userID, [], 'get')
                        # print(response2['result'][0]['user_uid'])
                        
                        user_uid = user_response['result'][0]['user_uid']
                        roles = str(user_response['result'][0]['role'])
                        print(user_uid)
                        print('Active Roles: ', roles)

                        roles += ",TENANT"
                        print('New Roles: ', roles)


                        response = db.update('space.users', {"user_uid": user_uid}, {'role': roles})
                        print("MYSPACE response: ", response)
                        print("MYSPACE response code: ", response['code'])


                        raise Exception("Found User ID")
                        # Write Property - Tenant ID - % to lease_tenant
                        # Add tenants to lease_assigned_contacts JSON object

                    except:
                        print("In userID exception")

                        # Create User ID

                        # Create UserID

                        # query = ["CALL space.new_user_uid;"]
                        # NewIDresponse = db.execute(query[0], [], 'get')
                    
                        # newUserID = NewIDresponse["result"][0]["new_id"]
                        if user_uid == 0:
                            print("UserID not found")
                            user_uid = db.call('space.new_user_uid')['result'][0]['new_id']
                        
                            print("MySpace userID: ", user_uid)

                            userQuery = ("""
                                    INSERT INTO space.users SET
                                        user_uid = \'""" + user_uid + """\',
                                        first_name = \'""" + tenant["first_name"] + """\',
                                        last_name = \'""" + tenant["last_name"] + """\',
                                        phone_number = \'""" + tenant["phone_number"] + """\',
                                        email = \'""" + tenant["email"] + """\',
                                        role = 'TENANT'
                                            """)
                            # print("Myspace Query: ", userQuery)
                            response["user_uid"] = db.execute(userQuery, [], "post")
                            print("MYSPACE User response: ", response)
                            # print("MYSPACE response code: ", response['code'])



                        # Create Tenant ID

                        print("create tenant ID")
                        # newTenantID = db.call('space.new_tenant_uid')['result'][0]['new_id']
                        # print("MySpace tenantID: ", newTenantID)

                        # tenantQuery = ("""
                        #         INSERT INTO space.tenantProfileInfo SET
                        #             tenant_uid = \'""" + newTenantID + """\',
                        #             tenant_user_id = \'""" + newUserID + """\',
                        #             tenant_first_name = \'""" + tenant["first_name"] + """\',
                        #             tenant_last_name = \'""" + tenant["last_name"] + """\',
                        #             tenant_phone_number = \'""" + tenant["phone_number"] + """\',
                        #             tenant_email = \'""" + tenant["email"] + """\',
                        #                 """)
                        # print("Myspace Query: ", tenantQuery)
                        # response = db.execute(tenantQuery, [], "post")
                        # print("MYSPACE response: ", response)
                        # print("MYSPACE response code: ", response['code'])

                        profile_info = {}

                        tenant_uid = db.call('space.new_tenant_uid')['result'][0]['new_id']
                        print("MySpace tenantID: ", tenant_uid)

                        profile_info["tenant_uid"] = tenant_uid
                        profile_info['tenant_user_id'] = user_uid
                        profile_info['tenant_first_name'] = tenant["first_name"]
                        profile_info['tenant_last_name'] = tenant["last_name"]
                        profile_info['tenant_phone_number'] = tenant["phone_number"]
                        profile_info['tenant_email'] = tenant["email"]
                        

                        # Check and add the keys using ternary expressions
                        profile_info['tenant_documents'] =  '[]'
                        profile_info['tenant_adult_occupants'] =  '[]'
                        profile_info['tenant_children_occupants'] =  '[]'
                        profile_info['tenant_vehicle_info'] =  '[]'
                        profile_info['tenant_references'] =  '[]'
                        profile_info['tenant_pet_occupants'] =  '[]'
                        profile_info['tenant_employment'] =  '[]'
                        # print("Updated Tenant Profile: ", tenant_profile)

                        

                        response = db.insert('tenantProfileInfo', profile_info)
                        response["tenant_uid"] = profile_info["tenant_uid"]
                        print("MYSPACE Tenant response: ", response)


                
                # Add Tenant to Lease Contacts
                print(tenant_uid)
                ltQuery = (""" 
                    INSERT INTO space.lease_tenant
                    SET lt_lease_id = \'""" + lease_uid + """\'
                        , lt_tenant_id = \'""" + tenant_uid + """\'
                        , lt_responsibility = \'""" + tenant_responsibiity + """\';
                    """)
                # print("Made it to here")
                response = db.execute(ltQuery, [], 'post')

                # Add Tenant to Lease
                lease_assigned_contacts.append(tenant_uid)


            # Add Tenant List to New Lease
            payload = {}
            print(lease_assigned_contacts)
            payload["lease_assigned_contacts"] = json.dumps(lease_assigned_contacts)
            payload["lease_uid"] = lease_uid
            payload["lease_property_id"] = lease_property_id
            
            # Verify LIST variables are not empty
            fields_with_lists = ["lease_adults", "lease_children", "lease_pets", "lease_vehicles", "lease_referred", "lease_documents", "lease_income", "lease_utilities"]
            for field in fields_with_lists:
                print("field list", field)
                if payload.get(field) in [None, '', 'undefined']:
                    print(field,"Is None")
                    payload[field] = '[]' 

            # Actual Insert Statement
            print("About to insert: ", payload)
            response["lease"] = db.insert('leases', payload)
            print("Data inserted into space.leases", response)

        
            response["msg"] = "Lease Referal Endpoint"

            return response
        






        
