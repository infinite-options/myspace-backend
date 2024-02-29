
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
import ast

ALLOWED_EXTENSIONS = {'txt', 'pdf', 'doc', 'docx'}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


class LeaseDetails(Resource):
    # decorators = [jwt_required()]

    def get(self, filter_id):
        print('in Lease Details', filter_id)
        response = {}

        with connect() as db:
            if filter_id[:3] == "110":
                print('in Owner Lease Details')

                leaseQuery = db.execute(""" 
                        -- OWNER, PROPERTY MANAGER, TENANT LEASES
SELECT * 
FROM (SELECT lease_uid,lease_property_id,lease_application_date,lease_start,
lease_end,lease_status,lease_assigned_contacts,lease_documents,lease_early_end_date,lease_renew_status,
move_out_date,lease_adults,lease_children,lease_pets,lease_referred,lease_effective_date,lease_vehicles,
lease_docuSign FROM space.leases WHERE (lease_status = "ACTIVE" OR lease_status = "ENDED") 
AND move_out_date IS NULL OR (move_out_date IS NOT NULL AND move_out_date > DATE_SUB(CURDATE(), INTERVAL 3 MONTH))
) AS l
LEFT JOIN space.leaseFees ON fees_lease_id = lease_uid
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
WHERE owner_uid = \'""" + filter_id + """\'
GROUP BY lease_uid
-- WHERE owner_uid = "110-000003"
-- WHERE contract_business_id = \'""" + filter_id + """\'
-- WHERE contract_business_id = "600-000003"
-- WHERE tenants LIKE '%""" + filter_id + """%'
-- WHERE tenants LIKE "%350-000040%"
;
                        """)
                
            elif filter_id[:3] == "600":
                print('in PM Lease Details')

                leaseQuery = db.execute(""" 
                        -- OWNER, PROPERTY MANAGER, TENANT LEASES
                        SELECT * 
                        FROM (SELECT lease_uid,lease_property_id,lease_application_date,lease_start,
lease_end,lease_status,lease_assigned_contacts,lease_documents,lease_early_end_date,lease_renew_status,
move_out_date,lease_adults,lease_children,lease_pets,lease_referred,lease_effective_date,lease_vehicles,
lease_docuSign FROM space.leases WHERE (lease_status = "ACTIVE" OR lease_status = "ENDED")
AND move_out_date IS NULL OR (move_out_date IS NOT NULL AND move_out_date > DATE_SUB(CURDATE(), INTERVAL 3 MONTH))
) AS l
						LEFT JOIN space.leaseFees ON fees_lease_id = lease_uid
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
                        -- WHERE owner_uid = \'""" + filter_id + """\'
                        -- WHERE owner_uid = "110-000003"
                        WHERE contract_business_id = \'""" + filter_id + """\'
                        -- WHERE contract_business_id = "600-000003"
                        -- WHERE tenants LIKE '%""" + filter_id + """%'
                        -- WHERE tenants LIKE "%350-000040%"
                        GROUP BY lease_uid
                        ;
                        """)
                
                # print(leaseQuery)

            elif filter_id[:3] == "350":
                print('in Tenant Lease Details')

                leaseQuery = db.execute(""" 
                        -- OWNER, PROPERTY MANAGER, TENANT LEASES
			SELECT * 
			FROM (SELECT lease_uid,lease_property_id,lease_application_date,lease_start,
			lease_end,lease_status,lease_assigned_contacts,lease_documents,lease_early_end_date,lease_renew_status,
			move_out_date,lease_adults,lease_children,lease_pets,lease_referred,lease_effective_date,lease_vehicles,
			lease_docuSign FROM space.leases WHERE (lease_status = "ACTIVE" OR lease_status = "ENDED")
			AND move_out_date IS NULL OR (move_out_date IS NOT NULL AND move_out_date > DATE_SUB(CURDATE(), INTERVAL 3 MONTH))
			) AS l
			LEFT JOIN space.leaseFees ON fees_lease_id = lease_uid
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
			-- WHERE owner_uid = \'""" + filter_id + """\'
			-- WHERE owner_uid = "110-000003"
			-- WHERE contract_business_id = \'""" + filter_id + """\'
			-- WHERE contract_business_id = "600-000003"
			WHERE tenants LIKE '%""" + filter_id + """%'
			-- WHERE tenants LIKE "%350-000040%"
			GROUP BY lease_uid
			;
                        """)
            else:
                leaseQuery = "UID Not Found"

            # print("Lease Query: ", leaseQuery)
            # items = execute(leaseQuery, "get", conn)
            # print(items)

            response["Lease_Details"] = leaseQuery

            # for i in range(len(leaseQuery['result'])):
            #     lease_id = leaseQuery['result'][i]["lease_uid"]
            #     leaseFeesQuery = db.execute("""
            #                             SELECT *
            #                             FROM space.leaseFees
            #                             WHERE fees_lease_id = \'""" + lease_id + """\';
            #                             """)
            #     if len(leaseFeesQuery['result']) >= 1:
            #         fees_dic = {}
            #         for j in range(len(leaseFeesQuery['result'])):
            #             fee_name = leaseFeesQuery['result'][j]["fee_name"]
            #             fees_dic[fee_name] = leaseFeesQuery['result'][j]
            #         leaseQuery['result'][i]["fees"] = fees_dic
            #     else:
            #         leaseQuery['result'][i]["fees"] = '[]'
            #     response["Lease_Details"] = leaseQuery

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

        data = request.form
        # print("data",data)

        # Remove tenenat_uid from data and store as an independant variable
        if 'tenant_uid' in data and data['tenant_uid']!="":
            print("TENANT_ID",data['tenant_uid'])
            tenant_uid = data.get('tenant_uid')
            # print(tenant_uid)
            # del data['tenant_uid']

        # ApplicationStatus = LeaseApplication.get(self, tenant_uid, data.get('lease_property_id'))
        # print(ApplicationStatus)
        # if ApplicationStatus != "New Application":
        #     print(ApplicationStatus)
        #
        #     # key = {'lease_uid': data.pop('lease_uid')}
        #     # key = ApplicationStatus
        #     # print(key)
        #     key = {'lease_uid': ApplicationStatus}
        #     print(key)
        #
        #     with connect() as db:
        #         response = db.update('leases', key, data)
        #
        #     print(response)

        # else:
            ApplicationStatus = "New"

            response = {}
            fields = ["lease_property_id", "lease_start", "lease_end", "lease_status", "lease_assigned_contacts",
                      "lease_documents", "lease_early_end_date", "lease_renew_status", "move_out_date","lease_move_in_date",
                      "lease_effective_date", "lease_application_date", "lease_docuSign", "lease_rent_available_topay", "lease_rent_due_by",
                      "lease_rent_late_by",
                      "lease_rent_perDay_late_fee", "lease_actual_rent", "lease_adults", "lease_children", "lease_pets",
                      "lease_vehicles", "lease_referred"]
            fields_with_lists = ["lease_adults", "lease_children", "lease_pets", "lease_vehicles", "lease_referred", "lease_assigned_contacts"
                                 , "lease_documents"]
            with connect() as db:
                # data = request.form
                # print("data", data["lease_fees"])
                newLease = {}
                for field in fields:
                    if field in data:
                        newLease[field] = data[field]
                for field in fields_with_lists:
                    if data.get(field) is None:
                        # print(field,"None")
                        newLease[field] = '[]'
                print("new_lease", newLease)
                db.insert('leases', newLease)

            print("Data inserted into space.leases")

            fields_leaseFees = ["charge", "due_by", "due_by_date", "late_by", "fee_name", "fee_type", "frequency", "available_topay",
                                "perDay_late_fee", "late_fee"]
            with connect() as db:
                print("Need to find lease_uid")
                leaseQuery = db.execute(""" 
                        -- FIND lease_uid
                        SELECT * FROM space.leases
                        ORDER BY lease_uid DESC
                        LIMIT 1;
                        """)
                # print(leaseQuery['result'][0]['lease_uid'])
                lease_id = leaseQuery['result'][0]['lease_uid']
                response["lease_uid"] = lease_id
                print("Lease ID", lease_id)

                if "lease_fees" in data:
                    print("lease_fees in data")
                    json_object = json.loads(data["lease_fees"])
                    print("lease fees json_object", json_object)
                    for fees in json_object:
                        print("fees",fees)
                        new_leaseFees = {}
                        new_leaseFees["fees_lease_id"] = lease_id
                        for item in fields_leaseFees:
                            if item in fees:
                                if item == "due_by_date":
                                    dateString = fees[item]
                                    dueByDate = datetime.datetime.strptime(dateString, '%m-%d-%Y')
                                    new_leaseFees["due_by_date"] = dueByDate
                                else:
                                    new_leaseFees[item] = fees[item]
                        db.insert('leaseFees', new_leaseFees)

            tenant_responsibiity = str(1)

            with connect() as db:
                print("Add record in lease_tenant table", lease_id, tenant_uid, tenant_responsibiity)
                # print("Made it to here")
                ltQuery = (""" 
                        INSERT INTO space.lease_tenant
                        SET lt_lease_id = \'""" + lease_id + """\'
                           , lt_tenant_id = \'""" + tenant_uid + """\'
                           , lt_responsibility = \'""" + tenant_responsibiity + """\';
                        """)

                response["lt_query"] = db.execute(ltQuery, [], 'post')

                # print(ltQuery)
                print("Data inserted into space.lease_tenant")

            # key = {'lease_uid': data.pop('lease_uid')}
            # print(key)

            response["UID"] = ApplicationStatus
        else:
            response['error'] = "Please Enter a correct tenant_id. Database was not updated"
        return response
	
    def put(self):
        print("In Lease Application PUT")
        response = {}
        data = request.form
        # data = request.form.to_dict()  #<== IF data came in as Form Data
        # print("data",data)
        # if data.get('lease_uid') is None:
        #     raise BadRequest("Request failed, no UID in payload.")
        lease_fields = ["lease_property_id", "lease_start", "lease_end", "lease_status", "lease_assigned_contacts",
                        "lease_early_end_date", "lease_renew_status", "move_out_date", "lease_move_in_date", "lease_effective_date",
                        "lease_docuSign", "lease_rent_available_topay", "lease_rent_due_by", "lease_rent_late_by",
                        "lease_rent_perDay_late_fee", "lease_actual_rent", "lease_adults", "lease_children",
                        "lease_pets", "lease_vehicles", "lease_referred"]
        fields_leaseFees = ["charge", "due_by", "due_by_date", "late_by", "fee_name", "fee_type", "frequency", "available_topay",
                            "perDay_late_fee", "late_fee"]

        if "lease_uid" in data:
            lease_id = data['lease_uid']
            payload = {}
            for field in data:
                if field in lease_fields:
                    payload[field] = data[field]
            with connect() as db:
                lease_uid = {'lease_uid': lease_id}
                query = db.select('leases', lease_uid)
                # print(f"QUERY: {query}")
                try:
                    lease_from_db = query.get('result')[0]
                    lease_docs = lease_from_db.get("lease_documents")
                    lease_docs = ast.literal_eval(lease_docs) if lease_docs else []  # convert to list of documents
                    print('type: ', type(lease_docs))
                    print(f'previously saved documents: {lease_docs}')
                except IndexError as e:
                    print(e)
                    raise BadRequest("Request failed, no such CONTRACT in the database.")

            files = request.files
            # print("files", files)
            # files_details = json.loads(data.get('lease_documents_details'))
            if files:
                detailsIndex = 0
                for key in files:
                    print("key", key)
                    file = files[key]
                    print("file", file)
                    # file_info = files_details[detailsIndex]
                    if file and allowed_file(file.filename):
                        key = f'leases/{lease_id}/{file.filename}'
                        print("key", key)
                        s3_link = uploadImage(file, key, '')
                        docObject = {}
                        docObject["link"] = s3_link
                        docObject["filename"] = file.filename
                        # docObject["type"] = file_info["fileType"]
                        lease_docs.append(docObject)
                    detailsIndex += 1

                payload['lease_documents'] = json.dumps(lease_docs)
                print(payload['lease_documents'])
            with connect() as db:
                key = {'lease_uid': lease_id}
                response["lease_update"] = db.update('leases', key, payload)

        if "lease_fees" in data:
            json_object = json.loads(data["lease_fees"])
            print("lease_fees_json_object",json_object) #rohit
            for fees in json_object:
                # print("fees",fees)
                if "lease_fees_id" in fees:
                    leaseFees_id = fees['lease_fees_id']
                    # del fees['lease_fees_id']
                    payload = {}
                    for field in fees:
                        if field in fields_leaseFees:
                            if field == "due_by_date":
                                dateString = fees[field]
                                print("ROHIT - dateString - ", dateString)
                                dueByDate = datetime.datetime.strptime(dateString, '%m-%d-%Y')
                                print("ROHIT - dueByDate - ", dueByDate)
                                payload["due_by_date"] = dueByDate
                            else:
                                payload[field] = fees[field]
                            
                    with connect() as db:
                        key = {'leaseFees_uid': leaseFees_id}
                        response["leaseFees_update"] = db.update('leaseFees', key, payload)
                else:
                    payload = {}
                    for field in fees:
                        if field in fields_leaseFees:
                            if field == "due_by_date":
                                dateString = fees[field]
                                print("ROHIT - dateString - ", dateString)
                                dueByDate = datetime.datetime.strptime(dateString, '%m-%d-%Y')
                                print("ROHIT - dueByDate - ", dueByDate)
                                payload["due_by_date"] = dueByDate
                            else:
                                payload[field] = fees[field]
                    payload["fees_lease_id"] = data['lease_uid']
                    with connect() as db:
                        response["leaseFees_update"] = db.insert('leaseFees', payload)

        return response
