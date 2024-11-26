
# from myspace_api import get_new_propertyUID

from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity


from data_pm import connect, uploadImage, deleteImage, deleteFolder, s3, processImage
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest
import ast
from queries import RentPropertiesQuery



def updateImages(imageFiles, property_uid):
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
    bucket.objects.filter(Prefix=f'properties/{property_uid}/').delete()
    images = []
    for i in range(len(imageFiles.keys())):
    
        filename = f'img_{i-1}'
        if i == 0:
            filename = 'img_cover'
        key = f'properties/{property_uid}/{filename}'
        image = uploadImage(
            imageFiles[filename], key, content[i])
            # imageFiles[filename], key, '')
    
        images.append(image)
    return images



class Properties(Resource):
    def get(self, uid):
        print('in Properties')
        response = {}
        # conn = connect()
        print("Property User UID: ", uid)

        if   uid[:3] == '110':
            print("In Owner ID")
            with connect() as db:
                # print("in connect loop")

                print("In Find Applications")
                applicationQuery = db.execute("""
                    -- FIND APPLICATIONS CURRENTLY IN PROGRESS
                    SELECT property_uid
                            , leases.*
                            , lease_fees
                            , t_details.*
                        FROM space.properties
                        LEFT JOIN space.leases ON property_uid = lease_property_id
                        LEFT JOIN (SELECT fees_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                                ('leaseFees_uid', leaseFees_uid,
                                'fee_name', fee_name,
                                'fee_type', fee_type,
                                'charge', charge,
                                'due_by', due_by,
                                'late_by', late_by,
                                'late_fee',late_fee,
                                'perDay_late_fee', perDay_late_fee,
                                'frequency', frequency,
                                'available_topay', available_topay,
                                'due_by_date', due_by_date
                                )) AS lease_fees
                                FROM space.leaseFees
                                GROUP BY fees_lease_id) AS lf ON fees_lease_id = lease_uid
                        LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                        LEFT JOIN space.property_owner ON property_id = property_uid
                        WHERE (leases.lease_status = "NEW" OR leases.lease_status = "SENT" OR leases.lease_status = "REJECTED" OR leases.lease_status = "REFUSED" OR leases.lease_status = "PROCESSING" OR leases.lease_status = "TENANT APPROVED")
                        AND property_owner_id = \'""" + uid + """\'   
                        """)
                response["Applications"] = applicationQuery
                # print("Query: ", applicationQuery)


                response["Property"] = RentPropertiesQuery(uid)



                print("In Maintenance Requests")

                maintenanceQuery = db.execute("""
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
                                    SELECT -- maintenance_quote_uid, 
                                        quote_maintenance_request_id AS qmr_id
                                        -- , quote_status
                                        , MAX(quote_rank) AS max_quote_rank
                                    FROM (
                                        SELECT -- *,
                                            maintenance_quote_uid, quote_maintenance_request_id, quote_status,
                                            -- , quote_pm_notes, quote_business_id, quote_services_expenses, quote_earliest_available_date,quote_earliest_available_time, quote_event_type, quote_event_duration, quote_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
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
                    LEFT JOIN ( SELECT * FROM space.contracts WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                        -- WHERE contract_business_id = \'""" + uid + """\'
                        -- WHERE contract_business_id = "600-000003"
                        -- WHERE owner_uid = "110-000003"
                #       WHERE owner_uid = \'""" + uid + """\'
                    """)

                # print("Query: ", maintenanceQuery)
                response["MaintenanceRequests"] = maintenanceQuery
        
        elif uid[:3] == '600':
            print("In Business ID")

            # APPLICATIONS
            with connect() as db:
                # print("in connect loop")
                print("In Find Applications")
                applicationQuery = db.execute("""
                -- FIND APPLICATIONS CURRENTLY IN PROGRESS
                SELECT property_uid
                    , leases.*
                    , lease_fees
                    , t_details.*
                FROM space.properties
                LEFT JOIN space.leases ON property_uid = lease_property_id
                LEFT JOIN (SELECT fees_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                        ('leaseFees_uid', leaseFees_uid,
                        'fee_name', fee_name,
                        'fee_type', fee_type,
                        'charge', charge,
                        'due_by', due_by,
                        'late_by', late_by,
                        'late_fee',late_fee,
                        'perDay_late_fee', perDay_late_fee,
                        'frequency', frequency,
                        'available_topay', available_topay,
                        'due_by_date', due_by_date
                        )) AS lease_fees
                        FROM space.leaseFees
                        GROUP BY fees_lease_id) AS lf ON fees_lease_id = lease_uid
                LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                LEFT JOIN space.contracts ON contract_property_id = property_uid
                WHERE leases.lease_status NOT IN ('ACTIVE', 'ACTIVE M2M', 'ENDED', 'TERMINATED')
                AND contract_status = "ACTIVE"
                AND contract_business_id = \'""" + uid + """\'                   
                """)
                response["Applications"] = applicationQuery

                #PROPERTIES
                response["Property"] = RentPropertiesQuery(uid)

                # NEW PM REQUESTS
                print("In New PM Requests")
                contractsQuery = db.execute("""
                    -- NEW PROPERTIES FOR MANAGER
                    SELECT *, CASE WHEN announcements IS NULL THEN false ELSE true END AS announcements_boolean
                    FROM space.o_details
                    LEFT JOIN space.properties ON property_id = property_uid
                    LEFT JOIN space.b_details ON contract_property_id = property_uid
                    LEFT JOIN (
                    SELECT announcement_properties, JSON_ARRAYAGG(JSON_OBJECT
                    ('announcement_uid', announcement_uid,
                    'announcement_title', announcement_title,
                    'announcement_msg', announcement_msg,
                    'announcement_mode', announcement_mode,
                    'announcement_date', announcement_date,
                    'announcement_receiver', announcement_receiver
                    )) AS announcements
                    FROM space.announcements
                    GROUP BY announcement_properties) as t ON announcement_properties = property_uid
                    WHERE contract_business_id = \'""" + uid + """\'  AND (contract_status = "NEW" OR contract_status = "SENT" OR contract_status = "REJECTED");
                    """)

                # print("Query: ", contractsQuery)
                response["NewPMRequests"] = contractsQuery

                # MAINTENANCE REQUESTS
                print("In Maintenance Requests")
                maintenanceQuery = db.execute("""
                        SELECT *
                            -- quote_business_id, quote_status, maintenance_request_status, quote_total_estimate
                            , CASE
                                    WHEN maintenance_request_status = 'NEW' OR maintenance_request_status = 'INFO'              THEN "NEW REQUEST"
                                    WHEN maintenance_request_status = "SCHEDULED"                                               THEN "SCHEDULED"
                                    WHEN maintenance_request_status = 'CANCELLED' or quote_status = "FINISHED"                  THEN "COMPLETED"
                                    WHEN quote_status = "SENT" OR quote_status = "REFUSED" OR quote_status = "REQUESTED"
                                    OR quote_status = "REJECTED" OR quote_status = "WITHDRAWN" OR quote_status = "MORE INFO"    THEN "QUOTES REQUESTED"
                                    WHEN quote_status = "ACCEPTED" OR quote_status = "SCHEDULE"                                 THEN "QUOTES ACCEPTED"
                                    WHEN quote_status = "COMPLETED"                                                             THEN "PAID"     
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
                                        WHEN max_quote_rank = "12" THEN "MORE INFO"     
                                        WHEN max_quote_rank = "20" THEN "SENT"
                                        WHEN max_quote_rank = "21" THEN "REJECTED"
                                        WHEN max_quote_rank = "22" THEN "WITHDRAWN"
                                        WHEN max_quote_rank = "30" THEN "ACCEPTED"
                                        WHEN max_quote_rank = "40" THEN "SCHEDULE"
                                        WHEN max_quote_rank = "50" THEN "SCHEDULED"
                                        WHEN max_quote_rank = "60" THEN "RESCHEDULED"
                                        WHEN max_quote_rank = "65" THEN "CANCELLED"
                                        WHEN max_quote_rank = "70" THEN "FINISHED"
                                        WHEN max_quote_rank = "80" THEN "COMPLETED"
                                        ELSE "0"
                                    END AS quote_status
                                FROM 
                                (
                                    SELECT -- maintenance_quote_uid, 
                                        quote_maintenance_request_id AS qmr_id
                                        -- , quote_status
                                        , MAX(quote_rank) AS max_quote_rank
                                    FROM (
                                        SELECT -- *,
                                            maintenance_quote_uid, quote_maintenance_request_id, quote_status,
                                            -- , quote_pm_notes, quote_business_id, quote_services_expenses, quote_earliest_available_date,quote_earliest_available_time, quote_event_type, quote_event_duration, quote_notes, quote_created_date, quote_total_estimate, quote_maintenance_images, quote_adjustment_date
                                        CASE
                                            WHEN quote_status = "REQUESTED" THEN "10"
                                            WHEN quote_status = "REFUSED" THEN "11"
                                            WHEN quote_status = "MORE INFO" THEN "12"
                                            WHEN quote_status = "SENT" THEN "20"
                                            WHEN quote_status = "REJECTED" THEN "21"
                                            WHEN quote_status = "WITHDRAWN"  THEN "22"
                                            WHEN quote_status = "ACCEPTED" THEN "30"
                                            WHEN quote_status = "SCHEDULE" THEN "40"
                                            WHEN quote_status = "SCHEDULED" THEN "50"
                                            WHEN quote_status = "RESCHEDULED" THEN "60"
                                            WHEN quote_status = "CANCELLED" THEN "65"
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
                    LEFT JOIN ( SELECT * FROM space.contracts WHERE contract_status = "ACTIVE") AS c ON maintenance_property_id = contract_property_id
                        WHERE contract_business_id = \'""" + uid + """\'
                        -- WHERE contract_business_id = "600-000003"
                        -- WHERE owner_uid = "110-000003"
                #       -- WHERE owner_uid = \'""" + uid + """\'
                    """)

                # print("Query: ", maintenanceQuery)
                response["MaintenanceRequests"] = maintenanceQuery
        
        elif uid[:3] == '350':
            print("In Tenant ID")
            with connect() as db:
                # print("in connect loop")
                print("In Find Applications")
                applicationQuery = db.execute("""
                    -- FIND APPLICATIONS CURRENTLY IN PROGRESS
               SELECT property_uid
                    , leases.*
                    , lease_fees
                    , t_details.*
                FROM space.properties
                LEFT JOIN space.leases ON property_uid = lease_property_id
                LEFT JOIN (SELECT fees_lease_id, JSON_ARRAYAGG(JSON_OBJECT
                        ('leaseFees_uid', leaseFees_uid,
                        'fee_name', fee_name,
                        'fee_type', fee_type,
                        'charge', charge,
                        'due_by', due_by,
                        'late_by', late_by,
                        'late_fee',late_fee,
                        'perDay_late_fee', perDay_late_fee,
                        'frequency', frequency,
                        'available_topay', available_topay,
                        'due_by_date', due_by_date
                        )) AS lease_fees
                        FROM space.leaseFees
                        GROUP BY fees_lease_id) AS lf ON fees_lease_id = lease_uid
                LEFT JOIN space.t_details ON lease_uid = lt_lease_id
                WHERE (leases.lease_status = "NEW" OR leases.lease_status = "SENT" OR leases.lease_status = "REJECTED" OR leases.lease_status = "REFUSED" OR leases.lease_status = "PROCESSING" OR leases.lease_status = "TENANT APPROVED")
                AND lt_tenant_id = \'""" + uid + """\';
                """)
            response["Applications"] = applicationQuery
            with connect() as db:
                # print("Query: ", propertiesQuery)

                propertiesQuery = db.execute(""" 
                        -- PROPERTIES BY TENANT
                        SELECT p.*
                            , CASE
                                    WHEN ISNULL(contract_uid) THEN "NO MANAGER"
                                    WHEN (lease_status = 'ACTIVE' AND payment_status IS NOT NULL) THEN payment_status
                                    WHEN (lease_status = 'ACTIVE' AND payment_status IS NULL) THEN 'UNPAID'
                                    ELSE 'VACANT'
                                END AS rent_status 
                        --     , contract_uid AS actual_contract_uid
                        --     , lease_status AS actual_lease_status
                        FROM (
                            SELECT * FROM space.p_details
                            -- WHERE business_uid = "600-000003"
                            -- WHERE owner_uid = "110-000003"
                            -- WHERE owner_uid = \'""" + uid + """\'
                            -- WHERE business_uid = \'""" + uid + """\'
                            WHERE tenant_uid = \'""" + uid + """\' AND contract_end_date < curdate()
                            -- WHERE tenant_uid = '350-000002' AND contract_end_date < curdate()
                            ) as p
                        LEFT JOIN (
                            SELECT * 
                            FROM space.pp_status
                            WHERE  (purchase_type = "RENT" OR ISNULL(purchase_type))
                                AND LEFT(pur_payer, 3) = '350'
                                AND (cf_month = DATE_FORMAT(NOW(), '%M') OR ISNULL(cf_month))
                                AND (cf_year = DATE_FORMAT(NOW(), '%Y') OR ISNULL(cf_year))
                            ) AS r ON p.property_uid = pur_property_id
                        LEFT JOIN (
                            SELECT -- * 
                                property_owner_id
                                , maintenance_property_id
                                , COUNT(maintenance_property_id) AS num_open_maintenace_req
                            FROM space.maintenanceRequests
                            LEFT JOIN space.o_details ON maintenance_property_id = property_id
                            WHERE  maintenance_request_status != "COMPLETED" AND maintenance_request_status != "CANCELLED"
                            GROUP BY maintenance_property_id
                            ) AS m ON p.property_uid = maintenance_property_id
                            order by lease_status; 
                        """)  

            # print("Query: ", propertiesQuery)
            response["Property"] = propertiesQuery
            # return response
        
        elif uid[:3] == '200':
            print("In Property ID")

            with connect() as db:
                # print("in connect loop")
                print("In Find Property with Appliances")
                propertiesQuery = db.execute("""
                    -- RETURN PROPERTIES WITH APPLIANCES
                    SELECT *
                    FROM space.p_details AS p      
                    -- WHERE property_uid = '200-000001'              
                    WHERE property_uid = \'""" + uid + """\'
                """)
            response["Property"] = propertiesQuery
        
        else:
            print("UID Not found")
            response["Property"] = "UID Not Found"
            
        
        return response        

    
    def post(self):
        print("\nIn Property POST")
        response = {}
        appliances = {}
        payload = request.form.to_dict()
        print("Property Add Payload: ", payload)

        # Verify uid has NOT been included in the data
        if payload.get('property_uid'):
            print("property_uid found.  Please call PUT endpoint")
            raise BadRequest("Request failed, UID found in payload.")

        with connect() as db:
            newPropertyUID = db.call('new_property_uid')['result'][0]['new_id']
            key = {'property_uid': newPropertyUID}
            print("Property Key: ", key)

            # --------------- PROCESS IMAGES ------------------

            processImage(key, payload)
            print("Payload after function: ", payload)
            
            # --------------- PROCESS IMAGES ------------------

        
            # Add Appliances (if provided)
            print("\nAppliances: ", payload.get('appliances'), type(payload.get('appliances')))
            try:
                appliances = json.loads(payload.pop('appliances'))    
                # appliances = "{\"appliances\":[\"050-000023\",\"050-000024\",\"050-000025\"]}"  
                print("Appliance Data: ", appliances, type(appliances))

                if appliances:
                        newAppliance = {}
                        # appliances is now a list of strings
                        for appliance in appliances:
                            print(f"Appliance: {appliance}")
                            # newRequestID = db.call('new_property_uid')['result'][0]['new_id']
                            # print(newRequestID)
                            NewApplianceID = db.call('new_appliance_uid')['result'][0]['new_id']
                            newAppliance['appliance_uid'] = NewApplianceID
                            print(NewApplianceID)
                            newAppliance['appliance_property_id'] = newPropertyUID
                            newAppliance['appliance_type'] = appliance
                            newAppliance['appliance_images'] = '[]'
                            newAppliance['appliance_documents'] = '[]'
                            response['Add Appliance'] = db.insert('appliances', newAppliance)
                            print(response['Add Appliance'])
                else:
                    print("No appliances provided in the form.")
            except:
                print(f"Add Appliance failed")
                response['add_appliance_error'] = f"No Appliance Data Provided"


            # Add Property Owner Info
            newPropertyOwner = {}
            if str(payload.get("property_owner_id", "")[:3]) == "100":
                property_user_id = payload.pop("property_owner_id")

                findOwnerIdQuery = db.execute(""" 
                            -- FIND OWNER UID
                            SELECT owner_uid
                            FROM space.ownerProfileInfo
                            -- WHERE owner_user_id = "100-000007"
                            WHERE owner_user_id = \'""" + property_user_id + """\' 
                            """)
                # print("findOwnerIdQuery: ", findOwnerIdQuery)
                property_owner_id  = findOwnerIdQuery['result'][0]['owner_uid']
            else:
                property_owner_id = payload.pop("property_owner_id")
            # print("Property Owner ID: ", property_owner_id)
                

            newPropertyOwner['property_id'] = newPropertyUID
            newPropertyOwner['property_owner_id'] = property_owner_id
            newPropertyOwner['po_owner_percent'] = payload.pop("po_owner_percent", 1)
            print("newPropertyOwner Payload: ", newPropertyOwner)
            response['Add Owner'] = db.insert('property_owner', newPropertyOwner)
            print("\nNew Property-Owner Relationship Added")
         

            # Add Property Info
            payload['property_images'] = '[]' if payload.get('property_images') in {None, '', 'null'} else payload.get('property_images', '[]')
            print("Add Property Payload: ", payload)    
            payload["property_listed_date"] = datetime.today().strftime('%m-%d-%Y') if payload.pop('property_listed') == '1' else ''
            # payload.pop('property_listed')
            payload["property_uid"] = newPropertyUID
            response['Add Property'] = db.insert('properties', payload)
            response['property_UID'] = newPropertyUID
            response['Images Added'] = payload.get('property_images', "None")

            print("\nNew Property Added")

        return response

    
    def put(self):
        print("\nIn Property PUT")
        response = {}

        payload = request.form.to_dict()
        print("Propoerty Update Payload: ", payload)

        # Verify uid has been included in the data
        if payload.get('property_uid') in {None, '', 'null'}:
            print("No property_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        # property_uid = payload.get('property_uid')
        key = {'property_uid': payload.pop('property_uid')}
        print("Property Key: ", key)


        # --------------- PROCESS IMAGES ------------------

        processImage(key, payload)
        print("Payload after function: ", payload)
        
        # --------------- PROCESS IMAGES ------------------


    

        # Write to Database
        with connect() as db:
            print("Checking Inputs: ", key, payload)
            response['property_info'] = db.update('properties', key, payload)
            # print("Response:" , response)
        return response


    def delete(self):
        print("In delete Property")
        response = {}

        with connect() as db:
            data = request.get_json(force=True)
            print(data)

            property_owner_id = data["property_owner_id"]
            property_id = data["property_id"]
            

            # Query
            delPropertyQuery = (""" 
                    -- DELETE PROPERTY FROM PROPERTY-OWNER TABLE
                    DELETE FROM space.property_owner
                    WHERE property_id = \'""" + property_id + """\'
                        AND property_owner_id = \'""" + property_owner_id + """\';         
                    """)

            # print("Query: ", delPropertyQuery)
            response_po = db.delete(delPropertyQuery) 
            # print("Query out", response_po["code"])
            # if response_po["code"] == 200:
            #     response["Deleted property_uid"] = property_id
            # else:
            #     response["Deleted property_uid"] = "Not successful"


            delPropertyQuery = (""" 
                    -- DELETE PROPERTY FROM PROPERTIES TABLE
                    DELETE FROM space.properties 
                    WHERE property_uid = \'""" + property_id + """\';         
                    """)

            # print("Query: ", delPropertyQuery)
            response = db.delete(delPropertyQuery) 
            # print("Query out", response["code"])


            #  Delete from S3 Bucket
            try:
                folder = 'properties'
                deleteFolder(folder, property_id)
                response["S3"] = "Folder deleted successfully"

            except:
                response["S3"] = "Folder delete FAILED"
            
            if response["code"] == 200:
                response["Deleted property_uid"] = property_id
            else:
                response["Deleted property_uid"] = "Not successful"

            if response_po["code"] == 200:
                response["Deleted po property_id"] = property_id
            else:
                response["Deleted po property_id"] = "Not successful"


            return response


