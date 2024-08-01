
# from myspace_api import get_new_propertyUID

from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

# from data import connect, disconnect, execute, helper_upload_img, helper_icon_img
from data_pm import connect, uploadImage, s3
import boto3
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest
import ast



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

            with connect() as db:
                propertiesQuery = db.execute("""  
                        -- PROPERTY RENT STATUS FOR PROPERTIES
                        SELECT p.*
                            , pur_property_id, purchase_type, pur_due_date, pur_amount_due
                            , if(ISNULL(pur_status_value), "0", pur_status_value) AS pur_status_value
                            , if(ISNULL(purchase_status), "UNPAID", purchase_status) AS purchase_status
                            , pur_description, cf_month, cf_year
                            , CASE
                                WHEN ISNULL(contract_uid) THEN "NO MANAGER"
                                WHEN ISNULL(lease_status) THEN "VACANT"
                                WHEN ISNULL(purchase_status) THEN "UNPAID"
                                ELSE purchase_status
                                END AS rent_status  
                        FROM (
                            -- Find properties
                            SELECT * FROM space.p_details
                            -- WHERE business_uid = "600-000032"
                            -- WHERE owner_uid = "110-000003"
                            WHERE owner_uid = \'""" + uid + """\'
                            -- WHERE business_uid = \'""" + uid + """\'
                            -- WHERE tenant_uid = \'""" + uid + """\'  
                            ) AS p
                        -- Link to rent status
                        LEFT JOIN (
                            SELECT -- *
                                pur_property_id
                                , purchase_type
                                , pur_due_date
                                , SUM(pur_amount_due) AS pur_amount_due
                                , MIN(pur_status_value) AS pur_status_value
                                , purchase_status
                                , pur_description
                                , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_month
                                , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_year
                            FROM space.purchases
                            WHERE purchase_type = "Rent"
                                AND LEFT(pur_payer, 3) = '350'
                                AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = MONTH(CURRENT_DATE)
                                AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = YEAR(CURRENT_DATE)
                            GROUP BY pur_due_date, pur_property_id
                            ) AS pp
                            ON property_uid = pur_property_id                  
                        """)
            # print("Query: ", propertiesQuery)
            response["Property"] = propertiesQuery


            print("In Maintenance Requests")
            with connect() as db:
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

            # print("Query: ", propertiesQuery)
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
                WHERE (leases.lease_status = "NEW" OR leases.lease_status = "SENT" OR leases.lease_status = "REJECTED" OR leases.lease_status = "REFUSED" OR leases.lease_status = "PROCESSING" OR leases.lease_status = "TENANT APPROVED")
                AND contract_business_id = \'""" + uid + """\'                   
                """)
            response["Applications"] = applicationQuery

            #PROPERTIES
            with connect() as db:
                print("in connect loop")
                propertiesQuery = db.execute(""" 
                        -- PROPERTY RENT STATUS FOR PROPERTIES
                        SELECT p.*
                            , pur_property_id, purchase_type, pur_due_date, pur_amount_due
                            , if(ISNULL(pur_status_value), "0", pur_status_value) AS pur_status_value
                            , if(ISNULL(purchase_status), "UNPAID", purchase_status) AS purchase_status
                            , pur_description, cf_month, cf_year
                            , CASE
                                WHEN ISNULL(contract_uid) THEN "NO MANAGER"
                                WHEN ISNULL(lease_status) THEN "VACANT"
                                WHEN ISNULL(purchase_status) THEN "UNPAID"
                                ELSE purchase_status
                                END AS rent_status  
                        FROM (
                            -- Find properties
                            SELECT * FROM space.p_details
                            -- WHERE business_uid = "600-000032"
                            -- WHERE owner_uid = "110-000003"
                            -- WHERE owner_uid = \'""" + uid + """\'
                            WHERE business_uid = \'""" + uid + """\'
                            -- WHERE tenant_uid = \'""" + uid + """\'  
                            ) AS p
                        -- Link to rent status
                        LEFT JOIN (
                            SELECT -- *
                                pur_property_id
                                , purchase_type
                                , pur_due_date
                                , SUM(pur_amount_due) AS pur_amount_due
                                , MIN(pur_status_value) AS pur_status_value
                                , purchase_status
                                , pur_description
                                , MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_month
                                , YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) AS cf_year
                            FROM space.purchases
                            WHERE purchase_type = "Rent"
                                AND LEFT(pur_payer, 3) = '350'
                                AND MONTH(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = MONTH(CURRENT_DATE)
                                AND YEAR(STR_TO_DATE(pur_due_date, '%m-%d-%Y')) = YEAR(CURRENT_DATE)
                            GROUP BY pur_due_date, pur_property_id
                            ) AS pp
                            ON property_uid = pur_property_id  
                        """)  

            # print("Query: ", propertiesQuery)
            response["Property"] = propertiesQuery

            # NEW PM REQUESTS
            print("In New PM Requests")
            with connect() as db:
            # print("in New PM Requests connect loop")
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

            # print("Query: ", propertiesQuery)
            response["NewPMRequests"] = contractsQuery

            # MAINTENANCE REQUESTS
            print("In Maintenance Requests")
            with connect() as db:
            # print("in Maintenance Requests connect loop")
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
                        WHERE contract_business_id = \'""" + uid + """\'
                        -- WHERE contract_business_id = "600-000003"
                        -- WHERE owner_uid = "110-000003"
                #       -- WHERE owner_uid = \'""" + uid + """\'
                    """)


            # print("Query: ", propertiesQuery)
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
                    SELECT p.*,
                        JSON_ARRAYAGG(
                            JSON_OBJECT(
                                'appliance_uid', a.appliance_uid,
                                'appliance_property_id', a.appliance_property_id,
                                'appliance_type', a.appliance_type,
                                'appliance_url', a.appliance_url,
                                'appliance_images', a.appliance_images,
                                'appliance_available', a.appliance_available,
                                'appliance_installed', a.appliance_installed,
                                'appliance_model_num', a.appliance_model_num,
                                'appliance_purchased', a.appliance_purchased,
                                'appliance_serial_num', a.appliance_serial_num,
                                'appliance_manufacturer', a.appliance_manufacturer,
                                'appliance_warranty_info', a.appliance_warranty_info,
                                'appliance_warranty_till', a.appliance_warranty_till,
                                'appliance_purchased_from', a.appliance_purchased_from,
                                'appliance_purchase_order', a.appliance_purchase_order
                            )
                        ) AS appliances
                    FROM space.properties p
                    LEFT JOIN space.appliances a ON p.property_uid = a.appliance_property_id
                    WHERE property_uid = '200-000001'
                    GROUP BY p.property_uid;
                """)
            response["Property"] = propertiesQuery
        
        else:
            print("UID Not found")
            response["Property"] = "UID Not Found"
            
        
        return response        

    
    def post(self):
        print("\nIn add Property")
        response = {}
        appliances = {}

        with connect() as db:
            data = request.form
            print("Incoming data: ", data)
            fields = [
                "property_owner_id"
                , "po_owner_percent"
                , 'property_available_to_rent'
                , "property_active_date"
                , 'property_address'
                , "property_unit"
                , "property_city"
                , "property_state"
                , "property_zip"
                , "property_type"
                , "property_num_beds"
                , "property_num_baths"
                , "property_area"
                , "property_listed_rent"
                , "property_deposit"
                , "property_pets_allowed"
                , "property_deposit_for_rent"
                , "property_taxes"
                , "property_mortgages"
                , "property_insurance"
                , "property_featured"
                , "property_value"
                , "property_value_year"
                , "property_area"
                , "property_description"
                , "property_notes"
                , "property_amenities_unit"
                , "property_amenities_community"
                , "property_amenities_nearby"
                , "property_latitude"
                , "property_longitude"
            ]

            newRequest = {}
            newProperty = {}

            # print("Property Type: ", data.get("property_type"))
            # print("Property Address: ", request.form.get('property_address'))

            for field in fields:
                # print("Field: ", field)
                # print("Form Data: ", data.get(field))
                newProperty[field] = data.get(field)
                # print("New Property Field: ", newProperty[field])
            # print("Current newProperty", newProperty, type(newProperty))

            keys_to_remove = ["property_owner_id", "po_owner_percent"]
            newRequest = {key: newProperty.pop(key) for key in keys_to_remove if key in newProperty}
            # print("Current newProperty", newProperty, type(newProperty))
            # print("Current newRequest", newRequest, type(newRequest))
            
            # newRequest['property_owner_id'] = request.form.get("property_owner_id")
            # newRequest['po_owner_percent'] = request.form.get("po_owner_percent")
            # print(newRequest)


            # # GET NEW UID
            print("Get New Property UID")
            newRequestID = db.call('new_property_uid')['result'][0]['new_id']
            newRequest['property_id'] = newRequestID
            newProperty['property_uid'] = newRequestID
            # print(newRequestID)

            # Image Upload 
            print("\nIn images")
            images = []
            i = 0
            imageFiles = {}
            favorite_image = data.get("img_favorite")
            while True:
                filename = f'img_{i}'  
                print("Put image file into Filename: ", filename)               
                file = request.files.get(filename)  # if File: puts file into files
                # print("File:" , file)
                s3Link = data.get(filename) # if S3 Link but filename into S3
                # print("S3Link: ", s3Link)
                if file:
                    imageFiles[filename] = file
                    unique_filename = filename + "_" + datetime.utcnow().strftime('%Y%m%d%H%M%SZ')
                    print("Unique File Name: ", unique_filename)
                    key = f'properties/{newRequestID}/{unique_filename}'
                    # print("Key: ", key)
                    # This line calls uploadImage to actually upload the file and create the S3Link
                    image = uploadImage(file, key, '')
                    # print("Image: ", image)
                    images.append(image)

                    if filename == favorite_image:
                        # print("Favorite Image: ", filename, favorite_image)
                        newProperty["property_favorite_image"] = image

                elif s3Link:
                    imageFiles[filename] = s3Link
                    images.append(s3Link)

                    if filename == favorite_image:
                        newProperty["property_favorite_image"] = s3Link
                else:
                    break
                i += 1
                print("Images after loop: ", images)
            
            newProperty['property_images'] = json.dumps(images)  
            print("Images to add to db: ", newProperty['property_images'])      


            # print(newRequest)
            # print("New Property-Owner request: ", newRequest)
            response = db.insert('property_owner', newRequest)
            response['property_owner'] = "Added"
            # print("\nNew Property-Owner Relationship Added")

            # print("New Propterty request: ", newProperty, type(newProperty))
            response = db.insert('properties', newProperty)
            response['property_UID'] = newRequestID
            response['images'] = newProperty['property_images']
            # print("\nNew Prop÷erty Added")

        
        
        # Add Appliances (if provided)

            # print("\nAppliances: ", data.get('appliances'), type(data.get('appliances')))
        
            try:
                appliances = json.loads(data.get('appliances'))    
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
                            newAppliance['appliance_property_id'] = newRequestID
                            newAppliance['appliance_type'] = appliance
                            add_appliance = db.insert('appliances', newAppliance)
                            print(add_appliance)
                else:
                    print("No appliances provided in the form.")


            except:
                print(f"Add Appliance failed")
                response['add_appliance_error'] = f"No Appliance Data Provided"



        return response
    
    
    def put(self):
        print("\nIn Property PUT")
        response = {}
        payload = request.form.to_dict()
        print("Propoerty Update Payload: ", payload)

        # Verify lease_uid has been included in the data
        if payload.get('property_uid') is None:
            print("No property_uid")
            raise BadRequest("Request failed, no UID in payload.")
        
        property_uid = payload.get('property_uid')
        key = {'property_uid': payload.pop('property_uid')}
        print("Property Key: ", key)


        # Check if images are being added OR deleted
        images = []
        # i = -1
        i = 0
        imageFiles = {}
        favorite_image = payload.get("img_favorite")
        while True:
            filename = f'img_{i}'
            print("Put image file into Filename: ", filename) 
            # if i == -1:
            #     filename = 'img_cover'
            file = request.files.get(filename)
            print("File:" , file)            
            s3Link = payload.get(filename)
            print("S3Link: ", s3Link)
            if file:
                imageFiles[filename] = file
                unique_filename = filename + "_" + datetime.utcnow().strftime('%Y%m%d%H%M%SZ')
                image_key = f'properties/{property_uid}/{unique_filename}'
                # This calls the uploadImage function that generates the S3 link
                image = uploadImage(file, image_key, '')
                images.append(image)

                if filename == favorite_image:
                    payload["property_favorite_image"] = image

            elif s3Link:
                imageFiles[filename] = s3Link
                images.append(s3Link)

                if filename == favorite_image:
                    payload["property_favorite_image"] = s3Link
            else:
                break
            i += 1
            print("Images after loop: ", images)


        # Check if images already exist
        if payload.get('property_images') is None:
            print("No Current Images")
            current_images = images
        else:
            current_images =ast.literal_eval(payload.get('property_images'))
            print("Current images: ", current_images, type(current_images))
            current_images.extend(images)
            print("New List of Images: ", current_images)

        payload['property_images'] = json.dumps(current_images)     

   

        with connect() as db:
            print("Checking Inputs: ", key, payload)
            response['property_info'] = db.update('properties', key, payload)
            print("Response:" , response)
        return response

    

        
        # images = updateImages(imageFiles, property_uid)
        # newProperty['property_images'] = json.dumps(images)
        # print("images",images)
        # if len(imageLinks) > 0:
        #     for item in images:
        #         imageLinks.append(item)
        #     newProperty['property_images'] = json.dumps(imageLinks)

        # else:
        
        

        # delete images from s3
        deleted_images_str = data.get("deleted_images")
        deleted_images = []
        
        if deleted_images_str is not None and isinstance(deleted_images_str, str):
            try:                
                deleted_images = ast.literal_eval(deleted_images_str)                                
            except (ValueError, SyntaxError) as e:
                print(f"Error parsing the deleted_images string: {e}")
                
        
        s3Client = boto3.client('s3')
        

        response = {'s3_delete_responses': []}
        if(deleted_images):
            try:                
                objects_to_delete = []
                for img in deleted_images:                    
                    key = "properties/" + img.split("properties/")[-1]
                    objects_to_delete.append(key)               

                for obj_key in objects_to_delete:                    
                    delete_response = s3Client.delete_object(Bucket='io-pm', Key=f'{obj_key}')
                    response['s3_delete_responses'].append({obj_key: delete_response})

            except Exception as e:
                print(f"Deletion from s3 failed: {str(e)}")
                response['s3_delete_error'] = f"Deletion from s3 failed: {str(e)}"




        # print("new_Property", newProperty)
        key = {'property_uid': property_uid}
        with connect() as db:
            response['database_response'] = db.update('properties', key, newProperty)
        response['images'] = newProperty['property_images']
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
            
            if response["code"] == 200:
                response["Deleted property_uid"] = property_id
            else:
                response["Deleted property_uid"] = "Not successful"

            if response_po["code"] == 200:
                response["Deleted po property_id"] = property_id
            else:
                response["Deleted po property_id"] = "Not successful"


            return response


