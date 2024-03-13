import datetime

from flask_restful import Resource
from data_pm import connect
# # from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
# import json
import calendar
# from calendar import monthrange
from decimal import Decimal



class MonthlyRentPurchase_CLASS(Resource):
    def get(self):
        print("In Monthly Rent CRON JOB")

        numCronPurchases = 0

        # Establish current month and year
        dt = datetime.datetime.today()
        month = dt.month
        year = dt.year
        nextMonth = (dt + relativedelta(months=1))
        print(dt, month, type(month), year, type(year), nextMonth.month, type(nextMonth.month), calendar.month_name[nextMonth.month], nextMonth.year, type(nextMonth.year))

        # Run query to find rents of ACTIVE leases
        with connect() as db:
            response = db.execute("""
                    -- CALCULATE RECURRING FEES
                    SELECT 
                    leaseFees_uid, fees_lease_id, fee_name, fee_type, charge, due_by, late_by, late_fee, perDay_late_fee, frequency, available_topay
                    -- , of_DNU, lease_rent_old_DNU
                    , lease_uid, lease_property_id
                    -- , lease_application_date
                    , lease_start, lease_end, lease_status
                    -- , lease_assigned_contacts, lease_documents
                    , lease_early_end_date, lease_renew_status, move_out_date
                    -- , lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date
                    -- , linked_application_id-DNU, lease_docuSign
                    -- , lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee
                    -- , lease_fees, lease_consent, lease_actual_rent, lease_test
                    -- , t_details.*
                    -- , o_details.*
                    , lt_lease_id, lt_tenant_id, lt_responsibility, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                    -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state
                    -- , tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip
                    -- , tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_current_address-DNU
                    , tenant_photo_url
                    , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                    -- , owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number
                    -- , owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents
                    , owner_photo_url
                    -- , b_details.*
                    , contract_uid, contract_property_id, contract_business_id
                    -- , contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                    -- , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations
                    -- , business_paypal, business_apple_pay, business_zelle, business_venmo, business_account_number, business_routing_number
                    -- , business_documents, business_address, business_unit, business_city, business_state, business_zip
                    , business_photo_url
                FROM space.leaseFees	
                LEFT JOIN space.leases ON fees_lease_id = lease_uid
                LEFT JOIN space.t_details ON lt_lease_id = lease_uid
                LEFT JOIN space.o_details ON lease_property_id = property_id
                LEFT JOIN space.b_details ON contract_property_id = property_id
                -- WHERE fee_name LIKE '%rent%' and lease_status = "ACTIVE";
                WHERE frequency = "Monthly" and lease_status = "ACTIVE" and contract_status = "ACTIVE";
                """)

            for i in range(len(response['result'])):
                # print("\n",i, response['result'][i]['leaseFees_uid'], response['result'][i]['contract_uid'], response['result'][i]['contract_business_id'])

                # Check Frequecy of Rent Payment
                # rentFrequency = response['result'][i]['frequency']

                # print(response['result'][i]['frequency'])
                # if rentFrequency == "Weekly":
                #     print("Weekly Rent Fee")
                # elif rentFrequency == "Anually":
                #     print("Annual Rent Fee") 
                # elif rentFrequency == "Monthly" or rentFrequency is None:
                #     print("Monthly Rent Fee")
                # else: print("Investigate")


                # Check if due_by is NONE
                # print(response['result'][i]['due_by'])
                if response['result'][i]['due_by'] is None or response['result'][i]['due_by'] == 0:
                    # print("Is NULL!!")
                    due_by = 1
                else:
                    due_by = response['result'][i]['due_by']
                # print(due_by, type(due_by))


                # Calculate number of days until rent is due
                if due_by < dt.day:
                    due_date = datetime.datetime(dt.year, dt.month + 1, due_by)
                else:
                    due_date = datetime.datetime(dt.year, dt.month, due_by)
                # print(due_date)

                days_for_rent = (due_date - dt).days
                # print("Rent due in : ", days_for_rent, " days")


                # Check if tenant responsiblity is NONE
                # print("What is in the db: ", response['result'][i]['lt_responsibility'])
                if response['result'][i]['lt_responsibility'] is None:
                    # print("Is NULL!!")
                    responsible_percent = 1.0
                else:
                    responsible_percent = response['result'][i]['lt_responsibility']
                # print("What we set programmatically: ", responsible_percent, type(responsible_percent))
                charge = response['result'][i]['charge']
                # print("Charge: ", charge, type(charge))
                amt_due = float(charge)  * responsible_percent
                # print("Amount due: ", amt_due)


                # Check if available_topay is NONE
                if response['result'][i]['available_topay'] is None:
                    # print("Is NULL!!")
                    payable = 10
                else:
                    payable = response['result'][i]['available_topay']
                # print(payable)

                print(i, days_for_rent, payable, response['result'][i]['lease_property_id'], response['result'][i]['leaseFees_uid'], response['result'][i]['contract_uid'], response['result'][i]['fee_name'])



                # CHECK IF RENT IS AVAILABLE TO PAY
                if days_for_rent == payable + (0):  # Remove/Change number to get query to run and return data
                # if days_for_rent > payable + (0):  # Remove/Change number to get query to run and return data
                    # print("Rent posted.  Please Pay")
                    numCronPurchases = numCronPurchases + 1

                    # Establish payer, initiator and receiver
                    contract_uid = response['result'][i]['contract_uid']
                    property = response['result'][i]['lease_property_id']
                    tenant = response['result'][i]['lt_tenant_id']
                    owner = response['result'][i]['property_owner_id']
                    manager = response['result'][i]['contract_business_id']
                    fee_name = response['result'][i]['fee_name']
                    # print("Purchase Parameters: ", i, contract_uid, tenant, owner, manager)

                    # Create JSON Object for Rent Purchase
                    newRequest = {}
                    newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                    # print(newRequestID)
                    newRequest['purchase_uid'] = newRequestID
                    newRequest['pur_timestamp'] = datetime.datetime.today().date().strftime("%m-%d-%Y")
                    newRequest['pur_property_id'] = property
                    newRequest['purchase_type'] = "Rent"
                    newRequest['pur_cf_type'] = "revenue"
                    newRequest['pur_amount_due'] = amt_due
                    newRequest['purchase_status'] = "UNPAID"
                    newRequest['pur_status_value'] = "0"
                    newRequest['pur_notes'] = fee_name

                    newRequest['pur_description'] = f"Rent for { calendar.month_name[nextMonth.month]} {nextMonth.year} CRON"
                    # newRequest['pur_description'] = f"Rent for MARCH {nextMonth.year} CRON"

                    newRequest['pur_receiver'] = owner
                    newRequest['pur_payer'] = tenant
                    newRequest['pur_initiator'] = manager
                    newRequest['purchase_date'] = datetime.datetime.today().date().strftime("%m-%d-%Y")

                    newRequest['pur_due_date'] = datetime.datetime(nextMonth.year, nextMonth.month, due_by).date().strftime("%m-%d-%Y")
                    # newRequest['pur_due_date'] = datetime.datetime(nextMonth.year, 3, due_by).date().strftime("%m-%d-%Y")
                    
                    # print(newRequest)
                    # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
                    db.insert('purchases', newRequest)


                    # For each entry posted to the purchases table, post any contract fees based on Rent
                    # Find contract fees based rent
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
                                    WHERE contract_uid = \'""" + contract_uid + """\' AND of_column LIKE '%rent%';
                                """)
                    # print(manager_fees)
                    

                    for j in range(len(manager_fees['result'])):

                        # Check if fees is monthly 
                        if manager_fees['result'][j]['frequency_column'] == 'Monthly' or manager_fees['result'][j]['frequency_column'] == 'monthly':

                            # Check if charge is a % or Fixed $ Amount
                            if manager_fees['result'][j]['fee_type_column'] == '%' or manager_fees['result'][j]['fee_type_column'] == 'PERCENT':
                                charge_amt = Decimal(manager_fees['result'][j]['charge_column']) * Decimal(amt_due) / 100
                            else:
                                charge_amt = Decimal(manager_fees['result'][j]['charge_column'])
                            # print("Charge Amount: ", charge_amt, property, contract_uid, manager_fees['result'][j]['charge_column'], response['result'][i]['charge'] )

                            # Create JSON Object for Fee Purchase
                            newPMRequest = {}
                            newPMRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                            # print(newPMRequestID)
                            newPMRequest['purchase_uid'] = newPMRequestID
                            newPMRequest['pur_timestamp'] = datetime.datetime.today().date().strftime("%m-%d-%Y")
                            newPMRequest['pur_property_id'] = property
                            newPMRequest['purchase_type'] = "Property Management Fee"
                            newPMRequest['pur_cf_type'] = "expense"
                            newPMRequest['pur_amount_due'] = charge_amt
                            newPMRequest['purchase_status'] = "UNPAID"
                            newPMRequest['pur_status_value'] = "0"
                            newPMRequest['pur_notes'] = manager_fees['result'][j]['fee_name_column']
                            newPMRequest['pur_description'] =  newRequestID # Original Rent Purchase ID  
                            # newPMRequest['pur_description'] = f"Fees for MARCH {nextMonth.year} CRON"
                            newPMRequest['pur_receiver'] = manager
                            newPMRequest['pur_payer'] = owner
                            newPMRequest['pur_initiator'] = manager
                            newPMRequest['purchase_date'] = datetime.datetime.today().date().strftime("%m-%d-%Y")

                            newPMRequest['pur_due_date'] = datetime.datetime(nextMonth.year, nextMonth.month, due_by).date().strftime("%m-%d-%Y")
                            # newPMRequest['pur_due_date'] = datetime.datetime(nextMonth.year, 3, due_by).date().strftime("%m-%d-%Y")
                            
                            # print(newPMRequest)
                            db.insert('purchases', newPMRequest)

                            # For each fee, post to purchases table

        response = {'message': f'Successfully completed CRON Job for {dt}' ,
                    'rows affected': f'{numCronPurchases}',
                'code': 200}

        return response


def MonthlyRentPurchase_CRON(self):
    print("In Monthly Rent CRON JOB")

    numCronPurchases = 0

    # Establish current month and year
    dt = datetime.datetime.today()
    month = dt.month
    year = dt.year
    nextMonth = (dt + relativedelta(months=1))
    print(dt, month, type(month), year, type(year), nextMonth.month, type(nextMonth.month), calendar.month_name[nextMonth.month], nextMonth.year, type(nextMonth.year))

    # Run query to find rents of ACTIVE leases
    with connect() as db:
        response = db.execute("""
                -- CALCULATE RECURRING FEES
                SELECT 
                leaseFees_uid, fees_lease_id, fee_name, fee_type, charge, due_by, late_by, late_fee, perDay_late_fee, frequency, available_topay
                -- , of_DNU, lease_rent_old_DNU
                , lease_uid, lease_property_id
                -- , lease_application_date
                , lease_start, lease_end, lease_status
                -- , lease_assigned_contacts, lease_documents
                , lease_early_end_date, lease_renew_status, move_out_date
                -- , lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date
                -- , linked_application_id-DNU, lease_docuSign
                -- , lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee
                -- , lease_fees, lease_consent, lease_actual_rent, lease_test
                -- , t_details.*
                -- , o_details.*
                , lt_lease_id, lt_tenant_id, lt_responsibility, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
                -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state
                -- , tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip
                -- , tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_current_address-DNU
                , tenant_photo_url
                , property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email
                -- , owner_ein_number, owner_ssn, owner_paypal, owner_apple_pay, owner_zelle, owner_venmo, owner_account_number, owner_routing_number
                -- , owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_documents
                , owner_photo_url
                -- , b_details.*
                , contract_uid, contract_property_id, contract_business_id
                -- , contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                -- , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations
                -- , business_paypal, business_apple_pay, business_zelle, business_venmo, business_account_number, business_routing_number
                -- , business_documents, business_address, business_unit, business_city, business_state, business_zip
                , business_photo_url
            FROM space.leaseFees	
            LEFT JOIN space.leases ON fees_lease_id = lease_uid
            LEFT JOIN space.t_details ON lt_lease_id = lease_uid
            LEFT JOIN space.o_details ON lease_property_id = property_id
            LEFT JOIN space.b_details ON contract_property_id = property_id
            -- WHERE fee_name LIKE '%rent%' and lease_status = "ACTIVE";
            WHERE frequency = "Monthly" and lease_status = "ACTIVE" and contract_status = "ACTIVE";
            """)

        for i in range(len(response['result'])):
            # print("\n",i, response['result'][i]['leaseFees_uid'], response['result'][i]['contract_uid'], response['result'][i]['contract_business_id'])

            # Check Frequecy of Rent Payment
            # rentFrequency = response['result'][i]['frequency']

            # print(response['result'][i]['frequency'])
            # if rentFrequency == "Weekly":
            #     print("Weekly Rent Fee")
            # elif rentFrequency == "Anually":
            #     print("Annual Rent Fee") 
            # elif rentFrequency == "Monthly" or rentFrequency is None:
            #     print("Monthly Rent Fee")
            # else: print("Investigate")


            # Check if due_by is NONE
            # print(response['result'][i]['due_by'])
            if response['result'][i]['due_by'] is None or response['result'][i]['due_by'] == 0:
                # print("Is NULL!!")
                due_by = 1
            else:
                due_by = response['result'][i]['due_by']
            # print(due_by, type(due_by))


            # Calculate number of days until rent is due
            if due_by < dt.day:
                due_date = datetime.datetime(dt.year, dt.month + 1, due_by)
            else:
                due_date = datetime.datetime(dt.year, dt.month, due_by)
            # print(due_date)

            days_for_rent = (due_date - dt).days
            # print("Rent due in : ", days_for_rent, " days")


            # Check if tenant responsiblity is NONE
            # print("What is in the db: ", response['result'][i]['lt_responsibility'])
            if response['result'][i]['lt_responsibility'] is None:
                # print("Is NULL!!")
                responsible_percent = 1.0
            else:
                responsible_percent = response['result'][i]['lt_responsibility']
            # print("What we set programmatically: ", responsible_percent, type(responsible_percent))
            charge = response['result'][i]['charge']
            # print("Charge: ", charge, type(charge))
            amt_due = float(charge)  * responsible_percent
            # print("Amount due: ", amt_due)


            # Check if available_topay is NONE
            if response['result'][i]['available_topay'] is None:
                # print("Is NULL!!")
                payable = 10
            else:
                payable = response['result'][i]['available_topay']
            # print(payable)

            print(i, days_for_rent, payable, response['result'][i]['lease_property_id'], response['result'][i]['leaseFees_uid'], response['result'][i]['contract_uid'], response['result'][i]['fee_name'])



            # CHECK IF RENT IS AVAILABLE TO PAY
            if days_for_rent == payable + (0):  # Remove/Change number to get query to run and return data
            # if days_for_rent > payable + (0):  # Remove/Change number to get query to run and return data
                # print("Rent posted.  Please Pay")
                numCronPurchases = numCronPurchases + 1

                # Establish payer, initiator and receiver
                contract_uid = response['result'][i]['contract_uid']
                property = response['result'][i]['lease_property_id']
                tenant = response['result'][i]['lt_tenant_id']
                owner = response['result'][i]['property_owner_id']
                manager = response['result'][i]['contract_business_id']
                fee_name = response['result'][i]['fee_name']
                # print("Purchase Parameters: ", i, contract_uid, tenant, owner, manager)

                # Create JSON Object for Rent Purchase
                newRequest = {}
                newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                # print(newRequestID)
                newRequest['purchase_uid'] = newRequestID
                newRequest['pur_timestamp'] = datetime.datetime.today().date().strftime("%m-%d-%Y")
                newRequest['pur_property_id'] = property
                newRequest['purchase_type'] = "Rent"
                newRequest['pur_cf_type'] = "revenue"
                newRequest['pur_amount_due'] = amt_due
                newRequest['purchase_status'] = "UNPAID"
                newRequest['pur_status_value'] = "0"
                # newRequest['pur_notes'] = f"Rent for { calendar.month_name[nextMonth.month]} {nextMonth.year}"
                newRequest['pur_description'] = f"Rent for { calendar.month_name[nextMonth.month]} {nextMonth.year} CRON"
                newRequest['pur_notes'] = fee_name
                # newRequest['pur_description'] = f"Rent for MARCH {nextMonth.year} CRON"

                newRequest['pur_receiver'] = owner
                newRequest['pur_payer'] = tenant
                newRequest['pur_initiator'] = manager
                newRequest['purchase_date'] = datetime.datetime.today().date().strftime("%m-%d-%Y")
                newRequest['pur_due_date'] = datetime.datetime(nextMonth.year, nextMonth.month, due_by).date().strftime("%m-%d-%Y")
                # newRequest['pur_due_date'] = datetime.datetime(nextMonth.year, 3, due_by).date().strftime("%m-%d-%Y")
                # print(newRequest)
                # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
                db.insert('purchases', newRequest)


                # For each entry posted to the purchases table, post any contract fees based on Rent
                # Find contract fees based rent
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
                                WHERE contract_uid = \'""" + contract_uid + """\' AND of_column LIKE '%rent%';
                            """)
                # print(manager_fees)
                

                for j in range(len(manager_fees['result'])):

                    # Check if fees is monthly 
                    if manager_fees['result'][j]['frequency_column'] == 'Monthly' or manager_fees['result'][j]['frequency_column'] == 'monthly':

                        # Check if charge is a % or Fixed $ Amount
                        if manager_fees['result'][j]['fee_type_column'] == '%' or manager_fees['result'][j]['fee_type_column'] == 'PERCENT':
                            charge_amt = Decimal(manager_fees['result'][j]['charge_column']) * Decimal(amt_due) / 100
                        else:
                            charge_amt = Decimal(manager_fees['result'][j]['charge_column'])
                        # print("Charge Amount: ", charge_amt, property, contract_uid, manager_fees['result'][j]['charge_column'], response['result'][i]['charge'] )

                        # Create JSON Object for Fee Purchase
                        newPMRequest = {}
                        newPMRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                        # print(newPMRequestID)
                        newPMRequest['purchase_uid'] = newPMRequestID
                        newPMRequest['pur_timestamp'] = datetime.datetime.today().date().strftime("%m-%d-%Y")
                        newPMRequest['pur_property_id'] = property
                        newPMRequest['purchase_type'] = "Property Management Fee"
                        newPMRequest['pur_cf_type'] = "expense"
                        newPMRequest['pur_amount_due'] = charge_amt
                        newPMRequest['purchase_status'] = "UNPAID"
                        newPMRequest['pur_status_value'] = "0"
                        newPMRequest['pur_notes'] = manager_fees['result'][j]['fee_name_column']
                        newPMRequest['pur_description'] =  newRequestID # Original Rent Purchase ID 
                        # newPMRequest['pur_description'] = f"Fees for MARCH {nextMonth.year} CRON"
                        newPMRequest['pur_receiver'] = manager
                        newPMRequest['pur_payer'] = owner
                        newPMRequest['pur_initiator'] = manager
                        newPMRequest['purchase_date'] = datetime.datetime.today().date().strftime("%m-%d-%Y")
                        newPMRequest['pur_due_date'] = datetime.datetime(nextMonth.year, nextMonth.month, due_by).date().strftime("%m-%d-%Y")
                        # newPMRequest['pur_due_date'] = datetime.datetime(nextMonth.year, 3, due_by).date().strftime("%m-%d-%Y")
                        # print(newPMRequest)
                        db.insert('purchases', newPMRequest)

                        # For each fee, post to purchases table

    response = {'message': f'Successfully completed CRON Job for {dt}' ,
                'rows affected': f'{numCronPurchases}',
            'code': 200}

    return response


class LateFees_CLASS(Resource):
    def get(self):
        print("In Late Fees")

        numCronPurchases = 0

        # Establish current month and year
        dt = datetime.datetime.today()
        month = dt.month
        year = dt.year
        nextMonth = (dt + relativedelta(months=1))
        print(dt, month, type(month), year, type(year))

        response = {'message': f'Successfully completed CRON Job for {dt}' ,
                    'rows affected': f'{numCronPurchases}',
                'code': 200}

        return response
    



# ORIGINAL CRON JOBS BELOW


class RentPurchase_CLASS(Resource):
    def get(self):

        with connect() as db:
            response = db.execute("""
            SELECT *
            FROM space.leases l
            LEFT JOIN space.leaseFees lf ON lf.fees_lease_id = l.lease_uid
            LEFT JOIN space.t_details lt ON l.lease_uid = lt.lt_lease_id
            LEFT JOIN space.b_details b ON b.contract_property_id = l.lease_property_id
            LEFT JOIN space.properties p ON p.property_uid = l.lease_property_id
            LEFT JOIN space.property_owner po ON po.property_id = l.lease_property_id
            WHERE lf.fee_name='Rent'
            AND l.lease_status='ACTIVE'
            AND b.contract_status = 'ACTIVE' 
            AND b.business_type = 'MANAGEMENT';""")

            for i in range(len(response['result'])):
                dt = datetime.now()
                month = dt.month
                year = dt.year
                due_by = response['result'][i]['due_by']
                due_date = datetime.datetime(dt.year, dt.month + 1, due_by)
                due_date_2 = datetime.datetime(dt.year, dt.month, due_by)
                days_for_rent = (due_date - dt).days
                days_for_rent_2 = (due_date_2 - dt).days

                if days_for_rent == 2 or days_for_rent_2 == 2:
                    get_rec_st = db.select('purchases',
                                           {'pur_property_id': response['result'][i]['lease_property_id'],
                                            'pur_notes': f"RENT FOR {month} {year}"})

                    if (len(get_rec_st.get('result'))) == 0:
                        newRequest = {}
                        newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                        newRequest['purchase_uid'] = newRequestID
                        newRequest['pur_timestamp'] = datetime.date.today()
                        newRequest['pur_property_id'] = response['result'][i]['lease_property_id']
                        newRequest['purchase_type'] = "RENT"
                        newRequest['pur_cf_type'] = "REVENUE"
                        newRequest['pur_amount_due'] = response['result'][i]['charge']
                        newRequest['purchase_status'] = "UNPAID"
                        newRequest['pur_notes'] = f"RENT FOR {month} {year}"
                        newRequest['pur_description'] = f"RENT FOR {month} {year}"
                        newRequest['pur_receiver'] = response['result'][i]['property_owner_id']
                        newRequest['pur_payer'] = response['result'][i]['lt_tenant_id']
                        newRequest['pur_initiator'] = response['result'][i]['business_uid']
                        due_by = response['result'][i]['due_by']
                        newRequest['purchase_date'] = datetime.date(dt.year, dt.month, due_by)
                        newRequest['pur_due_date'] = datetime.date(dt.year, dt.month, due_by)

                        if days_for_rent == 2:
                            newRequest['purchase_date'] = datetime.date(dt.year, dt.month + 1, due_by)
                            newRequest['pur_due_date'] = datetime.date(dt.year, dt.month + 1, due_by)
                        db.insert('purchases', newRequest)
        return 200


def RentPurchase(self):
    print("In RentPurchase")
    import datetime
    from data_pm import connect

    with connect() as db:
        response = db.execute("""
        SELECT *
        FROM space.leases l
        LEFT JOIN space.leaseFees lf ON lf.fees_lease_id = l.lease_uid
        LEFT JOIN space.t_details lt ON l.lease_uid = lt.lt_lease_id
        LEFT JOIN space.b_details b ON b.contract_property_id = l.lease_property_id
        LEFT JOIN space.properties p ON p.property_uid = l.lease_property_id
        LEFT JOIN space.property_owner po ON po.property_id = l.lease_property_id
        WHERE lf.fee_name='Rent'
        AND l.lease_status='ACTIVE'
        AND b.contract_status = 'ACTIVE' 
        AND b.business_type = 'MANAGEMENT';""")

        for i in range(len(response['result'])):
            dt = datetime.datetime.now()
            month = dt.month
            year = dt.year
            due_by = response['result'][i]['due_by']
            due_date = datetime.datetime(dt.year, dt.month + 1, due_by)
            due_date_2 = datetime.datetime(dt.year, dt.month, due_by)
            days_for_rent = (due_date - dt).days
            days_for_rent_2 = (due_date_2 - dt).days

            if days_for_rent == 2 or days_for_rent_2 == 2:
                get_rec_st = db.select('purchases',
                                       {'pur_property_id': response['result'][i]['lease_property_id'],
                                        'pur_notes': f"RENT FOR {month} {year}"})

                if (len(get_rec_st.get('result'))) == 0:
                    newRequest = {}
                    newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                    newRequest['purchase_uid'] = newRequestID
                    newRequest['pur_timestamp'] = datetime.date.today()
                    newRequest['pur_property_id'] = response['result'][i]['lease_property_id']
                    newRequest['purchase_type'] = "RENT"
                    newRequest['pur_cf_type'] = "REVENUE"
                    newRequest['pur_amount_due'] = response['result'][i]['charge']
                    newRequest['purchase_status'] = "UNPAID"
                    newRequest['pur_notes'] = f"RENT FOR {month} {year}"
                    newRequest['pur_description'] = f"RENT FOR {month} {year}"
                    newRequest['pur_receiver'] = response['result'][i]['property_owner_id']
                    newRequest['pur_payer'] = response['result'][i]['lt_tenant_id']
                    newRequest['pur_initiator'] = response['result'][i]['business_uid']
                    due_by = response['result'][i]['due_by']
                    newRequest['purchase_date'] = datetime.date(dt.year, dt.month, due_by)
                    newRequest['pur_due_date'] = datetime.date(dt.year, dt.month, due_by)

                    if days_for_rent == 2:
                        newRequest['purchase_date'] = datetime.date(dt.year, dt.month + 1, due_by)
                        newRequest['pur_due_date'] = datetime.date(dt.year, dt.month + 1, due_by)
                    db.insert('purchases', newRequest)
    return 200


def newPurchase(linked_bill_id, pur_property_id, payer, receiver, purchase_type,
                description, amount_due, purchase_notes, purchase_date, pur_cf_type, pur_initiator, next_payment,
                purchase_frequency):
    response = {}
    print('In new purchase')
    with connect() as db:
        print('')
        newPurchase = {
            "pur_bill_id": linked_bill_id,
            "pur_property_id": pur_property_id,
            "pur_payer": payer,
            "pur_receiver": receiver,
            "purchase_type": purchase_type,
            "pur_description": description,
            "pur_amount_due": amount_due,
            "pur_notes": purchase_notes,
            "pur_cf_type": pur_cf_type,
            "pur_initiator": pur_initiator,
        }
        print('newPurchase', newPurchase)
        newPurchaseID = db.call('new_purchase_uid')['result'][0]['new_id']
        newPurchase['pur_timestamp'] = datetime.now()
        newPurchase['purchase_uid'] = newPurchaseID
        newPurchase['purchase_status'] = 'UNPAID'
        due_date = date(next_payment.year, next_payment.month, next_payment.day) + relativedelta(months=1)
        newPurchase['pur_due_date'] = due_date
        newPurchase['purchase_date'] = due_date
        print("next_payment", due_date)
        response = db.insert('purchases', newPurchase)
        print('response', response)
    return newPurchaseID


class LeasetoMonthTest(Resource):
    def get(self):
        with connect() as db:
            response = {'message': 'Successfully committed SQL query',
                        'code': 200}
            response = db.execute("""
            SELECT *
            FROM space.leases l
            LEFT JOIN space.leaseFees lf ON lf.fees_lease_id = l.lease_uid
            LEFT JOIN space.t_details lt ON l.lease_uid = lt.lt_lease_id           
            LEFT JOIN space.b_details b ON b.contract_property_id = l.lease_property_id
            LEFT JOIN space.properties p ON p.property_uid = l.lease_property_id
            LEFT JOIN space.o_details o ON o.property_id = l.lease_property_id
            WHERE (l.lease_end <= DATE_FORMAT(NOW(), "%Y-%m-%d")
            OR l.lease_end = DATE_FORMAT(NOW(), "%Y-%m-%d"))
            AND l.lease_status='ACTIVE'
            AND (b.contract_status = 'ACTIVE' OR b.contract_status='END EARLY' OR b.contract_status='PM END EARLY' OR b.contract_status='OWNER END EARLY'); """)

            if len(response['result']) > 0:

                for i in range(len(response['result'])):
                    expiredResponse = db.execute("""
                                    SELECT *
                                    FROM space.leases l
                                    LEFT JOIN space.leaseFees lf ON lf.fees_lease_id = l.lease_uid
                                    LEFT JOIN space.properties p ON p.property_uid = l.lease_property_id          
                                    LEFT JOIN space.b_details b ON b.contract_property_id = l.lease_property_id
                                    WHERE (l.lease_status = 'ACTIVE' OR l.lease_status = 'PROCESSING' OR l.lease_status='TENANT APPROVED' OR l.lease_status='PENDING')
                                    AND l.lease_property_id = \'""" + response['result'][i]['lease_property_id'] + """\'
                                    AND l.lease_uid != \'""" + response['result'][i]['lease_uid'] + """\' ;
                                    -- AND (pM.management_status = 'ACCEPTED' OR pM.management_status='END EARLY' OR pM.management_status='PM END EARLY' OR pM.management_status='OWNER END EARLY')  """)

                    if len(expiredResponse['result']) > 0:
                        print('do not do month to month')
                    else:
                        print(response['result'][i]['lease_uid'])

                        pk = {
                            'lease_uid': response['result'][i]['lease_uid']}
                        updateLeaseEnd = {
                            'lease_end': (date.fromisoformat(
                                response['result'][i]['lease_end']) + relativedelta(months=1)).isoformat()}

                        # res = db.update('leases', pk, updateLeaseEnd)


                        tenants = response['result'][0]['tenant_uid']
                        # print('tenants1', tenants)

                        # if '[' in tenants:
                        #     # print('tenants2', tenants)
                        #     tenants = json.loads(tenants)
                        # print('tenants3', tenants)
                        # print('tenants4', tenants)
                        # if type(tenants) == str:
                        #     tenants = [tenants]
                            # print('tenants5', tenants)
                        # print('tenant_id', tenants)
                        """
                        *) This would not work as we have separate table for leaseFees. and rent_payments is not json now.

                        see the leases table
                        # """
                        # payment = json.loads(
                        #     response['result'][i]['rent_payments'])
                        # # print(payment, len(payment))
                        """
                        1) Contract fees table has not been decided yet
                        """
                        managementPayments = json.loads(
                            response['result'][i]['contract_fees'])

                        #     """
                        #     4) We will have to add condition in sql statement to get the records with Rent as fee_name.
                        #     All the conditions with payment will have to change
                        #     ex:     due_date = charge_date.replace(day=int(payment[r]['due_by']))
                        #     """

                        # for r in range(len(payment)):
                        if response['result'][i]['fee_name'] == 'Rent':
                            charge_date = date.today()
                            due_date = charge_date.replace(
                                day=int(response['result'][i]['due_by']))
                            charge_month = charge_date.strftime(
                                '%B')
                            if response['result'][i]['available_topay'] == 0:
                                available_date = due_date
                            else:
                                available_date = due_date - \
                                                 timedelta(
                                                     days=int(response['result'][i]['available_topay']))



                            # print(available_date, charge_date, charge_month)

                            if available_date == charge_date:
                                charge = int(response['result'][i]['charge'])
                                purchaseResponse = newPurchase(
                                    linked_bill_id=None,
                                    pur_property_id=response['result'][i]['lease_property_id'],
                                    payer=tenants,
                                    receiver=response['result'][i]['business_uid'],
                                    pur_initiator="CRON JOB",
                                    purchase_type='RENT',
                                    description=charge_month +
                                                ' ' + response['result'][i]['fee_name'],
                                    amount_due=charge,
                                    purchase_notes=charge_month,
                                    purchase_date=charge_date.isoformat(),
                                    purchase_frequency=response['result'][i]['frequency'],
                                    next_payment=due_date
                                )
                                for mpayment in managementPayments:
                                    weeks_current_month = len(
                                        calendar.monthcalendar(charge_date.year, int(charge_date.strftime("%m"))))
                                    if mpayment['frequency'] == 'Weekly' and mpayment['fee_type'] == '%':
                                        if mpayment['of'] == 'Gross Rent':
                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=response['result'][i]['property_uid'],
                                                payer=response['result'][i]['business_uid'],
                                                receiver=response['result'][i]['owner_id'],
                                                pur_initiator="CRON JOB",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            response['result'][i]['fee_name'],
                                                amount_due=weeks_current_month * (charge *
                                                                                  (1 - mpayment['charge'] / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=charge_date
                                            )
                                        else:

                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=response['result'][i]['property_uid'],
                                                payer=response['result'][i]['business_uid'],
                                                receiver=response['result'][i]['owner_id'],
                                                pur_initiator="CRON JOB",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            response['result'][i]['fee_name'],
                                                amount_due=weeks_current_month * (
                                                        (charge - mpayment['expense_amount']) * (
                                                        1 - mpayment['charge'] / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=charge_date
                                            )
                                    elif mpayment['frequency'] == 'Biweekly' and mpayment['fee_type'] == '%':

                                        # if gross charge (listed charge)
                                        if mpayment['of'] == 'Gross Rent':
                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=response['result'][i]['property_uid'],
                                                payer=response['result'][i]['business_uid'],
                                                receiver=response['result'][i]['owner_id'],
                                                pur_initiator="CRON JOB",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            response['result'][i]['fee_name'],
                                                amount_due=(weeks_current_month / 2) *
                                                           ((charge *
                                                             (1 - mpayment['charge'] / 100))),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=charge_date
                                            )
                                        else:

                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=response['result'][i]['property_uid'],
                                                payer=response['result'][i]['business_uid'],
                                                receiver=response['result'][i]['owner_id'],
                                                pur_initiator="CRON JOB",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            response['result'][i]['fee_name'],
                                                amount_due=(weeks_current_month / 2) *
                                                           ((charge - mpayment['expense_amount']) * (
                                                                   1 - mpayment['charge'] / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=charge_date
                                            )
                                    elif mpayment['frequency'] == 'Monthly' and mpayment['fee_type'] == '%':

                                        # if gross charge (listed charge)
                                        if mpayment['of'] == 'Gross Rent':
                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=response['result'][i]['property_uid'],
                                                payer=response['result'][i]['business_uid'],
                                                receiver=response['result'][i]['owner_id'],
                                                pur_initiator="CRON JOB",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            response['result'][i]['fee_name'],
                                                amount_due=(
                                                        charge * (1 - int(mpayment['charge']) / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=charge_date
                                            )

                                        # if net charge (listed charge-expenses)
                                        else:
                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=response['result'][i]['property_uid'],
                                                payer=response['result'][i]['business_uid'],
                                                receiver=response['result'][i]['owner_id'],
                                                pur_initiator="CRON JOB",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            response['result'][i]['fee_name'],
                                                amount_due=(
                                                        (charge - mpayment['expense_amount']) * (
                                                        1 - int(mpayment['charge']) / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=charge_date
                                            )
                                    else:
                                        print(
                                            'payment frequency one-time %')

                return response

class LeasetoMonth_CLASS(Resource):
    def get(self):
        with connect() as db:
            response = {'message': 'Successfully committed SQL query',
                        'code': 200}
            response = db.execute("""
            SELECT *
            FROM space.leases l
            LEFT JOIN space.leaseFees lf ON lf.fees_lease_id = l.lease_uid
            LEFT JOIN space.t_details lt ON l.lease_uid = lt.lt_lease_id           
            LEFT JOIN space.b_details b ON b.contract_property_id = l.lease_property_id
            LEFT JOIN space.properties p ON p.property_uid = l.lease_property_id
            LEFT JOIN space.o_details o ON o.property_id = l.lease_property_id
            WHERE (l.lease_end <= DATE_FORMAT(NOW(), "%Y-%m-%d")
            OR l.lease_end = DATE_FORMAT(NOW(), "%Y-%m-%d"))
            AND l.lease_status='ACTIVE'
            AND (b.contract_status = 'ACTIVE' OR b.contract_status='END EARLY' OR b.contract_status='PM END EARLY' OR b.contract_status='OWNER END EARLY'); """)

            if len(response['result']) > 0:

                for i in range(len(response['result'])):
                    expiredResponse = db.execute("""
                                    SELECT *
                                    FROM space.leases l
                                    LEFT JOIN space.leaseFees lf ON lf.fees_lease_id = l.lease_uid
                                    LEFT JOIN space.properties p ON p.property_uid = l.lease_property_id          
                                    LEFT JOIN space.b_details b ON b.contract_property_id = l.lease_property_id
                                    WHERE (l.lease_status = 'ACTIVE' OR l.lease_status = 'PROCESSING' OR l.lease_status='TENANT APPROVED' OR l.lease_status='PENDING')
                                    AND l.lease_property_id = \'""" + response['result'][i]['lease_property_id'] + """\'
                                    AND l.lease_uid != \'""" + response['result'][i]['lease_uid'] + """\' ;
                                    -- AND (pM.management_status = 'ACCEPTED' OR pM.management_status='END EARLY' OR pM.management_status='PM END EARLY' OR pM.management_status='OWNER END EARLY')  """)

                    if len(expiredResponse['result']) > 0:
                        print('do not do month to month')
                    else:
                        print(response['result'][i]['lease_uid'])

                        pk = {
                            'lease_uid': response['result'][i]['lease_uid']}
                        updateLeaseEnd = {
                            'lease_end': (date.fromisoformat(
                                response['result'][i]['lease_end']) + relativedelta(months=1)).isoformat()}

                        # res = db.update('leases', pk, updateLeaseEnd)


                        tenants = response['result'][0]['tenant_uid']
                        # print('tenants1', tenants)

                        # if '[' in tenants:
                        #     # print('tenants2', tenants)
                        #     tenants = json.loads(tenants)
                        # print('tenants3', tenants)
                        # print('tenants4', tenants)
                        # if type(tenants) == str:
                        #     tenants = [tenants]
                            # print('tenants5', tenants)
                        # print('tenant_id', tenants)
                        """
                        *) This would not work as we have separate table for leaseFees. and rent_payments is not json now.

                        see the leases table
                        # """
                        # payment = json.loads(
                        #     response['result'][i]['rent_payments'])
                        # # print(payment, len(payment))
                        """
                        1) Contract fees table has not been decided yet
                        """
                        managementPayments = json.loads(
                            response['result'][i]['contract_fees'])

                        #     """
                        #     4) We will have to add condition in sql statement to get the records with Rent as fee_name.
                        #     All the conditions with payment will have to change
                        #     ex:     due_date = charge_date.replace(day=int(payment[r]['due_by']))
                        #     """

                        # for r in range(len(payment)):
                        if response['result'][i]['fee_name'] == 'Rent':
                            charge_date = date.today()
                            due_date = charge_date.replace(
                                day=int(response['result'][i]['due_by']))
                            charge_month = charge_date.strftime(
                                '%B')
                            if response['result'][i]['available_topay'] == 0:
                                available_date = due_date
                            else:
                                available_date = due_date - \
                                                 timedelta(
                                                     days=int(response['result'][i]['available_topay']))



                            # print(available_date, charge_date, charge_month)

                            if available_date == charge_date:
                                charge = int(response['result'][i]['charge'])
                                purchaseResponse = newPurchase(
                                    linked_bill_id=None,
                                    pur_property_id=response['result'][i]['lease_property_id'],
                                    payer=tenants,
                                    receiver=response['result'][i]['business_uid'],
                                    pur_initiator="CRON JOB",
                                    purchase_type='RENT',
                                    description=charge_month +
                                                ' ' + response['result'][i]['fee_name'],
                                    amount_due=charge,
                                    purchase_notes=charge_month,
                                    purchase_date=charge_date.isoformat(),
                                    purchase_frequency=response['result'][i]['frequency'],
                                    next_payment=due_date
                                )
                                for mpayment in managementPayments:
                                    weeks_current_month = len(
                                        calendar.monthcalendar(charge_date.year, int(charge_date.strftime("%m"))))
                                    if mpayment['frequency'] == 'Weekly' and mpayment['fee_type'] == '%':
                                        if mpayment['of'] == 'Gross Rent':
                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=response['result'][i]['property_uid'],
                                                payer=response['result'][i]['business_uid'],
                                                receiver=response['result'][i]['owner_id'],
                                                pur_initiator="CRON JOB",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            response['result'][i]['fee_name'],
                                                amount_due=weeks_current_month * (charge *
                                                                                  (1 - mpayment['charge'] / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=charge_date
                                            )
                                        else:

                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=response['result'][i]['property_uid'],
                                                payer=response['result'][i]['business_uid'],
                                                receiver=response['result'][i]['owner_id'],
                                                pur_initiator="CRON JOB",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            response['result'][i]['fee_name'],
                                                amount_due=weeks_current_month * (
                                                        (charge - mpayment['expense_amount']) * (
                                                        1 - mpayment['charge'] / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=charge_date
                                            )
                                    elif mpayment['frequency'] == 'Biweekly' and mpayment['fee_type'] == '%':

                                        # if gross charge (listed charge)
                                        if mpayment['of'] == 'Gross Rent':
                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=response['result'][i]['property_uid'],
                                                payer=response['result'][i]['business_uid'],
                                                receiver=response['result'][i]['owner_id'],
                                                pur_initiator="CRON JOB",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            response['result'][i]['fee_name'],
                                                amount_due=(weeks_current_month / 2) *
                                                           ((charge *
                                                             (1 - mpayment['charge'] / 100))),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=charge_date
                                            )
                                        else:

                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=response['result'][i]['property_uid'],
                                                payer=response['result'][i]['business_uid'],
                                                receiver=response['result'][i]['owner_id'],
                                                pur_initiator="CRON JOB",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            response['result'][i]['fee_name'],
                                                amount_due=(weeks_current_month / 2) *
                                                           ((charge - mpayment['expense_amount']) * (
                                                                   1 - mpayment['charge'] / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=charge_date
                                            )
                                    elif mpayment['frequency'] == 'Monthly' and mpayment['fee_type'] == '%':

                                        # if gross charge (listed charge)
                                        if mpayment['of'] == 'Gross Rent':
                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=response['result'][i]['property_uid'],
                                                payer=response['result'][i]['business_uid'],
                                                receiver=response['result'][i]['owner_id'],
                                                pur_initiator="CRON JOB",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            response['result'][i]['fee_name'],
                                                amount_due=(
                                                        charge * (1 - int(mpayment['charge']) / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=charge_date
                                            )

                                        # if net charge (listed charge-expenses)
                                        else:
                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=response['result'][i]['property_uid'],
                                                payer=response['result'][i]['business_uid'],
                                                receiver=response['result'][i]['owner_id'],
                                                pur_initiator="CRON JOB",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            response['result'][i]['fee_name'],
                                                amount_due=(
                                                        (charge - mpayment['expense_amount']) * (
                                                        1 - int(mpayment['charge']) / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=charge_date
                                            )
                                    else:
                                        print(
                                            'payment frequency one-time %')

                return response

def LeasetoMonthTest(self):

    with connect() as db:
        response = {'message': 'Successfully committed SQL query',
                    'code': 200}
        response = db.execute("""
        SELECT *
        FROM space.leases l
        LEFT JOIN space.leaseFees lf ON lf.fees_lease_id = l.lease_uid
        LEFT JOIN space.t_details lt ON l.lease_uid = lt.lt_lease_id           
        LEFT JOIN space.b_details b ON b.contract_property_id = l.lease_property_id
        LEFT JOIN space.properties p ON p.property_uid = l.lease_property_id
        LEFT JOIN space.o_details o ON o.property_id = l.lease_property_id
        WHERE (l.lease_end <= DATE_FORMAT(NOW(), "%Y-%m-%d")
        OR l.lease_end = DATE_FORMAT(NOW(), "%Y-%m-%d"))
        AND l.lease_status='ACTIVE'
        AND (b.contract_status = 'ACTIVE' OR b.contract_status='END EARLY' OR b.contract_status='PM END EARLY' OR b.contract_status='OWNER END EARLY'); """)

        if len(response['result']) > 0:

            for i in range(len(response['result'])):
                expiredResponse = db.execute("""
                                SELECT *
                                FROM space.leases l
                                LEFT JOIN space.leaseFees lf ON lf.fees_lease_id = l.lease_uid
                                LEFT JOIN space.properties p ON p.property_uid = l.lease_property_id          
                                LEFT JOIN space.b_details b ON b.contract_property_id = l.lease_property_id
                                WHERE (l.lease_status = 'ACTIVE' OR l.lease_status = 'PROCESSING' OR l.lease_status='TENANT APPROVED' OR l.lease_status='PENDING')
                                AND l.lease_property_id = \'""" + response['result'][i]['lease_property_id'] + """\'
                                AND l.lease_uid != \'""" + response['result'][i]['lease_uid'] + """\' ;
                                -- AND (pM.management_status = 'ACCEPTED' OR pM.management_status='END EARLY' OR pM.management_status='PM END EARLY' OR pM.management_status='OWNER END EARLY')  """)

                if len(expiredResponse['result']) > 0:
                    print('do not do month to month')
                else:
                    print(response['result'][i]['lease_uid'])

                    pk = {
                        'lease_uid': response['result'][i]['lease_uid']}
                    updateLeaseEnd = {
                        'lease_end': (date.fromisoformat(
                            response['result'][i]['lease_end']) + relativedelta(months=1)).isoformat()}

                    # res = db.update('leases', pk, updateLeaseEnd)


                    tenants = response['result'][0]['tenant_uid']

                    """
                    *) This would not work as we have separate table for leaseFees. and rent_payments is not json now.

                    see the leases table
                    # """
                    # payment = json.loads(
                    #     response['result'][i]['rent_payments'])
                    # # print(payment, len(payment))
                    """
                    1) Contract fees table has not been decided yet
                    """
                    managementPayments = json.loads(
                        response['result'][i]['contract_fees'])

                    #     """
                    #     4) We will have to add condition in sql statement to get the records with Rent as fee_name.
                    #     All the conditions with payment will have to change
                    #     ex:     due_date = charge_date.replace(day=int(payment[r]['due_by']))
                    #     """

                    # for r in range(len(payment)):
                    if response['result'][i]['fee_name'] == 'Rent':
                        charge_date = date.today()
                        due_date = charge_date.replace(
                            day=int(response['result'][i]['due_by']))
                        charge_month = charge_date.strftime(
                            '%B')
                        if response['result'][i]['available_topay'] == 0:
                            available_date = due_date
                        else:
                            available_date = due_date - \
                                             timedelta(
                                                 days=int(response['result'][i]['available_topay']))



                        # print(available_date, charge_date, charge_month)

                        if available_date == charge_date:
                            charge = int(response['result'][i]['charge'])
                            purchaseResponse = newPurchase(
                                linked_bill_id=None,
                                pur_property_id=response['result'][i]['lease_property_id'],
                                payer=tenants,
                                receiver=response['result'][i]['business_uid'],
                                pur_initiator="CRON JOB",
                                purchase_type='RENT',
                                description=charge_month +
                                            ' ' + response['result'][i]['fee_name'],
                                amount_due=charge,
                                purchase_notes=charge_month,
                                purchase_date=charge_date.isoformat(),
                                purchase_frequency=response['result'][i]['frequency'],
                                next_payment=due_date
                            )
                            for mpayment in managementPayments:
                                weeks_current_month = len(
                                    calendar.monthcalendar(charge_date.year, int(charge_date.strftime("%m"))))
                                if mpayment['frequency'] == 'Weekly' and mpayment['fee_type'] == '%':
                                    if mpayment['of'] == 'Gross Rent':
                                        purchaseResponse = newPurchase(
                                            linked_bill_id=None,
                                            pur_property_id=response['result'][i]['property_uid'],
                                            payer=response['result'][i]['business_uid'],
                                            receiver=response['result'][i]['owner_id'],
                                            pur_initiator="CRON JOB",
                                            purchase_type='OWNER PAYMENT RENT',
                                            description=charge_month + ' ' +
                                                        response['result'][i]['fee_name'],
                                            amount_due=weeks_current_month * (charge *
                                                                              (1 - mpayment['charge'] / 100)),
                                            purchase_notes=charge_month,
                                            purchase_date=available_date,
                                            purchase_frequency=mpayment['frequency'],
                                            next_payment=charge_date
                                        )
                                    else:

                                        purchaseResponse = newPurchase(
                                            linked_bill_id=None,
                                            pur_property_id=response['result'][i]['property_uid'],
                                            payer=response['result'][i]['business_uid'],
                                            receiver=response['result'][i]['owner_id'],
                                            pur_initiator="CRON JOB",
                                            purchase_type='OWNER PAYMENT RENT',
                                            description=charge_month + ' ' +
                                                        response['result'][i]['fee_name'],
                                            amount_due=weeks_current_month * (
                                                    (charge - mpayment['expense_amount']) * (
                                                    1 - mpayment['charge'] / 100)),
                                            purchase_notes=charge_month,
                                            purchase_date=available_date,
                                            purchase_frequency=mpayment['frequency'],
                                            next_payment=charge_date
                                        )
                                elif mpayment['frequency'] == 'Biweekly' and mpayment['fee_type'] == '%':

                                    # if gross charge (listed charge)
                                    if mpayment['of'] == 'Gross Rent':
                                        purchaseResponse = newPurchase(
                                            linked_bill_id=None,
                                            pur_property_id=response['result'][i]['property_uid'],
                                            payer=response['result'][i]['business_uid'],
                                            receiver=response['result'][i]['owner_id'],
                                            pur_initiator="CRON JOB",
                                            purchase_type='OWNER PAYMENT RENT',
                                            description=charge_month + ' ' +
                                                        response['result'][i]['fee_name'],
                                            amount_due=(weeks_current_month / 2) *
                                                       ((charge *
                                                         (1 - mpayment['charge'] / 100))),
                                            purchase_notes=charge_month,
                                            purchase_date=available_date,
                                            purchase_frequency=mpayment['frequency'],
                                            next_payment=charge_date
                                        )
                                    else:

                                        purchaseResponse = newPurchase(
                                            linked_bill_id=None,
                                            pur_property_id=response['result'][i]['property_uid'],
                                            payer=response['result'][i]['business_uid'],
                                            receiver=response['result'][i]['owner_id'],
                                            pur_initiator="CRON JOB",
                                            purchase_type='OWNER PAYMENT RENT',
                                            description=charge_month + ' ' +
                                                        response['result'][i]['fee_name'],
                                            amount_due=(weeks_current_month / 2) *
                                                       ((charge - mpayment['expense_amount']) * (
                                                               1 - mpayment['charge'] / 100)),
                                            purchase_notes=charge_month,
                                            purchase_date=available_date,
                                            purchase_frequency=mpayment['frequency'],
                                            next_payment=charge_date
                                        )
                                elif mpayment['frequency'] == 'Monthly' and mpayment['fee_type'] == '%':

                                    # if gross charge (listed charge)
                                    if mpayment['of'] == 'Gross Rent':
                                        purchaseResponse = newPurchase(
                                            linked_bill_id=None,
                                            pur_property_id=response['result'][i]['property_uid'],
                                            payer=response['result'][i]['business_uid'],
                                            receiver=response['result'][i]['owner_id'],
                                            pur_initiator="CRON JOB",
                                            purchase_type='OWNER PAYMENT RENT',
                                            description=charge_month + ' ' +
                                                        response['result'][i]['fee_name'],
                                            amount_due=(
                                                    charge * (1 - int(mpayment['charge']) / 100)),
                                            purchase_notes=charge_month,
                                            purchase_date=available_date,
                                            purchase_frequency=mpayment['frequency'],
                                            next_payment=charge_date
                                        )

                                    # if net charge (listed charge-expenses)
                                    else:
                                        purchaseResponse = newPurchase(
                                            linked_bill_id=None,
                                            pur_property_id=response['result'][i]['property_uid'],
                                            payer=response['result'][i]['business_uid'],
                                            receiver=response['result'][i]['owner_id'],
                                            pur_initiator="CRON JOB",
                                            purchase_type='OWNER PAYMENT RENT',
                                            description=charge_month + ' ' +
                                                        response['result'][i]['fee_name'],
                                            amount_due=(
                                                    (charge - mpayment['expense_amount']) * (
                                                    1 - int(mpayment['charge']) / 100)),
                                            purchase_notes=charge_month,
                                            purchase_date=available_date,
                                            purchase_frequency=mpayment['frequency'],
                                            next_payment=charge_date
                                        )
                                else:
                                    print(
                                        'payment frequency one-time %')

            return response

def days_in_month(dt): return monthrange(
    dt.year, dt.month)[1]


class ExtendLease_CLASS(Resource):
    def get(self):
        with connect() as db:
            response = {'message': 'Successfully committed SQL query',
                        'code': 200}
            leaseResponse = db.execute("""
            SELECT r.*, GROUP_CONCAT(lt_tenant_id) as `tenants`
            FROM space.leases AS r
            LEFT JOIN space.t_details AS lt ON lt.lt_lease_id = r.lease_uid
            WHERE r.lease_property_id IN (SELECT *
            FROM (SELECT r.lease_property_id
                FROM space.leases r
                GROUP BY r.lease_property_id
                HAVING COUNT(r.lease_property_id) > 1)
            AS a)
            AND (r.lease_status = 'ACTIVE' OR r.lease_status = 'PROCESSING')
            GROUP BY lt_lease_id;""")
            today_datetime = datetime.now()
            today = datetime.strftime(today_datetime, '%Y-%m-%d')
            oldLease = ''
            newLease = ''
            oldLeases = []
            newLeases = []
            if len(leaseResponse['result']) > 0:
                for res in leaseResponse['result']:
                    if res['lease_status'] == 'ACTIVE':
                        oldLeases.append(res)
                    else:
                        newLeases.append(res)
            print('oldLeases', oldLeases)
            print('newLeases', newLeases)
            for ol in oldLeases:
                if ol['lease_end'] == today:
                    # print('old lease expiring today')
                    # print('here')

                    pk = {
                        'lease_uid': ol['lease_uid']
                    }
                    oldLeaseUpdate = {
                        'lease_status': 'EXPIRED'
                    }
                    oldLeaseUpdateRes = db.update(
                        'leases', pk, oldLeaseUpdate)
            for nl in newLeases:
                print("in new leases")
                if nl['lease_start'] == today:
                    print('new lease starting today')
                    # adding leaseTenants
                    tenants = nl['tenants']
                    # print('tenants1', tenants)
                    # if '[' in tenants:
                    #     # print('tenants2', tenants)
                    #     tenants = json.loads(tenants)
                    #     # print('tenants3', tenants)
                    # # print('tenants4', tenants)
                    # if type(tenants) == str:
                    #     tenants = [tenants]
                    # print('tenants5', tenants)
                    # print(tenants)
                    # creating purchases
                    ##### 1st change(rent paymnet not available, joined leaseFees table)
                    # rentPayments = json.loads(nl['rent_payments'])

                    res = db.execute("""
                    SELECT *
                    FROM space.leaseFees lf
                    LEFT JOIN space.leases r ON lf.fees_lease_id = r.lease_uid
                    LEFT JOIN space.t_details AS lt ON lt.lt_lease_id = lf.fees_lease_id
                    LEFT JOIN space.b_details p ON p.contract_property_id = r.lease_property_id
                    LEFT JOIN space.property_owner po ON po.property_id =  r.lease_property_id
                    WHERE r.lease_status='ACTIVE'
                    AND p.contract_status = 'ACTIVE'
                    -- AND lf.fees_lease_id = '300-000020';
                    AND lf.fees_lease_id = \'""" + nl['lease_uid'] + """\';
                    """)

                    if len(res['result']) > 0:
                        managementPayments = json.loads(res['result'][0]['contract_fees'])

                    for payment in res['result']:
                        print('paymetn', payment)
                        if payment['frequency'] == 'Monthly':

                            charge_date = date.fromisoformat(
                                nl['lease_start'])
                            due_date = charge_date.replace(
                                day=int(payment['due_by']))
                            lease_end = date.fromisoformat(
                                nl['lease_end'])
                            lease_start = date.fromisoformat(
                                nl['lease_start'])

                            # available date-> when the payment is available to pay
                            if payment['available_topay'] == 0:
                                available_date = due_date
                            else:
                                available_date = due_date - \
                                                 timedelta(
                                                     days=int(payment['available_topay']))
                            # print("today",today)
                            # print('available date',datetime.strftime(available_date, '%Y-%m-%d'))
                            while datetime.strftime(available_date, '%Y-%m-%d') < today and datetime.strftime(due_date,
                                                                                                              '%Y-%m-%d') < \
                                    nl['lease_end']:
                                charge_month = due_date.strftime(
                                    '%B')
                                # print("in while loop")
                                if (payment['fee_name'] == 'Rent'):
                                    if charge_month == charge_date.strftime(
                                            '%B') and charge_date.strftime('%d') != '01':
                                        # prorate first month
                                        # print('days_in_month(charge_date)', days_in_month(
                                        #     charge_date))
                                        print(type(int(float(payment['charge']))))
                                        daily_charge_begin = int(
                                            int(float(payment['charge'])) / days_in_month(charge_date))
                                        print("daily charge", daily_charge_begin)
                                        num_days_active_begin = days_in_month(
                                            charge_date) - charge_date.day + 1
                                        charge = round(
                                            num_days_active_begin * daily_charge_begin, 2)
                                        charge_date = lease_start

                                    else:
                                        charge = int(payment['charge'])
                                        charge_date = (charge_date.replace(
                                            day=int(payment['due_by'])))
                                    purchaseResponse = newPurchase(
                                        linked_bill_id=None,
                                        pur_property_id=nl['lease_property_id'],
                                        payer=payment['lt_tenant_id'],
                                        receiver=res['result'][0]['property_owner_id'],
                                        pur_initiator=res['result'][0]['business_uid'],
                                        purchase_type='RENT',
                                        pur_cf_type='REVENUE',
                                        description=charge_month +
                                                    ' ' + payment['fee_name'],
                                        amount_due=charge,
                                        purchase_notes=charge_month,
                                        purchase_date=available_date.isoformat(),
                                        purchase_frequency=payment['frequency'],
                                        next_payment=due_date
                                    )
                                    for mpayment in managementPayments:
                                        weeks_current_month = len(
                                            calendar.monthcalendar(due_date.year, int(due_date.strftime("%m"))))
                                        #
                                        if mpayment['frequency'] == 'Weekly' and mpayment['fee_type'] == '%':
                                            if mpayment['of'] == 'Gross Rent':

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT RENT',
                                                    description=charge_month + ' ' +
                                                                payment['fee_name'],
                                                    amount_due=weeks_current_month * (charge *
                                                                                      (1 - mpayment['charge'] / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )
                                            # if net charge (listed charge-expenses)
                                            else:

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT RENT',
                                                    description=charge_month + ' ' +
                                                                payment['fee_name'],
                                                    amount_due=weeks_current_month * (
                                                                (charge - mpayment['expense_amount']) * (
                                                                1 - mpayment['charge'] / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )
                                        elif mpayment['frequency'] == 'Biweekly' and mpayment['fee_type'] == '%':

                                            # if gross charge (listed charge)
                                            if mpayment['of'] == 'Gross Rent':

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT RENT',
                                                    description=charge_month + ' ' +
                                                                payment['fee_name'],
                                                    amount_due=(weeks_current_month / 2) *
                                                               ((charge *
                                                                 (1 - mpayment['charge'] / 100))),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )
                                            # if net charge (listed charge-expenses)
                                            else:

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT RENT',
                                                    description=charge_month + ' ' +
                                                                payment['fee_name'],
                                                    amount_due=(weeks_current_month / 2) *
                                                               ((charge - mpayment['expense_amount']) * (
                                                                       1 - mpayment['charge'] / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )

                                        elif mpayment['frequency'] == 'Monthly' and mpayment['fee_type'] == '%':

                                            # if gross charge (listed charge)
                                            if mpayment['of'] == 'Gross Rent':
                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT RENT',
                                                    description=charge_month + ' ' +
                                                                payment['fee_name'],
                                                    amount_due=(
                                                            charge * (1 - int(mpayment['charge']) / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )

                                            # if net charge (listed charge-expenses)
                                            else:
                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT RENT',
                                                    description=charge_month + ' ' +
                                                                payment['fee_name'],
                                                    amount_due=(
                                                            (charge - mpayment['expense_amount']) * (
                                                                1 - int(mpayment['charge']) / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )

                                        else:
                                            print(
                                                'payment frequency one-time %')

                                elif (payment['fee_name'] == 'Deposit'):
                                    purchaseResponse = newPurchase(
                                        linked_bill_id=None,
                                        pur_property_id=nl['lease_property_id'],
                                        payer=payment['lt_tenant_id'],
                                        receiver=res['result'][0]['property_owner_id'],
                                        pur_initiator=res['result'][0]['business_uid'],
                                        pur_cf_type="REVENUE",
                                        purchase_type='DEPOSIT',
                                        description=payment['fee_name'],
                                        amount_due=payment['charge'],
                                        purchase_notes=charge_month,
                                        purchase_date=available_date.isoformat(),
                                        purchase_frequency=payment['frequency'],
                                        next_payment=due_date
                                    )
                                else:
                                    purchaseResponse = newPurchase(
                                        linked_bill_id=None,
                                        pur_property_id=nl['lease_property_id'],
                                        payer=payment['lt_tenant_id'],
                                        receiver=res['result'][0]['property_owner_id'],
                                        pur_initiator=res['result'][0]['business_uid'],
                                        pur_cf_type="REVENUE",
                                        purchase_type='EXTRA CHARGES',
                                        description=charge_month +
                                                    ' ' + payment['fee_name'],
                                        amount_due=payment['charge'],
                                        purchase_notes=charge_month,
                                        purchase_date=available_date.isoformat(),
                                        purchase_frequency=payment['frequency'],
                                        next_payment=due_date
                                    )
                                    # manager payments weekly $ charge
                                    for mpayment in managementPayments:
                                        weeks_current_month = len(
                                            calendar.monthcalendar(due_date.year, int(due_date.strftime("%m"))))

                                        if mpayment['frequency'] == 'Weekly' and mpayment['fee_type'] == '%':
                                            if mpayment['of'] == 'Gross Rent':

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                    description=payment['fee_name'],
                                                    amount_due=weeks_current_month * (charge *
                                                                                      (1 - mpayment['charge'] / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )
                                            # if net charge (listed charge-expenses)
                                            else:

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                    description=payment['fee_name'],
                                                    amount_due=weeks_current_month * (
                                                                (charge - mpayment['expense_amount']) * (
                                                                1 - mpayment['charge'] / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )
                                        elif mpayment['frequency'] == 'Biweekly' and mpayment['fee_type'] == '%':

                                            # if gross charge (listed charge)
                                            if mpayment['of'] == 'Gross Rent':

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                    description=payment['fee_name'],
                                                    amount_due=(weeks_current_month / 2) *
                                                               ((charge *
                                                                 (1 - mpayment['charge'] / 100))),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )
                                            # if net charge (listed charge-expenses)
                                            else:

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                    description=payment['fee_name'],
                                                    amount_due=(weeks_current_month / 2) *
                                                               ((charge - mpayment['expense_amount']) * (
                                                                       1 - mpayment['charge'] / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )

                                        elif mpayment['frequency'] == 'Monthly' and mpayment['fee_type'] == '%':

                                            # if gross charge (listed charge)
                                            if mpayment['of'] == 'Gross Rent':
                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                    description=payment['fee_name'],
                                                    amount_due=(
                                                            charge * (1 - int(mpayment['charge']) / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )

                                            # if net charge (listed charge-expenses)
                                            else:
                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                    description=payment['fee_name'],
                                                    amount_due=(
                                                            (charge - mpayment['expense_amount']) * (
                                                                1 - int(mpayment['charge']) / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )

                                        else:
                                            print(
                                                'payment frequency one-time %')

                                # charge_date += relativedelta(months=1)
                                due_date += relativedelta(months=1)
                                available_date += relativedelta(
                                    months=1)
                        else:
                            # print('lease_start', type(
                            #     res['result'][0]['lease_start']))

                            charge_date = date.fromisoformat(
                                nl['lease_start'])
                            due_date = date.fromisoformat(
                                nl['lease_start']).replace(
                                day=int(payment['due_by']))
                            lease_end = date.fromisoformat(
                                nl['lease_end'])
                            # available date-> when the payment is available to pay
                            if payment['available_topay'] == 0:
                                available_date = due_date
                            else:
                                available_date = due_date - \
                                                 timedelta(
                                                     days=int(payment['available_topay']))

                            charge_month = due_date.strftime(
                                '%B')
                            if (payment['fee_name'] == 'Rent'):
                                purchaseResponse = newPurchase(
                                    linked_bill_id=None,
                                    pur_property_id=nl['lease_property_id'],
                                    payer=payment['lt_tenant_id'],
                                    receiver=res['result'][0]['property_owner_id'],
                                    pur_cf_type="REVENUE",
                                    purchase_type='RENT',
                                    description=payment['fee_name'],
                                    amount_due=payment['charge'],
                                    purchase_notes=charge_month,
                                    purchase_date=available_date.isoformat(),
                                    purchase_frequency=payment['frequency'],
                                    next_payment=due_date
                                )
                            elif (payment['fee_name'] == 'Deposit'):
                                purchaseResponse = newPurchase(
                                    linked_bill_id=None,
                                    pur_property_id=nl['lease_property_id'],
                                    payer=payment['lt_tenant_id'],
                                    receiver=res['result'][0]['property_owner_id'],
                                    pur_cf_type="REVENUE",
                                    pur_initiator=res['result'][0]['business_uid'],
                                    purchase_type='DEPOSIT',
                                    description=payment['fee_name'],
                                    amount_due=payment['charge'],
                                    purchase_notes=charge_month,
                                    purchase_date=available_date.isoformat(),
                                    purchase_frequency=payment['frequency'],
                                    next_payment=due_date
                                )

                            else:

                                purchaseResponse = newPurchase(
                                    linked_bill_id=None,
                                    pur_property_id=nl['lease_property_id'],
                                    payer=payment['lt_tenant_id'],
                                    receiver=res['result'][0]['property_owner_id'],
                                    pur_cf_type="REVENUE",
                                    pur_initiator=res['result'][0]['business_uid'],
                                    purchase_type='EXTRA CHARGES',
                                    description=payment['fee_name'],
                                    amount_due=payment['charge'],
                                    purchase_notes=charge_month,
                                    purchase_date=available_date.isoformat(),
                                    purchase_frequency=payment['frequency'],
                                    next_payment=due_date
                                )
                    pkNL = {
                        'lease_uid': nl['lease_uid']
                    }
                    newLeaseUpdate = {
                        'lease_status': 'ACTIVE'
                    }
                    # print(pkNL, newLeaseUpdate)
                    response = db.update(
                        'leases', pkNL, newLeaseUpdate)
        return response

def ExtendLease(self):

    with connect() as db:
        response = {'message': 'Successfully committed SQL query',
                    'code': 200}
        leaseResponse = db.execute("""
        SELECT r.*, GROUP_CONCAT(lt_tenant_id) as `tenants`
        FROM space.leases AS r
        LEFT JOIN space.t_details AS lt ON lt.lt_lease_id = r.lease_uid
        WHERE r.lease_property_id IN (SELECT *
        FROM (SELECT r.lease_property_id
            FROM space.leases r
            GROUP BY r.lease_property_id
            HAVING COUNT(r.lease_property_id) > 1)
        AS a)
        AND (r.lease_status = 'ACTIVE' OR r.lease_status = 'PROCESSING')
        GROUP BY lt_lease_id;""")
        today_datetime = datetime.now()
        today = datetime.strftime(today_datetime, '%Y-%m-%d')
        oldLease = ''
        newLease = ''
        oldLeases = []
        newLeases = []
        if len(leaseResponse['result']) > 0:
            for res in leaseResponse['result']:
                if res['lease_status'] == 'ACTIVE':
                    oldLeases.append(res)
                else:
                    newLeases.append(res)
        print('oldLeases', oldLeases)
        print('newLeases', newLeases)
        for ol in oldLeases:
            if ol['lease_end'] == today:
                # print('old lease expiring today')
                # print('here')

                pk = {
                    'lease_uid': ol['lease_uid']
                }
                oldLeaseUpdate = {
                    'lease_status': 'EXPIRED'
                }
                oldLeaseUpdateRes = db.update(
                    'leases', pk, oldLeaseUpdate)
        for nl in newLeases:
            print("in new leases")
            if nl['lease_start'] == today:
                print('new lease starting today')
                # adding leaseTenants
                tenants = nl['tenants']
                # print('tenants1', tenants)
                # if '[' in tenants:
                #     # print('tenants2', tenants)
                #     tenants = json.loads(tenants)
                #     # print('tenants3', tenants)
                # # print('tenants4', tenants)
                # if type(tenants) == str:
                #     tenants = [tenants]
                # print('tenants5', tenants)
                # print(tenants)
                # creating purchases
                ##### 1st change(rent paymnet not available, joined leaseFees table)
                # rentPayments = json.loads(nl['rent_payments'])

                res = db.execute("""
                SELECT *
                FROM space.leaseFees lf
                LEFT JOIN space.leases r ON lf.fees_lease_id = r.lease_uid
                LEFT JOIN space.t_details AS lt ON lt.lt_lease_id = lf.fees_lease_id
                LEFT JOIN space.b_details p ON p.contract_property_id = r.lease_property_id
                LEFT JOIN space.property_owner po ON po.property_id =  r.lease_property_id
                WHERE r.lease_status='ACTIVE'
                AND p.contract_status = 'ACTIVE'
                -- AND lf.fees_lease_id = '300-000020';
                AND lf.fees_lease_id = \'""" + nl['lease_uid'] + """\';
                """)

                if len(res['result']) > 0:
                    managementPayments = json.loads(res['result'][0]['contract_fees'])

                for payment in res['result']:
                    print('paymetn', payment)
                    if payment['frequency'] == 'Monthly':

                        charge_date = date.fromisoformat(
                            nl['lease_start'])
                        due_date = charge_date.replace(
                            day=int(payment['due_by']))
                        lease_end = date.fromisoformat(
                            nl['lease_end'])
                        lease_start = date.fromisoformat(
                            nl['lease_start'])

                        # available date-> when the payment is available to pay
                        if payment['available_topay'] == 0:
                            available_date = due_date
                        else:
                            available_date = due_date - \
                                             timedelta(
                                                 days=int(payment['available_topay']))
                        # print("today",today)
                        # print('available date',datetime.strftime(available_date, '%Y-%m-%d'))
                        while datetime.strftime(available_date, '%Y-%m-%d') < today and datetime.strftime(due_date,
                                                                                                          '%Y-%m-%d') < \
                                nl['lease_end']:
                            charge_month = due_date.strftime(
                                '%B')
                            # print("in while loop")
                            if (payment['fee_name'] == 'Rent'):
                                if charge_month == charge_date.strftime(
                                        '%B') and charge_date.strftime('%d') != '01':
                                    # prorate first month
                                    # print('days_in_month(charge_date)', days_in_month(
                                    #     charge_date))
                                    print(type(int(float(payment['charge']))))
                                    daily_charge_begin = int(
                                        int(float(payment['charge'])) / days_in_month(charge_date))
                                    print("daily charge", daily_charge_begin)
                                    num_days_active_begin = days_in_month(
                                        charge_date) - charge_date.day + 1
                                    charge = round(
                                        num_days_active_begin * daily_charge_begin, 2)
                                    charge_date = lease_start

                                else:
                                    charge = int(payment['charge'])
                                    charge_date = (charge_date.replace(
                                        day=int(payment['due_by'])))
                                purchaseResponse = newPurchase(
                                    linked_bill_id=None,
                                    pur_property_id=nl['lease_property_id'],
                                    payer=payment['lt_tenant_id'],
                                    receiver=res['result'][0]['property_owner_id'],
                                    pur_initiator=res['result'][0]['business_uid'],
                                    purchase_type='RENT',
                                    pur_cf_type='REVENUE',
                                    description=charge_month +
                                                ' ' + payment['fee_name'],
                                    amount_due=charge,
                                    purchase_notes=charge_month,
                                    purchase_date=available_date.isoformat(),
                                    purchase_frequency=payment['frequency'],
                                    next_payment=due_date
                                )
                                for mpayment in managementPayments:
                                    weeks_current_month = len(
                                        calendar.monthcalendar(due_date.year, int(due_date.strftime("%m"))))
                                    #
                                    if mpayment['frequency'] == 'Weekly' and mpayment['fee_type'] == '%':
                                        if mpayment['of'] == 'Gross Rent':

                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=nl['lease_property_id'],
                                                payer=res['result'][0]['property_owner_id'],
                                                receiver=res['result'][0]['business_uid'],
                                                pur_initiator="CRON JOB",
                                                pur_cf_type="EXPENSE",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            payment['fee_name'],
                                                amount_due=weeks_current_month * (charge *
                                                                                  (1 - mpayment['charge'] / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=due_date
                                            )
                                        # if net charge (listed charge-expenses)
                                        else:

                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=nl['lease_property_id'],
                                                payer=res['result'][0]['property_owner_id'],
                                                receiver=res['result'][0]['business_uid'],
                                                pur_initiator="CRON JOB",
                                                pur_cf_type="EXPENSE",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            payment['fee_name'],
                                                amount_due=weeks_current_month * (
                                                            (charge - mpayment['expense_amount']) * (
                                                            1 - mpayment['charge'] / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=due_date
                                            )
                                    elif mpayment['frequency'] == 'Biweekly' and mpayment['fee_type'] == '%':

                                        # if gross charge (listed charge)
                                        if mpayment['of'] == 'Gross Rent':

                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=nl['lease_property_id'],
                                                payer=res['result'][0]['property_owner_id'],
                                                receiver=res['result'][0]['business_uid'],
                                                pur_initiator="CRON JOB",
                                                pur_cf_type="EXPENSE",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            payment['fee_name'],
                                                amount_due=(weeks_current_month / 2) *
                                                           ((charge *
                                                             (1 - mpayment['charge'] / 100))),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=due_date
                                            )
                                        # if net charge (listed charge-expenses)
                                        else:

                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=nl['lease_property_id'],
                                                payer=res['result'][0]['property_owner_id'],
                                                receiver=res['result'][0]['business_uid'],
                                                pur_initiator="CRON JOB",
                                                pur_cf_type="EXPENSE",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            payment['fee_name'],
                                                amount_due=(weeks_current_month / 2) *
                                                           ((charge - mpayment['expense_amount']) * (
                                                                   1 - mpayment['charge'] / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=due_date
                                            )

                                    elif mpayment['frequency'] == 'Monthly' and mpayment['fee_type'] == '%':

                                        # if gross charge (listed charge)
                                        if mpayment['of'] == 'Gross Rent':
                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=nl['lease_property_id'],
                                                payer=res['result'][0]['property_owner_id'],
                                                receiver=res['result'][0]['business_uid'],
                                                pur_initiator="CRON JOB",
                                                pur_cf_type="EXPENSE",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            payment['fee_name'],
                                                amount_due=(
                                                        charge * (1 - int(mpayment['charge']) / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=due_date
                                            )

                                        # if net charge (listed charge-expenses)
                                        else:
                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=nl['lease_property_id'],
                                                payer=res['result'][0]['property_owner_id'],
                                                receiver=res['result'][0]['business_uid'],
                                                pur_initiator="CRON JOB",
                                                pur_cf_type="EXPENSE",
                                                purchase_type='OWNER PAYMENT RENT',
                                                description=charge_month + ' ' +
                                                            payment['fee_name'],
                                                amount_due=(
                                                        (charge - mpayment['expense_amount']) * (
                                                            1 - int(mpayment['charge']) / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=due_date
                                            )

                                    else:
                                        print(
                                            'payment frequency one-time %')

                            elif (payment['fee_name'] == 'Deposit'):
                                purchaseResponse = newPurchase(
                                    linked_bill_id=None,
                                    pur_property_id=nl['lease_property_id'],
                                    payer=payment['lt_tenant_id'],
                                    receiver=res['result'][0]['property_owner_id'],
                                    pur_initiator=res['result'][0]['business_uid'],
                                    pur_cf_type="REVENUE",
                                    purchase_type='DEPOSIT',
                                    description=payment['fee_name'],
                                    amount_due=payment['charge'],
                                    purchase_notes=charge_month,
                                    purchase_date=available_date.isoformat(),
                                    purchase_frequency=payment['frequency'],
                                    next_payment=due_date
                                )
                            else:
                                purchaseResponse = newPurchase(
                                    linked_bill_id=None,
                                    pur_property_id=nl['lease_property_id'],
                                    payer=payment['lt_tenant_id'],
                                    receiver=res['result'][0]['property_owner_id'],
                                    pur_initiator=res['result'][0]['business_uid'],
                                    pur_cf_type="REVENUE",
                                    purchase_type='EXTRA CHARGES',
                                    description=charge_month +
                                                ' ' + payment['fee_name'],
                                    amount_due=payment['charge'],
                                    purchase_notes=charge_month,
                                    purchase_date=available_date.isoformat(),
                                    purchase_frequency=payment['frequency'],
                                    next_payment=due_date
                                )
                                # manager payments weekly $ charge
                                for mpayment in managementPayments:
                                    weeks_current_month = len(
                                        calendar.monthcalendar(due_date.year, int(due_date.strftime("%m"))))

                                    if mpayment['frequency'] == 'Weekly' and mpayment['fee_type'] == '%':
                                        if mpayment['of'] == 'Gross Rent':

                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=nl['lease_property_id'],
                                                payer=res['result'][0]['property_owner_id'],
                                                receiver=res['result'][0]['business_uid'],
                                                pur_initiator="CRON JOB",
                                                pur_cf_type="EXPENSE",
                                                purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                description=payment['fee_name'],
                                                amount_due=weeks_current_month * (charge *
                                                                                  (1 - mpayment['charge'] / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=due_date
                                            )
                                        # if net charge (listed charge-expenses)
                                        else:

                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=nl['lease_property_id'],
                                                payer=res['result'][0]['property_owner_id'],
                                                receiver=res['result'][0]['business_uid'],
                                                pur_initiator="CRON JOB",
                                                pur_cf_type="EXPENSE",
                                                purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                description=payment['fee_name'],
                                                amount_due=weeks_current_month * (
                                                            (charge - mpayment['expense_amount']) * (
                                                            1 - mpayment['charge'] / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=due_date
                                            )
                                    elif mpayment['frequency'] == 'Biweekly' and mpayment['fee_type'] == '%':

                                        # if gross charge (listed charge)
                                        if mpayment['of'] == 'Gross Rent':

                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=nl['lease_property_id'],
                                                payer=res['result'][0]['property_owner_id'],
                                                receiver=res['result'][0]['business_uid'],
                                                pur_initiator="CRON JOB",
                                                pur_cf_type="EXPENSE",
                                                purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                description=payment['fee_name'],
                                                amount_due=(weeks_current_month / 2) *
                                                           ((charge *
                                                             (1 - mpayment['charge'] / 100))),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=due_date
                                            )
                                        # if net charge (listed charge-expenses)
                                        else:

                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=nl['lease_property_id'],
                                                payer=res['result'][0]['property_owner_id'],
                                                receiver=res['result'][0]['business_uid'],
                                                pur_initiator="CRON JOB",
                                                pur_cf_type="EXPENSE",
                                                purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                description=payment['fee_name'],
                                                amount_due=(weeks_current_month / 2) *
                                                           ((charge - mpayment['expense_amount']) * (
                                                                   1 - mpayment['charge'] / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=due_date
                                            )

                                    elif mpayment['frequency'] == 'Monthly' and mpayment['fee_type'] == '%':

                                        # if gross charge (listed charge)
                                        if mpayment['of'] == 'Gross Rent':
                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=nl['lease_property_id'],
                                                payer=res['result'][0]['property_owner_id'],
                                                receiver=res['result'][0]['business_uid'],
                                                pur_initiator="CRON JOB",
                                                pur_cf_type="EXPENSE",
                                                purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                description=payment['fee_name'],
                                                amount_due=(
                                                        charge * (1 - int(mpayment['charge']) / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=due_date
                                            )

                                        # if net charge (listed charge-expenses)
                                        else:
                                            purchaseResponse = newPurchase(
                                                linked_bill_id=None,
                                                pur_property_id=nl['lease_property_id'],
                                                payer=res['result'][0]['property_owner_id'],
                                                receiver=res['result'][0]['business_uid'],
                                                pur_initiator="CRON JOB",
                                                pur_cf_type="EXPENSE",
                                                purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                description=payment['fee_name'],
                                                amount_due=(
                                                        (charge - mpayment['expense_amount']) * (
                                                            1 - int(mpayment['charge']) / 100)),
                                                purchase_notes=charge_month,
                                                purchase_date=available_date,
                                                purchase_frequency=mpayment['frequency'],
                                                next_payment=due_date
                                            )

                                    else:
                                        print(
                                            'payment frequency one-time %')

                            # charge_date += relativedelta(months=1)
                            due_date += relativedelta(months=1)
                            available_date += relativedelta(
                                months=1)
                    else:
                        # print('lease_start', type(
                        #     res['result'][0]['lease_start']))

                        charge_date = date.fromisoformat(
                            nl['lease_start'])
                        due_date = date.fromisoformat(
                            nl['lease_start']).replace(
                            day=int(payment['due_by']))
                        lease_end = date.fromisoformat(
                            nl['lease_end'])
                        # available date-> when the payment is available to pay
                        if payment['available_topay'] == 0:
                            available_date = due_date
                        else:
                            available_date = due_date - \
                                             timedelta(
                                                 days=int(payment['available_topay']))

                        charge_month = due_date.strftime(
                            '%B')
                        if (payment['fee_name'] == 'Rent'):
                            purchaseResponse = newPurchase(
                                linked_bill_id=None,
                                pur_property_id=nl['lease_property_id'],
                                payer=payment['lt_tenant_id'],
                                receiver=res['result'][0]['property_owner_id'],
                                pur_cf_type="REVENUE",
                                purchase_type='RENT',
                                description=payment['fee_name'],
                                amount_due=payment['charge'],
                                purchase_notes=charge_month,
                                purchase_date=available_date.isoformat(),
                                purchase_frequency=payment['frequency'],
                                next_payment=due_date
                            )
                        elif (payment['fee_name'] == 'Deposit'):
                            purchaseResponse = newPurchase(
                                linked_bill_id=None,
                                pur_property_id=nl['lease_property_id'],
                                payer=payment['lt_tenant_id'],
                                receiver=res['result'][0]['property_owner_id'],
                                pur_cf_type="REVENUE",
                                pur_initiator=res['result'][0]['business_uid'],
                                purchase_type='DEPOSIT',
                                description=payment['fee_name'],
                                amount_due=payment['charge'],
                                purchase_notes=charge_month,
                                purchase_date=available_date.isoformat(),
                                purchase_frequency=payment['frequency'],
                                next_payment=due_date
                            )

                        else:

                            purchaseResponse = newPurchase(
                                linked_bill_id=None,
                                pur_property_id=nl['lease_property_id'],
                                payer=payment['lt_tenant_id'],
                                receiver=res['result'][0]['property_owner_id'],
                                pur_cf_type="REVENUE",
                                pur_initiator=res['result'][0]['business_uid'],
                                purchase_type='EXTRA CHARGES',
                                description=payment['fee_name'],
                                amount_due=payment['charge'],
                                purchase_notes=charge_month,
                                purchase_date=available_date.isoformat(),
                                purchase_frequency=payment['frequency'],
                                next_payment=due_date
                            )
                pkNL = {
                    'lease_uid': nl['lease_uid']
                }
                newLeaseUpdate = {
                    'lease_status': 'ACTIVE'
                }
                # print(pkNL, newLeaseUpdate)
                response = db.update(
                    'leases', pkNL, newLeaseUpdate)
    return response


class ExtendLeaseTest(Resource):
    def get(self):
        with connect() as db:
            response = {'message': 'Successfully committed SQL query',
                        'code': 200}
            leaseResponse = db.execute("""
            SELECT r.*, GROUP_CONCAT(lt_tenant_id) as `tenants`
            FROM space.leases AS r
            LEFT JOIN space.t_details AS lt ON lt.lt_lease_id = r.lease_uid
            WHERE r.lease_property_id IN (SELECT *
            FROM (SELECT r.lease_property_id
                FROM space.leases r
                GROUP BY r.lease_property_id
                HAVING COUNT(r.lease_property_id) > 1)
            AS a)
            AND (r.lease_status = 'ACTIVE' OR r.lease_status = 'PROCESSING')
            GROUP BY lt_lease_id;""")
            today_datetime = datetime.now()
            today = datetime.strftime(today_datetime, '%Y-%m-%d')
            oldLease = ''
            newLease = ''
            oldLeases = []
            newLeases = []
            if len(leaseResponse['result']) > 0:
                for res in leaseResponse['result']:
                    if res['lease_status'] == 'ACTIVE':
                        oldLeases.append(res)
                    else:
                        newLeases.append(res)
            print('oldLeases', oldLeases)
            print('newLeases', newLeases)
            for ol in oldLeases:
                if ol['lease_end'] == today:
                    # print('old lease expiring today')
                    # print('here')

                    pk = {
                        'lease_uid': ol['lease_uid']
                    }
                    oldLeaseUpdate = {
                        'lease_status': 'EXPIRED'
                    }
                    oldLeaseUpdateRes = db.update(
                        'leases', pk, oldLeaseUpdate)
            for nl in newLeases:
                print("in new leases")
                if nl['lease_start'] == today:
                    print('new lease starting today')
                    # adding leaseTenants
                    tenants = nl['tenants']
                    # print('tenants1', tenants)
                    # if '[' in tenants:
                    #     # print('tenants2', tenants)
                    #     tenants = json.loads(tenants)
                    #     # print('tenants3', tenants)
                    # # print('tenants4', tenants)
                    # if type(tenants) == str:
                    #     tenants = [tenants]
                    # print('tenants5', tenants)
                    # print(tenants)
                    # creating purchases
                    ##### 1st change(rent paymnet not available, joined leaseFees table)
                    # rentPayments = json.loads(nl['rent_payments'])

                    res = db.execute("""
                    SELECT *
                    FROM space.leaseFees lf
                    LEFT JOIN space.leases r ON lf.fees_lease_id = r.lease_uid
                    LEFT JOIN space.t_details AS lt ON lt.lt_lease_id = lf.fees_lease_id
                    LEFT JOIN space.b_details p ON p.contract_property_id = r.lease_property_id
                    LEFT JOIN space.property_owner po ON po.property_id =  r.lease_property_id
                    WHERE r.lease_status='ACTIVE'
                    AND p.contract_status = 'ACTIVE'
                    -- AND lf.fees_lease_id = '300-000020';
                    AND lf.fees_lease_id = \'""" + nl['lease_uid'] + """\';
                    """)

                    if len(res['result']) > 0:
                        managementPayments = json.loads(res['result'][0]['contract_fees'])

                    for payment in res['result']:
                        print('paymetn', payment)
                        if payment['frequency'] == 'Monthly':

                            charge_date = date.fromisoformat(
                                nl['lease_start'])
                            due_date = charge_date.replace(
                                day=int(payment['due_by']))
                            lease_end = date.fromisoformat(
                                nl['lease_end'])
                            lease_start = date.fromisoformat(
                                nl['lease_start'])

                            # available date-> when the payment is available to pay
                            if payment['available_topay'] == 0:
                                available_date = due_date
                            else:
                                available_date = due_date - \
                                                 timedelta(
                                                     days=int(payment['available_topay']))
                            # print("today",today)
                            # print('available date',datetime.strftime(available_date, '%Y-%m-%d'))
                            while datetime.strftime(available_date, '%Y-%m-%d') < today and datetime.strftime(due_date,
                                                                                                              '%Y-%m-%d') < \
                                    nl['lease_end']:
                                charge_month = due_date.strftime(
                                    '%B')
                                # print("in while loop")
                                if (payment['fee_name'] == 'Rent'):
                                    if charge_month == charge_date.strftime(
                                            '%B') and charge_date.strftime('%d') != '01':
                                        # prorate first month
                                        # print('days_in_month(charge_date)', days_in_month(
                                        #     charge_date))
                                        print(type(int(float(payment['charge']))))
                                        daily_charge_begin = int(
                                            int(float(payment['charge'])) / days_in_month(charge_date))
                                        print("daily charge", daily_charge_begin)
                                        num_days_active_begin = days_in_month(
                                            charge_date) - charge_date.day + 1
                                        charge = round(
                                            num_days_active_begin * daily_charge_begin, 2)
                                        charge_date = lease_start

                                    else:
                                        charge = int(payment['charge'])
                                        charge_date = (charge_date.replace(
                                            day=int(payment['due_by'])))
                                    purchaseResponse = newPurchase(
                                        linked_bill_id=None,
                                        pur_property_id=nl['lease_property_id'],
                                        payer=payment['lt_tenant_id'],
                                        receiver=res['result'][0]['property_owner_id'],
                                        pur_initiator=res['result'][0]['business_uid'],
                                        purchase_type='RENT',
                                        pur_cf_type='REVENUE',
                                        description=charge_month +
                                                    ' ' + payment['fee_name'],
                                        amount_due=charge,
                                        purchase_notes=charge_month,
                                        purchase_date=available_date.isoformat(),
                                        purchase_frequency=payment['frequency'],
                                        next_payment=due_date
                                    )
                                    for mpayment in managementPayments:
                                        weeks_current_month = len(
                                            calendar.monthcalendar(due_date.year, int(due_date.strftime("%m"))))
                                        #
                                        if mpayment['frequency'] == 'Weekly' and mpayment['fee_type'] == '%':
                                            if mpayment['of'] == 'Gross Rent':

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT RENT',
                                                    description=charge_month + ' ' +
                                                                payment['fee_name'],
                                                    amount_due=weeks_current_month * (charge *
                                                                                      (1 - mpayment['charge'] / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )
                                            # if net charge (listed charge-expenses)
                                            else:

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT RENT',
                                                    description=charge_month + ' ' +
                                                                payment['fee_name'],
                                                    amount_due=weeks_current_month * (
                                                                (charge - mpayment['expense_amount']) * (
                                                                1 - mpayment['charge'] / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )
                                        elif mpayment['frequency'] == 'Biweekly' and mpayment['fee_type'] == '%':

                                            # if gross charge (listed charge)
                                            if mpayment['of'] == 'Gross Rent':

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT RENT',
                                                    description=charge_month + ' ' +
                                                                payment['fee_name'],
                                                    amount_due=(weeks_current_month / 2) *
                                                               ((charge *
                                                                 (1 - mpayment['charge'] / 100))),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )
                                            # if net charge (listed charge-expenses)
                                            else:

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT RENT',
                                                    description=charge_month + ' ' +
                                                                payment['fee_name'],
                                                    amount_due=(weeks_current_month / 2) *
                                                               ((charge - mpayment['expense_amount']) * (
                                                                       1 - mpayment['charge'] / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )

                                        elif mpayment['frequency'] == 'Monthly' and mpayment['fee_type'] == '%':

                                            # if gross charge (listed charge)
                                            if mpayment['of'] == 'Gross Rent':
                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT RENT',
                                                    description=charge_month + ' ' +
                                                                payment['fee_name'],
                                                    amount_due=(
                                                            charge * (1 - int(mpayment['charge']) / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )

                                            # if net charge (listed charge-expenses)
                                            else:
                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT RENT',
                                                    description=charge_month + ' ' +
                                                                payment['fee_name'],
                                                    amount_due=(
                                                            (charge - mpayment['expense_amount']) * (
                                                                1 - int(mpayment['charge']) / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )

                                        else:
                                            print(
                                                'payment frequency one-time %')

                                elif (payment['fee_name'] == 'Deposit'):
                                    purchaseResponse = newPurchase(
                                        linked_bill_id=None,
                                        pur_property_id=nl['lease_property_id'],
                                        payer=payment['lt_tenant_id'],
                                        receiver=res['result'][0]['property_owner_id'],
                                        pur_initiator=res['result'][0]['business_uid'],
                                        pur_cf_type="REVENUE",
                                        purchase_type='DEPOSIT',
                                        description=payment['fee_name'],
                                        amount_due=payment['charge'],
                                        purchase_notes=charge_month,
                                        purchase_date=available_date.isoformat(),
                                        purchase_frequency=payment['frequency'],
                                        next_payment=due_date
                                    )
                                else:
                                    purchaseResponse = newPurchase(
                                        linked_bill_id=None,
                                        pur_property_id=nl['lease_property_id'],
                                        payer=payment['lt_tenant_id'],
                                        receiver=res['result'][0]['property_owner_id'],
                                        pur_initiator=res['result'][0]['business_uid'],
                                        pur_cf_type="REVENUE",
                                        purchase_type='EXTRA CHARGES',
                                        description=charge_month +
                                                    ' ' + payment['fee_name'],
                                        amount_due=payment['charge'],
                                        purchase_notes=charge_month,
                                        purchase_date=available_date.isoformat(),
                                        purchase_frequency=payment['frequency'],
                                        next_payment=due_date
                                    )
                                    # manager payments weekly $ charge
                                    for mpayment in managementPayments:
                                        weeks_current_month = len(
                                            calendar.monthcalendar(due_date.year, int(due_date.strftime("%m"))))

                                        if mpayment['frequency'] == 'Weekly' and mpayment['fee_type'] == '%':
                                            if mpayment['of'] == 'Gross Rent':

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                    description=payment['fee_name'],
                                                    amount_due=weeks_current_month * (charge *
                                                                                      (1 - mpayment['charge'] / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )
                                            # if net charge (listed charge-expenses)
                                            else:

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                    description=payment['fee_name'],
                                                    amount_due=weeks_current_month * (
                                                                (charge - mpayment['expense_amount']) * (
                                                                1 - mpayment['charge'] / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )
                                        elif mpayment['frequency'] == 'Biweekly' and mpayment['fee_type'] == '%':

                                            # if gross charge (listed charge)
                                            if mpayment['of'] == 'Gross Rent':

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                    description=payment['fee_name'],
                                                    amount_due=(weeks_current_month / 2) *
                                                               ((charge *
                                                                 (1 - mpayment['charge'] / 100))),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )
                                            # if net charge (listed charge-expenses)
                                            else:

                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                    description=payment['fee_name'],
                                                    amount_due=(weeks_current_month / 2) *
                                                               ((charge - mpayment['expense_amount']) * (
                                                                       1 - mpayment['charge'] / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )

                                        elif mpayment['frequency'] == 'Monthly' and mpayment['fee_type'] == '%':

                                            # if gross charge (listed charge)
                                            if mpayment['of'] == 'Gross Rent':
                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                    description=payment['fee_name'],
                                                    amount_due=(
                                                            charge * (1 - int(mpayment['charge']) / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )

                                            # if net charge (listed charge-expenses)
                                            else:
                                                purchaseResponse = newPurchase(
                                                    linked_bill_id=None,
                                                    pur_property_id=nl['lease_property_id'],
                                                    payer=res['result'][0]['property_owner_id'],
                                                    receiver=res['result'][0]['business_uid'],
                                                    pur_initiator="CRON JOB",
                                                    pur_cf_type="EXPENSE",
                                                    purchase_type='OWNER PAYMENT EXTRA CHARGES',
                                                    description=payment['fee_name'],
                                                    amount_due=(
                                                            (charge - mpayment['expense_amount']) * (
                                                                1 - int(mpayment['charge']) / 100)),
                                                    purchase_notes=charge_month,
                                                    purchase_date=available_date,
                                                    purchase_frequency=mpayment['frequency'],
                                                    next_payment=due_date
                                                )

                                        else:
                                            print(
                                                'payment frequency one-time %')

                                # charge_date += relativedelta(months=1)
                                due_date += relativedelta(months=1)
                                available_date += relativedelta(
                                    months=1)
                        else:
                            # print('lease_start', type(
                            #     res['result'][0]['lease_start']))

                            charge_date = date.fromisoformat(
                                nl['lease_start'])
                            due_date = date.fromisoformat(
                                nl['lease_start']).replace(
                                day=int(payment['due_by']))
                            lease_end = date.fromisoformat(
                                nl['lease_end'])
                            # available date-> when the payment is available to pay
                            if payment['available_topay'] == 0:
                                available_date = due_date
                            else:
                                available_date = due_date - \
                                                 timedelta(
                                                     days=int(payment['available_topay']))

                            charge_month = due_date.strftime(
                                '%B')
                            if (payment['fee_name'] == 'Rent'):
                                purchaseResponse = newPurchase(
                                    linked_bill_id=None,
                                    pur_property_id=nl['lease_property_id'],
                                    payer=payment['lt_tenant_id'],
                                    receiver=res['result'][0]['property_owner_id'],
                                    pur_cf_type="REVENUE",
                                    purchase_type='RENT',
                                    description=payment['fee_name'],
                                    amount_due=payment['charge'],
                                    purchase_notes=charge_month,
                                    purchase_date=available_date.isoformat(),
                                    purchase_frequency=payment['frequency'],
                                    next_payment=due_date
                                )
                            elif (payment['fee_name'] == 'Deposit'):
                                purchaseResponse = newPurchase(
                                    linked_bill_id=None,
                                    pur_property_id=nl['lease_property_id'],
                                    payer=payment['lt_tenant_id'],
                                    receiver=res['result'][0]['property_owner_id'],
                                    pur_cf_type="REVENUE",
                                    pur_initiator=res['result'][0]['business_uid'],
                                    purchase_type='DEPOSIT',
                                    description=payment['fee_name'],
                                    amount_due=payment['charge'],
                                    purchase_notes=charge_month,
                                    purchase_date=available_date.isoformat(),
                                    purchase_frequency=payment['frequency'],
                                    next_payment=due_date
                                )

                            else:

                                purchaseResponse = newPurchase(
                                    linked_bill_id=None,
                                    pur_property_id=nl['lease_property_id'],
                                    payer=payment['lt_tenant_id'],
                                    receiver=res['result'][0]['property_owner_id'],
                                    pur_cf_type="REVENUE",
                                    pur_initiator=res['result'][0]['business_uid'],
                                    purchase_type='EXTRA CHARGES',
                                    description=payment['fee_name'],
                                    amount_due=payment['charge'],
                                    purchase_notes=charge_month,
                                    purchase_date=available_date.isoformat(),
                                    purchase_frequency=payment['frequency'],
                                    next_payment=due_date
                                )
                    pkNL = {
                        'lease_uid': nl['lease_uid']
                    }
                    newLeaseUpdate = {
                        'lease_status': 'ACTIVE'
                    }
                    # print(pkNL, newLeaseUpdate)
                    response = db.update(
                        'leases', pkNL, newLeaseUpdate)
        return response


class MonthlyRent_CLASS(Resource):
    def get(self):
        print("In Monthly Rent")
        response = {}

        with connect() as db:
            print("in connect loop")
            monthlyRentQuery = db.execute(""" 
                -- BASIC QUERY FOR RENTS
                SELECT -- *,
                    leaseFees_uid, fees_lease_id, fee_name, fee_type, charge, due_by, late_by, late_fee, perDay_late_fee, frequency, available_topay
                    -- , of_DNU, lease_rent_old_DNU
                    , lease_uid, lease_property_id, lease_start, lease_end, lease_status
                    -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred
                    , lease_effective_date, lease_application_date
                    -- , linked_application_id-DNU, lease_docuSign
                    -- , lease_rent_available_topay, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lease_actual_rent
                FROM space.leaseFees
                LEFT JOIN space.leases ON fees_lease_id = lease_uid
                WHERE fee_name = "Rent" AND
                    DATEDIFF(LAST_DAY(CURDATE()), CURDATE()) < available_topay;
                    """)
            

            # print("Query: ", monthlyRentQuery)
            response['monthlyRents'] = monthlyRentQuery
            
            # Iterate over response result and add a Purchase entry to the Purchases Table
            if len(response['monthlyRents']['result']) > 0:
                for i in range(len(response['monthlyRents']['result'])):
                    # print(response['monthlyRents'])
                    # print(response['monthlyRents']['result'][i])
                    # print(response['monthlyRents']['result'][i]['lease_property_id'])
                    
                    # WRITE TO PURCHASE DB
                    newRequest = {}

                    # # GET NEW UID
                    newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                    print("New Purchase ID: ", newRequestID)
                    newRequest['purchase_uid'] = newRequestID

                    print(datetime.date.today())
                    newRequest['pur_timestamp'] = str(datetime.date.today())
                    print(newRequest['pur_timestamp'])
                    newRequest['pur_property_id'] = response['monthlyRents']['result'][i]['lease_property_id']
                    newRequest['purchase_type'] = "RENT"
                    newRequest['pur_cf_type'] = "REVENUE"
                    newRequest['purchase_date'] = str(datetime.date.today())
                    newRequest['pur_due_date'] = str(datetime.date.today()+ relativedelta(months=1, day=1))
                    # print("Date info complete")

                    #get the rent amount
                    newRequest['pur_amount_due'] = response['monthlyRents']['result'][i]['charge']
                    newRequest['purchase_status'] = "UNPAID"
                    newRequest['pur_notes'] = "RENT FOR NEXT MONTH"
                    newRequest['pur_description'] = "RENT FOR NEXT MONTH"
                    # print("Rent info complete")

                    #get property owner id
                    owner_id_st = db.select('property_owner',
                                            {'property_id': response['monthlyRents']['result'][i]['lease_property_id']})
                    owner_id = owner_id_st.get('result')[0]['property_owner_id']
                    newRequest['pur_receiver'] = owner_id
                    # print("Owner info complete")

                    #get property manager id
                    manager_id_st = db.select('b_details',
                                            {'contract_property_id': response['monthlyRents']['result'][i]['lease_property_id'],
                                            'business_type':"MANAGEMENT"})
                    manager_id = manager_id_st.get('result')[0]['business_user_id']
                    newRequest['pur_initiator'] = manager_id
                    # print("Manager info complete")

                    #get the tenant id
                    tenant_id_st = db.select('lease_tenant',
                                            {'lt_lease_id': response['monthlyRents']['result'][i]['fees_lease_id']})
                    tenant_id = tenant_id_st.get('result')[0]['lt_tenant_id']
                    newRequest['pur_payer'] = tenant_id
                    # print("Tenant info complete")

                    print(newRequest)
                    responsePurchase = db.insert('purchases', newRequest)
                    print(responsePurchase)

            return response

