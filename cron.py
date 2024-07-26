# import datetime

from flask_restful import Resource
from data_pm import connect
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import json
import calendar
# from calendar import monthrange
from decimal import Decimal



# class MonthlyRentPurchase_CLASS(Resource):
#     def get(self):
#         print("In Monthly Rent CRON JOB")

#         numCronPurchases = 0

#         # Establish current month and year
#         dt = datetime.today()
       
#         # Run query to find rents of ACTIVE leases
#         with connect() as db:    
#             response = db.execute("""
#                     -- CALCULATE RECURRING FEES
#                     SELECT leaseFees.*
#                         , lease_tenant.*
#                         , property_owner_id, po_owner_percent
#                         , contract_uid, contract_business_id, contract_status, contract_fees
#                         , lease_property_id, lease_start, lease_end, lease_status, lease_early_end_date, lease_renew_status
#                         -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, lease_move_in_date, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_consent, lease_actual_rent, lease_end_notice_period, lease_end_reason
#                     FROM space.leaseFees
#                     LEFT JOIN space.leases ON fees_lease_id = lease_uid
#                     LEFT JOIN space.lease_tenant ON fees_lease_id = lt_lease_id
#                     LEFT JOIN space.property_owner ON lease_property_id = property_id
#                     LEFT JOIN space.contracts ON lease_property_id = contract_property_id
#                     WHERE frequency != 'One-time' AND !ISNULL(frequency) AND frequency != ""
#                         AND lease_status IN ('ACTIVE', 'ACTIVE MTM', 'APPROVED') 
#                         AND contract_status = 'ACTIVE'
#                     ORDER BY frequency;
#                     """)

#             for i in range(len(response['result'])):
#                 print("\n",i, response['result'][i]['lease_property_id'], response['result'][i]['fees_lease_id'], response['result'][i]['leaseFees_uid'], response['result'][i]['contract_uid'], response['result'][i]['contract_business_id'])

            
#                 # Check Frequecy of Rent Payment.  Currently query only returns MONTHLY leases
#                 # rentFrequency = response['result'][i]['frequency']

#                 # print(response['result'][i]['frequency'])
#                 # if rentFrequency == "Weekly":
#                 #     print("Weekly Rent Fee")
#                 # elif rentFrequency == "Anually":
#                 #     print("Annual Rent Fee") 
#                 # elif rentFrequency == "Monthly" or rentFrequency is None:
#                 #     print("Monthly Rent Fee")
#                 # else: print("Investigate")


#                 # Check if available_topay is NONE
#                 if response['result'][i]['available_topay'] is None:
#                     # print("available_topay Is NULL!!")
#                     payable = 10
#                 else:
#                     payable = response['result'][i]['available_topay']
#                 # print("available_topay: ", payable)


#                 # Check if due_by is NONE
#                 # print(response['result'][i]['due_by'])
#                 if response['result'][i]['due_by'] is None or response['result'][i]['due_by'] == 0:
#                     # print("due_by Is NULL!!")
#                     due_by = 1
#                 else:
#                     due_by = response['result'][i]['due_by']
#                 # print("due_by: ", due_by, type(due_by))
#                 # print("dt.day: ", dt.day, type(dt.day))


#                 # Calculate Actual Rent due date
#                 if due_by < dt.day:
#                     # print(due_by, " < ", dt.day)
#                     due_date = datetime(dt.year, dt.month, due_by) + relativedelta(months=1)
#                 else:
#                     due_date = datetime(dt.year, dt.month, due_by)
#                 # print("due date: ", due_date,  type(due_date))
#                 pm_due_date = due_date + relativedelta(days=10)
#                 # print("PM due date: ", pm_due_date,  type(pm_due_date))

                
#                 # Calculate number of days until rent is due
#                 if response['result'][i]['frequency'] == 'Monthly':
#                     days_for_rent = (due_date - dt).days
#                     print("Rent due in : ", days_for_rent, " days", type(days_for_rent))
#                     print("Rent Posts in: ", days_for_rent - payable , " days", type(payable))
#                 elif response['result'][i]['frequency'] == 'Weekly':
#                     print("Weekly")
#                     days_for_rent = 500
                    
#                 elif response['result'][i]['frequency'] == 'Bi-Weekly':
#                     print("Bi-Weekly")
#                     days_for_rent = 500
                    
#                 elif response['result'][i]['frequency'] == 'Annually':
#                     print("Annually ", response['result'][i]['due_by_date'], type(response['result'][i]['due_by_date']) )
#                     days_for_rent = 500






#                 # CHECK IF RENT IS AVAILABLE TO PAY  ==> IF IT IS, ADD PURCHASES FOR TENANT TO PM AND PM TO OWNER
#                 if days_for_rent == payable + (0):  # Remove/Change number to get query to run and return data

#                 # IF Changing the dates manually
#                 # if days_for_rent >= 0:  # Remove/Change number to get query to run and return data
#                 #     due_date = datetime(2024, 1, due_by)            # Comment this out since due_date is set above
#                 #     pm_due_date = due_date + relativedelta(days=10) # Comment this out since due_date is set above


#                     print("Rent posted.  Please Pay")
#                     numCronPurchases = numCronPurchases + 1
#                     # print(i, response['result'][i])           


#                     # Perform Remainder Checks to ensure no blank fields
#                     # Check if late_fee is NONE
#                     # print(response['result'][i]['fee_name'], response['result'][i]['late_fee'], type(response['result'][i]['late_fee']))
#                     if response['result'][i]['late_fee'] is None or response['result'][i]['late_fee'] == 0 or response['result'][i]['late_fee'] == "":
#                         # print("Is NULL!!")
#                         late_fee = 0
#                     else:
#                         late_fee = response['result'][i]['late_fee']
#                     # print("late_fee: ", late_fee, type(late_fee))
                        
#                     # Check if perDay_late_fee is NONE
#                     # print(response['result'][i]['perDay_late_fee'])
#                     if response['result'][i]['perDay_late_fee'] is None or response['result'][i]['perDay_late_fee'] == 0:
#                         # print("Is NULL!!")
#                         perDay_late_fee = 0
#                     else:
#                         perDay_late_fee = response['result'][i]['perDay_late_fee']
#                     # print("perDay_late_fee: ", perDay_late_fee, type(perDay_late_fee))

#                     # Check if late_by is NONE
#                     # print(response['result'][i]['late_by'])
#                     if response['result'][i]['late_by'] is None or response['result'][i]['late_by'] == 0:
#                         # print("Is NULL!!")
#                         late_by = 1
#                     else:
#                         late_by = response['result'][i]['late_by']
#                     # print("late_by: ", late_by, type(late_by))

                    

#                     # Check if tenant responsiblity is NONE
#                     # print("What is in the db: ", response['result'][i]['lt_responsibility'])
#                     if response['result'][i]['lt_responsibility'] is None:
#                         # print("Is NULL!!")
#                         responsible_percent = 1.0
#                     else:
#                         responsible_percent = response['result'][i]['lt_responsibility']
#                     # print("What we set programmatically: ", responsible_percent, type(responsible_percent))
#                     charge = response['result'][i]['charge']
#                     # print("Charge: ", charge, type(charge))
#                     amt_due = float(charge)  * responsible_percent
#                     # print("Amount due: ", amt_due)


#                     # Establish payer, initiator and receiver
#                     contract_uid = response['result'][i]['contract_uid']
#                     property = response['result'][i]['lease_property_id']
#                     tenant = response['result'][i]['lt_tenant_id']
#                     owner = response['result'][i]['property_owner_id']
#                     manager = response['result'][i]['contract_business_id']
#                     fee_name = response['result'][i]['fee_name']
#                     # print("Purchase Parameters: ", i, contract_uid, tenant, owner, manager)


#                     # Common JSON Object Attributes
#                     newRequest = {}
                    
#                     newRequest['pur_timestamp'] = datetime.today().date().strftime("%m-%d-%Y")
#                     newRequest['pur_property_id'] = property
#                     newRequest['purchase_type'] = "Rent"
#                     newRequest['pur_cf_type'] = "revenue"
#                     newRequest['pur_amount_due'] = amt_due
#                     newRequest['purchase_status'] = "UNPAID"
#                     newRequest['pur_status_value'] = "0"
#                     newRequest['pur_notes'] = fee_name

#                     newRequest['pur_due_by'] = due_by
#                     newRequest['pur_late_by'] = late_by
#                     newRequest['pur_late_fee'] = late_fee
#                     newRequest['pur_perDay_late_fee'] = perDay_late_fee

#                     newRequest['purchase_date'] = datetime.today().date().strftime("%m-%d-%Y")
#                     newRequest['pur_description'] = f"Rent for {due_date.strftime('%B')} {due_date.year} CRON"

                    
#                     # Create JSON Object for Rent Purchase for Tenant-PM Payment
#                     newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
#                     grouping = newRequestID
#                     newRequest['purchase_uid'] = newRequestID
#                     newRequest['pur_group'] = grouping
#                     # print(newRequestID)
#                     newRequest['pur_receiver'] = manager
#                     newRequest['pur_payer'] = tenant
#                     newRequest['pur_initiator'] = manager
#                     newRequest['pur_due_date'] = due_date.date().strftime("%m-%d-%Y")
                    
                    
#                     # print(newRequest)
#                     # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
#                     db.insert('purchases', newRequest)



#                     # Create JSON Object for Rent Purchase for PM-Owner Payment
#                     newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
#                     newRequest['purchase_uid'] = newRequestID
#                     # print(newRequestID)
#                     newRequest['pur_receiver'] = owner
#                     newRequest['pur_payer'] = manager
#                     newRequest['pur_initiator'] = manager
#                     newRequest['pur_due_date'] = pm_due_date.date().strftime("%m-%d-%Y")
#                     newRequest['pur_group'] = grouping
                 
#                     # print(newRequest)
#                     # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
#                     db.insert('purchases', newRequest)




#                     # For each entry posted to the purchases table, post any contract fees based on Rent
#                     # Find contract fees based rent
#                     manager_fees = db.execute("""
#                                     SELECT -- *
#                                         contract_uid, contract_property_id, contract_business_id
#                                         -- , contract_start_date, contract_end_date
#                                         , contract_fees
#                                         -- , contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
#                                         , jt.*
#                                     FROM 
#                                         space.contracts,
#                                         JSON_TABLE(
#                                             contract_fees,
#                                             "$[*]" COLUMNS (
#                                                 of_column VARCHAR(50) PATH "$.of",
#                                                 charge_column VARCHAR(50) PATH "$.charge",
#                                                 fee_name_column VARCHAR(50) PATH "$.fee_name",
#                                                 fee_type_column VARCHAR(10) PATH "$.fee_type",
#                                                 frequency_column VARCHAR(20) PATH "$.frequency"
#                                             )
#                                         ) AS jt
#                                     -- WHERE contract_uid = '010-000003' AND of_column LIKE '%rent%';
#                                     WHERE contract_uid = \'""" + contract_uid + """\' AND of_column LIKE '%rent%';
#                                 """)
#                     # print(manager_fees)
                    

#                     for j in range(len(manager_fees['result'])):

#                         # Check if fees is monthly 
#                         if manager_fees['result'][j]['frequency_column'] == 'Monthly' or manager_fees['result'][j]['frequency_column'] == 'monthly':

#                             # Check if charge is a % or Fixed $ Amount
#                             if manager_fees['result'][j]['fee_type_column'] == '%' or manager_fees['result'][j]['fee_type_column'] == 'PERCENT':
#                                 charge_amt = Decimal(manager_fees['result'][j]['charge_column']) * Decimal(amt_due) / 100
#                             else:
#                                 charge_amt = Decimal(manager_fees['result'][j]['charge_column'])
#                             # print("Charge Amount: ", charge_amt, property, contract_uid, manager_fees['result'][j]['charge_column'], response['result'][i]['charge'] )

#                             # Create JSON Object for Fee Purchase
#                             newPMRequest = {}
#                             newPMRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
#                             # print(newPMRequestID)
#                             newPMRequest['purchase_uid'] = newPMRequestID
#                             newPMRequest['pur_timestamp'] = datetime.today().date().strftime("%m-%d-%Y")
#                             newPMRequest['pur_property_id'] = property
#                             newPMRequest['purchase_type'] = "Management"
#                             newPMRequest['pur_cf_type'] = "expense"
#                             newPMRequest['pur_amount_due'] = charge_amt
#                             newPMRequest['purchase_status'] = "UNPAID"
#                             newPMRequest['pur_status_value'] = "0"
#                             newPMRequest['pur_notes'] = manager_fees['result'][j]['fee_name_column']
#                             newPMRequest['pur_description'] =  f"{manager_fees['result'][j]['fee_name_column']} for {due_date.strftime('%B')} {due_date.year} "
#                             # newPMRequest['pur_description'] =  newRequestID # Original Rent Purchase ID  
#                             # newPMRequest['pur_description'] = f"Fees for MARCH {nextMonth.year} CRON"
#                             newPMRequest['pur_receiver'] = manager
#                             newPMRequest['pur_payer'] = owner
#                             newPMRequest['pur_initiator'] = manager
#                             newPMRequest['purchase_date'] = datetime.today().date().strftime("%m-%d-%Y")
#                             newPMRequest['pur_group'] = grouping

#                             # *********
#                             newPMRequest['pur_due_date'] = due_date.date().strftime("%m-%d-%Y")
#                             # newPMRequest['pur_due_date'] = datetime(nextMonth.year, nextMonth.month, due_by).date().strftime("%m-%d-%Y")
#                             # newPMRequest['pur_due_date'] = datetime(nextMonth.year, 1, due_by).date().strftime("%m-%d-%Y")
                            
#                             # print(newPMRequest)
#                             print("Number of CRON Purchases: ", numCronPurchases, dt)
#                             db.insert('purchases', newPMRequest)

#                             # For each fee, post to purchases table

#         response = {'message': f'Successfully completed CRON Job for {dt}' ,
#                     'rows affected': f'{numCronPurchases}',
#                 'code': 200}

#         return response

# def MonthlyRentPurchase_CRON(self):
#     print("In Monthly Rent CRON JOB")

#     numCronPurchases = 0

#     # Establish current month and year
#     dt = datetime.today()
    
#     # Run query to find rents of ACTIVE leases
#     with connect() as db:
#         response = db.execute("""
#                 -- CALCULATE RECURRING FEES
#                     SELECT leaseFees.*
#                         , lease_tenant.*
#                         , property_owner_id, po_owner_percent
#                         , contract_uid, contract_business_id, contract_status, contract_fees
#                         , lease_property_id, lease_start, lease_end, lease_status, lease_early_end_date, lease_renew_status
#                         -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, lease_move_in_date, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_consent, lease_actual_rent, lease_end_notice_period, lease_end_reason
#                     FROM space.leaseFees
#                     LEFT JOIN space.leases ON fees_lease_id = lease_uid
#                     LEFT JOIN space.lease_tenant ON fees_lease_id = lt_lease_id
#                     LEFT JOIN space.property_owner ON lease_property_id = property_id
#                     LEFT JOIN space.contracts ON lease_property_id = contract_property_id
#                     WHERE frequency != 'One-time' AND !ISNULL(frequency) AND frequency != ""
#                         AND lease_status IN ('ACTIVE', 'ACTIVE MTM', 'APPROVED') 
#                         AND contract_status = 'ACTIVE'
#                     ORDER BY frequency;
#                 """)

#         for i in range(len(response['result'])):
#             # print("\n",i, response['result'][i]['lease_property_id'], response['result'][i]['fees_lease_id'], response['result'][i]['leaseFees_uid'], response['result'][i]['contract_uid'], response['result'][i]['contract_business_id'])

        
#             # Check Frequecy of Rent Payment.  Currently query only returns MONTHLY leases
#             # rentFrequency = response['result'][i]['frequency']

#             # print(response['result'][i]['frequency'])
#             # if rentFrequency == "Weekly":
#             #     print("Weekly Rent Fee")
#             # elif rentFrequency == "Anually":
#             #     print("Annual Rent Fee") 
#             # elif rentFrequency == "Monthly" or rentFrequency is None:
#             #     print("Monthly Rent Fee")
#             # else: print("Investigate")


#             # Check if available_topay is NONE
#             if response['result'][i]['available_topay'] is None:
#                 # print("available_topay Is NULL!!")
#                 payable = 10
#             else:
#                 payable = response['result'][i]['available_topay']
#             # print("available_topay: ", payable)


#             # Check if due_by is NONE
#             # print(response['result'][i]['due_by'])
#             if response['result'][i]['due_by'] is None or response['result'][i]['due_by'] == 0:
#                 # print("due_by Is NULL!!")
#                 due_by = 1
#             else:
#                 due_by = response['result'][i]['due_by']
#             # print("due_by: ", due_by, type(due_by))
#             # print("dt.day: ", dt.day, type(dt.day))


#             # Calculate Actual Rent due date
#             if due_by < dt.day:
#                 # print(due_by, " < ", dt.day)
#                 due_date = datetime(dt.year, dt.month, due_by) + relativedelta(months=1)
#             else:
#                 due_date = datetime(dt.year, dt.month, due_by)
#             # print("due date: ", due_date,  type(due_date))
#             pm_due_date = due_date + relativedelta(days=10)
#             # print("PM due date: ", pm_due_date,  type(pm_due_date))

            
#             # Calculate number of days until rent is due
#             if response['result'][i]['frequency'] == 'Monthly':
#                 days_for_rent = (due_date - dt).days
#                 print("Rent due in : ", days_for_rent, " days", type(days_for_rent))
#                 print("Rent Posts in: ", days_for_rent - payable , " days", type(payable))
#             elif response['result'][i]['frequency'] == 'Weekly':
#                 print("Weekly")
#                 days_for_rent = 500
                
#             elif response['result'][i]['frequency'] == 'Bi-Weekly':
#                 print("Bi-Weekly")
#                 days_for_rent = 500
                
#             elif response['result'][i]['frequency'] == 'Annually':
#                 print("Annually ", response['result'][i]['due_by_date'], type(response['result'][i]['due_by_date']) )
#                 days_for_rent = 500






#             # CHECK IF RENT IS AVAILABLE TO PAY  ==> IF IT IS, ADD PURCHASES FOR TENANT TO PM AND PM TO OWNER
#             if days_for_rent == payable + (0):  # Remove/Change number to get query to run and return data

#             # IF Changing the dates manually
#             # if days_for_rent >= 0:  # Remove/Change number to get query to run and return data
#             #     due_date = datetime(2024, 1, due_by)            # Comment this out since due_date is set above
#             #     pm_due_date = due_date + relativedelta(days=10) # Comment this out since due_date is set above


#                 print("Rent posted.  Please Pay")
#                 numCronPurchases = numCronPurchases + 1
#                 # print(i, response['result'][i])           


#                 # Perform Remainder Checks to ensure no blank fields
#                 # Check if late_fee is NONE
#                 # print(response['result'][i]['fee_name'], response['result'][i]['late_fee'], type(response['result'][i]['late_fee']))
#                 if response['result'][i]['late_fee'] is None or response['result'][i]['late_fee'] == 0 or response['result'][i]['late_fee'] == "":
#                     # print("Is NULL!!")
#                     late_fee = 0
#                 else:
#                     late_fee = response['result'][i]['late_fee']
#                 # print("late_fee: ", late_fee, type(late_fee))
                    
#                 # Check if perDay_late_fee is NONE
#                 # print(response['result'][i]['perDay_late_fee'])
#                 if response['result'][i]['perDay_late_fee'] is None or response['result'][i]['perDay_late_fee'] == 0:
#                     # print("Is NULL!!")
#                     perDay_late_fee = 0
#                 else:
#                     perDay_late_fee = response['result'][i]['perDay_late_fee']
#                 # print("perDay_late_fee: ", perDay_late_fee, type(perDay_late_fee))

#                 # Check if late_by is NONE
#                 # print(response['result'][i]['late_by'])
#                 if response['result'][i]['late_by'] is None or response['result'][i]['late_by'] == 0:
#                     # print("Is NULL!!")
#                     late_by = 1
#                 else:
#                     late_by = response['result'][i]['late_by']
#                 # print("late_by: ", late_by, type(late_by))

                

#                 # Check if tenant responsiblity is NONE
#                 # print("What is in the db: ", response['result'][i]['lt_responsibility'])
#                 if response['result'][i]['lt_responsibility'] is None:
#                     # print("Is NULL!!")
#                     responsible_percent = 1.0
#                 else:
#                     responsible_percent = response['result'][i]['lt_responsibility']
#                 # print("What we set programmatically: ", responsible_percent, type(responsible_percent))
#                 charge = response['result'][i]['charge']
#                 # print("Charge: ", charge, type(charge))
#                 amt_due = float(charge)  * responsible_percent
#                 # print("Amount due: ", amt_due)


#                 # Establish payer, initiator and receiver
#                 contract_uid = response['result'][i]['contract_uid']
#                 property = response['result'][i]['lease_property_id']
#                 tenant = response['result'][i]['lt_tenant_id']
#                 owner = response['result'][i]['property_owner_id']
#                 manager = response['result'][i]['contract_business_id']
#                 fee_name = response['result'][i]['fee_name']
#                 # print("Purchase Parameters: ", i, contract_uid, tenant, owner, manager)


#                 # Common JSON Object Attributes
#                 newRequest = {}
                
#                 newRequest['pur_timestamp'] = datetime.today().date().strftime("%m-%d-%Y")
#                 newRequest['pur_property_id'] = property
#                 newRequest['purchase_type'] = "Rent"
#                 newRequest['pur_cf_type'] = "revenue"
#                 newRequest['pur_amount_due'] = amt_due
#                 newRequest['purchase_status'] = "UNPAID"
#                 newRequest['pur_status_value'] = "0"
#                 newRequest['pur_notes'] = fee_name

#                 newRequest['pur_due_by'] = due_by
#                 newRequest['pur_late_by'] = late_by
#                 newRequest['pur_late_fee'] = late_fee
#                 newRequest['pur_perDay_late_fee'] = perDay_late_fee

#                 newRequest['purchase_date'] = datetime.today().date().strftime("%m-%d-%Y")
#                 newRequest['pur_description'] = f"Rent for {due_date.strftime('%B')} {due_date.year} CRON"

                
#                 # Create JSON Object for Rent Purchase for Tenant-PM Payment
#                 newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
#                 grouping = newRequestID
#                 newRequest['purchase_uid'] = newRequestID
#                 # print(newRequestID)
#                 newRequest['pur_receiver'] = manager
#                 newRequest['pur_payer'] = tenant
#                 newRequest['pur_initiator'] = manager
#                 newRequest['pur_due_date'] = due_date.date().strftime("%m-%d-%Y")
#                 newRequest['pur_group'] = grouping
                
#                 # print(newRequest)
#                 # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
#                 db.insert('purchases', newRequest)



#                 # Create JSON Object for Rent Purchase for PM-Owner Payment
#                 newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
#                 newRequest['purchase_uid'] = newRequestID
#                 # print(newRequestID)
#                 newRequest['pur_receiver'] = owner
#                 newRequest['pur_payer'] = manager
#                 newRequest['pur_initiator'] = manager
#                 newRequest['pur_due_date'] = pm_due_date.date().strftime("%m-%d-%Y")
#                 newRequest['pur_group'] = grouping
                
#                 # print(newRequest)
#                 # print("Purchase Parameters: ", i, newRequestID, property, contract_uid, tenant, owner, manager)
#                 db.insert('purchases', newRequest)




#                 # For each entry posted to the purchases table, post any contract fees based on Rent
#                 # Find contract fees based rent
#                 manager_fees = db.execute("""
#                                 SELECT -- *
#                                     contract_uid, contract_property_id, contract_business_id
#                                     -- , contract_start_date, contract_end_date
#                                     , contract_fees
#                                     -- , contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
#                                     , jt.*
#                                 FROM 
#                                     space.contracts,
#                                     JSON_TABLE(
#                                         contract_fees,
#                                         "$[*]" COLUMNS (
#                                             of_column VARCHAR(50) PATH "$.of",
#                                             charge_column VARCHAR(50) PATH "$.charge",
#                                             fee_name_column VARCHAR(50) PATH "$.fee_name",
#                                             fee_type_column VARCHAR(10) PATH "$.fee_type",
#                                             frequency_column VARCHAR(20) PATH "$.frequency"
#                                         )
#                                     ) AS jt
#                                 -- WHERE contract_uid = '010-000003' AND of_column LIKE '%rent%';
#                                 WHERE contract_uid = \'""" + contract_uid + """\' AND of_column LIKE '%rent%';
#                             """)
#                 # print(manager_fees)
                

#                 for j in range(len(manager_fees['result'])):

#                     # Check if fees is monthly 
#                     if manager_fees['result'][j]['frequency_column'] == 'Monthly' or manager_fees['result'][j]['frequency_column'] == 'monthly':

#                         # Check if charge is a % or Fixed $ Amount
#                         if manager_fees['result'][j]['fee_type_column'] == '%' or manager_fees['result'][j]['fee_type_column'] == 'PERCENT':
#                             charge_amt = Decimal(manager_fees['result'][j]['charge_column']) * Decimal(amt_due) / 100
#                         else:
#                             charge_amt = Decimal(manager_fees['result'][j]['charge_column'])
#                         # print("Charge Amount: ", charge_amt, property, contract_uid, manager_fees['result'][j]['charge_column'], response['result'][i]['charge'] )

#                         # Create JSON Object for Fee Purchase
#                         newPMRequest = {}
#                         newPMRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
#                         # print(newPMRequestID)
#                         newPMRequest['purchase_uid'] = newPMRequestID
#                         newPMRequest['pur_timestamp'] = datetime.today().date().strftime("%m-%d-%Y")
#                         newPMRequest['pur_property_id'] = property
#                         newPMRequest['purchase_type'] = "Management"
#                         newPMRequest['pur_cf_type'] = "expense"
#                         newPMRequest['pur_amount_due'] = charge_amt
#                         newPMRequest['purchase_status'] = "UNPAID"
#                         newPMRequest['pur_status_value'] = "0"
#                         newPMRequest['pur_notes'] = manager_fees['result'][j]['fee_name_column']
#                         newPMRequest['pur_description'] =  f"{manager_fees['result'][j]['fee_name_column']} for {due_date.strftime('%B')} {due_date.year} "
#                         # newPMRequest['pur_description'] =  newRequestID # Original Rent Purchase ID  
#                         # newPMRequest['pur_description'] = f"Fees for MARCH {nextMonth.year} CRON"
#                         newPMRequest['pur_receiver'] = manager
#                         newPMRequest['pur_payer'] = owner
#                         newPMRequest['pur_initiator'] = manager
#                         newPMRequest['purchase_date'] = datetime.today().date().strftime("%m-%d-%Y")
#                         newPMRequest['pur_group'] = grouping

#                         # *********
#                         newPMRequest['pur_due_date'] = due_date.date().strftime("%m-%d-%Y")
#                         # newPMRequest['pur_due_date'] = datetime(nextMonth.year, nextMonth.month, due_by).date().strftime("%m-%d-%Y")
#                         # newPMRequest['pur_due_date'] = datetime(nextMonth.year, 1, due_by).date().strftime("%m-%d-%Y")
                        
#                         # print(newPMRequest)
#                         print("Number of CRON Purchases: ", numCronPurchases, dt)
#                         db.insert('purchases', newPMRequest)

#                         # For each fee, post to purchases table

#     response = {'message': f'Successfully completed CRON Job for {dt}' ,
#                 'rows affected': f'{numCronPurchases}',
#             'code': 200}

#     return response

# class LateFees_CLASS(Resource):
#     def get(self):
#         print("In Late Fees")

#         numCronPurchases = 0
#         numCronUpdates = 0

#         # Establish current day, month and year
#         dt = date.today()
#         month = dt.month
#         year = dt.year
#         print(dt, type(dt), month, type(month), year, type(year))

#         # FIND ALL Rents that are UNPAID OR PARTIALLY PAID
#         with connect() as db:
#             response = db.execute("""
#                 -- DETERMINE WHICH RENTS ARE PAID OR PARTIALLY PAID
#                 SELECT *
#                 FROM space.purchases
#                 LEFT JOIN space.contracts ON contract_property_id = pur_property_id
#                 LEFT JOIN space.property_owner ON property_id = pur_property_id
#                 WHERE purchase_type = "RENT" AND
#                     contract_status = "ACTIVE" AND
#                     (purchase_status = "UNPAID" OR purchase_status = "PARTIALLY PAID") AND 
#                     SUBSTRING(pur_payer, 1, 3) = '350';
#                 """)

#         # EXTRACT KEY DATES FOR EACH UNPAID RENT
#             for i in range(len(response['result'])):
#                 purchase_uid = response['result'][i]['purchase_uid']
#                 property_id = response['result'][i]['pur_property_id']
#                 description = response['result'][i]['pur_description']
#                 # print("\nNext Row: ", i, purchase_uid, property_id, description)

#                 # PAYMENT DATES
#                 # Set Due Date - If None set to due on 1st day of the Month
#                 due_by_str = response['result'][i]['pur_due_date'] if response['result'][i]['pur_due_date'] else "1"
#                 due_by = datetime.strptime(due_by_str, "%m-%d-%Y").date()
#                 # print("Due by: ", due_by, type(due_by))

#                 # Set Late By Date - If None set to late after 1 day
#                 late_by = int(response['result'][i]['pur_late_by'] if response['result'][i]['pur_late_by'] else 1)
#                 # print("Late by: ", late_by, type(late_by))
#                 late_date = due_by + timedelta(days=late_by)
#                 # print("Late Date: ", late_date, type(late_date))

#                 # Set Previous Day for Late Fee Calculations
#                 yesterday = dt - timedelta(days=1) 
#                 # print("previous_day: ", yesterday, type(yesterday) )

#                 # Number of Days Late
#                 numDays = (yesterday - late_date).days
#                 # print("Number of Days Late: ", numDays, type(numDays))

#                 # Set Date for PM to Pay Late Fees to Owner
#                 pm_due_date = dt + timedelta(days=30)
#                 # print("pm_due_date: ", pm_due_date, type(pm_due_date) )

#                 # print("Due, Late, Yesterday, NumDays, PM_Due: ", due_by, late_date, yesterday, numDays, pm_due_date)


#         # DETERMINE IF UNPAID RENT IS LATE
#                 if late_date < dt:
#                     # print("Rent is late!")


#         # EXTRACT KEY PARAMETERS FOR EACH UNPAID RENT
#                     # PAYMENT PARAMTERS
#                     rent_due = response['result'][i]['pur_amount_due']
#                     one_time_late_fee = response['result'][i]['pur_late_Fee']
#                     per_day_late_fee = response['result'][i]['pur_perDay_late_fee']
#                     purchase_notes = response['result'][i]['pur_notes']
#                     purchase_description = response['result'][i]['pur_description']
#                     fees = json.loads(response['result'][i]['contract_fees'])
#                     # PAYMENT PARTIES
#                     tenant = response['result'][i]['pur_payer']
#                     owner = response['result'][i]['property_owner_id']
#                     manager = response['result'][i]['contract_business_id']
                
#                     # print("\nPurchase UID: ", purchase_uid, type(purchase_uid))
#                     # print("Property id: ", property_id, type(property_id) )
#                     # print("Payment Description: ", description, type(description))
#                     # print("Payment Amount Due: ", rent_due, type(rent_due))
#                     # print("Lease Late Fees: ", one_time_late_fee, type(one_time_late_fee), per_day_late_fee, type(per_day_late_fee))
#                     # print("Purchase Notes: ", purchase_notes, purchase_description)
#                     # print("PM Contract Fees: ", fees, type(fees))
#                     # print("Tenant, Owner, PM: ", tenant, owner, manager, type(manager))

#         # CALCULATE THE LATE FEE AMOUNT
#                     late_fee = round(float(one_time_late_fee) + float(per_day_late_fee) * numDays, 2)
#                     # print("Late Fee: ", late_fee, type(late_fee))


#         # FIND ALL ROWS THAT ALREADY EXIST FOR THIS LATE FEE (IE DESCRIPTION MATCHES PURCHASE_ID)
#                     # Run Query to get all late fees
#                     lateFees = db.execute("""
#                             -- DETERMINE WHICH LATE FEES ALREADY EXIST
#                             SELECT *
#                             FROM space.purchases    
#                             WHERE purchase_type = "LATE FEE" AND
#                                 (purchase_status = "UNPAID" OR purchase_status = "PARTIALLY PAID")
#                             """)
#                     # print("\n",lateFees['result'][0:11], type(lateFees))


#         # UPDATE APPRORIATE ROWS
#                     putFlag = 0
#                     if len(lateFees['result']) > 0 and late_fee > 0:
#                         for j in range(len(lateFees['result'])):
#                             # print(lateFees['result'][j]['pur_description'])
#                             if  purchase_uid == lateFees['result'][j]['pur_description']:
#                                 putFlag = putFlag + 1
#                                 # print("\nFound Matching Entry ", putFlag, lateFees['result'][j]['pur_notes'])
#                                 # print("Entire Row: ", lateFees['result'][j])
#                                 payer = lateFees['result'][j]['pur_payer']
#                                 receiver = lateFees['result'][j]['pur_receiver']
#                                 key = {'purchase_uid': lateFees['result'][j]['purchase_uid']}
#                                 if payer[0:3] == '350' or payer[0:3] == '600':
#                                     payload = {'pur_amount_due': late_fee}
#                                     # print(key, payload)

#                                     response['purchase_table_update'] = db.update('space.purchases', key, payload)
#                                     # print("updated ", key, payload)
#                                     numCronUpdates = numCronUpdates + 1
#                                     # print(response)
#                                 elif payer[0:3] == '110':                                   
#                                     # print("Figure out what the appropriate Fee split is", purchase_notes )
#                                     for fee in fees:
#                                         # Extract only the monthly fees
#                                         if 'fee_type' in fee and (fee['frequency'] == 'Monthly' or fee['frequency'] == 'monthly') and fee['charge'] != "" and (fee['fee_type'] == "%" or fee['fee_type'] == "PERCENT") and fee['fee_name'] == lateFees['result'][j]['pur_notes']:
#                                             charge = fee['charge']
#                                             charge_type = fee['fee_type']
#                                             # print("\nCharge: ", charge, charge_type)

#                                             amount_due = float(late_fee) * float(charge) / 100
#                                             payload = {'pur_amount_due': amount_due}
#                                             # print(key, payload)

#                                             response['purchase_table_update'] = db.update('space.purchases', key, payload)
#                                             numCronUpdates = numCronUpdates + 1
#                                             # print("Updated PM", key, payload)
#                                 else:
#                                     print("No Match Found: ", payer)
                         
#                             continue
#         # INSERT NEW ROWS IF THIS IS THE FIRST TIME LATE FEES ARE ASSESSED
                    
#                     if putFlag == 0 and late_fee > 0:
#                         print("PUT Flag: ", putFlag, "New Late Fee for: ", i, purchase_uid)

#                         # Create JSON Object for Rent Purchase
#                         newRequest = {}
#                         newRequestID = db.call('space.new_purchase_uid')['result'][0]['new_id']
#                         grouping = newRequestID
#                         # print(newRequestID)

#                         # Common JSON Object Attributes
#                         newRequest['purchase_uid'] = newRequestID
#                         newRequest['pur_group'] = grouping
#                         # newRequest['pur_timestamp'] = datetime.today().date().strftime("%m-%d-%Y")
#                         newRequest['pur_timestamp'] = dt.strftime("%m-%d-%Y")
#                         newRequest['pur_property_id'] = property_id
#                         newRequest['purchase_type'] = "Late Fee"
#                         newRequest['pur_cf_type'] = "revenue"
#                         newRequest['pur_amount_due'] = late_fee
#                         newRequest['purchase_status'] = "UNPAID"
#                         newRequest['pur_status_value'] = "0"
                        

#                         newRequest['pur_due_by'] = 1
#                         newRequest['pur_late_by'] = 90
#                         newRequest['pur_late_fee'] = 0
#                         newRequest['pur_perDay_late_fee'] = 0

#                         # newRequest['purchase_date'] = datetime.today().date().strftime("%m-%d-%Y")
#                         newRequest['purchase_date'] = dt.strftime("%m-%d-%Y")
#                         newRequest['pur_description'] = purchase_uid
                        
                    
#                         # Create JSON Object for Rent Purchase for Tenant-PM Payment
#                         newRequest['pur_receiver'] = manager
#                         newRequest['pur_payer'] = tenant
#                         newRequest['pur_initiator'] = manager
#                         # newRequest['pur_due_date'] = datetime.today().date().strftime("%m-%d-%Y")
#                         newRequest['pur_due_date'] = dt.strftime("%m-%d-%Y")
                        

#                         if numDays ==  0:
#                             # print("\n", "Late Today")
#                             newRequest['pur_notes'] = "One Time Late Fee Applied"
#                         else:
#                             newRequest['pur_notes'] = "One Time Late Fee and Per Day Late Fee Applied"
#                             # newRequest['pur_notes'] = f"Late for { calendar.month_name[nextMonth.month]} {nextMonth.year} {response['result'][i]['purchase_uid']}"

#                         # print("\nInsert Tenant to Property Manager Late Fee")
#                         db.insert('space.purchases', newRequest)
#                         numCronPurchases = numCronPurchases + 1
#                         # print("Inserted into db: ", newRequest)


#                         # Create JSON Object for Rent Purchase for PM-Owner Payment
#                         newRequestID = db.call('space.new_purchase_uid')['result'][0]['new_id']
#                         newRequest['purchase_uid'] = newRequestID
#                         # print(newRequestID)
#                         newRequest['pur_receiver'] = owner
#                         newRequest['pur_payer'] = manager

                            
#                         # print(newRequest)
#                         # print("\nPurchase Parameters: ", i, newRequestID, grouping, tenant, owner, manager)
#                         # print("\nInsert Property Manager to Owner Late Fee")
#                         db.insert('space.purchases', newRequest)
#                         numCronPurchases = numCronPurchases + 1

                        
#                         # Create JSON Object for Rent Purchase for Owner-PM Payment
#                         # Determine Split between PM and Owner
#                         # print("\n  Contract Fees", response['result'][i]['contract_fees'])
#                         fees = json.loads(response['result'][i]['contract_fees'])
#                         # print("\nFees: ", fees, type(fees))

#                         for fee in fees:
#                             # print(fee)
#                             # Extract only the monthly fees
#                             if 'fee_type' in fee and (fee['frequency'] == 'Monthly' or fee['frequency'] == 'monthly') and fee['charge'] != "" and (fee['fee_type'] == "%" or fee['fee_type'] == "PERCENT"):
#                                 charge = fee['charge']
#                                 charge_type = fee['fee_type']
#                                 # print("\nCharge: ", charge, charge_type)

#                                 # Use this fee to create an Owner-PM late Fee PUT OR PST 
#                                 # Create JSON Object for Rent Purchase for PM-Owner Payment
#                                 newRequestID = db.call('space.new_purchase_uid')['result'][0]['new_id']
#                                 newRequest['purchase_uid'] = newRequestID
#                                 # print(newRequestID)
#                                 newRequest['pur_receiver'] = manager
#                                 newRequest['pur_payer'] = owner
#                                 newRequest['pur_cf_type'] = "expense"
#                                 newRequest['purchase_type'] = "Management - Late Fees"
#                                 newRequest['pur_notes'] = fee['fee_name']
#                                 newRequest['pur_amount_due'] = float(late_fee) * float(charge) / 100
                                
#                                 # print(newRequest)
#                                 # print("Purchase Parameters: ", i, newRequestID, grouping, tenant, owner, manager)
#                                 # print("\nInsert Owner to Property Manager Late Fee")
#                                 db.insert('space.purchases', newRequest)  
#                                 numCronPurchases = numCronPurchases + 1     


#         print(f"Late Fee CRON job for {dt} completed. {numCronPurchases} rows added. {numCronUpdates} rows updated.")
#         response = {'message': f'Successfully completed CRON Job for {dt}' ,
#                     'rows added': f'{numCronPurchases}', 'rows updated': f'{numCronUpdates}',
#                 'code': 200}

#         return response

# def LateFees_CRON(self):
#         print("In Late Fees")

#         numCronPurchases = 0
#         numCronUpdates = 0

#         # Establish current day, month and year
#         dt = date.today()
#         month = dt.month
#         year = dt.year
#         print(dt, type(dt), month, type(month), year, type(year))

#         # FIND ALL Rents that are UNPAID OR PARTIALLY PAID
#         with connect() as db:
#             response = db.execute("""
#                 -- DETERMINE WHICH RENTS ARE PAID OR PARTIALLY PAID
#                 SELECT *
#                 FROM space.purchases
#                 LEFT JOIN space.contracts ON contract_property_id = pur_property_id
#                 LEFT JOIN space.property_owner ON property_id = pur_property_id
#                 WHERE purchase_type = "RENT" AND
#                     contract_status = "ACTIVE" AND
#                     (purchase_status = "UNPAID" OR purchase_status = "PARTIALLY PAID") AND 
#                     SUBSTRING(pur_payer, 1, 3) = '350';
#                 """)

#         # EXTRACT KEY DATES FOR EACH UNPAID RENT
#             for i in range(len(response['result'])):
#                 purchase_uid = response['result'][i]['purchase_uid']
#                 property_id = response['result'][i]['pur_property_id']
#                 description = response['result'][i]['pur_description']
#                 # print("\nNext Row: ", i, purchase_uid, property_id, description)

#                 # PAYMENT DATES
#                 # Set Due Date - If None set to due on 1st day of the Month
#                 due_by_str = response['result'][i]['pur_due_date'] if response['result'][i]['pur_due_date'] else "1"
#                 due_by = datetime.strptime(due_by_str, "%m-%d-%Y").date()
#                 # print("Due by: ", due_by, type(due_by))

#                 # Set Late By Date - If None set to late after 1 day
#                 late_by = int(response['result'][i]['pur_late_by'] if response['result'][i]['pur_late_by'] else 1)
#                 # print("Late by: ", late_by, type(late_by))
#                 late_date = due_by + timedelta(days=late_by)
#                 # print("Late Date: ", late_date, type(late_date))

#                 # Set Previous Day for Late Fee Calculations
#                 yesterday = dt - timedelta(days=1) 
#                 # print("previous_day: ", yesterday, type(yesterday) )

#                 # Number of Days Late
#                 numDays = (yesterday - late_date).days
#                 # print("Number of Days Late: ", numDays, type(numDays))

#                 # Set Date for PM to Pay Late Fees to Owner
#                 pm_due_date = dt + timedelta(days=30)
#                 # print("pm_due_date: ", pm_due_date, type(pm_due_date) )

#                 # print("Due, Late, Yesterday, NumDays, PM_Due: ", due_by, late_date, yesterday, numDays, pm_due_date)


#         # DETERMINE IF UNPAID RENT IS LATE
#                 if late_date < dt:
#                     # print("Rent is late!")


#         # EXTRACT KEY PARAMETERS FOR EACH UNPAID RENT
#                     # PAYMENT PARAMTERS
#                     rent_due = response['result'][i]['pur_amount_due']
#                     one_time_late_fee = response['result'][i]['pur_late_Fee']
#                     per_day_late_fee = response['result'][i]['pur_perDay_late_fee']
#                     purchase_notes = response['result'][i]['pur_notes']
#                     purchase_description = response['result'][i]['pur_description']
#                     fees = json.loads(response['result'][i]['contract_fees'])
#                     # PAYMENT PARTIES
#                     tenant = response['result'][i]['pur_payer']
#                     owner = response['result'][i]['property_owner_id']
#                     manager = response['result'][i]['contract_business_id']
                
#                     # print("\nPurchase UID: ", purchase_uid, type(purchase_uid))
#                     # print("Property id: ", property_id, type(property_id) )
#                     # print("Payment Description: ", description, type(description))
#                     # print("Payment Amount Due: ", rent_due, type(rent_due))
#                     # print("Lease Late Fees: ", one_time_late_fee, type(one_time_late_fee), per_day_late_fee, type(per_day_late_fee))
#                     # print("Purchase Notes: ", purchase_notes, purchase_description)
#                     # print("PM Contract Fees: ", fees, type(fees))
#                     # print("Tenant, Owner, PM: ", tenant, owner, manager, type(manager))

#         # CALCULATE THE LATE FEE AMOUNT
#                     late_fee = round(float(one_time_late_fee) + float(per_day_late_fee) * numDays, 2)
#                     # print("Late Fee: ", late_fee, type(late_fee))


#         # FIND ALL ROWS THAT ALREADY EXIST FOR THIS LATE FEE (IE DESCRIPTION MATCHES PURCHASE_ID)
#                     # Run Query to get all late fees
#                     lateFees = db.execute("""
#                             -- DETERMINE WHICH LATE FEES ALREADY EXIST
#                             SELECT *
#                             FROM space.purchases    
#                             WHERE purchase_type = "LATE FEE" AND
#                                 (purchase_status = "UNPAID" OR purchase_status = "PARTIALLY PAID")
#                             """)
#                     # print("\n",lateFees['result'][0:11], type(lateFees))


#         # UPDATE APPRORIATE ROWS
#                     putFlag = 0
#                     if len(lateFees['result']) > 0 and late_fee > 0:
#                         for j in range(len(lateFees['result'])):
#                             # print(lateFees['result'][j]['pur_description'])
#                             if  purchase_uid == lateFees['result'][j]['pur_description']:
#                                 putFlag = putFlag + 1
#                                 # print("\nFound Matching Entry ", putFlag, lateFees['result'][j]['pur_notes'])
#                                 # print("Entire Row: ", lateFees['result'][j])
#                                 payer = lateFees['result'][j]['pur_payer']
#                                 receiver = lateFees['result'][j]['pur_receiver']
#                                 key = {'purchase_uid': lateFees['result'][j]['purchase_uid']}
#                                 if payer[0:3] == '350' or payer[0:3] == '600':
#                                     payload = {'pur_amount_due': late_fee}
#                                     # print(key, payload)

#                                     response['purchase_table_update'] = db.update('space.purchases', key, payload)
#                                     # print("updated ", key, payload)
#                                     numCronUpdates = numCronUpdates + 1
#                                     # print(response)
#                                 elif payer[0:3] == '110':                                   
#                                     # print("Figure out what the appropriate Fee split is", purchase_notes )
#                                     for fee in fees:
#                                         # Extract only the monthly fees
#                                         if 'fee_type' in fee and (fee['frequency'] == 'Monthly' or fee['frequency'] == 'monthly') and fee['charge'] != "" and (fee['fee_type'] == "%" or fee['fee_type'] == "PERCENT") and fee['fee_name'] == lateFees['result'][j]['pur_notes']:
#                                             charge = fee['charge']
#                                             charge_type = fee['fee_type']
#                                             # print("\nCharge: ", charge, charge_type)

#                                             amount_due = float(late_fee) * float(charge) / 100
#                                             payload = {'pur_amount_due': amount_due}
#                                             # print(key, payload)

#                                             response['purchase_table_update'] = db.update('space.purchases', key, payload)
#                                             numCronUpdates = numCronUpdates + 1
#                                             # print("Updated PM", key, payload)
#                                 else:
#                                     print("No Match Found: ", payer)
                         
#                             continue
#         # INSERT NEW ROWS IF THIS IS THE FIRST TIME LATE FEES ARE ASSESSED
                    
#                     if putFlag == 0 and late_fee > 0:
#                         print("PUT Flag: ", putFlag, "New Late Fee for: ", i, purchase_uid)

#                         # Create JSON Object for Rent Purchase
#                         newRequest = {}
#                         newRequestID = db.call('space.new_purchase_uid')['result'][0]['new_id']
#                         grouping = newRequestID
#                         # print(newRequestID)

#                         # Common JSON Object Attributes
#                         newRequest['purchase_uid'] = newRequestID
#                         newRequest['pur_group'] = grouping
#                         # newRequest['pur_timestamp'] = datetime.today().date().strftime("%m-%d-%Y")
#                         newRequest['pur_timestamp'] = dt.strftime("%m-%d-%Y")
#                         newRequest['pur_property_id'] = property_id
#                         newRequest['purchase_type'] = "Late Fee"
#                         newRequest['pur_cf_type'] = "revenue"
#                         newRequest['pur_amount_due'] = late_fee
#                         newRequest['purchase_status'] = "UNPAID"
#                         newRequest['pur_status_value'] = "0"
                        

#                         newRequest['pur_due_by'] = 1
#                         newRequest['pur_late_by'] = 90
#                         newRequest['pur_late_fee'] = 0
#                         newRequest['pur_perDay_late_fee'] = 0

#                         # newRequest['purchase_date'] = datetime.today().date().strftime("%m-%d-%Y")
#                         newRequest['purchase_date'] = dt.strftime("%m-%d-%Y")
#                         newRequest['pur_description'] = purchase_uid
                        
                    
#                         # Create JSON Object for Rent Purchase for Tenant-PM Payment
#                         newRequest['pur_receiver'] = manager
#                         newRequest['pur_payer'] = tenant
#                         newRequest['pur_initiator'] = manager
#                         # newRequest['pur_due_date'] = datetime.today().date().strftime("%m-%d-%Y")
#                         newRequest['pur_due_date'] = dt.strftime("%m-%d-%Y")
                        

#                         if numDays ==  0:
#                             # print("\n", "Late Today")
#                             newRequest['pur_notes'] = "One Time Late Fee Applied"
#                         else:
#                             newRequest['pur_notes'] = "One Time Late Fee and Per Day Late Fee Applied"
#                             # newRequest['pur_notes'] = f"Late for { calendar.month_name[nextMonth.month]} {nextMonth.year} {response['result'][i]['purchase_uid']}"

#                         # print("\nInsert Tenant to Property Manager Late Fee")
#                         db.insert('space.purchases', newRequest)
#                         numCronPurchases = numCronPurchases + 1
#                         # print("Inserted into db: ", newRequest)


#                         # Create JSON Object for Rent Purchase for PM-Owner Payment
#                         newRequestID = db.call('space.new_purchase_uid')['result'][0]['new_id']
#                         newRequest['purchase_uid'] = newRequestID
#                         # print(newRequestID)
#                         newRequest['pur_receiver'] = owner
#                         newRequest['pur_payer'] = manager

                            
#                         # print(newRequest)
#                         # print("\nPurchase Parameters: ", i, newRequestID, grouping, tenant, owner, manager)
#                         # print("\nInsert Property Manager to Owner Late Fee")
#                         db.insert('space.purchases', newRequest)
#                         numCronPurchases = numCronPurchases + 1

                        
#                         # Create JSON Object for Rent Purchase for Owner-PM Payment
#                         # Determine Split between PM and Owner
#                         # print("\n  Contract Fees", response['result'][i]['contract_fees'])
#                         fees = json.loads(response['result'][i]['contract_fees'])
#                         # print("\nFees: ", fees, type(fees))

#                         for fee in fees:
#                             # print(fee)
#                             # Extract only the monthly fees
#                             if 'fee_type' in fee and (fee['frequency'] == 'Monthly' or fee['frequency'] == 'monthly') and fee['charge'] != "" and (fee['fee_type'] == "%" or fee['fee_type'] == "PERCENT"):
#                                 charge = fee['charge']
#                                 charge_type = fee['fee_type']
#                                 # print("\nCharge: ", charge, charge_type)

#                                 # Use this fee to create an Owner-PM late Fee PUT OR PST 
#                                 # Create JSON Object for Rent Purchase for PM-Owner Payment
#                                 newRequestID = db.call('space.new_purchase_uid')['result'][0]['new_id']
#                                 newRequest['purchase_uid'] = newRequestID
#                                 # print(newRequestID)
#                                 newRequest['pur_receiver'] = manager
#                                 newRequest['pur_payer'] = owner
#                                 newRequest['pur_cf_type'] = "expense"
#                                 newRequest['purchase_type'] = "Management - Late Fees"
#                                 newRequest['pur_notes'] = fee['fee_name']
#                                 newRequest['pur_amount_due'] = float(late_fee) * float(charge) / 100
                                
#                                 # print(newRequest)
#                                 # print("Purchase Parameters: ", i, newRequestID, grouping, tenant, owner, manager)
#                                 # print("\nInsert Owner to Property Manager Late Fee")
#                                 db.insert('space.purchases', newRequest)  
#                                 numCronPurchases = numCronPurchases + 1     


#         print(f"Late Fee CRON job for {dt} completed. {numCronPurchases} rows added. {numCronUpdates} rows updated.")
#         response = {'message': f'Successfully completed CRON Job for {dt}' ,
#                     'rows added': f'{numCronPurchases}', 'rows updated': f'{numCronUpdates}',
#                 'code': 200}

#         return response

    



class PeriodicPurchases_CLASS(Resource):
    def get(self):
        print("In Periodic Purchases CRON Job)")

        # Get email notification that CRON Job ran
        # Ensure CRON job does not get missed

        numCronPurchases = 0

        # Establish current month and year
        dt = datetime.today()
        
        # Run query to find rents of ACTIVE leases with recurring fees
        with connect() as db:
            response = db.execute("""
                    -- CALCULATE RECURRING FEES
                    SELECT leaseFees.*
                        , lease_tenant.*
                        , property_owner_id, po_owner_percent
                        , contract_business_id, contract_status, contract_fees
                        , lease_property_id, lease_start, lease_end, lease_status, lease_early_end_date, lease_renew_status
                        -- , lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, lease_move_in_date, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_docuSign, lease_consent, lease_actual_rent, lease_end_notice_period, lease_end_reason
                    FROM space.leaseFees
                    LEFT JOIN space.leases ON fees_lease_id = lease_uid
                    LEFT JOIN space.lease_tenant ON fees_lease_id = lt_lease_id
                    LEFT JOIN space.property_owner ON lease_property_id = property_id
                    LEFT JOIN space.contracts ON lease_property_id = contract_property_id
                    WHERE frequency != 'One-time' AND !ISNULL(frequency) AND frequency != ""
                        AND lease_status IN ('ACTIVE', 'ACTIVE MTM', 'APPROVED') 
                        AND contract_status = 'ACTIVE'
                    ORDER BY frequency;
                    """)
            
            # print(response)



            for i in range(len(response['result'])):
                print('\n', response['result'][i]['leaseFees_uid'], response['result'][i]['fees_lease_id'])

                # Check if available_topay is NONE
                if response['result'][i]['available_topay'] is None:
                    # print("available_topay Is NULL!!")
                    payable = 10
                else:
                    payable = response['result'][i]['available_topay']
                # print("available_topay: ", payable)


                # Check if due_by is NONE
                # print(response['result'][i]['due_by'])
                if response['result'][i]['due_by'] is None or response['result'][i]['due_by'] == 0:
                    # print("due_by Is NULL!!")
                    due_by = 1
                else:
                    due_by = response['result'][i]['due_by']
                # print("due_by: ", due_by, type(due_by))
                # print("dt.day: ", dt.day, type(dt.day))


                # Calculate Actual Rent due date
                if due_by < dt.day:
                    # print(due_by, " < ", dt.day)
                    due_date = datetime(dt.year, dt.month, due_by) + relativedelta(months=1)
                else:
                    due_date = datetime(dt.year, dt.month, due_by)
                # print("due date: ", due_date,  type(due_date))
                pm_due_date = due_date + relativedelta(days=10)
                # print("PM due date: ", pm_due_date,  type(pm_due_date))

                
                # Calculate number of days until rent is due
                if response['result'][i]['frequency'] == 'Monthly':
                    days_for_rent = (due_date - dt).days
                    print("Rent due in : ", days_for_rent, " days", type(days_for_rent))
                    print("Rent Posts in: ", days_for_rent - payable , " days", type(payable))
                elif response['result'][i]['frequency'] == 'Weekly':
                    print("Weekly")
                    break
                elif response['result'][i]['frequency'] == 'Bi-Weekly':
                    print("Bi-Weekly")
                    break
                elif response['result'][i]['frequency'] == 'Annually':
                    print("Annually")
                    break

        
        return 






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

