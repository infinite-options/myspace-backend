
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


# NOT SURE I NEED THIS
# def get_new_paymentUID(conn):
#     print("In new UID request")
#     with connect() as db:
#         newPaymentQuery = db.execute("CALL space.new_payment_uid;", "get", conn)
#         if newPaymentQuery["code"] == 280:
#             return newPaymentQuery["result"][0]["new_id"]
#     return "Could not generate new payment UID", 500


class NewPayments(Resource):
    def post(self):
        print("In Make Payment")
        data = request.get_json(force=True)
        print(data)
        
        # data contains a list of dictionaries
        # PART 1: For each purchase id: Change purchase status and record payment
        # PART 2: If payment was for RENT, pay Management Fees
        # PART 3: If there is a convenience fee, add covnenience fee purchase and record payment

        with connect() as db:

            purchase_ids = data['pay_purchase_id']
            num_pi = len(purchase_ids)
            print("number of items: ", num_pi)

            # Iterating over the list
            for item in purchase_ids:
                print("Current Item: ", item)

                # PART 1
                # INSERT ITEM PAYMENT INTO PAYMENTS TABLE
                print("UPDATE PAYMENTS TABLE")

                # GET NEW PAYMENT UID
                # print("Get New Request UID")
                newPaymentRequestID = db.call('new_payment_uid')['result'][0]['new_id']
                # print(newPaymentRequestID)

                paymentQuery = (""" 
                        -- INSERT ITEM PAYMENT INTO PAYMENTS TABLE
                        INSERT INTO space.payments
                        SET payment_uid = \'""" + newPaymentRequestID + """\'
                            , pay_purchase_id =  \'""" + item['purchase_uid'] + """\'
                            , pay_amount = \'""" + item['pur_amount_due'] + """\'
                            , payment_notes = \'""" + data['payment_notes'] + """\'
                            , pay_charge_id = \'""" + data['pay_charge_id'] + """\'
                            , payment_type = \'""" + data['payment_type'] + """\'
                            , payment_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                            , paid_by = \'""" + data['paid_by'] + """\'
                            , payment_intent = \'""" + data['payment_intent'] + """\'
                            , payment_method = \'""" + data['payment_method'] + """\';     
                        """)

                # print("Item Payment Query: ", paymentQuery)
                response = db.execute(paymentQuery, [], 'post')


                #  UPDATE PURCHASES TABLE
                print("UPDATE PURCHASES TABLE")

                # DETERMINE WHAT VALUE TO INSERT INTO purchase-status (UNPAID, PAID, PARTIALLY PAID)
                purchaseInfo = db.execute("""
                            -- QUERY ORIGINAL PURCHASE TO CHECK HOW MUCH IS SUPPOSED TO BE PAID
                            SELECT *
                            FROM space.pp_status
                            -- WHERE purchase_uid = '400-000703';
                            WHERE purchase_uid = \'""" + item['purchase_uid'] + """\';
                            """)
                # print(purchaseInfo)
                amt_remaining_str = purchaseInfo['result'][0]['amt_remaining']
                # print('amt_remaining: ', amt_remaining_str, type(amt_remaining_str))
                amt_remaining = float(amt_remaining_str)
                # print('amt_remaining: ', amt_remaining, type(amt_remaining))
                amt_due = float(purchaseInfo['result'][0]['pur_amount_due'])
                # print('amt_due: ', amt_due, type(amt_due))

                # print(purchaseInfo['result'][0]['pur_due_date'], type(purchaseInfo['result'][0]['pur_due_date']))
                # print(datetime.now(), type(datetime.now()))

                # if purchaseInfo['result'][0]['pur_due_date'] == datetime.now():
                #     print("YES")

                pur_due_date = datetime.strptime(purchaseInfo['result'][0]['pur_due_date'], '%m-%d-%Y')
                # print('pur_due_date: ', pur_due_date, type(pur_due_date))
                
                purchase_status = "UNPAID"
                pur_status_value = "0"
                if amt_remaining <= 0: 
                    if pur_due_date >= datetime.now():
                        purchase_status = "PAID"
                        pur_status_value = "5"
                    else: 
                        purchase_status = "PAID LATE"
                        pur_status_value = "4"
                elif amt_remaining == amt_due: 
                    purchase_status = "UNPAID"
                    pur_status_value = "0"
                else:
                    purchase_status = "PARTIALLY PAID"
                    pur_status_value = "1"
                # print(purchase_status)

                # DEFINE KEY VALUE PAIR
                key = {'purchase_uid': item['purchase_uid']}
                payload = {'purchase_status': purchase_status}
                payload2= {'pur_status_value': pur_status_value}    
                # print(key, payload)

                # UPDATE PURCHASE TABLE WITH PURCHASE STATUS
                response['purchase_table_update'] = db.update('purchases', key, payload)
                response['purchase_status_update'] = db.update('purchases', key, payload2)
                # print(response)
                



                # PART 2
                # DETERMINE IF PAYMENT WAS FOR RENT
                print("Purchase Type: ", purchaseInfo['result'][0]['purchase_type'])
                purchaseType = purchaseInfo['result'][0]['purchase_type']
                if purchaseType == "Rent":
                    #Pay Management Fees associated with purchase_uid

                    managementInfo = db.execute("""
                            -- DETERMINE WHICH PURCHASES ARE MONTHLY MANAGEMENT FEES
                            SELECT *
                            FROM space.purchases
                            WHERE purchase_type = "Management" AND
                                -- pur_description = '400-000023';
                                pur_description = \'""" + item['purchase_uid'] + """\';
                            """)
                    # print(managementInfo)

                    for managementFee in managementInfo['result']:
                        # print("Current Item: ", managementFee)
                        # print(managementFee['purchase_uid'])

                        # SET STATUS AS MAIN PURCHASE UID
                        payload = {'purchase_status': purchase_status}
                        payload2= {'pur_status_value': pur_status_value}  

                        # DEFINE KEY VALUE PAIR
                        key = {'purchase_uid': managementFee['purchase_uid']}
                        payload = {'purchase_status': purchase_status}
                        payload2= {'pur_status_value': pur_status_value}    
                        # print(key, payload)

                        # UPDATE PURCHASE TABLE WITH PURCHASE STATUS
                        response['purchase_table_update'] = db.update('purchases', key, payload)
                        response['purchase_status_update'] = db.update('purchases', key, payload2)
                        # print(response)





                # PART 3
                # DETERMINE IF CONVENIENCE FEES WERE PAID
                print("Convenience fee paid: ", data['pay_fee'], type(data['pay_fee']))
                if data['pay_fee'] > 0:

                    # PART 2A: ENTER PURCHASE AND PAYMENT INFO FOR RECEIPT OF CONVENIENCE FEE
                    # INSERT INTO PURCHASES TABLE
                    newPurchaseRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                    # print(newPurchaseRequestID)

                    # DETERMINE HOW MUCH OF THE CONVENIENCE FEES WAS DUE TO EACH PURCHASE
                    # print(item['pur_amount_due'], type(item['pur_amount_due']))
                    # print(data['pay_total'], type(data['pay_total']))
                    # print(data['pay_fee'], type(data['pay_fee']))
                    itemFee = float(item['pur_amount_due']) / (data['pay_total'] - data['pay_fee']) * data['pay_fee']
                    itemFee = round(itemFee, 2)
                    print("Item Convenience Fee: ", itemFee)

                    feePurchaseQuery = (""" 
                            INSERT INTO space.purchases
                            SET purchase_uid = \'""" + newPurchaseRequestID + """\'
                                , pur_timestamp = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , pur_property_id = \'""" + purchaseInfo['result'][0]['pur_property_id'] + """\'  
                                , purchase_type = "Extra Charges"
                                , pur_cf_type = "revenue"
                                , purchase_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , pur_due_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , pur_amount_due = """ + str(itemFee) + """
                                , purchase_status = "PAID"
                                , pur_notes = "Credit Card Fee - Auto Posted"
                                , pur_description =  \'""" + item['purchase_uid'] + """\'
                                , pur_receiver = \'""" + purchaseInfo['result'][0]['pur_receiver'] + """\'
                                , pur_payer = \'""" + purchaseInfo['result'][0]['pur_payer'] + """\'
                                , pur_initiator = \'""" + purchaseInfo['result'][0]['pur_initiator'] + """\'
                                ;
                            """)

                    # print("Query: ", feePurchaseQuery)
                    queryResponse = db.execute(feePurchaseQuery, [], 'post')
                    # print(queryResponse)

                
                    # INSERT INTO PAYMENTS TABLE
                    # GET NEW UID
                    # print("Get New Request UID")
                    newPaymentRequestID = db.call('new_payment_uid')['result'][0]['new_id']
                    # print(newPaymentRequestID)

                    feePaymentQuery = (""" 
                            -- PAY PURCHASE UID
                            INSERT INTO space.payments
                            SET payment_uid = \'""" + newPaymentRequestID + """\'
                                , pay_purchase_id =  \'""" + newPurchaseRequestID + """\'
                                , pay_amount = """ + str(itemFee) + """
                                , payment_notes = \'""" + data['payment_notes'] + """\'
                                , pay_charge_id = \'""" + data['pay_charge_id'] + """\'
                                , payment_type = \'""" + data['payment_type'] + """\'
                                , payment_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , paid_by = \'""" + data['paid_by'] + """\'
                                , payment_intent = \'""" + data['payment_intent'] + """\'
                                , payment_method = \'""" + data['payment_method'] + """\';     
                            """)

                    # print("Query: ", feePaymentQuery)
                    response = db.execute(feePaymentQuery, [], 'post')


                    # PART 2B: ENTER PURCHASE AND PAYMENT INFO FOR PAYMENT OF CONVENIENCE FEE
                    # INSERT INTO PURCHASES TABLE
                    newPurchaseRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                    # print(newPurchaseRequestID)

                    feePurchaseQuery = (""" 
                            INSERT INTO space.purchases
                            SET purchase_uid = \'""" + newPurchaseRequestID + """\'
                                , pur_timestamp = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , pur_property_id = \'""" + purchaseInfo['result'][0]['pur_property_id'] + """\'  
                                , purchase_type = "Bill Posting"
                                , pur_cf_type = "expense"
                                , purchase_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , pur_due_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , pur_amount_due = """ + str(itemFee) + """
                                , purchase_status = "PAID"
                                , pur_notes = "Credit Card Fee - Auto Posted"
                                , pur_description =  \'""" + item['purchase_uid'] + """\'
                                , pur_receiver = "STRIPE"
                                , pur_payer = \'""" + purchaseInfo['result'][0]['pur_receiver'] + """\'
                                , pur_initiator = \'""" + purchaseInfo['result'][0]['pur_initiator'] + """\'
                                ;
                            """)

                    # print("Query: ", feePurchaseQuery)
                    queryResponse = db.execute(feePurchaseQuery, [], 'post')
                    # print(queryResponse)

                
                    # INSERT INTO PAYMENTS TABLE
                    # GET NEW UID
                    # print("Get New Request UID")
                    newPaymentRequestID = db.call('new_payment_uid')['result'][0]['new_id']
                    # print(newPaymentRequestID)

                    feePaymentQuery = (""" 
                            -- PAY PURCHASE UID
                            INSERT INTO space.payments
                            SET payment_uid = \'""" + newPaymentRequestID + """\'
                                , pay_purchase_id =  \'""" + newPurchaseRequestID + """\'
                                , pay_amount = """ + str(itemFee) + """
                                , payment_notes = \'""" + data['payment_notes'] + """\'
                                , pay_charge_id = \'""" + data['pay_charge_id'] + """\'
                                , payment_type = \'""" + data['payment_type'] + """\'
                                , payment_date = DATE_FORMAT(CURDATE(), '%m-%d-%Y')
                                , paid_by = \'""" + purchaseInfo['result'][0]['pur_receiver'] + """\'
                                , payment_intent = \'""" + data['payment_intent'] + """\'
                                , payment_method = \'""" + data['payment_method'] + """\';     
                            """)

                    # print("Query: ", feePaymentQuery)
                    response = db.execute(feePaymentQuery, [], 'post')


                else:
                    print("No convenience fee")


        return data 
    

class Payments(Resource):
    def post(self):
        print("In Make Payment")
        response = {}
        with connect() as db:
            data = request.get_json(force=True)
            # print(data)

            fields = [
                'pay_purchase_id'
                , 'pay_amount'
                , 'payment_notes'
                , 'pay_charge_id'
                , 'payment_type'
                , 'payment_date'
                , 'payment_verify'
                , 'paid_by'
                , 'payment_intent'
                , 'payment_method'
                , 'payment_date_cleared'
                , 'payment_client_secret'
            ]

            # PUTS JSON DATA INTO EACH FIELD
            newRequest = {}
            for field in fields:
                newRequest[field] = data.get(field)
                # print(field, " = ", newRequest[field])


            # GET NEW UID
            # print("Get New Request UID")
            newRequestID = db.call('new_payment_uid')['result'][0]['new_id']
            newRequest['payment_uid'] = newRequestID
            # print(newRequestID)

            # SET TRANSACTION DATE TO NOW
            newRequest['payment_date'] = date.today()
            newRequest['payment_verify'] = "Unverified"

            # INSERT DATA INTO PAYMENTS TABLE
            # print(newRequest)
            response['Payment_Insert'] = db.insert('payments', newRequest)
            response['Payments_UID'] = newRequestID
            # print(response)

            # DETERMINE WHAT VALUE TO INSERT INTO purchase-status (UNPAID, PAID, PARTIALLY PAID)
            amt_due = db.execute("""
                        -- CHECK HOW MUCH IS SUPPOSED TO BE PAID
                        SELECT -- *,
                            amt_remaining
                        FROM space.pp_status
                        WHERE purchase_uid = \'""" + newRequest['pay_purchase_id'] + """\';
                        """)
            # print(amt_due)
            # print("Amount Due: ", amt_due['result'][0]['amt_remaining'])

            purchase_status = "UNPAID"
            print(type(amt_due['result'][0]['amt_remaining']))
            if float(amt_due['result'][0]['amt_remaining']) <= 0: 
                purchase_status = "PAID"
                # print(purchase_status)

                # payload = {'purchase_uid': newRequest['pay_purchase_id'], 'purchase_status': purchase_status}
                payload = {'purchase_status': purchase_status}
                key = {'purchase_uid': newRequest['pay_purchase_id']}
                # print(key, payload)

                
                # response['b'] = db.update('purchases', key, purchase_status)
                response['purchase_table_update'] = db.update('purchases', key, payload)
                response['purchase_status'] = purchase_status

            else:
                response['purchase_status'] = 'UNPAID'


            # # DETERMINE PURCHASE STATUS

            # # DETERMINE AMOUNT DUE
            # # print("Purchase ID: ", newRequest['pay_purchase_id'])
            # amt_due = db.execute("""
            #         -- CHECK HOW MUCH IS SUPPOSED TO BE PAID
            #         SELECT -- *,
            #             pur_amount_due
            #         FROM space.purchases
            #         WHERE purchase_uid = \'""" + newRequest['pay_purchase_id'] + """\';
            #         """)
            # # print("Amount Due: ", amt_due['result'][0]['pur_amount_due'])

            # # DETERMINE HOW MUCH WAS PAID BEFORE
            # purchase_status = "Payment Error"

            # # print(type(newRequest['pay_amount']))
            # # print(type(amt_due['result'][0]['pur_amount_due']))

            # if newRequest['pay_amount'] >= float(amt_due['result'][0]['pur_amount_due']): purchase_status = "PAID"
            # elif newRequest['pay_amount'] < float(amt_due['result'][0]['pur_amount_due']): purchase_status = "PARTIALLY PAID"
            
            # # print(purchase_status)
            # # print(newRequest['pay_purchase_id'])

            # # purRequest = {}
            # # purRequest['purchase_status'] = purchase_status
            # # pur_response = db.insert('purchases', purRequest)

            # key = {'purchase_uid': newRequest['pay_purchase_id']}
            # # print(key)
            # payload = {'purchase_status': purchase_status}
            # # print(payload)
            # response['Purchase_Status_Update'] = db.update('purchases', key, payload)
            # response['Purchase_Status'] = purchase_status

        return response  


    def put(self):
        print('in Update Payment Status')
        with connect() as db:
            payload = request.get_json()
            # print(payload)

            if payload.get('pay_purchase_id') is None:
                raise BadRequest("Request failed, no UID in payload.")
        
            payload_purchase_uid = {'purchase_uid': payload.pop('pay_purchase_id')}
            # print(payload_purchase_uid)
            purchase_uid = payload_purchase_uid['purchase_uid']
            # print(purchase_uid)


            # DETERMINE WHAT VALUE TO INSERT INTO purchase-status (UNPAID, PAID, PARTIALLY PAID)
            amt_due = db.execute("""
                        -- CHECK HOW MUCH IS SUPPOSED TO BE PAID
                        SELECT -- *,
                            amt_remaining
                        FROM space.pp_status
                        WHERE purchase_uid = \'""" + purchase_uid + """\';
                        """)
            # print(amt_due)
            # print("Amount Due: ", amt_due['result'][0]['amt_remaining'])

            purchase_status = "UNPAID"
            # print(type(amt_due['result'][0]['amt_remaining']))
            if float(amt_due['result'][0]['amt_remaining']) <= 0: purchase_status = "PAID"
            # print(purchase_status)

            payload = {'purchase_uid': '400-000531', 'purchase_status': purchase_status}

            
            response = db.update('purchases', payload_purchase_uid, payload)
            response['purchase_status'] = purchase_status
        return response


class PaymentStatus(Resource):
    # decorators = [jwt_required()]

    def get(self, user_id):
        print('in PaymentStatus')
        response = {}

        # print("User ID: ", user_id)

        with connect() as db:
            # print("in connect loop")
      

            # MONIES RECECEIVED
            moneyReceived = db.execute("""
                -- MONEY RECEIVED
                SELECT * FROM space.pp_details
                WHERE payment_status != 'UNPAID' 
                    -- AND pur_receiver = '600-000003' 
                    AND pur_receiver = \'""" + user_id + """\'
                """)
            # print("Query: ", moneyReceived)
            response["MoneyReceived"] = moneyReceived
            print("1")


            # MONIES PAID
            moneyPaid = db.execute("""
                -- MONEY PAID
                SELECT * FROM space.pp_details
                WHERE payment_status != 'UNPAID' 
                    -- AND pur_payer = '600-000003' 
                    AND pur_payer = \'""" + user_id + """\'
                """)
            # print("Query: ", moneyPaid)
            response["MoneyPaid"] = moneyPaid
            print("2")


            # ACCOUNTS RECEIVABLE
            moneyToBeReceived = db.execute("""
                -- MONEY TO BE RECEIVED
                SELECT * FROM space.pp_details
                WHERE payment_status IN ('UNPAID','PARTIALLY PAID')
                    -- AND pur_receiver = '600-000003' 
                    AND pur_receiver = \'""" + user_id + """\'
                """)
            # print("Query: ", moneyToBeReceived)
            response["MoneyToBeReceived"] = moneyToBeReceived
            print("3")



            # ACCOUNTS PAYABLE (ALL MONEY TO BE PAID REGARDLESS OF RENT COLLECTION)
            if user_id[0:3] == '600':
                print("Manager Rent Status")
                moneyToBePaid = db.execute("""
                -- MONEY TO BE PAID
                SELECT *
                FROM space.pp_details AS ppd
                LEFT JOIN (
                    SELECT 
                        payment_status AS ps
                        , pur_group AS pg
                        , pur_payer AS pp
                        , CONCAT(pur_group, " ", payment_status) AS pgps
                    FROM space.pp_status 
                    WHERE LEFT(pur_payer, 3) = '350'
                ) AS pps ON ppd.pur_group = pps.pg
                WHERE ppd.payment_status IN ('UNPAID','PARTIALLY PAID')
                    -- AND ppd.pur_payer = '600-000003'
                    AND pur_payer = \'""" + user_id + """\'
                UNION    
                -- MONEY TO BE PAID
                SELECT *
                FROM space.pp_details AS ppd
                LEFT JOIN (
                    SELECT 
                        payment_status AS ps
                        , pur_group AS pg
                        , pur_payer AS pp
                        , CONCAT(pur_group, " ", payment_status) AS pgps                       
                    FROM space.pp_status 
                    WHERE LEFT(pur_payer, 3) = '350'
                ) AS pps ON ppd.pur_group = pps.pg
                WHERE ppd.payment_status IN ('UNPAID','PARTIALLY PAID')
                    -- AND ppd.pur_receiver = '600-000003'
                    AND LEFT(ppd.pur_payer, 3) != '350' 
                    AND ppd.pur_receiver = \'""" + user_id + """\'
                """)
            else:
                print("Non-Manager User")
                moneyToBePaid = db.execute("""
                -- MONEY TO BE PAID
                SELECT *
                FROM space.pp_details AS ppd
                LEFT JOIN (
                    SELECT 
                        payment_status AS ps
                        , pur_group AS pg
                        , pur_payer AS pp
                        , CONCAT(pur_group, " ", payment_status) AS pgps
                    FROM space.pp_status 
                    WHERE LEFT(pur_payer, 3) = '350'
                ) AS pps ON ppd.pur_group = pps.pg
                WHERE ppd.payment_status IN ('UNPAID','PARTIALLY PAID')
                    -- AND ppd.pur_payer = '600-000003'                     
                    AND pur_payer = \'""" + user_id + """\'
                """)
            # print("Query: ", moneyToBePaid)
            response["MoneyToBePaid"] = moneyToBePaid
            print("4")


            # ACCOUNTS PAYABLE (ASSOCIATED RENTS HAVE BEEN PAID)
            if user_id[0:3] == '600':
                print("Manager Rent Status")
                moneyPayable = db.execute("""
                -- MONEY PAYABLE
                SELECT -- *,
                        purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer, pur_group
                        , pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_month_num, cf_year
                        , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email
                        -- , initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email
                        , payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email
                        -- , property_uid, property_available_to_rent, property_active_date
                        , property_address, property_unit, property_city, property_state, property_zip -- , property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image
                        , property_id, property_owner_id, po_owner_percent
                        , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email -- , owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_photo_url
                        -- , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                        , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email -- , business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                        -- , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_actual_rent, leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lt_lease_id, lt_tenant_id, lt_responsibility
                        , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                        , ps, pg, pp, pgps
                FROM space.pp_details AS ppd
                LEFT JOIN (
                    SELECT 
                        payment_status AS ps
                        , pur_group AS pg
                        , pur_payer AS pp
                        , CONCAT(pur_group, " ", payment_status) AS pgps
                    FROM space.pp_status 
                    WHERE LEFT(pur_payer, 3) = '350'
                ) AS pps ON ppd.pur_group = pps.pg
                WHERE ppd.payment_status IN ('UNPAID','PARTIALLY PAID')
                    AND ps IN ('PAID','PARTIALLY PAID','PAID LATE')
                    -- AND ppd.pur_payer = '600-000003'
                    AND pur_payer = \'""" + user_id + """\'
                UNION    
                -- MONEY TO BE PAID
                SELECT -- *,
                        purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer, pur_group
                        , pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_month_num, cf_year
                        , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email
                        -- , initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email
                        , payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email
                        -- , property_uid, property_available_to_rent, property_active_date
                        , property_address, property_unit, property_city, property_state, property_zip -- , property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image
                        , property_id, property_owner_id, po_owner_percent
                        , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email -- , owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_photo_url
                        -- , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                        , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email -- , business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                        -- , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_actual_rent, leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lt_lease_id, lt_tenant_id, lt_responsibility
                        , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                        , ps, pg, pp, pgps
                FROM space.pp_details AS ppd
                LEFT JOIN (
                    SELECT 
                        payment_status AS ps
                        , pur_group AS pg
                        , pur_payer AS pp
                        , CONCAT(pur_group, " ", payment_status) AS pgps                       
                    FROM space.pp_status 
                    WHERE LEFT(pur_payer, 3) = '350'
                ) AS pps ON ppd.pur_group = pps.pg
                WHERE ppd.payment_status IN ('UNPAID','PARTIALLY PAID')
                    AND ps IN ('PAID','PARTIALLY PAID','PAID LATE')
                    AND LEFT(ppd.pur_payer, 3) != '350' 
                    -- AND ppd.pur_receiver = '600-000003'
                    AND ppd.pur_receiver = \'""" + user_id + """\'
                """)
            else:
                print("Non-Manager User")
                moneyPayable = db.execute("""
                    -- MONEY PAYABLE
                    SELECT -- *,
                        purchase_uid, pur_timestamp, pur_property_id, purchase_type, pur_cf_type, pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description, pur_receiver, pur_initiator, pur_payer, pur_group
                        , pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining, cf_month, cf_month_num, cf_year
                        , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email
                        -- , initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email
                        , payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email
                        -- , property_uid, property_available_to_rent, property_active_date
                        , property_address, property_unit, property_city, property_state, property_zip -- , property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image
                        , property_id, property_owner_id, po_owner_percent
                        , owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email -- , owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_photo_url
                        -- , contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date
                        , business_uid, business_user_id, business_type, business_name, business_phone_number, business_email -- , business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url
                        -- , lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_actual_rent, leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lt_lease_id, lt_tenant_id, lt_responsibility
                        , tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number -- , tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_photo_url
                        , ps, pg, pp, pgps

                    FROM space.pp_details AS ppd
                    LEFT JOIN (
                        SELECT 
                            payment_status AS ps
                            , pur_group AS pg
                            , pur_payer AS pp
                            , CONCAT(pur_group, " ", payment_status) AS pgps
                        FROM space.pp_status 
                        WHERE LEFT(pur_payer, 3) = '350'
                    ) AS pps ON ppd.pur_group = pps.pg
                    WHERE ppd.payment_status IN ('UNPAID','PARTIALLY PAID')
                        AND ps IN ('PAID','PARTIALLY PAID','PAID LATE')
                        -- AND ppd.pur_payer = '600-000003'
                        AND pur_payer = \'""" + user_id + """\'
                    """)
            # print("Query: ", moneyPayable)
            response["MoneyPayable"] = moneyPayable
            print("5")


            # CURRENT CF MONTH REVENUE
            cfMonthRevenue = db.execute("""
                -- MONEY TO BE RECEIVED
                SELECT * FROM space.pp_details
                WHERE cf_month = DATE_FORMAT(NOW(), '%M')
                    AND cf_year = DATE_FORMAT(NOW(), '%Y')
                    -- AND pur_receiver = '600-000003' 
                    AND pur_receiver = \'""" + user_id + """\'
                """)
            # print("Query: ", cfMonthRevenue)
            response["cfMonthRevenue"] = cfMonthRevenue
            print("6")


            # CURRENT CF MONTH EXPENSE
            cfMonthExpense = db.execute("""
                -- MONEY TO BE PAID
                SELECT * FROM space.pp_details
                WHERE cf_month = DATE_FORMAT(NOW(), '%M')
                    AND cf_year = DATE_FORMAT(NOW(), '%Y')
                    -- AND pur_payer = '600-000003' 
                    AND pur_payer = \'""" + user_id + """\'
                """)
            # print("Query: ", cfMonthExpense)
            response["cfMonthExpense"] = cfMonthExpense
            print("7")

            # CURRENT CF MONTH EXPENSE BY PROPERTY
            cfMonthExpenseByProperty = db.execute("""
                -- MONEY TO BE PAID
                SELECT pur_property_id, pur_cf_type -- , pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description,
                    , pur_receiver -- , pur_initiator, pur_payer, pur_group, pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining,
                    , cf_month, cf_year -- , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email, initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email, payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email, property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_photo_url, contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date, business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url, lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_actual_rent, leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lt_lease_id, lt_tenant_id, lt_responsibility, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_current_address-DNU, tenant_photo_url
                    , SUM(pur_amount_due) AS pur_amount_due
                    , SUM(pur_amount_due-amt_remaining) AS received_actual FROM space.pp_details
                WHERE cf_month = DATE_FORMAT(NOW(), '%M')
                    AND cf_year = DATE_FORMAT(NOW(), '%Y') AND pur_payer = \'""" + user_id + """\'
                GROUP BY pur_property_id;
                """)
            # print("Query: ", cfMonthExpenseByProperty)
            response["cfMonthExpenseByProperty"] = cfMonthExpenseByProperty
            print("8")

            # CURRENT CF MONTH REVENUE BY PROPERTY
            cfMonthRevenueByProperty = db.execute("""
                -- MONEY TO BE RECEIVED
                SELECT pur_property_id, pur_cf_type -- , pur_bill_id, purchase_date, pur_due_date, pur_amount_due, purchase_status, pur_status_value, pur_notes, pur_description,
                    , pur_receiver -- , pur_initiator, pur_payer, pur_group, pay_purchase_id, latest_date, total_paid, payment_status, amt_remaining,
                    , cf_month, cf_year -- , receiver_user_id, receiver_profile_uid, receiver_user_type, receiver_user_name, receiver_user_phone, receiver_user_email, initiator_user_id, initiator_profile_uid, initiator_user_type, initiator_user_name, initiator_user_phone, initiator_user_email, payer_user_id, payer_profile_uid, payer_user_type, payer_user_name, payer_user_phone, payer_user_email, property_uid, property_available_to_rent, property_active_date, property_address, property_unit, property_city, property_state, property_zip, property_longitude, property_latitude, property_type, property_num_beds, property_num_baths, property_value, property_area, property_listed_rent, property_deposit, property_pets_allowed, property_deposit_for_rent, property_images, property_taxes, property_mortgages, property_insurance, property_featured, property_description, property_notes, property_amenities_unit, property_amenities_community, property_amenities_nearby, property_favorite_image, property_id, property_owner_id, po_owner_percent, owner_uid, owner_user_id, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_ein_number, owner_ssn, owner_address, owner_unit, owner_city, owner_state, owner_zip, owner_photo_url, contract_uid, contract_property_id, contract_business_id, contract_start_date, contract_end_date, contract_fees, contract_assigned_contacts, contract_documents, contract_name, contract_status, contract_early_end_date, business_uid, business_user_id, business_type, business_name, business_phone_number, business_email, business_ein_number, business_services_fees, business_locations, business_documents, business_address, business_unit, business_city, business_state, business_zip, business_photo_url, lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_assigned_contacts, lease_documents, lease_early_end_date, lease_renew_status, move_out_date, lease_adults, lease_children, lease_pets, lease_vehicles, lease_referred, lease_effective_date, lease_application_date, lease_docuSign, lease_actual_rent, leaseFees_uid, fees_lease_id, lease_rent_due_by, lease_rent_late_by, lease_rent_late_fee, lease_rent_perDay_late_fee, lease_fees, lt_lease_id, lt_tenant_id, lt_responsibility, tenant_uid, tenant_user_id, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number, tenant_ssn, tenant_current_salary, tenant_salary_frequency, tenant_current_job_title, tenant_current_job_company, tenant_drivers_license_number, tenant_drivers_license_state, tenant_address, tenant_unit, tenant_city, tenant_state, tenant_zip, tenant_previous_address, tenant_documents, tenant_adult_occupants, tenant_children_occupants, tenant_vehicle_info, tenant_references, tenant_pet_occupants, tenant_current_address-DNU, tenant_photo_url
                    , SUM(pur_amount_due) AS pur_amount_due
                    , SUM(pur_amount_due-amt_remaining) AS received_actual FROM space.pp_details
                WHERE cf_month = DATE_FORMAT(NOW(), '%M')
                    AND cf_year = DATE_FORMAT(NOW(), '%Y') AND pur_payer = \'""" + user_id + """\'
                GROUP BY pur_property_id;
                """)
            # print("Query: ", cfMonthRevenueByProperty)
            response["cfMonthRevenueByProperty"] = cfMonthRevenueByProperty
            print("9")
            
            # print(response)
            return response


class PaymentOwner(Resource):
            
    # decorators = [jwt_required()]

    def get(self, user_id, owner_id):
        print('in PaymentStatus')
        response = {}

        # print("User ID: ", user_id)

        with connect() as db:
            # print("in connect loop")

            # ACCOUNTS PAYABLE (ASSOCIATED RENTS HAVE BEEN PAID)
            if (user_id[0:3] == '600', owner_id[0:3] == '110'):
                print("In Manager Owner Payable Rents")
                moneyPayable = db.execute("""
                -- MONEY PAYABLE
                SELECT *
                FROM space.pp_details AS ppd
                LEFT JOIN (
                    SELECT 
                        payment_status AS ps
                        , pur_group AS pg
                        , pur_payer AS pp
                        , CONCAT(pur_group, " ", payment_status) AS pgps
                    FROM space.pp_status 
                    WHERE LEFT(pur_payer, 3) = '350'
                ) AS pps ON ppd.pur_group = pps.pg
                WHERE ppd.payment_status IN ('UNPAID','PARTIALLY PAID')
                    AND ps IN ('PAID','PARTIALLY PAID','PAID LATE')
                    -- AND ppd.pur_payer = '600-000003'
                    -- AND ppd.property_owner_id = '110-000003'
                    AND pur_payer = \'""" + user_id + """\'
                    AND ppd.property_owner_id = \'""" + owner_id + """\'
                UNION    
                -- MONEY TO BE PAID
                SELECT *
                FROM space.pp_details AS ppd
                LEFT JOIN (
                    SELECT 
                        payment_status AS ps
                        , pur_group AS pg
                        , pur_payer AS pp
                        , CONCAT(pur_group, " ", payment_status) AS pgps                       
                    FROM space.pp_status 
                    WHERE LEFT(pur_payer, 3) = '350'
                ) AS pps ON ppd.pur_group = pps.pg
                WHERE ppd.payment_status IN ('UNPAID','PARTIALLY PAID')
                    AND ps IN ('PAID','PARTIALLY PAID','PAID LATE')
                    AND LEFT(ppd.pur_payer, 3) != '350' 
                    -- AND ppd.pur_receiver = '600-000003'
                    -- AND ppd.property_owner_id = '110-000003'
                    AND ppd.pur_receiver = \'""" + user_id + """\'
                    AND ppd.property_owner_id = \'""" + owner_id + """\'
                -- ORDER BY cf_year, cf_month
                ORDER BY STR_TO_DATE(CONCAT('1 ', cf_month, ' ', cf_year), '%d %M %Y') DESC, pgps;
                """)
            else:
                print("Not a Valid Case")

            # print("Query: ", paidStatus)
            response["OwnerMoneyPayable"] = moneyPayable

            return response

class PaymentMethod(Resource):
    def post(self):
        response = {    }
        payload = request.get_json()
        print(type(payload), payload)
        i = 0

        with connect() as db:

            if isinstance(payload, list):
                for item in payload:
                    print(item)
                    query_response = db.insert('paymentMethods', item)
                    print(query_response)
                    response[i] = query_response
                    i += 1

            else:
                query_response = db.insert('paymentMethods', payload)
                print(query_response)
                response = query_response

        return response
    
    def put(self):
        print("In Payment Method PUT")
        response = {}
        payload = request.get_json()
        print("PUT Payload: ", payload)
        if payload.get('paymentMethod_uid') is None:
            raise BadRequest("Request failed, no UID in payload.")
        key = {'paymentMethod_uid': payload.pop('paymentMethod_uid')}
        with connect() as db:
            response = db.update('paymentMethods', key, payload)
        return response

    def get(self, user_id):
        print('in PaymentMethod GET')
        with connect() as db:
            paymentMethodQuery = db.execute("""
                -- FIND APPLICATIONS CURRENTLY IN PROGRESS
                SELECT *
                FROM space.paymentMethods
                WHERE paymentMethod_profile_id = \'""" + user_id + """\';
                 """)
        return paymentMethodQuery

    def delete(self, user_id):
        print("In paymentMethods DELETE")
        print(user_id)
        response = {}
        with connect() as db:

            paymentQuery = ("""
                    DELETE 
                    FROM space.paymentMethods
                    WHERE paymentMethod_uid = \'""" + user_id + """\';
                    """)

            response["delete_paymentMethods"] = db.delete(paymentQuery)


        return response

class RequestPayment(Resource):
    def post(self):
        print('in Request Payment')
        with connect() as db:
            data = request.get_json()

            new_bill_uid = db.call('space.new_bill_uid')['result'][0]['new_id']
            bill_description = data["bill_description"]
            bill_amount = data["bill_amount"]
            bill_created_by = data["bill_created_by"]
            bill_utility_type = data["bill_utility_type"]
            bill_split = data["bill_split"]
            bill_property_id = data["bill_property_id"]
            bill_docs = data["bill_docs"]
            bill_maintenance_quote_id = data["bill_maintenance_quote_id"]
            bill_notes = data["bill_notes"]

            billQuery = (""" 
                    -- CREATE NEW BILL
                    INSERT INTO space.bills
                    SET bill_uid = \'""" + new_bill_uid + """\'
                    , bill_timestamp = CURRENT_TIMESTAMP()
                    , bill_description = \'""" + bill_description + """\'
                    , bill_amount = \'""" + str(bill_amount) + """\'
                    , bill_created_by = \'""" + bill_created_by + """\'
                    , bill_utility_type = \'""" + bill_utility_type + """\'
                    , bill_split = \'""" + bill_split + """\'
                    , bill_property_id = \'""" + json.dumps(bill_property_id, sort_keys=False) + """\'
                    , bill_docs = \'""" + json.dumps(bill_docs, sort_keys=False) + """\'
                    , bill_notes = \'""" + bill_description + """\'
                    , bill_maintenance_quote_id = \'""" + bill_maintenance_quote_id + """\';          
                    """)

            response = db.execute(billQuery, [], 'post')

            newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
            purchase_uid = newRequestID

            pur_payer_st = db.select('property_owner', {'property_id': data["bill_property_id"]})
            pur_payer = pur_payer_st.get('result')[0]['property_owner_id']

            maintenance_request_id_st = db.select('maintenanceQuotes', {'maintenance_quote_uid': bill_maintenance_quote_id})
            quote_maintenance_request_id = maintenance_request_id_st.get('result')[0]['quote_maintenance_request_id']
            quote_maintenance_close_dt = db.select('maintenanceRequests', {'maintenance_request_uid': quote_maintenance_request_id})
            quote_close_date = quote_maintenance_close_dt.get('result')[0]['maintenance_request_closed_date']

            purchaseQuery = (""" 
                                    INSERT INTO space.purchases
                                    SET purchase_uid = \'""" + purchase_uid + """\'
                                        , pur_timestamp = CURRENT_TIMESTAMP()
                                        , pur_property_id = \'""" + bill_property_id + """\'
                                        , purchase_type = "MAINTENANCE"
                                        , pur_cf_type = "expense"
                                        , pur_bill_id = \'""" + new_bill_uid + """\'
                                        , purchase_date = \'""" + quote_close_date + """\'
                                        , pur_due_date = DATE_ADD(LAST_DAY(CURRENT_DATE()), INTERVAL 30 DAY)
                                        , pur_amount_due = \'""" + str(bill_amount) + """\'
                                        , purchase_status = "UNPAID"
                                        , pur_notes = "AUTO CREATED WHEN MAINTENANCE WAS COMPLETED"
                                        , pur_description = "AUTO CREATED WHEN MAINTENANCE WAS COMPLETED"
                                        , pur_receiver = \'""" + bill_created_by + """\'
                                        , pur_payer = \'""" + pur_payer + """\'
                                        , pur_initiator = \'""" + bill_created_by + """\';
                                    """)

            # print("Query: ", purchaseQuery)
            queryResponse = db.execute(purchaseQuery, [], 'post')

        return queryResponse
