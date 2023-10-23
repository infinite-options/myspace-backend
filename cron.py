import datetime
from flask_restful import Resource
from data_pm import connect
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import json
import calendar
from calendar import monthrange


class RentPurchaseTest(Resource):
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
                # dt = datetime.datetime.now()
                dt = datetime.now()
                month = dt.month
                year = dt.year
                due_by = response['result'][i]['due_by']
                due_date = datetime(dt.year, dt.month + 1, due_by)
                due_date_2 = datetime(dt.year, dt.month, due_by)
                # due_date = datetime.datetime(dt.year, dt.month + 1, due_by)
                # due_date_2 = datetime.datetime(dt.year, dt.month, due_by)
                days_for_rent = (due_date - dt).days
                days_for_rent_2 = (due_date_2 - dt).days

                if days_for_rent == 10 or days_for_rent_2 == 10:
                    get_rec_st = db.select('purchases',
                                           {'pur_property_id': response['result'][i]['lease_property_id'],
                                            'pur_notes': f"RENT FOR {month} {year}"})

                    if (len(get_rec_st.get('result'))) == 0:
                        newRequest = {}
                        newRequestID = db.call('new_purchase_uid')['result'][0]['new_id']
                        newRequest['purchase_uid'] = newRequestID
                        newRequest['pur_timestamp'] = datetime.today()
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
                        newRequest['purchase_date'] = datetime(year, month, due_by)
                        newRequest['pur_due_date'] = datetime(year, month, due_by)

                        if days_for_rent == 10:
                            newRequest['purchase_date'] = datetime(year, month + 1, due_by)
                            newRequest['pur_due_date'] = datetime(year, month + 1, due_by)
                        db.insert('purchases', newRequest)
        return 200


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