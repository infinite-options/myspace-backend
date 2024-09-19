# from flask import request
from data_pm import connect, uploadImage, s3


def NextDueDate():
    print("In NextDueDate Query FUNCTION CALL")

    try:
        # Run query to find rents of ACTIVE leases
        with connect() as db:    
            response = db.execute("""
                    -- CALCULATE NEXT DUE DATE FOR RECURRING FEES
                    SELECT *
                    FROM (
                        SELECT lf.* 
                            , lease_uid, lease_property_id, lease_status, lease_assigned_contacts, lease_documents
                            , lt_lease_id, lt_tenant_id, lt_responsibility
                            , property_id, property_owner_id, po_owner_percent
                            , contract_uid, contract_property_id, contract_business_id, contract_fees, contract_status 
                            -- FIND NEXT DUE DATE
                            , DATE_FORMAT(
                                CASE 
                                    WHEN frequency = 'Monthly' THEN 
                                        IF(CURDATE() <= STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d'), 
                                            STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d'), 
                                            DATE_ADD(STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d'), INTERVAL 1 MONTH)
                                        )
                                    WHEN frequency = 'Semi-Monthly' THEN 
                                        CASE 
                                            WHEN CURDATE() <= STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d') THEN 
                                                STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d')
                                            WHEN CURDATE() <= STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by + 15), '%Y-%m-%d') THEN 
                                                STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by + 15), '%Y-%m-%d')
                                            ELSE 
                                                DATE_ADD(STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()) + 1, '-', due_by), '%Y-%m-%d'), INTERVAL 0 MONTH)
                                        END
                                    WHEN frequency = 'Quarterly' THEN 
                                        CASE 
                                            WHEN CURDATE() <= STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d') THEN 
                                                STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d')
                                            ELSE 
                                                DATE_ADD(STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d'), INTERVAL 3 MONTH)
                                        END
                                    WHEN frequency = 'Semi-Annually' THEN 
                                        CASE 
                                            WHEN CURDATE() <= STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d') THEN 
                                                STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d')
                                            ELSE 
                                                DATE_ADD(STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()), '-', due_by), '%Y-%m-%d'), INTERVAL 6 MONTH)
                                        END
                                    WHEN frequency = 'Annually' THEN
                                        IF(CURDATE() <= STR_TO_DATE(due_by_date, '%m-%d-%Y'), 
                                            STR_TO_DATE(due_by_date, '%m-%d-%Y'), 
                                            DATE_ADD(STR_TO_DATE(due_by_date, '%m-%d-%Y'), INTERVAL 1 YEAR)
                                        )
                                    WHEN frequency = 'Weekly' THEN 
                                        DATE_ADD(CURDATE(), INTERVAL (due_by - DAYOFWEEK(CURDATE()) + 7) % 7 DAY)
                                    WHEN frequency = 'Bi-Weekly' THEN
                                        DATE_ADD(STR_TO_DATE(due_by_date, '%m-%d-%Y'), INTERVAL (FLOOR(DATEDIFF(CURDATE(), STR_TO_DATE(due_by_date, '%m-%d-%Y')) / 14) + 1) * 14 DAY)
                                END, '%m-%d-%Y') AS next_due_date
                        FROM (
                            SELECT * FROM space.leases WHERE lease_status = 'ACTIVE'
                            ) AS l
                        LEFT JOIN (
                            SELECT * FROM space.leaseFees WHERE frequency != 'One Time'
                            ) AS lf ON fees_lease_id = lease_uid  				-- get lease fees
                        LEFT JOIN space.lease_tenant ON fees_lease_id = lt_lease_id            	-- get tenant responsible for rent
                        LEFT JOIN space.property_owner ON lease_property_id = property_id      	-- get property owner and ownership percentage
                        LEFT JOIN (
                            SELECT * FROM space.contracts WHERE contract_status = 'ACTIVE'
                            ) AS c ON lease_property_id = contract_property_id  				-- to make sure contract is active
                        ) AS ndd
                    LEFT JOIN space.purchases ON lease_property_id = pur_property_id
                        AND fee_name = pur_notes
                        AND charge = pur_amount_due
                        AND lt_tenant_id = pur_payer
                        AND STR_TO_DATE(next_due_date, '%m-%d-%Y') = STR_TO_DATE(pur_due_date, '%m-%d-%Y')
                    """)
            
            # print("Function Query Complete")
            # print("THis is the Function response: ", response)
        return response
    except:
        print("Error in NextDueDate Query ")

