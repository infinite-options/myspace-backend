SELECT * FROM space_prod.users;
SELECT * FROM space_prod.user_profile;

SELECT * FROM space_prod.ownerProfileInfo;
SELECT * FROM space_prod.property_owner;
SELECT * FROM space_prod.properties;
SELECT * FROM space_prod.appliances;
SELECT * FROM space_prod.maintenanceRequests;
SELECT * FROM space_prod.maintenanceQuotes;

SELECT * FROM space_prod.tenantProfileInfo;
SELECT * FROM space_prod.lease_tenant;
SELECT * FROM space_prod.leases;
SELECT * FROM space_prod.leaseFees;
SELECT * FROM space_prod.leaseDocuments;

SELECT * FROM space_prod.businessProfileInfo;
SELECT * FROM space_prod.contracts;
SELECT * FROM space_prod.contractFees;

SELECT * FROM space_prod.purchases;
SELECT * FROM space_prod.payments;
SELECT * FROM space_prod.bills;
SELECT * FROM space_prod.lists;
SELECT * FROM space_prod.property_utility;

SELECT * FROM space_prod.o_details;
SELECT * FROM space_prod.p_details;
SELECT * FROM space_prod.pp_details;
SELECT * FROM space_prod.pp_status;
SELECT * FROM space_prod.t_details;
SELECT * FROM space_prod.user_profiles;

SELECT * FROM space_prod.tenantDocuments;






-- EXTRACT lease_rent info from leases Table
-- SIMPLE JSON TABLE EXPRESSION NOTE: MAY NEED $[*] IF THE DATA SET IS CONTAINED WITHIN []                
-- SELECT *
-- FROM skedul.test,
-- JSON_TABLE(test.JSON_VAR2, '$'
-- 	COLUMNS (
-- 		a FOR ORDINALITY,
--         id VARCHAR(40)  PATH '$.id')
--         ) AS r;

-- SELECT *
-- FROM space_prod.leases;

-- CREATE TABLE space_prod.leaseFees AS
-- SELECT a AS leaseFees_uid, lease_uid AS fees_lease_id
-- 	, lease_rent, of_, charge
-- , due_by
-- , late_by
-- , fee_name
-- , fee_type
-- , late_fee
-- , frequency
-- , available_topay
-- , perDay_late_fee
-- FROM space_prod.leases,
-- JSON_TABLE(leases.lease_rent, '$[*]'
-- 	COLUMNS (
-- 		a FOR ORDINALITY,
-- 		of_ VARCHAR(40)  PATH '$.of',
-- 		charge DECIMAL(6,2)  PATH '$.charge',
-- 		due_by INT  PATH '$.due_by',
-- 		late_by INT  PATH '$.late_by',
-- 		fee_name VARCHAR(40)  PATH '$.fee_name',
-- 		fee_type VARCHAR(40)  PATH '$.fee_type',
-- 		late_fee VARCHAR(40)  PATH '$.late_fee',
-- 		frequency VARCHAR(40)  PATH '$.frequency',
-- 		available_topay INT  PATH '$.available_topay',
-- 		perDay_late_fee DECIMAL(4,2)  PATH '$.perDay_late_fee')
--         ) as l;
--         
--         
--         
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000001' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000001');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000002' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000001');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000003' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000002');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000004' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000003');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000005' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000003');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000006' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000004');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000007' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000004');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000008' WHERE (`leaseFees_uid` = '3') and (`fees_lease_id` = '300-000004');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000009' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000005');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000010' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000005');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000011' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000006');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000012' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000006');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000013' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000008');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000014' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000008');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000015' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000009');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000017' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000010');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000016' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000009');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000018' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000010');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000019' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000011');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000020' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000011');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000021' WHERE (`leaseFees_uid` = '3') and (`fees_lease_id` = '300-000011');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000022' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000012');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000023' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000012');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000024' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000013');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000025' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000013');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000026' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000014');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000027' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000014');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000028' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000015');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000029' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000015');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000030' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000016');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000031' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000016');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000032' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000017');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000033' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000017');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000034' WHERE (`leaseFees_uid` = '1') and (`fees_lease_id` = '300-000018');
-- UPDATE `space`.`leaseFees` SET `leaseFees_uid` = '350-000035' WHERE (`leaseFees_uid` = '2') and (`fees_lease_id` = '300-000018');

-- PROPERTY MANAGEMENT AND OWNER LEASES
SELECT -- *,
	lease_uid, lease_property_id, lease_start, lease_end, lease_status, lease_early_end_date, lease_renew_status, move_out_date
    , property_owner_id, po_owner_percent, owner_first_name, owner_last_name, owner_phone_number, owner_email, owner_address, owner_unit, owner_city, owner_state, owner_zip
    , property_address, property_unit, property_city, property_state, property_zip, property_type
	, leaseFees_uid, fee_name, fee_type, charge, due_by, late_by, late_fee, perDay_late_fee, frequency, available_topay
    , ld_created_date, ld_type, ld_name, ld_description, ld_shared, ld_link
    , lt_tenant_id, lt_responsibility, tenant_first_name, tenant_last_name, tenant_email, tenant_phone_number
    , contract_uid, contract_status, business_uid
FROM space_prod.leases
LEFT JOIN space_prod.o_details ON property_id = lease_property_id
LEFT JOIN space_prod.properties ON property_uid = lease_property_id
LEFT JOIN space_prod.leaseFees ON lease_uid = fees_lease_id
LEFT JOIN space_prod.leaseDocuments ON lease_uid = ld_lease_id
LEFT JOIN space_prod.t_details ON lease_uid = lt_lease_id
LEFT JOIN space_prod.b_details ON contract_property_id = lease_property_id
WHERE lease_status = "ACTIVE"
	AND contract_status = "ACTIVE"
	AND fee_name = "RENT"
    AND ld_type = "LEASE"
    -- AND business_uid = "600-000003"

	AND lt_tenant_id = "350-000038"





-- EXTRACT lease_documents info from leases_documents Table

-- CREATE TABLE space_prod.leaseDocuments AS

-- SELECT a AS ld_uid, lease_uid AS ld_lease_id
-- , link
-- , name
-- , shared
-- , description
-- , created_date
-- FROM space_prod.leases,
-- JSON_TABLE(leases.lease_documents, '$[*]'
-- 	COLUMNS (
-- 		a FOR ORDINALITY,
-- 		link VARCHAR(2056)  PATH '$.link',
--         name VARCHAR(128)  PATH '$.name',
--         shared VARCHAR(10)  PATH '$.shared',
--         description VARCHAR(128)  PATH '$.description',
--         created_date DATE PATH '$.created_date')
--         ) as l;


