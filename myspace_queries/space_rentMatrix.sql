SELECT -- *
	property_id, property_address, property_unit, property_owner_id,
    IF(m_amount_due IS NULL, "", m_amount_due) AS month_next,
	IF(m0_amount_due IS NULL, "", m0_amount_due) AS month_current,
	IF(m1_amount_due IS NULL, "", m1_amount_due) AS month1,
	IF(m2_amount_due IS NULL, "", m2_amount_due) AS month2,
	IF(m3_amount_due IS NULL, "", m3_amount_due) AS month3,
	IF(m4_amount_due IS NULL, "", m4_amount_due) AS month4,
	IF(m5_amount_due IS NULL, "", m5_amount_due) AS month5,
	IF(m6_amount_due IS NULL, "", m6_amount_due) AS month6,
	IF(m7_amount_due IS NULL, "", m7_amount_due) AS month7,
	IF(m8_amount_due IS NULL, "", m8_amount_due) AS month8,
	IF(m9_amount_due IS NULL, "", m9_amount_due) AS month9,
	IF(m10_amount_due IS NULL, "", m10_amount_due) AS month10,
	IF(m11_amount_due IS NULL, "", m11_amount_due) AS month11
FROM space.p_details
LEFT JOIN (
	SELECT -- *,
		pur_property_id, purchase_type, pur_due_date, SUM(pur_amount_due) AS m_amount_due, pur_payer
	FROM space.pp_status
	WHERE STR_TO_DATE(pur_due_date, '%m-%d-%Y') BETWEEN DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL -1 MONTH), '%Y-%m-01') AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL -1 MONTH))
		AND purchase_type = 'Rent' AND pur_payer LIKE '350%'
	GROUP BY pur_property_id
) AS m ON m.pur_property_id = property_id
LEFT JOIN (
	SELECT -- *,
		pur_property_id, purchase_type, pur_due_date, SUM(pur_amount_due) AS m0_amount_due, pur_payer
	FROM space.pp_status
	WHERE STR_TO_DATE(pur_due_date, '%m-%d-%Y') BETWEEN DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 0 MONTH), '%Y-%m-01') AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 0 MONTH))
		AND purchase_type = 'Rent' AND pur_payer LIKE '350%'
	GROUP BY pur_property_id
) AS m0 ON m0.pur_property_id = property_id
LEFT JOIN (
	SELECT -- *,
		pur_property_id, purchase_type, pur_due_date, SUM(pur_amount_due) AS m1_amount_due, pur_payer
	FROM space.pp_status
	WHERE STR_TO_DATE(pur_due_date, '%m-%d-%Y') BETWEEN DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m-01') AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 MONTH))
		AND purchase_type = 'Rent' AND pur_payer LIKE '350%'
	GROUP BY pur_property_id
) AS m1 ON m1.pur_property_id = property_id
LEFT JOIN (
	SELECT -- *,
		pur_property_id, purchase_type, pur_due_date, SUM(pur_amount_due) AS m2_amount_due, pur_payer
	FROM space.pp_status
	WHERE STR_TO_DATE(pur_due_date, '%m-%d-%Y') BETWEEN DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 2 MONTH), '%Y-%m-01') AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 2 MONTH))
		AND purchase_type = 'Rent' AND pur_payer LIKE '350%'
	GROUP BY pur_property_id
) AS m2 ON m2.pur_property_id = property_id
LEFT JOIN (
	SELECT -- *,
		pur_property_id, purchase_type, pur_due_date, SUM(pur_amount_due) AS m3_amount_due, pur_payer
	FROM space.pp_status
	WHERE STR_TO_DATE(pur_due_date, '%m-%d-%Y') BETWEEN DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 3 MONTH), '%Y-%m-01') AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 3 MONTH))
		AND purchase_type = 'Rent' AND pur_payer LIKE '350%'
	GROUP BY pur_property_id
) AS m3 ON m3.pur_property_id = property_id
LEFT JOIN (
	SELECT -- *,
		pur_property_id, purchase_type, pur_due_date, SUM(pur_amount_due) AS m4_amount_due, pur_payer
	FROM space.pp_status
	WHERE STR_TO_DATE(pur_due_date, '%m-%d-%Y') BETWEEN DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 4 MONTH), '%Y-%m-01') AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 4 MONTH))
		AND purchase_type = 'Rent' AND pur_payer LIKE '350%'
	GROUP BY pur_property_id
) AS m4 ON m4.pur_property_id = property_id
LEFT JOIN (
	SELECT -- *,
		pur_property_id, purchase_type, pur_due_date, SUM(pur_amount_due) AS m5_amount_due, pur_payer
	FROM space.pp_status
	WHERE STR_TO_DATE(pur_due_date, '%m-%d-%Y') BETWEEN DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 5 MONTH), '%Y-%m-01') AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 5 MONTH))
		AND purchase_type = 'Rent' AND pur_payer LIKE '350%'
	GROUP BY pur_property_id
) AS m5 ON m5.pur_property_id = property_id
LEFT JOIN (
	SELECT -- *,
		pur_property_id, purchase_type, pur_due_date, SUM(pur_amount_due) AS m6_amount_due, pur_payer
	FROM space.pp_status
	WHERE STR_TO_DATE(pur_due_date, '%m-%d-%Y') BETWEEN DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 6 MONTH), '%Y-%m-01') AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 6 MONTH))
		AND purchase_type = 'Rent' AND pur_payer LIKE '350%'
	GROUP BY pur_property_id
) AS m6 ON m6.pur_property_id = property_id
LEFT JOIN (
	SELECT -- *,
		pur_property_id, purchase_type, pur_due_date, SUM(pur_amount_due) AS m7_amount_due, pur_payer
	FROM space.pp_status
	WHERE STR_TO_DATE(pur_due_date, '%m-%d-%Y') BETWEEN DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 7 MONTH), '%Y-%m-01') AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 7 MONTH))
		AND purchase_type = 'Rent' AND pur_payer LIKE '350%'
	GROUP BY pur_property_id
) AS m7 ON m7.pur_property_id = property_id
LEFT JOIN (
	SELECT -- *,
		pur_property_id, purchase_type, pur_due_date, SUM(pur_amount_due) AS m8_amount_due, pur_payer
	FROM space.pp_status
	WHERE STR_TO_DATE(pur_due_date, '%m-%d-%Y') BETWEEN DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 8 MONTH), '%Y-%m-01') AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 8 MONTH))
		AND purchase_type = 'Rent' AND pur_payer LIKE '350%'
	GROUP BY pur_property_id
) AS m8 ON m8.pur_property_id = property_id
LEFT JOIN (
	SELECT -- *,
		pur_property_id, purchase_type, pur_due_date, SUM(pur_amount_due) AS m9_amount_due, pur_payer
	FROM space.pp_status
	WHERE STR_TO_DATE(pur_due_date, '%m-%d-%Y') BETWEEN DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 9 MONTH), '%Y-%m-01') AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 9 MONTH))
		AND purchase_type = 'Rent' AND pur_payer LIKE '350%'
	GROUP BY pur_property_id
) AS m9 ON m9.pur_property_id = property_id
LEFT JOIN (
	SELECT -- *,
		pur_property_id, purchase_type, pur_due_date, SUM(pur_amount_due) AS m10_amount_due, pur_payer
	FROM space.pp_status
	WHERE STR_TO_DATE(pur_due_date, '%m-%d-%Y') BETWEEN DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 10 MONTH), '%Y-%m-01') AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 10 MONTH))
		AND purchase_type = 'Rent' AND pur_payer LIKE '350%'
	GROUP BY pur_property_id
) AS m10 ON m10.pur_property_id = property_id
LEFT JOIN (
	SELECT -- *,
		pur_property_id, purchase_type, pur_due_date, SUM(pur_amount_due) AS m11_amount_due, pur_payer
	FROM space.pp_status
	WHERE STR_TO_DATE(pur_due_date, '%m-%d-%Y') BETWEEN DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 11 MONTH), '%Y-%m-01') AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 11 MONTH))
		AND purchase_type = 'Rent' AND pur_payer LIKE '350%'
	GROUP BY pur_property_id
) AS m11 ON m11.pur_property_id = property_id
-- WHERE property_owner_id = '110-000003'