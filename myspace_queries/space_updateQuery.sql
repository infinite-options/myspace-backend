SET SQL_SAFE_UPDATES = 0;

UPDATE space_dev.purchases
SET purchase_type = 'Management'
WHERE purchase_type = 'Property Management Fee';