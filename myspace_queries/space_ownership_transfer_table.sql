-- CREATE OWNERSHIP TRANSFER TABLE
CREATE TABLE IF NOT EXISTS space_prod.ownershipTransfer (
    transfer_id VARCHAR(50) PRIMARY KEY,
    property_id VARCHAR(100) NOT NULL,
    ownerId VARCHAR(100) NOT NULL,
    to_owner_id VARCHAR(100) NOT NULL,
    current_percent DECIMAL(5,2) NULL,
    proposed_percent DECIMAL(5,2) NOT NULL,
    updated_percent DECIMAL(5,2) NULL,
    transfer_status ENUM('PENDING', 'APPROVED', 'REJECTED','CANCELLED') DEFAULT 'PENDING',
    
    INDEX idx_property_id (property_id),
    INDEX idx_to_owner_id (to_owner_id),
    INDEX idx_transfer_status (transfer_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- STORED PROCEDURE TO GENERATE NEW TRANSFER ID
DELIMITER $$

DROP PROCEDURE IF EXISTS space_prod.new_transfer_uid$$

CREATE PROCEDURE space_prod.new_transfer_uid()
BEGIN
    SELECT 
        CONCAT('350-', LPAD(IFNULL(MAX(CAST(SUBSTRING(transfer_id, 5) AS UNSIGNED)), 0) + 1, 6, '0'))
    AS new_id
    FROM space_prod.ownershipTransfer;
END$$

DELIMITER ;

