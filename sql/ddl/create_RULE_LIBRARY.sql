
CREATE TABLE rule_library (
    rule_id            VARCHAR(50) PRIMARY KEY,
    rule_name          VARCHAR(200) NOT NULL,
    rule_description   TEXT,
    rule_category      VARCHAR(50),
    rule_type          VARCHAR(20),          -- SQL or R
    rule_severity      VARCHAR(20),          -- warning/error/info
    rule_sql           TEXT,                 -- SQL template for QC
    enabled            BOOLEAN DEFAULT TRUE,
    created_at_utc     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified_utc  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
