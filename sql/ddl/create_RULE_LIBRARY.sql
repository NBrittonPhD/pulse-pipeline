CREATE TABLE RULE_LIBRARY (
    rule_id            VARCHAR(50) PRIMARY KEY,
    rule_category      VARCHAR(50),
    rule_name          VARCHAR(200) NOT NULL,
    rule_description   TEXT,
    rule_severity      VARCHAR(20) NOT NULL,  -- WARNING or ERROR
    rule_expression    TEXT NOT NULL,          -- SQL or R expression
    created_at_utc     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified_utc  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active          BOOLEAN DEFAULT TRUE
);