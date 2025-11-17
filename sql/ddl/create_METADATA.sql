
CREATE TABLE METADATA (
    metadata_id        VARCHAR(50) PRIMARY KEY,
    table_name         VARCHAR(100) NOT NULL,
    variable_name      VARCHAR(100) NOT NULL,
    variable_label     TEXT,
    variable_definition TEXT,
    variable_unit      VARCHAR(50),
    data_type          VARCHAR(50),
    allowed_values     TEXT,
    missing_value_code TEXT,
    status             VARCHAR(50),
    notes              TEXT,
    created_at_utc     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified_utc  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
