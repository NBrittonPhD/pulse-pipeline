
CREATE TABLE TRANSFORM_LOG (
    transform_id          VARCHAR(50) PRIMARY KEY,
    ingest_id             VARCHAR(50) NOT NULL,
    lake_table_name       VARCHAR(100),
    lake_variable_name    VARCHAR(100),
    transform_type        VARCHAR(50),
    old_value             TEXT,
    new_value             TEXT,
    transform_timestamp_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
