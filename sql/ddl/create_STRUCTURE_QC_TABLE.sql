
CREATE TABLE STRUCTURE_QC_TABLE (
    qc_id              VARCHAR(50) PRIMARY KEY,
    ingest_id          VARCHAR(50) NOT NULL,
    lake_table_name    VARCHAR(100) NOT NULL,
    variable_name      VARCHAR(100),
    issue_type         VARCHAR(50),
    expected_value     TEXT,
    observed_value     TEXT,
    qc_timestamp_utc   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
