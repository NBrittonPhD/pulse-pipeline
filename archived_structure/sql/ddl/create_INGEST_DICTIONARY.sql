
CREATE TABLE IF NOT EXISTS reference.ingest_dictionary (
    source_table_name    VARCHAR(100) NOT NULL,
    source_variable_name VARCHAR(150) NOT NULL,
    lake_table_name      VARCHAR(100) NOT NULL,
    lake_variable_name   VARCHAR(150) NOT NULL,

    -- governance metadata
    created_at_utc       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified_utc    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by           VARCHAR(100) DEFAULT CURRENT_USER,

    PRIMARY KEY (
        source_table_name,
        source_variable_name
    )
);
