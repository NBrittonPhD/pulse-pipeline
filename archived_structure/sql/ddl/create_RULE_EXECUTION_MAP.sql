
CREATE TABLE rule_execution_map (
    map_id            VARCHAR(50) PRIMARY KEY,
    rule_id           VARCHAR(50) REFERENCES rule_library(rule_id),
    lake_table        VARCHAR(200),
    lake_variable     VARCHAR(200),
    rule_params       JSONB,
    enabled           BOOLEAN DEFAULT TRUE,
    created_at_utc    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
