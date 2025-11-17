
CREATE TABLE rule_execution_log (
    log_id           SERIAL PRIMARY KEY,
    ingest_id        VARCHAR(50),
    rule_id          VARCHAR(50),
    lake_table       VARCHAR(200),
    lake_variable    VARCHAR(200),
    failing_rows     INTEGER,
    executed_at_utc  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
