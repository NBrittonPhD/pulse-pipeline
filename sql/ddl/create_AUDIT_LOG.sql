
CREATE TABLE AUDIT_LOG (
    audit_id           VARCHAR(50) PRIMARY KEY,
    ingest_id          VARCHAR(50),
    action             VARCHAR(200),
    details            TEXT,
    executed_by        VARCHAR(100),
    executed_at_utc    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
