
CREATE TABLE RELEASE_LOG (
    release_id         VARCHAR(50) PRIMARY KEY,
    ingest_id          VARCHAR(50),
    release_tag        VARCHAR(100),
    release_timestamp_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes              TEXT
);
