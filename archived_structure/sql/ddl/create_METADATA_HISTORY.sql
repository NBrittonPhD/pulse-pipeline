
CREATE TABLE METADATA_HISTORY (
    history_id         VARCHAR(50) PRIMARY KEY,
    metadata_id        VARCHAR(50) REFERENCES METADATA(metadata_id),
    changed_at_utc     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by         VARCHAR(100),
    old_value_json     TEXT,
    new_value_json     TEXT
);
