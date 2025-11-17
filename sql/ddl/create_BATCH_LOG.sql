
CREATE TABLE BATCH_LOG (
    ingest_id          VARCHAR(50) PRIMARY KEY,
    source_id          VARCHAR(50),
    file_name          TEXT,
    file_size_bytes    BIGINT,
    checksum_md5       VARCHAR(50),
    row_count          BIGINT,
    ingestion_method   VARCHAR(50),
    ingested_at_utc    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
