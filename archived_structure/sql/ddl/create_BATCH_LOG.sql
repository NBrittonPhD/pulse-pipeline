
CREATE TABLE batch_log (
    ingest_id          VARCHAR(100) PRIMARY KEY,
    source_id          VARCHAR(20) NOT NULL,
    file_name          VARCHAR(500) NOT NULL,
    file_size_bytes    BIGINT CHECK (file_size_bytes >= 0),
    sha256_checksum    VARCHAR(200) NOT NULL,
    row_count_raw      BIGINT CHECK (row_count_raw >= 0),
    ingested_at_utc    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ingested_by        VARCHAR(100) NOT NULL,
    ingest_method      VARCHAR(20) NOT NULL CHECK (ingest_method IN ('push','pull','api_sync','manual_upload')),
    load_status        VARCHAR(20) NOT NULL CHECK (load_status IN ('success','warning','failed')),
    error_message      TEXT,
    validated_flag     BOOLEAN DEFAULT FALSE,
    archived_flag      BOOLEAN NOT NULL DEFAULT FALSE,

    -- foreign key to source_registry
    CONSTRAINT fk_source
        FOREIGN KEY (source_id) REFERENCES source_registry(source_id)
);

