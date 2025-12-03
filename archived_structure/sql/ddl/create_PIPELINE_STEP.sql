CREATE TABLE governance.pipeline_step (
    step_id            VARCHAR(50) PRIMARY KEY,
    step_order         INTEGER NOT NULL,
    step_name          VARCHAR(100) NOT NULL,
    step_description   TEXT,
    step_type          VARCHAR(20),
    code_snippet       TEXT,
    enabled            BOOLEAN DEFAULT TRUE,
    created_at_utc     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified_utc  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
