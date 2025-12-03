-- =============================================================================
-- PIPELINE_STEP
-- Defines the ordered pipeline steps the runner executes.
-- Fully schema-qualified: governance.pipeline_step
-- =============================================================================

CREATE TABLE IF NOT EXISTS governance.pipeline_step (
    step_id             VARCHAR(50) PRIMARY KEY,          -- stable ID (e.g., "STEP_001")
    step_order          INTEGER NOT NULL,                 -- execution order
    step_name           VARCHAR(100) NOT NULL,            -- short human-friendly name
    step_description    TEXT,                             -- descriptive text for docs / governance
    step_type           VARCHAR(20) NOT NULL,             -- "R", "SQL", or "RMD"
    code_snippet        TEXT NOT NULL,                    -- function call or file name
    enabled             BOOLEAN NOT NULL DEFAULT TRUE,    -- supports pausing a step
    created_at_utc      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified_utc   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Keep last_modified_utc in sync when rows change
CREATE OR REPLACE FUNCTION governance.trg_update_last_modified_pipeline_step()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_modified_utc = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_last_modified_pipeline_step
BEFORE UPDATE ON governance.pipeline_step
FOR EACH ROW
EXECUTE FUNCTION governance.trg_update_last_modified_pipeline_step();

-- Optional index to speed up ordered retrieval
CREATE INDEX IF NOT EXISTS pipeline_step_order_idx
    ON governance.pipeline_step (step_order);