-- =============================================================================
-- create_METADATA_HISTORY.sql
-- =============================================================================
-- Purpose:      Track field-level changes to reference.metadata across versions.
--               Every time sync_metadata() runs, it compares the new dictionary
--               to the current database state and writes one row per field that
--               changed. This provides a full audit trail of what changed, when,
--               and from what value to what value.
--
-- Change types:
--   INITIAL  — First time a variable is loaded (no prior version)
--   ADD      — New variable added to dictionary
--   UPDATE   — Existing variable field value changed
--   REMOVE   — Variable removed from dictionary (soft-deleted in metadata)
--
-- Depends on:   reference schema must exist
-- Author:       Noel
-- Last Updated: 2026-01-30
-- =============================================================================

-- =============================================================================
-- CREATE TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS reference.metadata_history (

    -- Primary key
    history_id              SERIAL PRIMARY KEY,

    -- Version info
    version_number          INTEGER NOT NULL,

    -- What changed
    lake_table_name         TEXT NOT NULL,
    lake_variable_name      TEXT NOT NULL,
    source_type             TEXT,
    field_changed           TEXT NOT NULL,
    old_value               TEXT,
    new_value               TEXT,

    -- Change classification
    change_type             TEXT NOT NULL
                            CHECK (change_type IN ('INITIAL', 'ADD', 'UPDATE', 'REMOVE')),

    -- Audit fields
    changed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by              TEXT DEFAULT CURRENT_USER
);

-- =============================================================================
-- INDEXES FOR COMMON QUERIES
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_metadata_history_version
    ON reference.metadata_history(version_number);

CREATE INDEX IF NOT EXISTS idx_metadata_history_table
    ON reference.metadata_history(lake_table_name);

CREATE INDEX IF NOT EXISTS idx_metadata_history_change_type
    ON reference.metadata_history(change_type);

CREATE INDEX IF NOT EXISTS idx_metadata_history_changed_at
    ON reference.metadata_history(changed_at);

-- =============================================================================
-- TABLE AND COLUMN COMMENTS
-- =============================================================================
COMMENT ON TABLE reference.metadata_history IS
    'Audit trail of all field-level changes to reference.metadata. '
    'One row per field changed per variable per sync version.';

COMMENT ON COLUMN reference.metadata_history.change_type IS
    'INITIAL = first load, ADD = new variable, UPDATE = field value changed, REMOVE = variable soft-deleted';

COMMENT ON COLUMN reference.metadata_history.field_changed IS
    'Name of the column in reference.metadata that changed (e.g., data_type, variable_label).';
