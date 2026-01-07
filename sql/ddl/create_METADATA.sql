-- =============================================================================
-- TABLE: reference.metadata
-- =============================================================================
-- Purpose:
--   Stores the expected schema definitions for all lake tables. This table is
--   synced from expected_schema_dictionary.xlsx and serves as the authoritative
--   source for schema validation in Step 3.
--
--   One row per variable (column) per table per schema version.
--
-- Key Features:
--   - Schema versioning (schema_version, effective_from, effective_to)
--   - Complete column metadata (data_type, udt_name, is_nullable, etc.)
--   - Source lineage (source_type, source_table_name, source_variable_name)
--   - Reproducibility hashes (column_schema_hash, table_schema_hash)
--   - is_active flag for soft-deletes and version management
--
-- Dependencies:
--   - reference schema must exist
--   - Synced from reference/expected_schema_dictionary.xlsx
--
-- Governance:
--   - Rows should not be deleted; use is_active = FALSE for deprecation
--   - New schema versions create new rows; old versions remain for audit
--   - Used by validate_schema() in Step 3
--
-- Author: Noel
-- Last Updated: 2026-01-07
-- =============================================================================

CREATE TABLE IF NOT EXISTS reference.metadata (

    -- -------------------------------------------------------------------------
    -- Primary Key
    -- -------------------------------------------------------------------------
    metadata_id              SERIAL PRIMARY KEY,

    -- -------------------------------------------------------------------------
    -- Schema Versioning
    -- -------------------------------------------------------------------------
    schema_version           TEXT NOT NULL,
    effective_from           DATE NOT NULL,
    effective_to             DATE,

    -- -------------------------------------------------------------------------
    -- Table Identification
    -- -------------------------------------------------------------------------
    table_schema             TEXT NOT NULL DEFAULT 'raw',
    lake_table_name          TEXT NOT NULL,

    -- -------------------------------------------------------------------------
    -- Variable (Column) Identification
    -- -------------------------------------------------------------------------
    lake_variable_name       TEXT NOT NULL,

    -- -------------------------------------------------------------------------
    -- Data Type Information
    -- -------------------------------------------------------------------------
    data_type                TEXT NOT NULL,
    udt_name                 TEXT,
    length                   INTEGER,
    precision                INTEGER,
    scale                    INTEGER,
    default_value            TEXT,

    -- -------------------------------------------------------------------------
    -- Column Constraints
    -- -------------------------------------------------------------------------
    is_nullable              BOOLEAN DEFAULT TRUE,
    is_required              BOOLEAN DEFAULT FALSE,
    is_primary_key           BOOLEAN DEFAULT FALSE,
    ordinal_position         INTEGER,

    -- -------------------------------------------------------------------------
    -- Type Descriptor (human-readable type classification)
    -- -------------------------------------------------------------------------
    type_descriptor          TEXT,

    -- -------------------------------------------------------------------------
    -- Source Lineage
    -- -------------------------------------------------------------------------
    source_type              TEXT,
    source_table_name        TEXT,
    source_variable_name     TEXT,

    -- -------------------------------------------------------------------------
    -- Reproducibility Hashes
    -- -------------------------------------------------------------------------
    column_schema_hash       TEXT,
    table_schema_hash        TEXT,

    -- -------------------------------------------------------------------------
    -- Governance Metadata
    -- -------------------------------------------------------------------------
    is_active                BOOLEAN NOT NULL DEFAULT TRUE,
    synced_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by               TEXT NOT NULL DEFAULT 'sync_metadata',

    -- -------------------------------------------------------------------------
    -- Unique Constraint: One active row per variable per table per version
    -- -------------------------------------------------------------------------
    CONSTRAINT uq_metadata_variable UNIQUE (
        schema_version,
        lake_table_name,
        lake_variable_name,
        is_active
    )
);

-- =============================================================================
-- Indexes for Performance
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_metadata_table_name
    ON reference.metadata (lake_table_name);

CREATE INDEX IF NOT EXISTS idx_metadata_schema_version
    ON reference.metadata (schema_version);

CREATE INDEX IF NOT EXISTS idx_metadata_is_active
    ON reference.metadata (is_active);

CREATE INDEX IF NOT EXISTS idx_metadata_source_type
    ON reference.metadata (source_type);

-- =============================================================================
-- Comments for Documentation
-- =============================================================================

COMMENT ON TABLE reference.metadata IS
    'Expected schema definitions for all lake tables. Synced from expected_schema_dictionary.xlsx. Used by Step 3 schema validation.';

COMMENT ON COLUMN reference.metadata.schema_version IS
    'Version identifier for this schema definition (e.g., "2025.0")';

COMMENT ON COLUMN reference.metadata.is_required IS
    'TRUE if this column must exist in the raw table; missing required columns are ERROR severity';

COMMENT ON COLUMN reference.metadata.is_active IS
    'TRUE for current active schema; FALSE for deprecated/superseded versions';

COMMENT ON COLUMN reference.metadata.column_schema_hash IS
    'SHA-256 hash of column definition for change detection';

COMMENT ON COLUMN reference.metadata.table_schema_hash IS
    'SHA-256 hash of entire table schema for change detection';
