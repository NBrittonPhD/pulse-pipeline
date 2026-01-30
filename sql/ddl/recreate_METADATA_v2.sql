-- =============================================================================
-- recreate_METADATA_v2.sql
-- =============================================================================
-- Purpose:      Restructure reference.metadata from an observed-schema store
--               into a pure dictionary table synced from the core metadata
--               dictionary Excel file. This is a breaking migration:
--
--               DROPPED (observed-schema columns no longer stored here):
--                 schema_version, effective_from, effective_to, table_schema,
--                 udt_name, length, precision, scale, default_value,
--                 is_nullable, is_primary_key, ordinal_position,
--                 type_descriptor, column_schema_hash, table_schema_hash,
--                 synced_at
--
--               ADDED (dictionary definition columns):
--                 variable_label, variable_definition, value_labels,
--                 variable_unit, valid_min, valid_max, allowed_values,
--                 is_identifier, is_phi, validated_table_target,
--                 validated_variable_name, notes, needs_further_review,
--                 version_number, updated_at
--
--               KEPT (carried forward):
--                 lake_table_name, lake_variable_name, source_type,
--                 source_table_name, source_variable_name, data_type,
--                 target_type, is_required, is_active, created_at, created_by
--
-- WARNING:      This drops all existing data in reference.metadata.
--               The new sync_metadata() function will repopulate it from
--               CURRENT_core_metadata_dictionary.xlsx.
--
-- Depends on:   reference schema must exist
-- Author:       Noel
-- Last Updated: 2026-01-30
-- =============================================================================

-- =============================================================================
-- DROP EXISTING TABLE
-- =============================================================================
DROP TABLE IF EXISTS reference.metadata CASCADE;

-- =============================================================================
-- CREATE NEW TABLE
-- =============================================================================
CREATE TABLE reference.metadata (

    -- =========================================================================
    -- IDENTITY: Which variable in which table from which source
    -- =========================================================================
    lake_table_name         TEXT NOT NULL,
    lake_variable_name      TEXT NOT NULL,
    source_type             TEXT NOT NULL,
    source_table_name       TEXT,
    source_variable_name    TEXT,

    -- =========================================================================
    -- TYPE: Expected and target data types
    -- =========================================================================
    data_type               TEXT,
    target_type             TEXT,

    -- =========================================================================
    -- DESCRIPTION: Human-readable metadata about the variable
    -- =========================================================================
    variable_label          TEXT,
    variable_definition     TEXT,
    value_labels            TEXT,
    variable_unit           TEXT,

    -- =========================================================================
    -- VALIDATION: Constraints and allowed ranges
    -- =========================================================================
    valid_min               NUMERIC,
    valid_max               NUMERIC,
    allowed_values          TEXT,

    -- =========================================================================
    -- FLAGS: Classification markers
    -- =========================================================================
    is_identifier           BOOLEAN DEFAULT FALSE,
    is_phi                  BOOLEAN DEFAULT FALSE,
    is_required             BOOLEAN DEFAULT FALSE,

    -- =========================================================================
    -- MAPPING: Target location in validated schema
    -- =========================================================================
    validated_table_target  TEXT,
    validated_variable_name TEXT,

    -- =========================================================================
    -- NOTES: Free-text fields for governance
    -- =========================================================================
    notes                   TEXT,
    needs_further_review    TEXT,

    -- =========================================================================
    -- VERSION TRACKING
    -- =========================================================================
    version_number          INTEGER DEFAULT 1,
    is_active               BOOLEAN DEFAULT TRUE,

    -- =========================================================================
    -- AUDIT TIMESTAMPS
    -- =========================================================================
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by              TEXT DEFAULT CURRENT_USER,

    -- =========================================================================
    -- CONSTRAINTS
    -- =========================================================================
    PRIMARY KEY (lake_table_name, lake_variable_name, source_type)
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================
CREATE INDEX idx_metadata_version
    ON reference.metadata(version_number);

CREATE INDEX idx_metadata_active
    ON reference.metadata(is_active);

CREATE INDEX idx_metadata_source_type
    ON reference.metadata(source_type);

-- =============================================================================
-- TABLE AND COLUMN COMMENTS
-- =============================================================================
COMMENT ON TABLE reference.metadata IS
    'Dictionary definitions synced from CURRENT_core_metadata_dictionary.xlsx. '
    'Version-controlled with soft deletes (is_active = FALSE for removed variables). '
    'One row per (lake_table_name, lake_variable_name, source_type).';

COMMENT ON COLUMN reference.metadata.version_number IS
    'Metadata version number, incremented on each sync.';

COMMENT ON COLUMN reference.metadata.is_active IS
    'TRUE = active variable, FALSE = soft-deleted (removed from dictionary).';

COMMENT ON COLUMN reference.metadata.target_type IS
    'Target SQL type from type_decision_table. Populated by orchestrator, not by core dict sync.';
