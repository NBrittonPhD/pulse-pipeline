-- =============================================================================
-- create_HARMONIZATION_MAP.sql
-- =============================================================================
-- Purpose:      Create reference.harmonization_map table for defining column
--               mappings from staging tables to validated tables.
--
-- Schema:       reference
-- Grain:        One row per (source_type, source_table, source_column,
--               target_table, target_column) mapping
--
-- Population:   Populated by sync_harmonization_map() which reads
--               reference.metadata (synced from
--               CURRENT_core_metadata_dictionary.xlsx) and uses
--               validated_table_target and validated_variable_name
--               as the authoritative mapping source.
--
-- Dependencies: reference schema must exist
--
-- Author:       Noel
-- Last Updated: 2026-02-04
-- =============================================================================

CREATE TABLE IF NOT EXISTS reference.harmonization_map (
    -- Primary key
    map_id                   SERIAL PRIMARY KEY,

    -- Source identification (from reference.metadata)
    source_type              TEXT NOT NULL,       -- CISIR, CLARITY, TRAUMA_REGISTRY
    source_table             TEXT NOT NULL,       -- staging table (= lake_table_name)
    source_column            TEXT NOT NULL,       -- staging column (= lake_variable_name)

    -- Target identification (from metadata dictionary)
    target_table             TEXT NOT NULL,       -- validated table (= validated_table_target)
    target_column            TEXT NOT NULL,       -- validated column (= validated_variable_name)

    -- Transformation specification
    transform_type           TEXT NOT NULL DEFAULT 'direct'
        CHECK (transform_type IN ('direct', 'rename', 'coalesce', 'expression', 'constant')),
    transform_expression     TEXT,                -- SQL expression when transform_type = 'expression'

    -- Governance
    is_active                BOOLEAN DEFAULT TRUE,
    priority                 INTEGER DEFAULT 100, -- Lower = higher priority for conflicts
    notes                    TEXT,

    -- Audit
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint enables upsert (ON CONFLICT DO UPDATE)
    CONSTRAINT unique_source_target
        UNIQUE (source_type, source_table, source_column, target_table, target_column)
);

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_harm_map_source
    ON reference.harmonization_map (source_type, source_table);

CREATE INDEX IF NOT EXISTS idx_harm_map_target
    ON reference.harmonization_map (target_table);

CREATE INDEX IF NOT EXISTS idx_harm_map_active
    ON reference.harmonization_map (is_active);

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE reference.harmonization_map IS
'Defines column mappings from staging tables to validated tables for cross-source harmonization. Populated from reference.metadata (CURRENT_core_metadata_dictionary.xlsx) via sync_harmonization_map().';

COMMENT ON COLUMN reference.harmonization_map.source_type IS
'Data source: CISIR, CLARITY, or TRAUMA_REGISTRY. From reference.metadata.source_type.';

COMMENT ON COLUMN reference.harmonization_map.source_table IS
'Staging table name. From reference.metadata.lake_table_name.';

COMMENT ON COLUMN reference.harmonization_map.source_column IS
'Staging column name. From reference.metadata.lake_variable_name.';

COMMENT ON COLUMN reference.harmonization_map.target_table IS
'Validated table name. From reference.metadata.validated_table_target.';

COMMENT ON COLUMN reference.harmonization_map.target_column IS
'Validated column name. From reference.metadata.validated_variable_name.';

COMMENT ON COLUMN reference.harmonization_map.transform_type IS
'Mapping type: direct (same name), rename (different name), coalesce (multi-source merge), expression (SQL computation), constant (literal value).';

COMMENT ON COLUMN reference.harmonization_map.transform_expression IS
'SQL expression used when transform_type is expression, coalesce, or constant. NULL for direct/rename.';

COMMENT ON COLUMN reference.harmonization_map.is_active IS
'Enable/disable individual mappings without deleting them.';

COMMENT ON COLUMN reference.harmonization_map.priority IS
'Conflict resolution priority. Lower number = higher priority. Default 100.';
