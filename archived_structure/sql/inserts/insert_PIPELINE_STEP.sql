-- ===============================================================
-- PIPELINE_STEP Population
-- Defines the full ordered pipeline for PULSE
-- ===============================================================

INSERT INTO governance.pipeline_step (
    step_id, step_order, step_name, step_description,
    step_type, code_snippet, enabled
)
VALUES
-- 1. Register Source
('STEP_001', 1, 'register_source',
 'Register source in SOURCE_REGISTRY, validate vocab, create directories, and write audit logs.',
 'R', 'run_step1_register_source()', TRUE),

-- 2. Log Batch Ingestion (governed ingestion + archival)
('STEP_002', 2, 'log_batch_ingest',
 'Create governed BATCH_LOG entries, compute checksums, archive raw files safely into date-stamped folders.',
 'R', 'log_batch_ingest()',TRUE),

-- 3. Schema Validation
('STEP_003', 3, 'validate_schema',
 'Validate schema vs METADATA; populate STRUCTURE_QC_TABLE.',
 'R', 'validate_schema()', TRUE),

-- 4. Data Profiling
('STEP_004', 4, 'profile_data',
 'Compute missingness, duplicates, and summary metrics; populate DATA_PROFILE.',
 'R', 'profile_data()', TRUE),

-- 5. Metadata Sync
('STEP_005', 5, 'sync_metadata',
 'Sync METADATA and METADATA_HISTORY with CORE_METADATA_DICTIONARY.xlsx.',
 'R', 'sync_metadata()', TRUE),

-- 6. Harmonization
('STEP_006', 6, 'harmonize_data',
 'Apply type casting, unit conversion, and reference table mapping; write validated tables.',
 'R', 'harmonize_data()', TRUE),

-- 7. Execute QC Rules
('STEP_007', 7, 'execute_qc_rules',
 'Run all governed rules from RULE_LIBRARY; populate RULE_EXECUTION_LOG.',
 'R', 'execute_qc_rules()', TRUE),

-- 8. Generate QC Packet
('STEP_008', 8, 'generate_qc_packet',
 'Create standardized QC packet (CSV/HTML/PDF) using rule + structure + profile outputs.',
 'R', 'generate_qc_packet()', TRUE),

-- 9. Finalize Release
('STEP_009', 9, 'finalize_release',
 'Assign release tag, record lineage, update RELEASE_LOG and AUDIT_LOG.',
 'R', 'finalize_release()', TRUE),

-- 10. Governance Docs
('STEP_010', 10, 'update_governance_docs',
 'Render SOP, dashboards, and compliance outputs using R Markdown.',
 'RMD', 'rmd/sop_template.Rmd', TRUE)
 
ON CONFLICT (step_id) DO UPDATE
SET
    step_order       = EXCLUDED.step_order,
    step_name        = EXCLUDED.step_name,
    step_description = EXCLUDED.step_description,
    step_type        = EXCLUDED.step_type,
    code_snippet     = EXCLUDED.code_snippet,
    enabled          = EXCLUDED.enabled,
    last_modified_utc = CURRENT_TIMESTAMP;
