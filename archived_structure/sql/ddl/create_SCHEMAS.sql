-- ------------------------------------------------------------
-- PULSE Data Lake: Schema Creation Script
-- Creates all core schemas used by the ingestion + QC pipeline.
-- Safe to run multiple times (IF NOT EXISTS).
-- ------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS validated;
CREATE SCHEMA IF NOT EXISTS governance;
CREATE SCHEMA IF NOT EXISTS reference;

COMMENT ON SCHEMA raw IS 'Landing zone for raw, unvalidated tables from ingestion.';
COMMENT ON SCHEMA staging IS 'Intermediate zone for type casting, shaping, harmonization.';
COMMENT ON SCHEMA validated IS 'Final governed zone used by downstream analytics.';
COMMENT ON SCHEMA governance IS 'System tables powering QC and metadata.';
COMMENT ON SCHEMA reference IS 'Lookup tables, mapping tables, and controlled vocabularies.';
