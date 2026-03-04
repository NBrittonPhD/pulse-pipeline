# Governance — Step 3
## Schema Validation Engine

---

## Governance Objectives

Step 3 ensures schema compliance by validating raw table structures against governed expectations in `reference.metadata` before data profiling and harmonization. It produces a complete audit trail of structural issues, enabling reproducible validation and governed decision-making about data quality.

---

## Data Quality Controls

### Schema Source

Expected schemas are loaded from `reference.metadata`, which is synced from `reference/CURRENT_core_metadata_dictionary.xlsx` by Step 4 (`sync_metadata()`). The expected schema is filtered by `source_type` to prevent cross-source false positives.

### Schema Version Management

- All expected schemas are versioned (`version_number` in `reference.metadata`)
- Field-level change history tracked in `reference.metadata_history`
- Only active variables (`is_active = TRUE`) used for validation
- Removed variables are soft-deleted (not physically deleted)
- Historical changes retained for audit and reproducibility

### Issue Classification

| Severity | Blocking | Issue Codes |
|----------|----------|-------------|
| `critical` | Yes | `SCHEMA_MISSING_COLUMN`, `SCHEMA_UNEXPECTED_COLUMN` |
| `warning` | No | `SCHEMA_TYPE_MISMATCH`, `TYPE_TARGET_MISMATCH`, `TYPE_TARGET_MISSING` |

### Halt Behavior

- `halt_on_error = TRUE` (default): Pipeline stops immediately on critical issues
- `halt_on_error = FALSE`: All issues logged, pipeline continues
- Critical issues are always recorded regardless of halt setting

---

## Audit Trail

### `governance.structure_qc_table`

The primary governance artifact for Step 3. Append-only table storing all detected schema issues.

**Key Columns:**
- `qc_issue_id` (UUID PK) — unique issue identifier, auto-generated
- `ingest_id` (FK) — links to the batch being validated
- `source_id`, `source_type` — source context
- `schema_version` — which metadata version was used for comparison
- `lake_table_name`, `lake_variable_name` — where the issue was found
- `issue_code` — machine-readable identifier (e.g., `SCHEMA_MISSING_COLUMN`)
- `issue_type` — human-readable description
- `issue_group` — category (`structural` or `dtype`)
- `severity` — `critical`, `warning`, or `info`
- `is_blocking` — whether this issue should block the pipeline
- `issue_message` — human-readable summary
- `expected_value`, `observed_value` — comparison details
- `expected_schema_hash`, `observed_schema_hash` — reproducibility hashes
- `check_context` — detection context (e.g., `variable_level`)
- `check_run_at`, `created_at` — timestamps
- `created_by` — always `"validate_schema"`
- `notes` — optional free-text for human remediation

**Behavior:**
- Append-only: rows must never be updated or deleted (except for re-validation)
- Written by `validate_schema()` via `DBI::dbWriteTable(append = TRUE)`
- All issues include `ingest_id` for batch-scoped querying

### `reference.metadata`

Dictionary definitions used as the expected schema source.

**Step 3 Behavior:**
- Read-only during validation (written by Step 4's `sync_metadata()`)
- Filtered by `source_type` and `is_active = TRUE`
- `version_number` captured in `schema_version` for governance traceability

---

## Reproducibility

### Deterministic Validation

1. Same `ingest_id` always validates the same set of tables (derived from `governance.ingest_file_log`)
2. Same `version_number` in `reference.metadata` always produces the same expected schema
3. Issues keyed by `ingest_id` + `lake_table_name` + `lake_variable_name` + `issue_code`
4. Schema hashes (`expected_schema_hash`, `observed_schema_hash`) support independent verification

### Re-validation

To re-validate a batch:
1. Delete existing issues: `DELETE FROM governance.structure_qc_table WHERE ingest_id = ?`
2. Re-run `validate_schema(con, ingest_id)`

---

## Compliance Checklist

- [ ] `reference.metadata` table populated with active schema definitions
- [ ] Schema version number matches expected release
- [ ] All required columns defined with `is_required = TRUE`
- [ ] `governance.structure_qc_table` captures all detected issues
- [ ] Critical issues halt pipeline when `halt_on_error = TRUE`
- [ ] Validation results queryable by `ingest_id`
- [ ] All unit tests passing

---

## Related Governance Artifacts

| Artifact | Location | Purpose |
|----------|----------|---------|
| Structure QC Table | `governance.structure_qc_table` | Issue audit log |
| Metadata Table | `reference.metadata` | Expected schema definitions |
| Metadata History | `reference.metadata_history` | Field-level change audit trail |
| Core Metadata Dictionary | `reference/CURRENT_core_metadata_dictionary.xlsx` | Source of truth for variable definitions |
| Batch Log | `governance.batch_log` | Links ingest_id to source metadata |
| Ingest File Log | `governance.ingest_file_log` | Table discovery for validation |
| Structure QC DDL | `sql/ddl/create_STRUCTURE_QC_TABLE.sql` | Table creation script |
| Metadata DDL | `sql/ddl/recreate_METADATA_v2.sql` | Table creation script |
