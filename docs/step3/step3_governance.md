# Governance — Step 3
## Schema Validation Engine

---

## Governance Objectives

Step 3 ensures schema compliance by validating raw table structures against governed expectations before data profiling and harmonization.

---

## Data Quality Controls

### Schema Version Management

- All expected schemas are versioned (`version_number` in `reference.metadata`)
- Field-level change history tracked in `reference.metadata_history`
- Only active variables (`is_active = TRUE`) used for validation
- Removed variables are soft-deleted (not physically deleted)
- Historical changes retained for audit and reproducibility

### Issue Classification

| Severity | Blocking | Examples |
|----------|----------|----------|
| `critical` | Yes | Missing required columns, unexpected extra columns |
| `warning` | No | Type mismatches, target type mismatches, missing target types |

### Halt Behavior

- `halt_on_error = TRUE` (default): Pipeline stops on critical issues
- `halt_on_error = FALSE`: All issues logged, pipeline continues
- Critical issues always recorded regardless of halt setting

---

## Audit Trail

### `reference.metadata`

- `version_number`: Metadata version, incremented on each sync
- `is_active`: TRUE = active variable, FALSE = soft-deleted
- `created_at`: Row creation timestamp
- `updated_at`: Last modified timestamp

### `governance.structure_qc_table`

- `ingest_id`: Links to batch being validated
- `check_run_at`: When validation was executed
- `created_at`: Row creation timestamp
- `created_by`: Always "validate_schema"

---

## Reproducibility

### Deterministic Validation

1. Same `ingest_id` always validates same tables
2. Same `version_number` always produces same expected schema
3. Issues keyed by `ingest_id + lake_table_name + lake_variable_name + issue_code`

### Re-validation

To re-validate a batch:
1. Delete existing issues: `DELETE FROM governance.structure_qc_table WHERE ingest_id = ?`
2. Re-run `validate_schema(con, ingest_id)`

---

## Compliance Checklist

- [ ] `reference.metadata` table exists with active schema definitions
- [ ] Version number matches expected release
- [ ] All required columns defined with `is_required = TRUE`
- [ ] `structure_qc_table` captures all detected issues
- [ ] Critical issues halt pipeline when configured
- [ ] Validation results queryable by `ingest_id`

---

## Related Governance Artifacts

| Artifact | Location | Purpose |
|----------|----------|---------|
| Core Metadata Dictionary | `reference/CURRENT_core_metadata_dictionary.xlsx` | Source of truth for variable definitions |
| Metadata Table | `reference.metadata` | Database-queryable dictionary definitions (synced by Step 4) |
| Metadata History | `reference.metadata_history` | Field-level change audit trail |
| Structure QC Table | `governance.structure_qc_table` | Issue audit log |
| Batch Log | `governance.batch_log` | Links ingest_id to source metadata |
