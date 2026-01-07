# Governance — Step 3
## Schema Validation Engine

---

## Governance Objectives

Step 3 ensures schema compliance by validating raw table structures against governed expectations before data profiling and harmonization.

---

## Data Quality Controls

### Schema Version Management

- All expected schemas are versioned (`schema_version`)
- Version history preserved via `effective_from` / `effective_to` dates
- Only active schemas (`is_active = TRUE`) used for validation
- Historical schemas retained for audit and reproducibility

### Issue Classification

| Severity | Blocking | Examples |
|----------|----------|----------|
| `critical` | Yes | Missing required columns, type mismatch on PK fields |
| `warning` | No | Missing optional columns, unexpected extra columns |
| `info` | No | Column order drift, minor discrepancies |

### Halt Behavior

- `halt_on_error = TRUE` (default): Pipeline stops on critical issues
- `halt_on_error = FALSE`: All issues logged, pipeline continues
- Critical issues always recorded regardless of halt setting

---

## Audit Trail

### `reference.metadata`

- `synced_at`: When schema was synced from Excel
- `created_at`: Row creation timestamp
- `created_by`: Identifier of sync process

### `governance.structure_qc_table`

- `ingest_id`: Links to batch being validated
- `check_run_at`: When validation was executed
- `created_at`: Row creation timestamp
- `created_by`: Always "validate_schema"

---

## Reproducibility

### Deterministic Validation

1. Same `ingest_id` always validates same tables
2. Same `schema_version` always produces same expected schema
3. Issues keyed by `ingest_id + lake_table_name + lake_variable_name + issue_code`

### Re-validation

To re-validate a batch:
1. Delete existing issues: `DELETE FROM governance.structure_qc_table WHERE ingest_id = ?`
2. Re-run `validate_schema(con, ingest_id)`

---

## Compliance Checklist

- [ ] `reference.metadata` table exists with active schema definitions
- [ ] Schema version matches expected release
- [ ] All required columns defined with `is_required = TRUE`
- [ ] Primary keys marked with `is_primary_key = TRUE`
- [ ] `structure_qc_table` captures all detected issues
- [ ] Critical issues halt pipeline when configured
- [ ] Validation results queryable by `ingest_id`

---

## Related Governance Artifacts

| Artifact | Location | Purpose |
|----------|----------|---------|
| Expected Schema Dictionary | `reference/expected_schema_dictionary.xlsx` | Source of truth for schema definitions |
| Metadata Table | `reference.metadata` | Database-queryable schema definitions |
| Structure QC Table | `governance.structure_qc_table` | Issue audit log |
| Batch Log | `governance.batch_log` | Links ingest_id to source metadata |
