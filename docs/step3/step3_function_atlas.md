# Function Atlas — Step 3
## Schema Validation Engine

---

This reference lists all functions used in Step 3, grouped by purpose, with details about inputs, outputs, and responsibilities.

---

## Core Functions

### `validate_schema()`

**File:** `r/steps/validate_schema.R`

**Purpose:** Main schema validation orchestrator. Compares all raw tables from an ingest batch against expected schema definitions in `reference.metadata`, writes issues to `governance.structure_qc_table`.

**Signature:**
```r
validate_schema(
    con,                    # DBIConnection (required)
    ingest_id,              # character: batch identifier (required)
    source_id = NULL,       # character: source identifier (optional, derived from batch_log)
    source_type = NULL,     # character: source type (optional, derived from metadata)
    halt_on_error = TRUE    # logical: stop on critical issues (default TRUE)
)
```

**Returns:** List with:
- `success`: logical (TRUE if no critical errors)
- `issues_count`: integer total issues
- `critical_count`: integer critical issues
- `warning_count`: integer warning issues
- `info_count`: integer info issues
- `tables_validated`: integer count of tables checked
- `issues`: tibble of all detected issues

**Side Effects:**
- Reads from `governance.batch_log`, `governance.ingest_file_log`, `reference.metadata`, `information_schema.columns`
- Writes issue rows to `governance.structure_qc_table`
- Stops with error if `halt_on_error = TRUE` and critical issues found

---

### `compare_fields()`

**File:** `r/utilities/compare_fields.R`

**Purpose:** Compare expected schema vs observed schema for a single table. Detects missing columns, extra columns, type mismatches, and target type discrepancies. Pure function — does not write to the database.

**Signature:**
```r
compare_fields(
    expected_schema,        # data.frame: expected field definitions (required)
    observed_schema,        # data.frame: observed field definitions (required)
    lake_table_name,        # character: table being validated (required)
    schema_version = NULL   # character: schema version identifier (optional, derived if NULL)
)
```

**Required columns in `expected_schema`:**
- `lake_table_name`, `lake_variable_name`, `data_type`, `is_required`, `target_type`

**Required columns in `observed_schema`:**
- `lake_table_name`, `lake_variable_name`, `data_type`, `udt_name`

**Returns:** List with:
- `status`: `"success"` (always, unless inputs invalid)
- `lake_table_name`: character scalar
- `schema_version`: character scalar
- `n_issues`: integer count of detected issues
- `issues`: tibble with issue details

**Issue Types Detected:**

| Issue Code | Issue Group | Severity | Blocking | Description |
|------------|-------------|----------|----------|-------------|
| `SCHEMA_MISSING_COLUMN` | structural | critical | yes | Required column not present in observed schema |
| `SCHEMA_UNEXPECTED_COLUMN` | structural | critical | yes | Column present in observed but not in expected schema |
| `SCHEMA_TYPE_MISMATCH` | dtype | warning | no | Data type differs between expected and observed |
| `TYPE_TARGET_MISSING` | dtype | warning | no | No target type defined in type_decision_table |
| `TYPE_TARGET_MISMATCH` | dtype | warning | no | Observed type does not match target staging type |

**Side Effects:** None (pure function)

---

## Cross-Step Dependencies

### `sync_metadata()`

**File:** `r/reference/sync_metadata.R`

**Purpose:** Step 4 function. Synchronizes the core metadata dictionary from Excel to `reference.metadata` with full version tracking and field-level audit trail. Used as an optional pre-step before validation.

**Signature:**
```r
sync_metadata(
    con,                        # DBIConnection (required)
    dict_path,                  # character: path to CURRENT_core_metadata_dictionary.xlsx (required)
    source_type_filter = NULL   # character: optional filter to specific source_type (optional)
)
```

**Returns:** List with sync statistics (`version_number`, `total_variables`, `adds`, `updates`, `removes`, `total_changes`, `rows_synced`)

---

## Dependency Graph

```
3_validate_schema.R (user script)
    ├── pulse-init-all.R (bootstrap)
    ├── sync_metadata() (optional pre-sync, Step 4)
    └── validate_schema() (step function)
            ├── governance.batch_log (verify ingest_id)
            ├── reference.metadata (expected schema)
            ├── governance.ingest_file_log (table discovery)
            ├── information_schema.columns (observed schema)
            ├── compare_fields() (per-table comparison)
            └── governance.structure_qc_table (issue output)
```

---

## Database Tables

### `governance.structure_qc_table`

Schema validation issues. One row per issue detected. Append-only.

**Key Columns:**
- `qc_issue_id` (PK, UUID) — auto-generated unique identifier
- `ingest_id` (FK) — links to `governance.batch_log`
- `source_id`, `source_type` — source context
- `schema_version` — which expected schema version was used
- `lake_table_name`, `lake_variable_name` — where the issue was found
- `issue_code` — machine-readable issue identifier
- `issue_type` — human-readable issue description
- `issue_group` — category (`structural`, `dtype`)
- `severity` — `critical`, `warning`, or `info`
- `is_blocking` — whether this issue should halt the pipeline
- `issue_message` — human-readable summary
- `expected_value`, `observed_value` — comparison details
- `expected_schema_hash`, `observed_schema_hash` — reproducibility hashes
- `check_context` — where the issue was detected (e.g., `variable_level`)
- `check_run_at`, `created_at` — timestamps
- `created_by` — always `"validate_schema"`
- `notes` — optional free-text for human remediation

### `reference.metadata`

Dictionary definitions synced from `CURRENT_core_metadata_dictionary.xlsx`. One row per (`lake_table_name`, `lake_variable_name`, `source_type`).

**Key Columns:**
- `lake_table_name`, `lake_variable_name`, `source_type` — composite key
- `data_type` — expected data type
- `target_type` — desired SQL type for staging
- `is_required` — whether column must exist
- `variable_label`, `variable_definition`, `value_labels` — documentation
- `version_number` — metadata version, incremented on each sync
- `is_active` — TRUE = active, FALSE = soft-deleted
- `created_at`, `updated_at` — timestamps
