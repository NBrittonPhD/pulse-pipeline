# Function Atlas — Step 3
## Schema Validation Engine

---

## Core Functions

### `validate_schema()`

**File:** `r/steps/validate_schema.R`

**Purpose:** Main schema validation orchestrator. Compares all raw tables from an ingest batch against expected schema definitions.

**Signature:**
```r
validate_schema(
    con,                    # DBIConnection (required)
    ingest_id,              # character: batch identifier (required)
    source_id = NULL,       # character: source identifier (optional, derived from batch_log)
    source_type = NULL,     # character: source type (optional)
    halt_on_error = TRUE    # logical: stop on critical issues
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
- Writes rows to `governance.structure_qc_table`

---

### `compare_fields()`

**File:** `r/utilities/compare_fields.R`

**Purpose:** Compare expected schema vs observed schema for a single table. Detects missing columns, extra columns, type mismatches, and target type discrepancies.

**Signature:**
```r
compare_fields(
    expected_schema,        # tibble: expected field definitions
    observed_schema,        # tibble: observed field definitions
    lake_table_name,        # character: table being validated
    schema_version = NULL   # character: schema version identifier
)
```

**Returns:** List with:
- `status`: "success" or "error"
- `n_issues`: integer count of issues
- `issues`: tibble with issue details

**Issue Types Detected:**
- `SCHEMA_MISSING_COLUMN`: Required column not present in observed schema
- `SCHEMA_UNEXPECTED_COLUMN`: Column present in observed but not in expected schema
- `SCHEMA_TYPE_MISMATCH`: Data type differs between expected and observed
- `TYPE_TARGET_MISMATCH`: Observed type does not match target staging type
- `TYPE_TARGET_MISSING`: No target type defined in type_decision_table

---

### `sync_metadata()`

**File:** `r/reference/sync_metadata.R`

**Purpose:** Synchronize the core metadata dictionary from Excel to `reference.metadata` with full version tracking and field-level audit trail.

**Signature:**
```r
sync_metadata(
    con,                        # DBIConnection (required)
    dict_path,                  # character: path to CURRENT_core_metadata_dictionary.xlsx
    source_type_filter = NULL   # character: optional filter to specific source_type
)
```

**Returns:** List with:
- `version_number`: integer new version number
- `total_variables`: integer count of variables synced
- `adds`: integer count of new variables (including initial)
- `updates`: integer count of updated variables
- `removes`: integer count of soft-deleted variables
- `total_changes`: integer total field-level changes
- `rows_synced`: integer count of rows written

**Behavior:**
- Compares new dictionary against current DB state via `compare_metadata()`
- Writes field-level changes to `reference.metadata_history`
- Upserts `reference.metadata` (INSERT new, UPDATE existing, soft-delete removed)
- Writes audit event to `governance.audit_log`

---

## Dependency Graph

```
3_validate_schema.R (user script)
    └── validate_schema.R (step function)
            ├── compare_fields.R (utility)
            └── sync_metadata.R (optional pre-sync, Step 4)
                    ├── load_metadata_dictionary.R
                    ├── compare_metadata.R
                    └── CURRENT_core_metadata_dictionary.xlsx
```

---

## Database Tables

### `reference.metadata`

Dictionary definitions synced from `CURRENT_core_metadata_dictionary.xlsx`. One row per (`lake_table_name`, `lake_variable_name`, `source_type`).

**Key Columns:**
- `lake_table_name`, `lake_variable_name`, `source_type`
- `data_type`, `target_type`, `is_required`
- `variable_label`, `variable_definition`, `value_labels`
- `version_number`, `is_active`, `created_at`, `updated_at`

### `governance.structure_qc_table`

Schema validation issues. One row per issue detected.

**Key Columns:**
- `ingest_id`, `source_id`, `source_type`
- `lake_table_name`, `lake_variable_name`
- `issue_code`, `issue_type`, `severity`, `is_blocking`
- `expected_value`, `observed_value`
- `check_run_at`, `created_at`, `created_by`
