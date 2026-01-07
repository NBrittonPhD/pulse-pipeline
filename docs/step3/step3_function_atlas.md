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

**Purpose:** Compare expected schema vs observed schema for a single table. Detects missing columns, extra columns, type mismatches, PK mismatches, and column order drift.

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
- `SCHEMA_MISSING_COLUMN`: Expected column not present
- `SCHEMA_UNEXPECTED_COLUMN`: Column present but not expected
- `SCHEMA_TYPE_MISMATCH`: Data type differs from expected
- `SCHEMA_PK_MISMATCH`: Primary key designation differs
- `SCHEMA_COLUMN_ORDER_DRIFT`: Column position differs from expected

---

### `sync_metadata()`

**File:** `r/reference/sync_metadata.R`

**Purpose:** Synchronize expected schema definitions from Excel to `reference.metadata` table.

**Signature:**
```r
sync_metadata(
    con,                    # DBIConnection (required)
    xlsx_path = NULL,       # character: path to Excel file (defaults to reference/)
    mode = "replace",       # character: "replace", "upsert", or "append"
    created_by = "sync_metadata"  # character: audit identifier
)
```

**Returns:** List with:
- `status`: "success" or "error"
- `rows_synced`: integer count of rows written
- `tables_synced`: integer count of distinct tables
- `schema_version`: character version synced
- `error_message`: NULL or error details

**Modes:**
- `replace`: Delete all existing rows, insert fresh
- `append`: Insert only, fail on duplicates
- `upsert`: Update existing, insert new (currently falls back to replace)

---

## Dependency Graph

```
3_validate_schema.R (user script)
    └── validate_schema.R (step function)
            ├── compare_fields.R (utility)
            └── sync_metadata.R (optional sync)
                    └── expected_schema_dictionary.xlsx
```

---

## Database Tables

### `reference.metadata`

Expected schema definitions. One row per variable per table per version.

**Key Columns:**
- `schema_version`, `effective_from`, `effective_to`
- `lake_table_name`, `lake_variable_name`
- `data_type`, `udt_name`, `is_nullable`, `is_required`, `is_primary_key`
- `is_active`, `synced_at`, `created_at`, `created_by`

### `governance.structure_qc_table`

Schema validation issues. One row per issue detected.

**Key Columns:**
- `ingest_id`, `source_id`, `source_type`
- `lake_table_name`, `lake_variable_name`
- `issue_code`, `issue_type`, `severity`, `is_blocking`
- `expected_value`, `observed_value`
- `check_run_at`, `created_at`, `created_by`
