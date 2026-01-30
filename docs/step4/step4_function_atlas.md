# Function Atlas — Step 4
## Metadata Synchronization

---

This reference lists all functions used in Step 4, grouped by purpose, with details about inputs, outputs, and responsibilities.

---

## Core Functions

### `sync_metadata()`

**File:** `r/reference/sync_metadata.R`

**Purpose:** Main Step 4 orchestrator. Loads the Excel dictionary, compares to current database state, writes change history, upserts metadata, and logs an audit event.

**Signature:**
```r
sync_metadata(
    con,                        # DBIConnection (required)
    dict_path,                  # character: path to CURRENT_core_metadata_dictionary.xlsx (required)
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

**Side Effects:**
- Writes to `reference.metadata` (upsert via temp table + INSERT ON CONFLICT)
- Appends to `reference.metadata_history` (field-level change records)
- Appends to `governance.audit_log` (sync event summary)

---

### `load_metadata_dictionary()`

**File:** `r/reference/load_metadata_dictionary.R`

**Purpose:** Load the Excel dictionary and standardize it for database synchronization. Validates required columns, standardizes Y/N fields to boolean, checks for duplicate composite keys.

**Signature:**
```r
load_metadata_dictionary(
    dict_path,                  # character: path to Excel dictionary file (required)
    source_type_filter = NULL   # character: optional filter to specific source_type
)
```

**Returns:** Tibble with standardized dictionary columns matching `reference.metadata` schema.

**Side Effects:** None (pure function).

---

### `compare_metadata()`

**File:** `r/utilities/compare_metadata.R`

**Purpose:** Compare new dictionary (from Excel) to current database state, returning one row per field-level change. Classifies each change as INITIAL, ADD, UPDATE, or REMOVE.

**Signature:**
```r
compare_metadata(
    new_dict,                   # tibble: from load_metadata_dictionary() (required)
    current_dict                # tibble: from database query (required, may be 0-row)
)
```

**Returns:** Tibble with columns:
- `lake_table_name`, `lake_variable_name`, `source_type`
- `field_changed`, `old_value`, `new_value`, `change_type`

**Side Effects:** None (pure function).

**Tracked Fields:**
`source_table_name`, `source_variable_name`, `data_type`, `variable_label`, `variable_definition`, `value_labels`, `variable_unit`, `valid_min`, `valid_max`, `allowed_values`, `is_identifier`, `is_phi`, `is_required`, `validated_table_target`, `validated_variable_name`, `notes`, `needs_further_review`

---

## Utility Functions

### `get_current_metadata_version()`

**File:** `r/reference/get_current_metadata_version.R`

**Purpose:** Return the current (maximum) version number from `reference.metadata`. Returns 0 if the table is empty.

**Signature:**
```r
get_current_metadata_version(
    con                         # DBIConnection (required)
)
```

**Returns:** Integer version number.

**Side Effects:** None (read-only query).

---

## Dependency Graph

```
4_sync_metadata.R (user script)
    └── sync_metadata.R (step function)
            ├── load_metadata_dictionary.R (Excel loader)
            ├── compare_metadata.R (field-level diff)
            ├── write_audit_event.R (audit logging)
            └── CURRENT_core_metadata_dictionary.xlsx (source of truth)
```

---

## Database Tables

### `reference.metadata`

Dictionary definitions synced from `CURRENT_core_metadata_dictionary.xlsx`. One row per (`lake_table_name`, `lake_variable_name`, `source_type`).

**Key Columns:**
- `lake_table_name`, `lake_variable_name`, `source_type` (composite PK)
- `source_table_name`, `source_variable_name`, `data_type`
- `variable_label`, `variable_definition`, `value_labels`
- `variable_unit`, `valid_min`, `valid_max`, `allowed_values`
- `is_identifier`, `is_phi`, `is_required`
- `validated_table_target`, `validated_variable_name`
- `notes`, `needs_further_review`
- `version_number`, `is_active`, `created_at`, `updated_at`

### `reference.metadata_history`

Field-level change audit trail. One row per field changed per variable per version.

**Key Columns:**
- `history_id` (PK, serial)
- `version_number`, `lake_table_name`, `lake_variable_name`, `source_type`
- `field_changed`, `old_value`, `new_value`
- `change_type` (INITIAL / ADD / UPDATE / REMOVE)
- `changed_at`, `changed_by`
