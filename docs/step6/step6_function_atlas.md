# Function Atlas — Step 6
## Harmonization (Staging to Validated)

---

This reference lists all functions used in Step 6, grouped by purpose, with details about inputs, outputs, and responsibilities.

---

## Core Functions

### `harmonize_data()`

**File:** `r/steps/harmonize_data.R`

**Purpose:** Main Step 6 orchestrator. Validates inputs, verifies the ingest exists, determines target tables, calls `harmonize_table()` for each, writes an audit event, and returns a summary. Each target table is processed independently with `tryCatch` so a failure in one table does not abort the others.

**Signature:**
```r
harmonize_data(
    con,                          # DBIConnection (required)
    ingest_id,                    # character: batch identifier (required)
    target_tables = NULL,         # character vector: validated tables to process (NULL = all)
    source_type_filter = NULL     # character: limit to one source type (NULL = all)
)
```

**Returns:** List with:
- `tables_processed`: integer count of target tables processed
- `total_rows`: integer total rows inserted across all tables
- `sources_processed`: integer total source tables processed
- `by_table`: named list mapping table name to row count
- `status`: `"success"` or `"partial"`

**Side Effects:**
- Writes to `validated.*` tables (via `harmonize_table`)
- Writes to `governance.transform_log` (via `harmonize_table`)
- Writes to `governance.audit_log` (via `write_audit_event`)

---

### `harmonize_table()`

**File:** `r/harmonization/harmonize_table.R`

**Purpose:** Harmonize all source staging tables into one validated target table. Loads active mappings, groups by `(source_type, source_table)`, deletes prior data for idempotency, builds and executes INSERT queries, and logs each operation to `governance.transform_log`. Each source group is wrapped in `tryCatch`.

**Signature:**
```r
harmonize_table(
    con,                          # DBIConnection (required)
    target_table,                 # character: validated table name (required)
    ingest_id,                    # character: batch identifier (required)
    source_type_filter = NULL     # character: optional, limit to one source type
)
```

**Returns:** List with:
- `target_table`: character name of the validated table
- `rows_inserted`: integer total rows inserted
- `sources_processed`: integer count of source tables successfully processed
- `sources_skipped`: integer count of missing staging tables
- `sources_failed`: integer count of source tables that errored

**Side Effects:**
- Reads from `staging.*` tables
- Deletes prior rows from `validated.{target_table}` for this `ingest_id`
- Deletes prior rows from `governance.transform_log` for this `ingest_id` + target
- Writes to `validated.{target_table}` (insert)
- Writes to `governance.transform_log` (append)

---

## Harmonization Functions

### `sync_harmonization_map()`

**File:** `r/harmonization/sync_harmonization_map.R`

**Purpose:** Synchronize harmonization mappings from the metadata dictionary (`reference.metadata`) to `reference.harmonization_map`. Reads `validated_table_target` and `validated_variable_name` columns, expands comma-separated targets, auto-detects `transform_type` (direct/rename), and upserts to the mapping table. Preserves manually curated expression/coalesce/constant overrides.

**Signature:**
```r
sync_harmonization_map(
    con,                # DBIConnection (required)
    source = "metadata" # character: "metadata" or path to Excel override
)
```

**Returns:** List with:
- `count`: integer total mappings synced
- `tables_mapped`: integer distinct validated target tables
- `sources_mapped`: integer distinct source types

**Side Effects:**
- Reads from `reference.metadata`
- Writes to `reference.harmonization_map` (upsert via temp table + ON CONFLICT)

---

### `load_harmonization_map()`

**File:** `r/harmonization/load_harmonization_map.R`

**Purpose:** Load active harmonization mappings from `reference.harmonization_map` for a specific validated target table, with optional source_type filtering. Returns a tibble of column-level mappings.

**Signature:**
```r
load_harmonization_map(
    con,                  # DBIConnection (required)
    target_table,         # character: validated table name (required)
    source_type = NULL    # character: optional filter for a single source type
)
```

**Returns:** Tibble with columns: `map_id`, `source_type`, `source_table`, `source_column`, `target_table`, `target_column`, `transform_type`, `transform_expression`, `priority`.

**Side Effects:** Reads from database (SELECT only).

---

### `build_harmonization_query()`

**File:** `r/harmonization/build_harmonization_query.R`

**Purpose:** Build a SQL SELECT statement that transforms one staging table into the format expected by a validated table. Adds governance constants (`source_type`, `source_table`, `ingest_id`), maps each source column to its target, handles all 5 transform types, and applies safe type casting when staging and validated column types differ.

**Signature:**
```r
build_harmonization_query(
    con,            # DBIConnection (for identifier quoting and schema lookups)
    mappings,       # tibble: column mappings for one (source_type, source_table)
    source_table,   # character: staging table name
    source_type,    # character: CISIR, CLARITY, or TRAUMA_REGISTRY
    ingest_id       # character: batch identifier for tagging
)
```

**Returns:** Character string: a SQL SELECT statement ready to be wrapped with `INSERT INTO`.

**Side Effects:** Reads from `information_schema.columns` to verify source columns and detect type mismatches.

**Type Casting Logic:**
- Queries both staging and validated column types from `information_schema`
- When types differ, applies appropriate CAST
- TEXT-to-NUMERIC casts use regex guard: `CASE WHEN col ~ '^-?[0-9]*\.?[0-9]+...' THEN CAST(...) ELSE NULL END`
- Missing source columns are mapped as `NULL` with a warning

---

## Dependency Graph

```
6_harmonize_data.R (user script)
    ├── sync_harmonization_map.R (optional mapping sync)
    │       └── reference.metadata (reads)
    │       └── reference.harmonization_map (upserts)
    │
    └── harmonize_data.R (step orchestrator)
            ├── harmonize_table.R (per-table harmonizer)
            │       ├── load_harmonization_map.R (mapping loader)
            │       ├── build_harmonization_query.R (SQL query builder)
            │       ├── staging.* (reads)
            │       ├── validated.* (writes)
            │       └── governance.transform_log (writes)
            │
            ├── write_audit_event.R (audit logging)
            │
            └── profile_data.R (optional validated profiling, Step 5)
```

---

## Database Tables

### `reference.harmonization_map`

Column-level mappings from staging to validated tables. One row per `(source_type, source_table, source_column, target_table, target_column)`.

**Key Columns:**
- `map_id` (PK, serial)
- `source_type`, `source_table`, `source_column` — staging source identification
- `target_table`, `target_column` — validated target identification
- `transform_type` — one of: `direct`, `rename`, `expression`, `constant`, `coalesce`
- `transform_expression` — SQL expression for expression/constant/coalesce types
- `is_active` — enable/disable individual mappings
- `priority` — conflict resolution (lower = higher priority)
- Unique constraint on `(source_type, source_table, source_column, target_table, target_column)`

### `governance.transform_log`

Audit trail of harmonization operations. One row per source-to-target operation per ingest.

**Key Columns:**
- `transform_id` (PK, serial), `ingest_id`
- `source_schema`, `source_table`, `source_row_count`
- `target_schema`, `target_table`, `target_row_count`
- `operation_type` — one of: `insert`, `append`, `upsert`, `replace`
- `columns_mapped` — number of columns in the mapping
- `status` — one of: `success`, `partial`, `failed`
- `error_message`, `started_at`, `completed_at`, `duration_seconds`
- FK to `governance.batch_log` via `ingest_id`

### `validated.*` (23 tables)

Unified clinical domain tables. Each includes provenance columns:
- `source_type` — CISIR, CLARITY, or TRAUMA_REGISTRY
- `source_table` — exact staging table name
- `ingest_id` — batch identifier for lineage
