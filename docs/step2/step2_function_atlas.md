# Function Atlas — Step 2
## Batch Logging & File-Level Lineage

---

This reference lists all functions used in Step 2, grouped by purpose, with details about inputs, outputs, and responsibilities.

---

## Core Functions

### `log_batch_ingest()`

**File:** `r/steps/log_batch_ingest.R`

**Purpose:** Step 2A. Creates the `batch_log` row for a new ingest event and inserts one pending `ingest_file_log` row per discovered file.

**Signature:**
```r
log_batch_ingest(
    con,                    # DBIConnection (required)
    ingest_id,              # character: unique batch identifier (required)
    source_id,              # character: source identifier (required)
    source_type,            # character: source system type (required)
    file_paths              # character vector: paths to files to ingest (required)
)
```

**Returns:** TRUE (invisible)

**Side Effects:**
- Inserts one row into `governance.batch_log` with status `"started"`
- Inserts `"pending"` rows into `governance.ingest_file_log` (one per file, with `file_name` and `file_path`)
- Stops with error if `ingest_id` already exists in `batch_log`

---

### `ingest_batch()`

**File:** `r/steps/log_batch_ingest.R`

**Purpose:** Step 2B. Iterates through pending files, calls `ingest_one_file()` for each, updates file-level lineage, finalizes batch status, and optionally promotes to staging.

**Signature:**
```r
ingest_batch(
    con,                    # DBIConnection (required)
    ingest_id,              # character: batch identifier (required)
    raw_path,               # character: path to raw data directory (required)
    source_id,              # character: source identifier (required)
    source_type,            # character: source system type (required)
    type_decisions = NULL   # data.frame: type_decision_table for staging promotion (optional)
)
```

**Returns:** List with:
- `ingest_id`: character
- `status`: `"success"` / `"partial"` / `"error"`
- `n_files`: integer total files
- `n_success`: integer successful ingests
- `n_error`: integer failed ingests

**Side Effects:**
- Calls `ingest_one_file()` for each pending file
- Updates `governance.ingest_file_log` rows with results (`row_count`, `checksum`, `file_size_bytes`, `load_status`)
- Updates `governance.batch_log` with final status (`files_success`, `files_error`, `batch_completed_at_utc`)
- When `type_decisions` is provided: promotes each unique successfully ingested raw table to `staging.<lake_table>` via `promote_to_staging()`

---

### `ingest_one_file()`

**File:** `r/steps/ingest.R`

**Purpose:** Ingests a single CSV file into `raw.<lake_table>` using strict source-type enforcement and dictionary-based variable harmonization.

**Signature:**
```r
ingest_one_file(
    con,                    # DBIConnection (required)
    file_path,              # character: full path to CSV file (required)
    source_type             # character: source system type for strict filtering (required)
)
```

**Returns (success):**
```r
list(
    status          = "success",
    lake_table      = "table_name",
    row_count       = 100,
    file_size_bytes = 4096,
    checksum        = "md5_hash",
    error_message   = NULL
)
```

**Returns (error):**
```r
list(
    status          = "error",
    lake_table      = NA,
    row_count       = NA,
    file_size_bytes = NA,
    checksum        = NA,
    error_message   = "description"
)
```

**Side Effects:**
- Reads CSV via `vroom` with all columns as character (`cols(.default = "c")`)
- Normalizes headers via `normalize_name()`
- Loads and filters `reference.ingest_dictionary` by `source_type`
- Resolves `lake_table` from file name via dictionary `source_table_name` matching
- Maps source variables to lake variables using dictionary mappings
- Appends to `raw.<lake_table>` (creates table if missing via `align_df_to_raw_table()`)
- Computes MD5 checksum via `digest::digest()`

---

### `run_step2_batch_logging()`

**File:** `r/steps/run_step2_batch_logging.R`

**Purpose:** Step 2 wrapper executed by the pipeline runner. Derives `source_type` from `reference.ingest_dictionary`, calls `log_batch_ingest()` and `ingest_batch()`, and records step completion.

**Signature:**
```r
run_step2_batch_logging(
    con,                    # DBIConnection (required)
    ingest_id,              # character: unique batch identifier (required)
    settings = NULL         # list: pipeline settings (optional)
)
```

**Returns:** List (the return value from `ingest_batch()`)

**Side Effects:**
- Loads `source_id` from `config/source_params.yml` via `load_source_params()`
- Derives `source_type` by matching incoming file names against `reference.ingest_dictionary`
- Calls `log_batch_ingest()` with discovered files
- Calls `ingest_batch()` to perform ingestion (without `type_decisions` — staging promotion does not occur in the runner path)
- Writes `STEP_002` definition to `governance.pipeline_step` via `write_pipeline_step()`

**Note:** Staging promotion via `promote_to_staging()` only occurs when running `2_ingest_and_log_files.R` directly (where `type_decisions` is loaded from `type_decision_table.xlsx`). The runner path does not load type decisions and therefore does not promote to staging.

---

## Utility Functions

### `get_ingest_dict()`

**File:** `r/steps/ingest.R`

**Purpose:** Loads the full `reference.ingest_dictionary` table with consistently lowercased column names.

**Signature:**
```r
get_ingest_dict(
    con                     # DBIConnection (required)
)
```

**Returns:** data.frame from `reference.ingest_dictionary`

**Side Effects:**
- Reads from `reference.ingest_dictionary` table

---

### `pick_one()`

**File:** `r/steps/ingest.R`

**Purpose:** Utility to extract a single scalar value from a vector of potential lake table matches. Returns the first unique non-empty value, or `NA` if none found.

**Signature:**
```r
pick_one(
    x                       # character vector: candidate values (required)
)
```

**Returns:** Single character value or `NA_character_`

**Side Effects:** None (pure function)

---

### `align_df_to_raw_table()`

**File:** `r/steps/ingest.R`

**Purpose:** Defensive table alignment for robust append-mode ingestion. Ensures the data.frame columns match the existing raw table structure, adding missing columns to either side as needed.

**Signature:**
```r
align_df_to_raw_table(
    con,                    # DBIConnection (required)
    lake_table,             # character: target raw table name (required)
    df                      # data.frame: data to align (required)
)
```

**Returns:** Aligned data.frame with columns reordered to match table structure

**Side Effects:**
- Creates the raw table via `DBI::dbWriteTable()` if it does not exist (returns immediately)
- Adds missing columns to the existing table as TEXT via `ALTER TABLE ADD COLUMN`
- Adds missing columns to the data.frame as `NA_character_`

---

### `promote_to_staging()`

**File:** `r/build_tools/promote_to_staging.R`

**Purpose:** Promotes a single `raw.<lake_table>` to `staging.<lake_table>` by SQL CAST using target types from the type_decision_table. Called automatically by `ingest_batch()` when `type_decisions` is provided.

**Signature:**
```r
promote_to_staging(
    con,                    # DBIConnection (required)
    lake_table_name,        # character: table to promote (required)
    type_decisions          # data.frame: type_decision_table with final_type/suggested_type columns (required)
)
```

**Returns:**
```r
list(
    status        = "promoted" | "error",
    lake_table    = "table_name",
    n_rows        = 100,
    n_columns     = 15,
    n_typed       = 12,          # columns that got a non-TEXT type
    ddl           = "CREATE ...",  # the SQL statement used
    error_message = NULL | "description"
)
```

**Side Effects:**
- Verifies `raw.<table>` exists
- Gets column names from `information_schema.columns`
- Looks up target type per column (`final_type` -> `suggested_type` -> `TEXT` fallback)
- Drops `staging.<table>` if it exists
- Creates `staging.<table>` via `CREATE TABLE AS SELECT CAST(...)` from `raw.<table>`
- Executes within a transaction (BEGIN / DROP / CREATE / COMMIT); rolls back on error

---

## Dependency Graph

```
2_ingest_and_log_files.R (user script)
    ├── type_decisions (loaded from type_decision_table.xlsx)
    ├── log_batch_ingest() (batch + file logging)
    └── ingest_batch() (orchestrator)
            ├── ingest_one_file() (per-file ingest)
            │       ├── get_ingest_dict() (dictionary lookup)
            │       ├── pick_one() (lake table resolution)
            │       └── align_df_to_raw_table() (table alignment)
            └── promote_to_staging() (raw → staging type casting, optional)

run_step2_batch_logging() (runner wrapper)
    ├── load_source_params()
    ├── log_batch_ingest()
    ├── ingest_batch()
    └── write_pipeline_step()
```

---

## Database Tables

### `governance.batch_log`

Ingest-level summary. One row per batch ingestion event.

**Key Columns:**
- `ingest_id` (PK) — unique batch identifier
- `source_id` (FK) — registered source
- `ingest_timestamp` — when the batch was created
- `status` — `started` / `success` / `partial` / `error`
- `error_message` — error details (if applicable)
- `file_count`, `files_success`, `files_error`
- `batch_started_at_utc`, `batch_completed_at_utc`

### `governance.ingest_file_log`

File-level lineage. One row per file per batch.

**Key Columns:**
- `ingest_file_id` (PK, BIGSERIAL) — auto-incrementing surrogate key
- `ingest_id` (FK) — links to `governance.batch_log`
- `file_name`, `file_path` — file identity
- `lake_table_name` — resolved destination table
- `load_status` — `pending` / `success` / `error` / `skipped`
- `row_count`, `file_size_bytes`, `checksum`
- `logged_at_utc`, `completed_at_utc`
