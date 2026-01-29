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
- Inserts one row into `governance.batch_log`
- Inserts `"pending"` rows into `governance.ingest_file_log` (one per file)
- Stops if `ingest_id` already exists in `batch_log`

---

### `ingest_batch()`

**File:** `r/steps/log_batch_ingest.R`

**Purpose:** Step 2B. Iterates through pending files, calls `ingest_one_file()` for each, updates file-level lineage, and finalizes the batch status.

**Signature:**
```r
ingest_batch(
    con,                    # DBIConnection (required)
    ingest_id,              # character: batch identifier (required)
    raw_path,               # character: path to raw data directory (required)
    source_id,              # character: source identifier (required)
    source_type             # character: source system type (required)
)
```

**Returns:** List with:
- `ingest_id`: character
- `status`: "success" / "partial" / "error"
- `n_files`: integer total files
- `n_success`: integer successful ingests
- `n_error`: integer failed ingests

**Side Effects:**
- Calls `ingest_one_file()` for each pending file
- Updates `governance.ingest_file_log` rows with results (row_count, checksum, file_size_bytes, load_status)
- Updates `governance.batch_log` with final status and counts

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
    status = "success",
    lake_table = "table_name",
    row_count = 100,
    file_size_bytes = 4096,
    checksum = "md5_hash"
)
```

**Returns (error):**
```r
list(
    status = "error",
    lake_table = NA,
    row_count = NA,
    file_size_bytes = NA,
    checksum = NA,
    error_message = "description"
)
```

**Side Effects:**
- Reads CSV via `vroom`
- Normalizes headers to lowercase
- Maps source variables to lake variables per `reference.ingest_dictionary`
- Appends to `raw.<lake_table>` (creates table if missing via `align_df_to_raw_table()`)
- Computes MD5 checksum via `digest`

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
- Creates the raw table if it does not exist
- Adds missing columns to the table as TEXT
- Adds missing columns to the data.frame as NA

---

## Dependency Graph

```
2_ingest_and_log_files.R (user script)
    └── log_batch_ingest() (batch + file logging)
    └── ingest_batch() (orchestrator)
            └── ingest_one_file() (per-file ingest)
                    ├── get_ingest_dict() (dictionary lookup)
                    └── align_df_to_raw_table() (table alignment)
```

---

## Database Tables

### `governance.batch_log`

Ingest-level summary. One row per batch ingestion event.

**Key Columns:**
- `ingest_id` (PK), `source_id`, `source_type`
- `file_count`, `files_success`, `files_error`
- `status` (success / partial / error)
- `batch_started_at_utc`, `batch_completed_at_utc`

### `governance.ingest_file_log`

File-level lineage. One row per file per batch.

**Key Columns:**
- `ingest_file_id` (PK), `ingest_id` (FK)
- `file_name`, `file_path`, `lake_table_name`
- `load_status` (pending / success / error)
- `row_count`, `file_size_bytes`, `checksum`
- `completed_at_utc`
