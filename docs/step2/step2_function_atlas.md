
---

# ⭐ **2. step2_function_atlas.md**

```md
# Function Atlas — Step 2  
## Batch Logging & File-Level Lineage

---

This reference lists all functions used in Step 2, grouped by purpose, with details about inputs, outputs, and responsibilities.

---

## `log_batch_ingest()`

**File:** `r/steps/log_batch_ingest.R`  
**Purpose:**  
Create the batch_log row and one pending ingest_file_log row per file.

**Inputs:**
- `con`
- `ingest_id`
- `source_id`
- `source_type`
- `file_paths`

**Outputs:**  
- Inserts into `governance.batch_log`
- Inserts `"pending"` rows into `governance.ingest_file_log`

---

## `ingest_batch()`

**File:** `r/steps/log_batch_ingest.R`  
**Purpose:**  
Iterate through pending files, ingest them, update lineage, and finalize the batch.

**Outputs:**  
A list containing:
- `ingest_id`
- `status` (success / partial / error)
- `n_files`
- `n_success`
- `n_error`

**Side Effects:**  
- Updates ingest_file_log rows  
- Updates batch_log summary fields  

---

## `ingest_one_file()`

**File:** `r/action/ingest.R`  
**Purpose:**  
Ingest a *single* CSV file using strict source typing and dictionary-based mapping.

**Key Behaviors:**
- Enforces `source_type` match
- Harmonizes variable names via dictionary
- Appends to the appropriate `raw.<lake_table>` table
- Computes metadata (row_count, checksum, file size)

**Returns (success):**
```r
list(
  status = "success",
  lake_table,
  row_count,
  file_size_bytes,
  checksum
)
```
**Returns (error):**
```r
list(status = "error", lake_table = NA, ...)
```

## infer_lake_table()
**File:** r/action/ingest.R
**Purpose:** 
Determine which RAW table the file should map to.

**Rules:**
- Filter dictionary by source_type
- Match source_table_name
- Support pattern rules (e.g., labs_YYYY → labs)

## get_ingest_dict()
**File:** r/action/ingest.R
**Purpose:**
- Load reference.ingest_dictionary with consistent lowercase fields.

## Internal Function Relationships (Conceptual)
graph TD
    A[log_batch_ingest()] -->|creates pending rows| B[(ingest_file_log)]
    A --> C[(batch_log)]
    D[ingest_batch()] --> E[ingest_one_file()]
    E --> F[(raw.<lake_table>)]
    D -->|updates| B
    D -->|finalizes| C

