# PULSE Pipeline — Developer Onboarding  
## Step 2 × Cluster 2: Batch Logging & File-Level Lineage

---

Step 2 is the ingestion gateway for all raw files entering the PULSE pipeline.  
It establishes batch-level lineage (`batch_log`) and file-level lineage (`ingest_file_log`) and performs actual ingestion into the RAW zone using strict `source_type` rules.

Step 2 ensures reproducibility, auditability, and correct mapping between incoming files and lake tables.

---

## What Step 2 Does

**Core responsibilities:**

1. Detect all incoming files in the source's raw folder.
2. Create a `batch_log` entry for the new ingest event.
3. Create an `ingest_file_log` row for every file (initially `"pending"`).
4. Ingest each file safely and deterministically using:
   - dictionary-based table resolution  
   - strict `source_type` enforcement  
   - controlled harmonization and renaming
5. Update lineage for:
   - `row_count`
   - `file_size_bytes`
   - `checksum` (MD5)
   - `lake_table_name`
   - `load_status`
6. Finalize the batch with a `success`, `partial`, or `error` status.

---

## Files Involved (Step 2)

### Core R Scripts  
- `r/scripts/2_ingest_and_log_files.R`  
  Human-friendly wrapper that executes the whole step.

- `r/steps/log_batch_ingest.R`  
  Contains:
  - `log_batch_ingest()`
  - `ingest_batch()`

- `r/action/ingest.R`  
  Contains:
  - `ingest_one_file()`
  - `infer_lake_table()`
  - `get_ingest_dict()`

---

## Required Database Objects

### `governance.batch_log`  
Tracks batch-level ingestion events.

### `governance.ingest_file_log`  
Tracks file-level ingestion details.

---

## Execution Flow

```mermaid
flowchart TD
    A[Incoming CSV Files] --> B[log_batch_ingest()]
    B --> C[(governance.batch_log)]
    B --> D[(governance.ingest_file_log: pending)]
    C --> E[ingest_batch()]
    E --> F[ingest_one_file()]
    F --> G[(raw.<lake_table>)]
    E --> H[(ingest_file_log: success/error)]
    E --> I[(batch_log: final status)]
```

## How to Run Step 2
source("r/scripts/2_ingest_and_log_files.R")

### Produces:
- Fully updated governance lineage
- Raw tables appended
- A “STEP 2 SUMMARY” printed to the console

## When Step 2 Is Considered Complete
- All file lineage rows exist
- Raw tables contain newly appended data
- Checksums, row counts, and sizes are written
- batch_log has a final status
- No unlogged or partially logged files
- No cross-source bleeding (strict type enforcement working)

