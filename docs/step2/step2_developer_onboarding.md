# Developer Onboarding — Step 2
## Batch Logging & File-Level Lineage

---

## Quick Start

### Run Step 2 (Batch Logging & Ingestion)

```r
# 1. Edit the USER INPUT SECTION in the script
# 2. Run:
source("r/scripts/2_ingest_and_log_files.R")
```

---

## Prerequisites

Before running Step 2:

1. **Step 1 complete**: Source must be registered in `governance.source_registry`
2. **PostgreSQL running**: The PULSE database must be accessible
3. **Environment variables set**: `PULSE_DB`, `PULSE_HOST`, `PULSE_USER`, `PULSE_PW`
4. **Core tables bootstrapped**: Run `source("pulse-init-all.R")` once
5. **Raw files present**: CSV files must exist in `raw/<source_id>/incoming/`
6. **Ingest dictionary populated**: `reference.ingest_dictionary` must contain mappings for the source

---

## Configuration Options

In `r/scripts/2_ingest_and_log_files.R`:

```r
# 1. Your source identifier
source_id <- "cisir2026_toy"   # EDIT ME

# 2. Source type (MUST match source_type in reference.ingest_dictionary)
source_type <- "CISIR"          # EDIT ME
```

The `ingest_id` is auto-generated with a timestamp:

```r
ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
ingest_id <- glue::glue("ING_{source_id}_{ts}")
```

### Optional: Type Decisions for Staging Promotion

If `reference/type_decisions/type_decision_table.xlsx` exists, it is loaded automatically. This enables raw-to-staging promotion with SQL type casting. If the file is missing, staging promotion is skipped with a warning.

---

## Common Tasks

### Ingest Files for a Source

```r
source("r/scripts/2_ingest_and_log_files.R")
```

### Check Batch Status

```sql
SELECT ingest_id, source_id, status, file_count,
       files_success, files_error,
       batch_started_at_utc, batch_completed_at_utc
FROM governance.batch_log
ORDER BY batch_started_at_utc DESC;
```

### Check File-Level Lineage

```sql
SELECT ingest_file_id, ingest_id, file_name,
       lake_table_name, load_status, row_count,
       file_size_bytes, checksum, completed_at_utc
FROM governance.ingest_file_log
WHERE ingest_id = 'ING_cisir2026_toy_20260128_143000'
ORDER BY ingest_file_id;
```

### View Failed Ingestions

```sql
SELECT file_name, lake_table_name, load_status
FROM governance.ingest_file_log
WHERE load_status = 'error'
ORDER BY completed_at_utc DESC;
```

---

## Understanding Results

### What Gets Created

| Artifact | Location |
|----------|----------|
| Batch log entry | `governance.batch_log` |
| File lineage rows | `governance.ingest_file_log` |
| Raw data tables | `raw.<lake_table>` (one per matched table) |
| Staging tables (optional) | `staging.<lake_table>` (when type_decisions loaded) |

### Console Output

On success, the script prints:

```
==============================
  STEP 2 SUMMARY
==============================
Ingest ID:       ING_cisir2026_toy_20260128_143000
Final Status:    success
Files Processed: 12
 - Success: 12
 - Error:   0
Staging:         auto-promoted (see messages above)
==============================
```

---

## Troubleshooting

### "ingest_id '...' already exists."

The `ingest_id` has already been used. Each batch must have a unique `ingest_id`. The auto-generated format includes a timestamp, so re-running the script produces a new ID.

### "No CSV files found in ..."

No `.csv` files were detected in `raw/<source_id>/incoming/`. Verify that files exist and have the `.csv` extension.

### "No ingest_dictionary rows found for source_type='...'"

The `source_type` value doesn't match any rows in `reference.ingest_dictionary`. Check spelling and case — the filter uses `tolower()` internally but the value must have matching entries.

### "No lake_table match for file '...' under source_type='...'"

The file name (minus `.csv`) doesn't match any `source_table_name` in the ingest dictionary for the given `source_type`. Verify the file is named according to the dictionary mapping.

### "No variable mappings found for lake_table='...' and source_type='...'"

A `lake_table` was resolved but no column mappings exist for it. Check that `reference.ingest_dictionary` has `lake_variable_name` entries for the table.

### "Raw directory does not exist: ..."

The folder `raw/<source_id>/incoming/` doesn't exist. Run Step 1 (source registration) first to create the folder structure.

### "Could not load type_decision_table.xlsx"

The type decision file is missing or unreadable. Staging promotion will be skipped. This is a warning, not a fatal error — raw ingestion still proceeds.

---

## File Locations

| Purpose | Path |
|---------|------|
| User script | `r/scripts/2_ingest_and_log_files.R` |
| Batch logging + orchestration | `r/steps/log_batch_ingest.R` |
| Single-file ingestion | `r/steps/ingest.R` |
| Step wrapper (runner) | `r/steps/run_step2_batch_logging.R` |
| Staging promotion | `r/build_tools/promote_to_staging.R` |
| Pipeline runner | `r/runner.R` |
| Bootstrap | `pulse-init-all.R` |
| Pipeline settings | `config/pipeline_settings.yml` |
| Ingest dictionary | `reference/ingest_dictionary.xlsx` |
| Type decisions | `reference/type_decisions/type_decision_table.xlsx` |
| Batch Log DDL | `sql/ddl/create_BATCH_LOG.sql` |
| Ingest File Log DDL | `sql/ddl/create_INGEST_FILE_LOG.sql` |
| Step 2 seed data | `sql/inserts/pipeline_steps/STEP_002_batch_logging_and_ingestion.sql` |
| Unit tests | `tests/testthat/test_step2_batch_logging.R` |
