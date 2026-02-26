# Developer Onboarding — Step 6
## Harmonization (Staging to Validated)

---

## Quick Start

### Run Step 6 (Harmonization)

```r
# 1. Ensure Steps 1-5 are complete (source registered, data ingested,
#    schema validated, metadata synced, raw data profiled)
# 2. Ensure DDLs have been run:
#    - sql/ddl/create_HARMONIZATION_MAP.sql
#    - sql/ddl/create_TRANSFORM_LOG.sql
#    - sql/ddl/create_VALIDATED_*.sql (23 files)

# 3. Run the script
source("r/scripts/6_harmonize_data.R")
```

---

## Prerequisites

Before running Step 6:

1. **Steps 1-5 completed**: Source registered, data ingested, schema validated, metadata synced, raw data profiled
2. **Staging tables populated**: All 47 staging tables exist with typed columns
3. **Validated table DDLs executed**: All 23 `sql/ddl/create_VALIDATED_*.sql` scripts run against the database
4. **Governance DDLs executed**: `create_HARMONIZATION_MAP.sql` and `create_TRANSFORM_LOG.sql`
5. **Valid ingest_id**: An ingest batch must exist in `governance.batch_log`

---

## Configuration Options

In `r/scripts/6_harmonize_data.R`:

```r
# The ingest_id from Step 2 (check governance.batch_log for available IDs)
ingest_id <- "ING_cisir2026_toy_20260128_170418"

# Which validated tables to populate (NULL = all with active mappings)
target_tables <- NULL
# Or specify: target_tables <- c("demographics", "admission", "labs")

# Filter to specific source type (NULL = all sources)
source_type_filter <- NULL
# Or specify: source_type_filter <- "CISIR"

# Sync mappings from metadata dictionary before harmonizing?
sync_mappings_first <- TRUE

# Profile validated tables after harmonization?
profile_after <- TRUE
```

---

## Common Tasks

### Harmonize an Ingest Batch

```r
source("r/scripts/6_harmonize_data.R")
```

### Harmonize Programmatically

```r
source("pulse-init-all.R")
source("r/harmonization/sync_harmonization_map.R")
source("r/steps/harmonize_data.R")
con <- connect_to_pulse()

# Sync mappings first
sync_result <- sync_harmonization_map(con)

# Run harmonization
result <- harmonize_data(con, ingest_id = "ING_cisir2026_toy_20260128_170418")
DBI::dbDisconnect(con)
```

### Harmonize a Single Table

```r
source("pulse-init-all.R")
source("r/harmonization/harmonize_table.R")
con <- connect_to_pulse()

result <- harmonize_table(con,
    target_table = "demographics",
    ingest_id    = "ING_cisir2026_toy_20260128_170418"
)
DBI::dbDisconnect(con)
```

### Harmonize One Source Only

```r
# Only harmonize CISIR data
result <- harmonize_data(con,
    ingest_id          = "ING_cisir2026_toy_20260128_170418",
    source_type_filter = "CISIR"
)
```

### Re-harmonize (Idempotent)

Simply run the same script again with the same `ingest_id`. Prior harmonization data is automatically deleted before re-harmonization.

### Sync Mappings Without Harmonizing

```r
source("pulse-init-all.R")
source("r/harmonization/sync_harmonization_map.R")
con <- connect_to_pulse()
sync_result <- sync_harmonization_map(con)
DBI::dbDisconnect(con)
```

### Check Available Ingests

```sql
SELECT ingest_id, source_id, status, n_files, created_at
FROM governance.batch_log
ORDER BY created_at DESC;
```

### View Harmonization Results

```sql
-- Transform log summary
SELECT target_table, source_table, target_row_count, status, duration_seconds
FROM governance.transform_log
WHERE ingest_id = 'ING_cisir2026_toy_20260128_170418'
ORDER BY target_table, source_table;

-- Row counts per validated table
SELECT target_table, SUM(target_row_count) AS total_rows,
       COUNT(DISTINCT source_table) AS source_count
FROM governance.transform_log
WHERE ingest_id = 'ING_cisir2026_toy_20260128_170418'
  AND status = 'success'
GROUP BY target_table
ORDER BY target_table;

-- Check mapping coverage
SELECT target_table, COUNT(*) AS mapping_count,
       COUNT(DISTINCT source_type) AS source_types
FROM reference.harmonization_map
WHERE is_active = TRUE
GROUP BY target_table
ORDER BY target_table;

-- View mappings for a specific table
SELECT source_type, source_table, source_column,
       target_column, transform_type
FROM reference.harmonization_map
WHERE target_table = 'demographics'
  AND is_active = TRUE
ORDER BY source_type, source_column;
```

---

## Understanding Results

### Return Structure

```r
result <- harmonize_data(con, ingest_id)

result$tables_processed   # Number of validated tables harmonized
result$total_rows         # Total rows inserted across all tables
result$sources_processed  # Total source tables processed
result$by_table           # Named list: table_name -> row count
result$status             # "success" or "partial"
```

### Transform Log Fields

| Field | Description |
|-------|-------------|
| `source_schema` | Always `staging` |
| `source_table` | Staging table name |
| `source_row_count` | Rows in the staging table |
| `target_schema` | Always `validated` |
| `target_table` | Validated table name |
| `target_row_count` | Rows inserted |
| `columns_mapped` | Number of columns in the mapping |
| `status` | `success`, `partial`, or `failed` |
| `duration_seconds` | Wall-clock time for the operation |

---

## Troubleshooting

### "ingest_id not found in governance.batch_log"

Verify the ingest_id exists:
```r
DBI::dbGetQuery(con, "SELECT ingest_id FROM governance.batch_log")
```

### "No active mappings found in reference.harmonization_map"

Run the mapping sync first:
```r
source("r/harmonization/sync_harmonization_map.R")
sync_harmonization_map(con)
```

### "validated.{table} does not exist"

Run the DDL for the missing table:
```bash
psql -d primeai_lake -f sql/ddl/create_VALIDATED_DEMOGRAPHICS.sql
```

### "staging.{table} not found. Skipping."

The staging table for that source doesn't exist. This is logged to `governance.transform_log` with `status = 'failed'`. Check that ingestion completed for that source.

### "WARNING: N source column(s) not found in staging.{table}"

The mapping references columns that don't exist in the staging table. These are mapped as `NULL`. Update the metadata dictionary or mapping table to fix.

### Safe type casting returns NULL

When TEXT values like "Negative" or "<0.02" fail the numeric regex guard, they are inserted as `NULL`. This is intentional — check `governance.transform_log` for partial status.

---

## File Locations

| Purpose | Path |
|---------|------|
| User script | `r/scripts/6_harmonize_data.R` |
| Orchestrator | `r/steps/harmonize_data.R` |
| Mapping sync | `r/harmonization/sync_harmonization_map.R` |
| Mapping loader | `r/harmonization/load_harmonization_map.R` |
| Query builder | `r/harmonization/build_harmonization_query.R` |
| Table harmonizer | `r/harmonization/harmonize_table.R` |
| DDL generator (build tool) | `r/build_tools/generate_validated_ddls.R` |
| Validated summary (sandbox) | `r/sandbox/view_validated_summary.R` |
| Harmonization map DDL | `sql/ddl/create_HARMONIZATION_MAP.sql` |
| Transform log DDL | `sql/ddl/create_TRANSFORM_LOG.sql` |
| Validated table DDLs | `sql/ddl/create_VALIDATED_*.sql` (23 files) |
