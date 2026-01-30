# Developer Onboarding — Step 3
## Schema Validation Engine

---

## Quick Start

### Run Step 3 (Schema Validation)

```r
# 1. Edit the wrapper script with your ingest_id
#    Open: r/scripts/3_validate_schema.R
#    Set: ingest_id <- "ING_your_source_20260107_123456"

# 2. Run the script
source("r/scripts/3_validate_schema.R")
```

---

## Prerequisites

Before running Step 3:

1. **Step 1 completed**: Source must be registered in `governance.source_registry`
2. **Step 2 completed**: Files must be ingested with valid `ingest_id` in `governance.batch_log`
3. **Metadata synced**: `reference.metadata` must be populated (run Step 4: `source("r/scripts/4_sync_metadata.R")`)

---

## Configuration Options

In `r/scripts/3_validate_schema.R`:

```r
# The ingest_id from Step 2
ingest_id <- "ING_trauma_registry2026_toy_20260107_114716"

# Source type (for logging)
source_type <- "TRAUMA_REGISTRY"

# Stop on critical errors? (TRUE recommended for production)
halt_on_error <- FALSE

# Re-sync metadata from Excel before validation?
sync_metadata_first <- FALSE
```

---

## Common Tasks

### Sync Metadata from Excel

If you've updated `CURRENT_core_metadata_dictionary.xlsx`:

```r
source("r/scripts/4_sync_metadata.R")
```

Or programmatically:

```r
source("pulse-init-all.R")
source("r/reference/sync_metadata.R")
con <- connect_to_pulse()
sync_metadata(con, dict_path = "reference/CURRENT_core_metadata_dictionary.xlsx")
DBI::dbDisconnect(con)
```

### View Validation Issues

```sql
-- All issues for a batch
SELECT * FROM governance.structure_qc_table
WHERE ingest_id = 'ING_your_source_20260107_123456'
ORDER BY severity, lake_table_name;

-- Summary by severity
SELECT severity, COUNT(*) as n_issues
FROM governance.structure_qc_table
WHERE ingest_id = 'ING_your_source_20260107_123456'
GROUP BY severity;
```

### Re-run Validation

```r
# Clear old issues first
DBI::dbExecute(con, "
    DELETE FROM governance.structure_qc_table
    WHERE ingest_id = 'ING_your_source_20260107_123456'
")

# Then re-run Step 3
source("r/scripts/3_validate_schema.R")
```

---

## Understanding Results

### Return Structure

```r
result <- validate_schema(con, ingest_id)

result$success          # TRUE if no critical issues
result$tables_validated # Number of tables checked
result$issues_count     # Total issues found
result$critical_count   # Critical severity issues
result$warning_count    # Warning severity issues
result$info_count       # Info severity issues
result$issues           # Tibble with all issue details
```

### Issue Codes

| Code | Meaning |
|------|---------|
| `SCHEMA_MISSING_COLUMN` | Required column not found in table |
| `SCHEMA_UNEXPECTED_COLUMN` | Column exists but not in expected schema |
| `SCHEMA_TYPE_MISMATCH` | Data type differs from expected |
| `TYPE_TARGET_MISMATCH` | Observed type does not match target staging type |
| `TYPE_TARGET_MISSING` | No target type defined in type_decision_table |

---

## Troubleshooting

### "ingest_id not found in batch_log"

The ingest_id must exist in `governance.batch_log`. Check:
```sql
SELECT * FROM governance.batch_log ORDER BY created_at DESC LIMIT 5;
```

### "No active metadata found"

Run Step 4 to populate `reference.metadata`:
```r
source("r/scripts/4_sync_metadata.R")
```

### "No expected schema for table"

The table isn't defined in `CURRENT_core_metadata_dictionary.xlsx` for this source type. Either:
1. Add the table/variable to the Excel dictionary and re-sync (Step 4)
2. Or this is expected (unmapped source table)

---

## File Locations

| Purpose | Path |
|---------|------|
| User script | `r/scripts/3_validate_schema.R` |
| Step function | `r/steps/validate_schema.R` |
| Field comparison | `r/utilities/compare_fields.R` |
| Metadata sync | `r/reference/sync_metadata.R` |
| Metadata dictionary | `reference/CURRENT_core_metadata_dictionary.xlsx` |
| Metadata DDL | `sql/ddl/recreate_METADATA_v2.sql` |
| QC table DDL | `sql/ddl/create_STRUCTURE_QC_TABLE.sql` |
| Unit tests | `tests/testthat/test_step3_schema_validation.R` |
