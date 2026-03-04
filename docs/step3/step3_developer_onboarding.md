# Developer Onboarding — Step 3
## Schema Validation Engine

---

## Quick Start

### Run Step 3 (Schema Validation)

```r
# 1. Edit the USER INPUT SECTION in the script
# 2. Run:
source("r/scripts/3_validate_schema.R")
```

---

## Prerequisites

Before running Step 3:

1. **Step 1 complete**: Source must be registered in `governance.source_registry`
2. **Step 2 complete**: Files must be ingested with valid `ingest_id` in `governance.batch_log`
3. **Metadata synced**: `reference.metadata` must be populated (run Step 4: `source("r/scripts/4_sync_metadata.R")`)
4. **PostgreSQL running**: The PULSE database must be accessible
5. **Environment variables set**: `PULSE_DB`, `PULSE_HOST`, `PULSE_USER`, `PULSE_PW`

---

## Configuration Options

In `r/scripts/3_validate_schema.R`:

```r
# The ingest_id from Step 2 that you want to validate
ingest_id <- "ING_trauma_registry2026_toy_20260128_170308"   # EDIT ME

# Source type (for logging; optional if derivable from batch_log)
source_type <- "CISIR"   # EDIT ME
```

### Behavior Settings

```r
# Stop on critical errors? (TRUE recommended for production)
halt_on_error <- FALSE

# Re-sync metadata from Excel before validation?
sync_metadata_first <- FALSE
```

---

## Common Tasks

### Validate a Batch

```r
source("r/scripts/3_validate_schema.R")
```

### Sync Metadata Before Validation

Set `sync_metadata_first <- TRUE` in the script, or run Step 4 separately:

```r
source("r/scripts/4_sync_metadata.R")
```

### View Validation Issues

```sql
-- All issues for a batch
SELECT lake_table_name, lake_variable_name, issue_code,
       severity, expected_value, observed_value
FROM governance.structure_qc_table
WHERE ingest_id = 'ING_your_source_20260107_123456'
ORDER BY severity, lake_table_name;

-- Summary by severity
SELECT severity, COUNT(*) AS n_issues
FROM governance.structure_qc_table
WHERE ingest_id = 'ING_your_source_20260107_123456'
GROUP BY severity;
```

### Re-run Validation

```sql
-- Clear old issues first
DELETE FROM governance.structure_qc_table
WHERE ingest_id = 'ING_your_source_20260107_123456';
```

Then re-run the script.

### Review Results

```r
source("r/review/review_step3_validation.R")
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

| Code | Severity | Meaning |
|------|----------|---------|
| `SCHEMA_MISSING_COLUMN` | critical | Required column not found in observed table |
| `SCHEMA_UNEXPECTED_COLUMN` | critical | Column exists in table but not in expected schema |
| `SCHEMA_TYPE_MISMATCH` | warning | Data type differs between expected and observed |
| `TYPE_TARGET_MISMATCH` | warning | Observed type does not match target staging type |
| `TYPE_TARGET_MISSING` | warning | No target type defined in type_decision_table |

### Console Output

On completion, the script prints:

```
=================================================================
  STEP 3 SUMMARY
=================================================================
  Status:           SUCCESS
  Tables Validated: 15
  Total Issues:     23
  Critical:         0
  Warnings:         23
  Info:             0
  Duration:         2.45 seconds
=================================================================
```

---

## Troubleshooting

### "ingest_id '...' not found in governance.batch_log."

The `ingest_id` must exist in `governance.batch_log`. Check recent batches:

```sql
SELECT ingest_id, source_id, status
FROM governance.batch_log
ORDER BY batch_started_at_utc DESC LIMIT 5;
```

### "No active metadata found in reference.metadata. Run sync_metadata() first."

The `reference.metadata` table is empty or has no active rows. Run Step 4 to populate it:

```r
source("r/scripts/4_sync_metadata.R")
```

### "No active metadata found for source_type '...'."

The `source_type` doesn't match any rows in `reference.metadata`. Check available source types:

```sql
SELECT DISTINCT source_type FROM reference.metadata WHERE is_active = TRUE;
```

### "Could not derive source_type for ingest '...'."

The system couldn't automatically determine the source type. Pass `source_type` explicitly in the script.

### "No expected schema for '...' Skipping."

The table isn't defined in `reference.metadata` for this source type. Either add it to the Excel dictionary and re-sync (Step 4), or this is expected for unmapped source tables.

---

## File Locations

| Purpose | Path |
|---------|------|
| User script | `r/scripts/3_validate_schema.R` |
| Validation orchestrator | `r/steps/validate_schema.R` |
| Field comparison utility | `r/utilities/compare_fields.R` |
| Results review | `r/review/review_step3_validation.R` |
| Metadata sync (Step 4) | `r/reference/sync_metadata.R` |
| Pipeline runner | `r/runner.R` |
| Bootstrap | `pulse-init-all.R` |
| Metadata dictionary | `reference/CURRENT_core_metadata_dictionary.xlsx` |
| Structure QC DDL | `sql/ddl/create_STRUCTURE_QC_TABLE.sql` |
| Metadata DDL | `sql/ddl/recreate_METADATA_v2.sql` |
| Unit tests | `tests/testthat/test_step3_schema_validation.R` |
