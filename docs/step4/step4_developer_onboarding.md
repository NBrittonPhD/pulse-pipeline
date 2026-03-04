# Developer Onboarding — Step 4
## Metadata Synchronization

---

## Quick Start

### Run Step 4 (Metadata Sync)

```r
# 1. Ensure the Excel dictionary is up to date:
#    reference/CURRENT_core_metadata_dictionary.xlsx

# 2. Run:
source("r/scripts/4_sync_metadata.R")
```

---

## Prerequisites

Before running Step 4:

1. **Steps 1-3 completed**: Source registered, data ingested, schema validated
2. **Excel dictionary current**: `reference/CURRENT_core_metadata_dictionary.xlsx` contains all variable definitions
3. **Database tables exist**: `reference.metadata` and `reference.metadata_history` (created by DDLs)
4. **PostgreSQL running**: The PULSE database must be accessible
5. **Environment variables set**: `PULSE_DB`, `PULSE_HOST`, `PULSE_USER`, `PULSE_PW`

---

## Configuration Options

In `r/scripts/4_sync_metadata.R`:

```r
# Path to the metadata dictionary Excel file
dict_path <- "reference/CURRENT_core_metadata_dictionary.xlsx"

# Optional: Filter to a specific source type (set to NULL for all sources)
# Options: "CISIR", "CLARITY", "TRAUMA_REGISTRY", or NULL
source_type_filter <- NULL
```

---

## Common Tasks

### Sync All Sources

```r
source("r/scripts/4_sync_metadata.R")
```

### Sync a Single Source Type

```r
source("pulse-init-all.R")
source("r/reference/sync_metadata.R")
con <- connect_to_pulse()
sync_metadata(con, dict_path = "reference/CURRENT_core_metadata_dictionary.xlsx",
              source_type_filter = "CISIR")
DBI::dbDisconnect(con)
```

### Check Current Version

```r
source("pulse-init-all.R")
source("r/reference/get_current_metadata_version.R")
con <- connect_to_pulse()
get_current_metadata_version(con)
DBI::dbDisconnect(con)
```

### View Change History

```sql
-- All changes for the latest version
SELECT * FROM reference.metadata_history
WHERE version_number = (SELECT MAX(version_number) FROM reference.metadata)
ORDER BY change_type, lake_table_name, lake_variable_name;

-- Summary by change type for a version
SELECT change_type, COUNT(DISTINCT lake_variable_name) as n_variables
FROM reference.metadata_history
WHERE version_number = 5
GROUP BY change_type;

-- History for a specific variable
SELECT * FROM reference.metadata_history
WHERE lake_table_name = 'my_table' AND lake_variable_name = 'my_variable'
ORDER BY version_number;
```

### View Active Metadata

```sql
-- All active variables
SELECT lake_table_name, lake_variable_name, source_type, data_type, is_required
FROM reference.metadata
WHERE is_active = TRUE
ORDER BY lake_table_name, lake_variable_name;

-- Count by source type
SELECT source_type, COUNT(*) as n_variables
FROM reference.metadata
WHERE is_active = TRUE
GROUP BY source_type;
```

### Review Results

```r
source("r/review/review_step4_metadata.R")
```

---

## Understanding Results

### Return Structure

```r
result <- sync_metadata(con, dict_path)

result$version_number   # New version number assigned
result$total_variables  # Total variables in the dictionary
result$adds             # New variables added (including initial)
result$updates          # Variables with changed fields
result$removes          # Variables soft-deleted
result$total_changes    # Total field-level change records
result$rows_synced      # Total rows written to metadata table
```

### Change Types

| Type | Meaning |
|------|---------|
| `INITIAL` | First sync — variable loaded for the first time |
| `ADD` | Variable exists in Excel but not in database |
| `UPDATE` | Variable exists in both but a field value changed |
| `REMOVE` | Variable exists in database but not in Excel |

### Console Output

On completion, the script prints:

```
=================================================================
[run_step4] STEP 4 COMPLETE
=================================================================
  Version:         6
  Total Variables: 1268
  Adds:            0
  Updates:         3
  Removes:         0
  Total Changes:   3
  Duration:        1.87 seconds
=================================================================
```

---

## Troubleshooting

### "Dictionary file not found"

Verify the path to the Excel file:
```r
file.exists("reference/CURRENT_core_metadata_dictionary.xlsx")
```

### "Missing required columns"

The Excel file must have these columns: `source_type`, `lake_table_name`, `lake_variable_name`, `data_type`. Check spelling and case.

### "Duplicate keys in dictionary"

The composite key (`lake_table_name`, `lake_variable_name`, `source_type`) must be unique. Check the Excel for duplicate rows:
```r
dict <- readxl::read_excel("reference/CURRENT_core_metadata_dictionary.xlsx")
dict %>% dplyr::count(lake_table_name, lake_variable_name, source_type) %>% dplyr::filter(n > 1)
```

### "No changes detected"

If the Excel dictionary hasn't changed since the last sync, no history records are written and the version still increments. This is expected behavior.

### "Invalid database connection"

Check that the PULSE database is running and environment variables are set:
```r
Sys.getenv("PULSE_DB")
Sys.getenv("PULSE_HOST")
```

---

## File Locations

| Purpose | Path |
|---------|------|
| User script | `r/scripts/4_sync_metadata.R` |
| Sync orchestrator | `r/reference/sync_metadata.R` |
| Dictionary loader | `r/reference/load_metadata_dictionary.R` |
| Comparison engine | `r/utilities/compare_metadata.R` |
| Version helper | `r/reference/get_current_metadata_version.R` |
| Audit event writer | `r/steps/write_audit_event.R` |
| Results review | `r/review/review_step4_metadata.R` |
| Bootstrap | `pulse-init-all.R` |
| Metadata dictionary | `reference/CURRENT_core_metadata_dictionary.xlsx` |
| Metadata DDL | `sql/ddl/recreate_METADATA_v2.sql` |
| History DDL | `sql/ddl/create_METADATA_HISTORY.sql` |
| Unit tests | `tests/testthat/test_step4_metadata_sync.R` |
