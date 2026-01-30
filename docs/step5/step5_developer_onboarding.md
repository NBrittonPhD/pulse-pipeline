# Developer Onboarding — Step 5
## Data Profiling

---

## Quick Start

### Run Step 5 (Data Profiling)

```r
# 1. Ensure Steps 1-4 are complete (source registered, data ingested,
#    schema validated, metadata synced)

# 2. Run the script
source("r/scripts/5_profile_data.R")
```

---

## Prerequisites

Before running Step 5:

1. **Steps 1-4 completed**: Source registered, data ingested, schema validated, metadata synced
2. **Database tables exist**: All 5 `governance.data_profile*` tables (created by DDLs)
3. **Valid ingest_id**: An ingest batch must exist in `governance.batch_log` with successfully loaded tables in `governance.ingest_file_log`

---

## Configuration Options

In `r/scripts/5_profile_data.R`:

```r
# The ingest_id from Step 2 (check governance.batch_log for available IDs)
ingest_id <- "ING_cisir2026_toy_20260128_170000"

# Which schema to profile: "raw" (before harmonization) or "staging" (after)
schema_to_profile <- "raw"

# Path to profiling config (uses defaults if file missing)
config_path <- "config/profiling_settings.yml"
```

In `config/profiling_settings.yml`:

```yaml
quality_score_thresholds:
  excellent: {max_missing_pct: 5, max_critical_issues: 0}
  good: {max_missing_pct: 10, max_critical_issues: 2}
  fair: {max_missing_pct: 20, max_critical_issues: 5}

missingness_thresholds:
  critical: 0    # Any missing in identifiers
  high: 20       # >20% = warning
  moderate: 10   # 10-20% = info

sentinel_detection:
  numeric_sentinels: [999, 9999, -999, -9999, -1, 99, 88, 77]
  string_sentinels: ["NA", "N/A", "NULL", "UNKNOWN", "UNK", "MISSING", "NOT RECORDED"]
  min_frequency_pct: 1.0
  max_unique_for_detection: 50

identifier_columns: [ACCOUNTNO, MEDRECNO, TRAUMANO, account_number, mrn, trauma_no, cisir_id]
identifier_patterns: ["_id$", "_no$", "^id_", "^accountno", "^medrecno", "^traumano"]
```

---

## Common Tasks

### Profile an Ingest Batch

```r
source("r/scripts/5_profile_data.R")
```

### Profile Programmatically

```r
source("pulse-init-all.R")
source("r/steps/profile_data.R")
con <- connect_to_pulse()
result <- profile_data(con, ingest_id = "ING_cisir2026_toy_20260128_170000",
                       schema_to_profile = "raw")
DBI::dbDisconnect(con)
```

### Re-profile (Idempotent)

Simply run the same script again with the same `ingest_id`. Prior profiling data is automatically deleted before re-profiling.

### Check Available Ingests

```sql
SELECT ingest_id, source_id, status, n_files, created_at
FROM governance.batch_log
ORDER BY created_at DESC;
```

### View Profiling Results

```sql
-- Per-table quality scores
SELECT table_name, row_count, variable_count, quality_score,
       avg_valid_pct, max_missing_pct,
       critical_issue_count, warning_issue_count, info_issue_count,
       worst_variable, worst_variable_missing_pct
FROM governance.data_profile_summary
WHERE ingest_id = 'ING_cisir2026_toy_20260128_170000'
ORDER BY quality_score DESC, table_name;

-- Variables with highest missingness
SELECT table_name, variable_name, inferred_type,
       total_missing_pct, valid_pct, na_pct, sentinel_pct
FROM governance.data_profile
WHERE ingest_id = 'ING_cisir2026_toy_20260128_170000'
  AND total_missing_pct > 10
ORDER BY total_missing_pct DESC;

-- All critical and warning issues
SELECT table_name, variable_name, issue_type, severity,
       description, value, recommendation
FROM governance.data_profile_issue
WHERE ingest_id = 'ING_cisir2026_toy_20260128_170000'
  AND severity IN ('critical', 'warning')
ORDER BY severity, table_name, variable_name;

-- Detected sentinel values
SELECT table_name, variable_name, sentinel_value,
       sentinel_count, sentinel_pct, detection_method, confidence
FROM governance.data_profile_sentinel
WHERE ingest_id = 'ING_cisir2026_toy_20260128_170000'
ORDER BY sentinel_pct DESC;

-- Numeric distribution statistics
SELECT table_name, variable_name, stat_min, stat_max,
       stat_mean, stat_median, stat_sd
FROM governance.data_profile_distribution
WHERE ingest_id = 'ING_cisir2026_toy_20260128_170000'
  AND distribution_type = 'numeric'
ORDER BY table_name, variable_name;
```

---

## Understanding Results

### Return Structure

```r
result <- profile_data(con, ingest_id, "raw")

result$tables_profiled     # Number of raw tables profiled
result$variables_profiled  # Total columns across all tables
result$sentinels_detected  # Number of sentinel values found
result$critical_issues     # Critical-severity issue count
result$warning_issues      # Warning-severity issue count
result$info_issues         # Info-severity issue count
result$overall_score       # Worst per-table quality score
```

### Quality Scores

| Score | Meaning |
|-------|---------|
| `Excellent` | All tables have <=5% missing, no critical issues |
| `Good` | All tables have <=10% missing, <=2 critical issues |
| `Fair` | All tables have <=20% missing, <=5 critical issues |
| `Needs Review` | At least one table exceeds Fair thresholds |

### Missingness Categories

| Category | Counted As Missing? | Example |
|----------|---------------------|---------|
| NA | Yes | R `NA` |
| Empty | Yes | `""` |
| Whitespace | Yes | `"  "`, `"\t"` |
| Sentinel | Yes | `"999"`, `"UNKNOWN"` |
| Valid | No | Any other value |

---

## Troubleshooting

### "ingest_id not found in governance.batch_log"

Verify the ingest_id exists:
```r
DBI::dbGetQuery(con, "SELECT ingest_id FROM governance.batch_log")
```

### "No successfully loaded tables found"

Check that Step 2 ingestion completed with `load_status = 'success'`:
```sql
SELECT * FROM governance.ingest_file_log
WHERE ingest_id = 'ING_cisir2026_toy_20260128_170000'
ORDER BY load_status;
```

### "Config file not found"

Profiling works without a config file (uses hardcoded defaults). To use custom thresholds, ensure `config/profiling_settings.yml` exists.

### "Table has 0 rows — skipping"

Empty tables receive an `Excellent` score with no variable profiles. This is expected behavior.

---

## File Locations

| Purpose | Path |
|---------|------|
| User script | `r/scripts/5_profile_data.R` |
| Orchestrator | `r/steps/profile_data.R` |
| Config loader | `r/utilities/load_profiling_config.R` |
| Type inference | `r/utilities/infer_column_type.R` |
| Sentinel detection | `r/profiling/detect_sentinels.R` |
| Missingness profiler | `r/profiling/profile_missingness.R` |
| Distribution profiler | `r/profiling/profile_distribution.R` |
| Issue generator | `r/profiling/generate_issues.R` |
| Quality scorer | `r/profiling/calculate_quality_score.R` |
| Table profiler | `r/profiling/profile_table.R` |
| Profiling config | `config/profiling_settings.yml` |
| Profile DDL | `sql/ddl/create_DATA_PROFILE.sql` |
| Distribution DDL | `sql/ddl/create_DATA_PROFILE_DISTRIBUTION.sql` |
| Sentinel DDL | `sql/ddl/create_DATA_PROFILE_SENTINEL.sql` |
| Issue DDL | `sql/ddl/create_DATA_PROFILE_ISSUE.sql` |
| Summary DDL | `sql/ddl/create_DATA_PROFILE_SUMMARY.sql` |
| Unit tests | `tests/testthat/test_step5_data_profiling.R` |
