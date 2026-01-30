# Function Atlas — Step 5
## Data Profiling

---

This reference lists all functions used in Step 5, grouped by purpose, with details about inputs, outputs, and responsibilities.

---

## Core Functions

### `profile_data()`

**File:** `r/steps/profile_data.R`

**Purpose:** Main Step 5 orchestrator. Loads profiling config, verifies the ingest exists, retrieves tables from the file log, deletes prior profiling data for idempotency, profiles each table, writes results to 5 governance tables, computes overall score, and logs an audit event.

**Signature:**
```r
profile_data(
    con,                        # DBIConnection (required)
    ingest_id,                  # character: batch identifier (required)
    schema_to_profile = "raw",  # character: "raw" or "staging"
    config_path = NULL          # character: path to profiling_settings.yml (optional)
)
```

**Returns:** List with:
- `tables_profiled`: integer count of tables profiled
- `variables_profiled`: integer count of variables profiled
- `sentinels_detected`: integer count of sentinel values detected
- `critical_issues`: integer count of critical-severity issues
- `warning_issues`: integer count of warning-severity issues
- `info_issues`: integer count of info-severity issues
- `overall_score`: character quality score (worst per-table score)

**Side Effects:**
- Deletes prior profiling data for `(ingest_id, schema_name)` from all 5 profiling tables
- Writes to `governance.data_profile` (append)
- Writes to `governance.data_profile_distribution` (append)
- Writes to `governance.data_profile_sentinel` (append)
- Writes to `governance.data_profile_issue` (append)
- Writes to `governance.data_profile_summary` (append)
- Writes to `governance.audit_log` (append)

---

### `profile_table()`

**File:** `r/profiling/profile_table.R`

**Purpose:** Profile all columns of a single table. Calls leaf functions for each column, then aggregates into a per-table summary with quality score. Does NOT write to the database — returns structured tibbles.

**Signature:**
```r
profile_table(
    con,            # DBIConnection (for reading the table data)
    schema_name,    # character: "raw" or "staging"
    table_name,     # character: table name without schema prefix
    ingest_id,      # character: batch identifier
    config          # list from load_profiling_config()
)
```

**Returns:** Named list with 5 tibbles:
- `profile`: variable-level missingness (one row per column)
- `distributions`: distribution statistics (one row per column)
- `sentinels`: detected sentinel values (zero or more rows per column)
- `issues`: quality issues (zero or more rows per column)
- `summary`: table-level summary (exactly 1 row)

**Side Effects:** Reads from database (SELECT only).

---

## Leaf Functions

### `detect_sentinels()`

**File:** `r/profiling/detect_sentinels.R`

**Purpose:** Detect sentinel/placeholder values in a column using config-based matching (high confidence) and frequency analysis for repeat-digit patterns (medium confidence).

**Signature:**
```r
detect_sentinels(
    values,         # character vector of column values
    column_name,    # character scalar
    column_type,    # character: from infer_column_type()
    config          # list from load_profiling_config()
)
```

**Returns:** Tibble with columns: `sentinel_value`, `sentinel_count`, `sentinel_pct`, `detection_method`, `confidence` (zero rows if none detected).

**Side Effects:** None (pure function).

---

### `profile_missingness()`

**File:** `r/profiling/profile_missingness.R`

**Purpose:** Classify every value into exactly one of five mutually exclusive categories: NA, empty string, whitespace-only, sentinel, or valid. Returns counts and percentages for each category.

**Signature:**
```r
profile_missingness(
    values,                         # character vector of column values
    sentinel_values = character(0)  # character vector of sentinel strings
)
```

**Returns:** Named list with: `total_count`, `valid_count`, `na_count`, `empty_count`, `whitespace_count`, `sentinel_count`, `na_pct`, `empty_pct`, `whitespace_pct`, `sentinel_pct`, `total_missing_count`, `total_missing_pct`, `valid_pct`, `unique_count`, `unique_pct`.

**Side Effects:** None (pure function).

---

### `profile_distribution()`

**File:** `r/profiling/profile_distribution.R`

**Purpose:** Compute distribution statistics appropriate to the column type. Numeric columns get min/max/mean/median/sd/quartiles. All other types get a frequency table with top-N values as JSON and mode statistics.

**Signature:**
```r
profile_distribution(
    values,                         # character vector of column values
    column_type,                    # character: from infer_column_type()
    sentinel_values = character(0), # character vector of sentinel strings
    config = list()                 # list from load_profiling_config()
)
```

**Returns:** Named list with: `distribution_type`, `stat_min`, `stat_max`, `stat_mean`, `stat_median`, `stat_sd`, `stat_q25`, `stat_q75`, `stat_iqr`, `top_values_json`, `mode_value`, `mode_count`, `mode_pct`.

**Side Effects:** None (pure function).

---

### `generate_issues()`

**File:** `r/profiling/generate_issues.R`

**Purpose:** Evaluate profiling results for a single variable and flag quality issues at the appropriate severity level. Checks 5 issue types: identifier missing, high/moderate missingness, constant value, high cardinality.

**Signature:**
```r
generate_issues(
    variable_name,      # character scalar
    table_name,         # character scalar
    missingness_result, # list from profile_missingness()
    column_type,        # character: from infer_column_type()
    unique_count,       # integer: unique values among valid data
    total_count,        # integer: total row count
    config              # list from load_profiling_config()
)
```

**Returns:** Tibble with columns: `variable_name`, `table_name`, `issue_type`, `severity`, `description`, `value`, `recommendation` (zero rows if no issues detected).

**Side Effects:** None (pure function).

---

### `calculate_quality_score()`

**File:** `r/profiling/calculate_quality_score.R`

**Purpose:** Rate a table's overall quality based on its worst missingness percentage and count of critical issues. Returns one of four quality levels.

**Signature:**
```r
calculate_quality_score(
    max_missing_pct,    # numeric: highest total_missing_pct
    critical_count,     # integer: number of critical-severity issues
    config              # list from load_profiling_config()
)
```

**Returns:** Character scalar: `"Excellent"`, `"Good"`, `"Fair"`, or `"Needs Review"`.

**Side Effects:** None (pure function).

---

## Utility Functions

### `load_profiling_config()`

**File:** `r/utilities/load_profiling_config.R`

**Purpose:** Load profiling configuration from YAML with built-in defaults. If the YAML file is missing, defaults are used silently so profiling can run without a config file.

**Signature:**
```r
load_profiling_config(
    config_path = NULL  # character: path to profiling_settings.yml (optional)
)
```

**Returns:** Named list of configuration settings (thresholds, sentinel lists, identifier patterns, display options).

**Side Effects:** None (pure function).

---

### `infer_column_type()`

**File:** `r/utilities/infer_column_type.R`

**Purpose:** Classify a TEXT column as numeric, categorical, date, or identifier based on column name patterns and value content. Priority: identifier (name match) > numeric (>90% parse) > date (>80% parse) > categorical (fallback).

**Signature:**
```r
infer_column_type(
    values,         # character vector of column values
    column_name,    # character scalar
    config = NULL   # list from load_profiling_config() (optional)
)
```

**Returns:** One of: `"identifier"`, `"numeric"`, `"date"`, `"categorical"`.

**Side Effects:** None (pure function).

---

## Dependency Graph

```
5_profile_data.R (user script)
    └── profile_data.R (step orchestrator)
            ├── load_profiling_config.R (config loader)
            ├── profile_table.R (table-level profiler)
            │       ├── infer_column_type.R (type inference)
            │       ├── detect_sentinels.R (sentinel detection)
            │       ├── profile_missingness.R (missingness analysis)
            │       ├── profile_distribution.R (distribution stats)
            │       ├── generate_issues.R (issue flagging)
            │       └── calculate_quality_score.R (quality scoring)
            ├── write_audit_event.R (audit logging)
            └── config/profiling_settings.yml (configuration)
```

---

## Database Tables

### `governance.data_profile`

Variable-level missingness profiling. One row per column per table per ingest.

**Key Columns:**
- `profile_id` (PK, serial), `ingest_id`, `schema_name`, `table_name`, `variable_name`
- `inferred_type` (numeric, categorical, date, identifier)
- `total_count`, `valid_count`, `na_count`, `empty_count`, `whitespace_count`, `sentinel_count`
- `na_pct`, `empty_pct`, `whitespace_pct`, `sentinel_pct`
- `total_missing_count`, `total_missing_pct`, `valid_pct`
- `unique_count`, `unique_pct`, `profiled_at`

### `governance.data_profile_distribution`

Distribution statistics. One row per column per table per ingest.

**Key Columns:**
- `distribution_id` (PK, serial), `ingest_id`, `schema_name`, `table_name`, `variable_name`
- `distribution_type` (numeric, categorical)
- `stat_min`, `stat_max`, `stat_mean`, `stat_median`, `stat_sd`, `stat_q25`, `stat_q75`, `stat_iqr`
- `top_values_json`, `mode_value`, `mode_count`, `mode_pct`

### `governance.data_profile_sentinel`

Detected sentinel/placeholder values. Zero or more rows per column.

**Key Columns:**
- `sentinel_id` (PK, serial), `ingest_id`, `schema_name`, `table_name`, `variable_name`
- `sentinel_value`, `sentinel_count`, `sentinel_pct`
- `detection_method` (config_list, frequency_analysis)
- `confidence` (high, medium)

### `governance.data_profile_issue`

Quality issues flagged during profiling. Zero or more rows per column.

**Key Columns:**
- `issue_id` (PK, serial), `ingest_id`, `schema_name`, `table_name`, `variable_name`
- `issue_type`, `severity` (critical, warning, info)
- `description`, `value`, `recommendation`

### `governance.data_profile_summary`

Per-table quality summary. One row per table per ingest.

**Key Columns:**
- `summary_id` (PK, serial), `ingest_id`, `schema_name`, `table_name`
- `row_count`, `variable_count`
- `avg_valid_pct`, `min_valid_pct`, `max_missing_pct`
- `critical_issue_count`, `warning_issue_count`, `info_issue_count`
- `quality_score` (Excellent, Good, Fair, Needs Review)
- `worst_variable`, `worst_variable_missing_pct`
