# Governance — Step 5
## Data Profiling

---

## Governance Objectives

Step 5 ensures that every raw table ingested into the data lake is systematically profiled for data quality before harmonization begins. Profiling results are stored in governed database tables, enabling reproducible quality assessment, issue tracking, and compliance review.

---

## Data Quality Controls

### Missingness Classification

Every value in every column is classified into exactly one of five mutually exclusive categories:

| Category | Definition |
|----------|------------|
| NA | R `NA` values |
| Empty | Empty strings (`""`) |
| Whitespace | Whitespace-only strings (e.g., `"  "`, `"\t"`) |
| Sentinel | Known placeholder values (e.g., `999`, `UNKNOWN`) |
| Valid | Everything else |

This classification order ensures no value is double-counted.

### Sentinel Detection

Two detection methods are employed:

| Method | Confidence | Description |
|--------|------------|-------------|
| `config_list` | High | Value matches a configured sentinel list (numeric or string) |
| `frequency_analysis` | Medium | Repeat-digit patterns (e.g., 99, 999) detected in numeric columns above frequency threshold |

Configured sentinels are defined in `config/profiling_settings.yml`.

### Column Type Inference

Because all `raw.*` tables store data as TEXT, column types are inferred from content:

| Priority | Type | Detection Rule |
|----------|------|----------------|
| 1 | Identifier | Column name matches exact list or regex pattern |
| 2 | Numeric | >90% of non-missing values parse as numbers |
| 3 | Date | >80% of sampled values parse in common date formats |
| 4 | Categorical | Default fallback |

### Quality Scoring

Each table receives a quality score based on its worst-case metrics:

| Score | Max Missing % | Max Critical Issues |
|-------|---------------|---------------------|
| Excellent | <=5% | 0 |
| Good | <=10% | <=2 |
| Fair | <=20% | <=5 |
| Needs Review | >20% or >5 | >5 |

The overall ingest score is the worst per-table score.

### Idempotency

- Prior profiling data for `(ingest_id, schema_name)` is deleted before re-profiling
- Re-running Step 5 produces identical results with no duplicate rows
- Delete-before-insert pattern scoped to the specific ingest and schema

---

## Issue Types

| Issue Type | Severity | Trigger | Recommendation |
|------------|----------|---------|----------------|
| `identifier_missing` | critical | Any missing values in identifier column | Investigate source data — identifier fields must be complete |
| `high_missingness` | warning | >20% total missing (non-identifier) | Review data source; consider imputation or exclusion |
| `moderate_missingness` | info | 10-20% total missing (non-identifier) | Monitor; may need review before use |
| `constant_value` | info | Only 1 unique valid value | Column provides no discriminating information |
| `high_cardinality` | info | >90% unique values (non-identifier, >10 rows) | Verify expected; may indicate free-text |

---

## Audit Trail

### `governance.data_profile`

- One row per column per table per ingest
- `inferred_type`: How the system classified the column
- `profiled_at`: Timestamp of profiling run
- All missingness counts and percentages preserved

### `governance.data_profile_distribution`

- Numeric: min, max, mean, median, sd, quartiles, IQR
- Categorical: top values as JSON, mode value/count/pct

### `governance.data_profile_sentinel`

- Each detected sentinel recorded with value, count, percentage
- `detection_method`: How the sentinel was identified
- `confidence`: Reliability of the detection

### `governance.data_profile_issue`

- Each quality issue recorded with type, severity, description
- `value`: Associated metric (e.g., missing percentage)
- `recommendation`: Actionable guidance

### `governance.data_profile_summary`

- One row per table with aggregate quality metrics
- `quality_score`: Overall table quality rating
- `worst_variable`: Column with highest missingness

### `governance.audit_log`

- One event per profiling run
- `action`: `data_profiling|success|schema|raw.*`
- `details`: JSON with tables_profiled, variables_profiled, sentinels_detected, issue counts, overall_score

---

## Reproducibility

### Deterministic Profiling

1. Same raw data always produces the same profiling results
2. Same config always produces the same thresholds and sentinel lists
3. Issue detection is deterministic — same inputs yield same issues

### Re-profiling

To re-profile an ingest:
1. Run `source("r/scripts/5_profile_data.R")` with the same `ingest_id`
2. Prior results are deleted automatically (idempotency)
3. Fresh profiling results written to all 5 tables
4. New audit log event created

---

## Compliance Checklist

- [ ] All 5 profiling governance tables exist in database
- [ ] Profiling config file exists at `config/profiling_settings.yml`
- [ ] All raw tables from the ingest have been profiled
- [ ] Every column has a missingness profile in `governance.data_profile`
- [ ] Every column has a distribution record in `governance.data_profile_distribution`
- [ ] Sentinel values detected and recorded in `governance.data_profile_sentinel`
- [ ] Quality issues flagged at appropriate severity levels
- [ ] Per-table quality scores calculated and stored in `governance.data_profile_summary`
- [ ] Overall ingest score is the worst per-table score
- [ ] Audit log event written for the profiling run
- [ ] Idempotent re-run produces no duplicate rows

---

## Related Governance Artifacts

| Artifact | Location | Purpose |
|----------|----------|---------|
| Profiling Config | `config/profiling_settings.yml` | Thresholds, sentinel lists, identifier patterns |
| Profile Table | `governance.data_profile` | Variable-level missingness |
| Distribution Table | `governance.data_profile_distribution` | Numeric/categorical stats |
| Sentinel Table | `governance.data_profile_sentinel` | Detected placeholder values |
| Issue Table | `governance.data_profile_issue` | Quality issues flagged |
| Summary Table | `governance.data_profile_summary` | Per-table quality scores |
| Profile DDL | `sql/ddl/create_DATA_PROFILE.sql` | Table creation script |
| Distribution DDL | `sql/ddl/create_DATA_PROFILE_DISTRIBUTION.sql` | Table creation script |
| Sentinel DDL | `sql/ddl/create_DATA_PROFILE_SENTINEL.sql` | Table creation script |
| Issue DDL | `sql/ddl/create_DATA_PROFILE_ISSUE.sql` | Table creation script |
| Summary DDL | `sql/ddl/create_DATA_PROFILE_SUMMARY.sql` | Table creation script |
| Audit Log | `governance.audit_log` | Profiling event records |
