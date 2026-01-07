# SOP Summary — Step 3
## Schema Validation Engine

---

Step 3 validates that all ingested raw tables conform to the expected schema definitions stored in `reference.metadata`.

---

## Purpose

- Compare observed table structures against expected schema definitions.
- Detect structural anomalies: missing columns, extra columns, type mismatches, PK discrepancies.
- Log all issues to `governance.structure_qc_table` for audit and governance.
- Optionally halt pipeline on critical issues.
- Prepare validated data for profiling (Step 4).

---

## Step-by-Step Summary

1. **Receive ingest_id.**
   Must match an existing entry in `governance.batch_log`.

2. **Load expected schema.**
   Read active schema definitions from `reference.metadata` (synced from Excel).

3. **Identify raw tables.**
   Query `governance.ingest_file_log` for tables loaded in this batch.

4. **Validate each table.**
   Call `compare_fields()` to check expected vs observed structure.

5. **Classify issues by severity.**
   - `critical`: Missing required columns, type mismatches on key fields
   - `warning`: Missing optional columns, unexpected extra columns
   - `info`: Column order drift, minor discrepancies

6. **Write issues.**
   Insert all detected issues into `governance.structure_qc_table`.

7. **Halt or continue.**
   If `halt_on_error = TRUE` and critical issues exist, stop execution.

---

## Outputs

- Schema comparison results for all tables
- Issues logged in `governance.structure_qc_table`
- Validation summary (tables validated, issue counts by severity)
- Ready for Step 4 data profiling

---

## Mermaid Flowchart

```mermaid
flowchart TD
    A[ingest_id] --> B[validate_schema()]
    B --> C[(reference.metadata)]
    B --> D[(ingest_file_log)]
    D --> E[Get raw tables]
    E --> F[For each table]
    F --> G[compare_fields()]
    G --> H{Issues found?}
    H -- Yes --> I[(structure_qc_table)]
    H -- No --> J[Next table]
    I --> K{Critical issues?}
    K -- Yes & halt_on_error --> L[HALT]
    K -- No --> M[Continue]
    J --> M
    M --> N[Return summary]
```

---

## Completion Criteria

- All tables from ingest validated
- No undetected structural anomalies
- All issues logged with proper severity
- Critical issues halt pipeline when configured
- Validation reproducible via ingest_id

---

## Key Files

| File | Purpose |
|------|---------|
| `r/scripts/3_validate_schema.R` | User-facing wrapper script |
| `r/steps/validate_schema.R` | Core validation orchestrator |
| `r/utilities/compare_fields.R` | Field comparison helper |
| `r/reference/sync_metadata.R` | Excel to database sync |
| `sql/ddl/create_METADATA.sql` | Expected schema table DDL |
| `sql/ddl/create_STRUCTURE_QC_TABLE.sql` | Issue logging table DDL |
| `reference/expected_schema_dictionary.xlsx` | Governed schema definitions |
