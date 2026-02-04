# SOP Summary — Step 2
## Batch Logging & File-Level Lineage

---

Step 2 ensures that ingestion events are fully logged, reproducible, deterministic, and strictly separated by source_type.

---

## Purpose

- Track every file involved in an ingest.
- Create permanent batch-level and file-level lineage.
- Ingest raw data with strict mapping and error-safe behavior.
- Prepare data and metadata for schema validation (Step 3).

---

## Step-by-Step Summary

1. **Detect incoming files.**
   All `.csv` files under `raw/<source_id>/incoming/`.

2. **Create batch_log entry.**
   `log_batch_ingest()` inserts a single batch row.

3. **Create pending lineage rows.**
   One `ingest_file_log` row per file.

4. **Ingest files.**
   `ingest_batch()` calls `ingest_one_file()` individually.

5. **Strict Type Enforcement.**
   Each file is matched only within its `source_type`.

6. **Update lineage.**
   Success: row_count, file_size_bytes, checksum.
   Error: `load_status = "error"` with table set if determinable.

7. **Finalize batch.**
   Status becomes `success`, `partial`, or `error`.

8. **Promote to staging.**
   If `type_decisions` are provided, `ingest_batch()` automatically promotes each successfully ingested raw table to `staging.<lake_table>` using SQL-based type casting via `promote_to_staging()`. Each column is CAST to its target type from `type_decision_table.xlsx` (final_type → suggested_type → TEXT fallback).

---

## Outputs

- Appended RAW tables (`raw.<lake_table>`)
- Typed staging tables (`staging.<lake_table>`) — auto-promoted when type_decisions available
- Complete lineage in `batch_log`
- Complete lineage in `ingest_file_log`
- Deterministic file mappings
- Ready for Step 3 structural validation

---

## Mermaid Flowchart

```mermaid
flowchart TD
    A[Incoming Files] --> B[log_batch_ingest()]
    B --> C[(batch_log)]
    B --> D[(ingest_file_log: pending)]
    D --> E[ingest_batch()]
    E --> F[ingest_one_file()]
    F --> G[(raw.<lake_table>)]
    E --> H[(ingest_file_log: success/error)]
    E --> I[(batch_log: final status)]
    I --> J{type_decisions?}
    J -->|Yes| K[promote_to_staging()]
    K --> L[(staging.<lake_table>)]
    J -->|No| M[Skip staging]
```

---

## Completion Criteria

- No unlogged files
- No missing lineage fields
- All ingestion failures properly recorded
- No cross-source type contamination
- Batch status correct
- Raw data appended where appropriate
