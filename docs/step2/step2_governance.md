# Governance — Step 2
## Batch Logging & File-Level Lineage

---

## Governance Objectives

Step 2 produces all early lineage records in the PULSE data pipeline. Its mission is to document exactly what files were received, how they were ingested, and where the data landed. Every file that enters the system must have a corresponding governance record, regardless of whether ingestion succeeded or failed.

---

## Data Quality Controls

### Strict Source Type Enforcement

Every file is matched against `reference.ingest_dictionary` using the `source_type` field. The dictionary is filtered to only rows matching the current `source_type` before any table or variable resolution occurs. This prevents cross-source contamination.

### Dictionary-Based Variable Mapping

Source variable names are mapped to lake variable names using `reference.ingest_dictionary`. No implicit inference is allowed — only explicitly defined mappings are used. Both file headers and dictionary entries are normalized symmetrically via `normalize_name()` before matching.

### Defensive Column Alignment

When appending to existing raw tables, `align_df_to_raw_table()` handles schema drift:
- Columns in the file but not in the table are added to the table as TEXT
- Columns in the table but not in the file are filled with NA
- Column order is matched to the existing table structure

### Duplicate Batch Prevention

`log_batch_ingest()` checks for existing `ingest_id` before inserting. If the ID already exists, the function stops with an error to prevent duplicate lineage records.

---

## Audit Trail

### `governance.batch_log`

Batch-level ingestion summary. One row per ingest event.

**Key Columns:**
- `ingest_id` (PK) — unique batch identifier
- `source_id` — registered source (FK to `governance.source_registry`)
- `ingest_timestamp` — when the batch was created
- `status` — `started`, `success`, `partial`, or `error`
- `error_message` — error details (if applicable)
- `file_count` — total files in the batch
- `files_success` — count of successfully ingested files
- `files_error` — count of failed files
- `batch_started_at_utc`, `batch_completed_at_utc` — timing

**Behavior:**
- Initial row written by `log_batch_ingest()` with status `"started"`
- Final status updated by `ingest_batch()` based on file outcomes
- Status logic: all success = `"success"`, all error = `"error"`, mixed = `"partial"`

### `governance.ingest_file_log`

File-level lineage. One row per file per batch.

**Key Columns:**
- `ingest_file_id` (PK) — auto-incrementing surrogate key (BIGSERIAL)
- `ingest_id` (FK) — links to `governance.batch_log`
- `file_name` — original file name
- `file_path` — full normalized path on disk
- `lake_table_name` — resolved destination table
- `load_status` — `pending`, `success`, `error`, or `skipped`
- `row_count` — number of rows ingested
- `file_size_bytes` — file size on disk
- `checksum` — MD5 hash of the file
- `logged_at_utc` — when the row was created (auto-set)
- `completed_at_utc` — when ingestion finished

**Behavior:**
- Pending rows written by `log_batch_ingest()` (one per file)
- Updated by `ingest_batch()` with ingestion results
- Failed files still get lineage records with `load_status = "error"`

### `staging.<lake_table>` (optional)

Auto-promoted typed tables created from raw data using SQL CAST.

**Behavior:**
- Created by `promote_to_staging()` during `ingest_batch()` when `type_decisions` is provided
- Each column is CAST to its target type from `type_decision_table.xlsx` (`final_type` -> `suggested_type` -> `TEXT` fallback)
- Table is dropped and recreated within a transaction on each promotion
- Rolls back on error to prevent partial state

---

## Governance Principles

1. **Every discovered file must have a lineage record.** No file enters the system untracked.
2. **source_type is enforced strictly.** Prevents cross-dataset contamination.
3. **No implicit inference.** Only mappings defined in `reference.ingest_dictionary` are allowed.
4. **Lineage is immutable once written.** Status updates occur but original records persist.
5. **Failure to ingest does not mean failure to log.** Lineage always captures the failure.

---

## Reproducibility

### Deterministic Ingestion

1. Same files + same dictionary = same table mappings and variable names
2. Dictionary-based resolution is deterministic (no heuristics)
3. MD5 checksums enable file-level identity verification
4. All columns read as character (TEXT) to prevent type coercion artifacts

### Re-ingestion

Running Step 2 again requires a new `ingest_id`. The system prevents duplicate batch IDs. Raw table appends are additive, not destructive.

---

## Compliance Checklist

- [ ] Batch log entry exists in `governance.batch_log`
- [ ] All files have lineage rows in `governance.ingest_file_log`
- [ ] Successful files have `row_count`, `file_size_bytes`, and `checksum` populated
- [ ] Failed files have `load_status = "error"` recorded
- [ ] `source_type` matches `reference.ingest_dictionary` entries
- [ ] No cross-source contamination in raw tables
- [ ] Batch status correctly reflects file outcomes
- [ ] Staging tables promoted when type_decisions available
- [ ] All unit tests passing

---

## Related Governance Artifacts

| Artifact | Location | Purpose |
|----------|----------|---------|
| Batch Log | `governance.batch_log` | Batch-level ingestion lineage |
| Ingest File Log | `governance.ingest_file_log` | File-level ingestion lineage |
| Ingest Dictionary | `reference.ingest_dictionary` | Source-to-lake column mappings |
| Type Decisions | `reference/type_decisions/type_decision_table.xlsx` | Target SQL types per variable |
| Pipeline Settings | `config/pipeline_settings.yml` | Controlled vocabularies |
| Batch Log DDL | `sql/ddl/create_BATCH_LOG.sql` | Table creation script |
| Ingest File Log DDL | `sql/ddl/create_INGEST_FILE_LOG.sql` | Table creation script |
| Step 2 Seed | `sql/inserts/pipeline_steps/STEP_002_batch_logging_and_ingestion.sql` | Step definition seed |
