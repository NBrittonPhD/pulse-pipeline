# Governance Documentation — Step 2
## Batch Logging & File-Level Lineage

---

Step 2 produces all early lineage records in the PULSE data pipeline.
Its mission is to **document exactly what files were received and how they were ingested.**

---

# 1. `governance.batch_log`

**Purpose:**
Record each ingestion event.

**Key Fields:**
- `ingest_id` (PK)
- `source_id`
- `file_count`
- `files_success`
- `files_error`
- `status` (success / partial / error)
- Timestamps: `batch_started_at_utc`, `batch_completed_at_utc`

**Written By:**
- `log_batch_ingest()` (initial row)
- `ingest_batch()` (final status update)

---

# 2. `governance.ingest_file_log`

**Purpose:**
Record metadata and status for *each* file in a batch.

**Key Fields:**
- `ingest_file_id` (PK)
- `ingest_id` (FK)
- `file_name`, `file_path`
- `lake_table_name`
- `load_status` (pending, success, error)
- `row_count`
- `file_size_bytes`
- `checksum`
- `completed_at_utc`

**Written By:**
- `log_batch_ingest()`
- Updated by `ingest_batch()`

---

# 3. `staging.<lake_table>`

**Purpose:**
Auto-promoted typed tables created from raw data using SQL CAST.

**Key Details:**
- Created by `promote_to_staging()` during `ingest_batch()` when `type_decisions` is provided
- Each column is CAST to its target type from `type_decision_table.xlsx` (final_type → suggested_type → TEXT fallback)
- Table is dropped and recreated within a transaction on each promotion
- Currently 47 staging tables corresponding to all 47 raw tables

**Written By:**
- `promote_to_staging()` (called by `ingest_batch()`)

---

# Governance Principles

1. **Every discovered file must have a lineage record.**
2. **source_type is enforced strictly.**
   Prevents cross-dataset contamination.
3. **No implicit inference.**
   Only mappings defined in `reference.ingest_dictionary` are allowed.
4. **Lineage is immutable once written.**
5. **Failure to ingest ≠ failure to log.**
   Lineage always captures the failure.

---

# Downstream Dependencies

- **Step 3:** Schema Validation (uses `raw.*` tables and lineage)
- **Step 5:** Data Profiling (profiles `raw.*` or `staging.*` tables)
- **Step 6:** Harmonization (uses `staging.*` tables as input for `validated.*`)
- **Step 10:** Governance Release Documentation
- Audits, reproducibility, error analysis
