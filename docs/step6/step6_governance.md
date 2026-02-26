# Governance — Step 6
## Harmonization (Staging to Validated)

---

## Governance Objectives

Step 6 ensures that data from multiple clinical sources is combined into a unified validated schema with full traceability. Every harmonization operation is logged, every row is tagged with its source provenance, and column mappings are driven by the governed metadata dictionary rather than hardcoded logic.

---

## Data Quality Controls

### Metadata-Driven Mappings

All column mappings flow from the core metadata dictionary:

```
CURRENT_core_metadata_dictionary.xlsx
    → sync_metadata() → reference.metadata
    → sync_harmonization_map() → reference.harmonization_map
    → build_harmonization_query() → SQL SELECT
    → INSERT INTO validated.{table}
```

No hardcoded column names exist in the harmonization engine. Changes to the dictionary automatically propagate to the mappings on the next sync.

### Transform Types

| Type | Governance Implication |
|------|------------------------|
| `direct` | Auto-detected: source and target column names match |
| `rename` | Auto-detected: source and target column names differ |
| `expression` | Manually curated: SQL expression preserved across syncs |
| `constant` | Manually curated: literal value preserved across syncs |
| `coalesce` | Manually curated: multi-column merge preserved across syncs |

The upsert logic in `sync_harmonization_map()` preserves manually curated `expression`, `constant`, and `coalesce` overrides — only `direct` and `rename` types are auto-updated from metadata.

### Safe Type Casting

When staging column types differ from validated column types:

- **TEXT to NUMERIC**: Regex guard ensures non-numeric values (e.g., "Negative", "<0.02", "0-2") become `NULL` instead of failing the entire insert
- **Other mismatches**: Direct CAST applied
- **Missing source columns**: Mapped as `NULL` with a logged warning

This prevents dirty data from causing batch failures while preserving data that can be correctly converted.

### Source Provenance

Every row in every validated table includes three governance columns:

| Column | Purpose |
|--------|---------|
| `source_type` | Origin system (CISIR, CLARITY, TRAUMA_REGISTRY) |
| `source_table` | Exact staging table name |
| `ingest_id` | Batch identifier linking to `governance.batch_log` |

This enables downstream queries to filter by source, trace any row back to its origin, and support cross-source deduplication in future steps.

### Idempotency

- Prior data for `(ingest_id, target_table)` is deleted before re-harmonization
- Prior `transform_log` entries for the same scope are also deleted
- Re-running Step 6 produces identical results with no duplicate rows
- Delete-before-insert pattern scoped to the specific ingest

---

## Audit Trail

### `reference.harmonization_map`

- One row per column-level mapping
- `is_active` flag enables/disables mappings without deletion
- `priority` field for conflict resolution
- `created_at` / `updated_at` timestamps for change tracking
- Unique constraint prevents duplicate mappings

### `governance.transform_log`

- One row per source-to-target operation per ingest
- `source_row_count` and `target_row_count` for reconciliation
- `columns_mapped` count for mapping coverage tracking
- `status`: `success`, `partial`, or `failed` with `error_message`
- `started_at`, `completed_at`, `duration_seconds` for performance monitoring
- FK to `governance.batch_log` for full lineage chain

### `governance.audit_log`

- One event per harmonization run
- `action`: `harmonization|success|schema|validated.*` or `harmonization|partial|schema|validated.*`
- `details`: JSON with `tables_processed`, `tables_ok`, `tables_failed`, `total_rows`, `sources_processed`, `source_type_filter`, `duration_seconds`

---

## Lineage Chain

Full data lineage from source file to validated row:

```
Source CSV file
    → governance.ingest_file_log (file_name, checksum, row_count)
    → raw.{lake_table} (all TEXT)
    → staging.{lake_table} (typed via promote_to_staging)
    → reference.harmonization_map (column mapping)
    → governance.transform_log (operation audit)
    → validated.{target_table} (unified, with source_type + ingest_id)
```

---

## Reproducibility

### Deterministic Harmonization

1. Same staging data + same mappings always produce the same validated output
2. Mapping sync from metadata is deterministic — same dictionary yields same mappings
3. Type casting is deterministic — same input types yield same CAST behavior
4. Idempotent re-runs produce identical results

### Re-harmonization

To re-harmonize an ingest:
1. Run `source("r/scripts/6_harmonize_data.R")` with the same `ingest_id`
2. Prior validated data and transform_log entries are deleted automatically
3. Fresh harmonization results written
4. New audit log event created

---

## Compliance Checklist

- [ ] All 23 validated table DDLs exist in database
- [ ] `reference.harmonization_map` DDL exists and table is populated
- [ ] `governance.transform_log` DDL exists
- [ ] Mappings sync correctly from metadata dictionary
- [ ] All staging tables for the ingest have been processed
- [ ] Every validated row has `source_type`, `source_table`, and `ingest_id` populated
- [ ] Transform log records every source-to-target operation
- [ ] Failed operations are logged with `status = 'failed'` and `error_message`
- [ ] Skipped staging tables are logged with `status = 'failed'` and explanation
- [ ] Audit log event written for the harmonization run
- [ ] Idempotent re-run produces no duplicate rows
- [ ] Manually curated mappings (expression/constant/coalesce) preserved across syncs

---

## Related Governance Artifacts

| Artifact | Location | Purpose |
|----------|----------|---------|
| Harmonization Map | `reference.harmonization_map` | Column-level staging-to-validated mappings |
| Transform Log | `governance.transform_log` | Operation-level audit trail |
| Audit Log | `governance.audit_log` | Harmonization event records |
| Harmonization Map DDL | `sql/ddl/create_HARMONIZATION_MAP.sql` | Table creation script |
| Transform Log DDL | `sql/ddl/create_TRANSFORM_LOG.sql` | Table creation script |
| Validated DDLs | `sql/ddl/create_VALIDATED_*.sql` | 23 validated table creation scripts |
| Metadata Dictionary | `reference/CURRENT_core_metadata_dictionary.xlsx` | Upstream source of mappings |
| Metadata DB Table | `reference.metadata` | Synced dictionary (source for mapping sync) |
