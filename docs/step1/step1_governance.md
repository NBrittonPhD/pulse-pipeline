# Governance — Step 1
## Source Registration

---

## Governance Objectives

Source registration is the beginning of all lineage, compliance, and reproducibility in the PULSE ecosystem. Step 1 produces formal governance artifacts, ensures metadata consistency against controlled vocabularies, and establishes the ingestion contract for every data source.

---

## Data Quality Controls

### Controlled Vocabulary Validation

Every source registration is validated against controlled vocabularies defined in `config/pipeline_settings.yml`:

| Field | Allowed Values |
|-------|----------------|
| `system_type` | CSV, XLSX, SQL, API, FHIR, Other |
| `update_frequency` | daily, weekly, biweekly, monthly, quarterly, annually, ad_hoc |
| `ingest_method` | push, pull, api, sftp, manual |
| `pii_classification` | PHI, Limited, NonPHI |

Validation is enforced by `validate_source_entry()` before any database writes occur. Registration fails immediately if any field violates the vocabulary.

### Required Fields

All of the following must be provided (non-NULL, non-empty):

- `source_id` — unique, machine-readable identifier
- `source_name` — human-readable name
- `system_type` — upstream data format
- `update_frequency` — expected cadence
- `data_owner` — responsible party
- `ingest_method` — ingestion pathway
- `expected_schema_version` — semantic version string
- `pii_classification` — data sensitivity level
- `active` — boolean flag to enable/disable the source

### Insert vs Update Logic

`register_source()` checks whether `source_id` already exists in `governance.source_registry`:
- **New source**: INSERT with full metadata, create folder structure
- **Existing source**: UPDATE metadata fields, skip folder creation, log as `source_update`

---

## Audit Trail

### `governance.source_registry`

The authoritative record of every onboarded data source.

**Key Columns:**
- `source_id` — unique identifier (PK)
- `source_name`, `system_type`, `update_frequency`, `data_owner`, `ingest_method`
- `expected_schema_version`, `retention_policy`, `pii_classification`
- `active` — boolean flag to enable/disable the source
- `created_at_utc`, `last_modified_utc`, `created_by`

**Behavior:**
- Rows written only by `register_source()`
- Controlled vocabulary validation enforced before write
- Insert vs update based on `source_id` existence

### `governance.audit_log`

Captures structured governance events.

**Step 1 Behavior:**
- Event type: `source_registration` (new) or `source_update` (existing)
- `audit_id`: UUID-based unique identifier
- `details`: JSON-encoded metadata including all source parameters
- Required for compliance and provenance

### `governance.pipeline_step`

Configuration table defining all pipeline steps. This is a definition/registry table, not per-run execution history. Each row describes a step's identity, ordering, type, and enabled status.

**Step 1 Behavior:**
- Written by `run_step1_register_source()` via `write_pipeline_step()`
- Ensures `STEP_001` is defined with correct metadata (step_order, step_name, step_type, code_snippet, enabled)
- Uses upsert logic: inserts if `step_id` is new, updates metadata if it already exists

---

## Folder Structure

`create_source_folders()` reads `directory_structure.yml` and creates:

```
raw/{source_id}/incoming/      — landing zone for raw CSV files
raw/{source_id}/archive/       — archived raw files after ingestion
staging/{source_id}/incoming/  — staging zone processing
staging/{source_id}/archive/   — archived staging files
validated/{source_id}/         — validated zone storage
governance/logs/               — governance log files
governance/qc/                 — QC output files
governance/reports/            — governance reports
```

Folders are only created for new sources (not on updates).

---

## Reproducibility

### Deterministic Registration

1. Same source parameters always produce the same registry entry
2. Validation against controlled vocabularies is deterministic
3. Folder creation is idempotent — existing folders are not overwritten

### Re-registration

Running Step 1 again with the same `source_id` updates the existing record rather than creating a duplicate. A new audit log event is created with `event_type = "source_update"`.

---

## Compliance Checklist

- [ ] Source registry entry exists in `governance.source_registry`
- [ ] All metadata fields pass controlled vocabulary validation
- [ ] Folder structure created on disk
- [ ] Audit log entry written to `governance.audit_log`
- [ ] Pipeline step record written to `governance.pipeline_step`
- [ ] `pii_classification` correctly reflects data sensitivity
- [ ] `data_owner` identifies the responsible party
- [ ] All unit tests passing
- [ ] Integration test verifies end-to-end execution

---

## Related Governance Artifacts

| Artifact | Location | Purpose |
|----------|----------|---------|
| Source Registry | `governance.source_registry` | Authoritative source metadata |
| Audit Log | `governance.audit_log` | Registration event records |
| Pipeline Step | `governance.pipeline_step` | Step definition/configuration registry |
| Pipeline Settings | `config/pipeline_settings.yml` | Controlled vocabularies |
| Folder Template | `directory_structure.yml` | Folder creation template |
| Source Registry DDL | `sql/ddl/create_SOURCE_REGISTRY.sql` | Table creation script |
| Audit Log DDL | `sql/ddl/create_AUDIT_LOG.sql` | Table creation script |
| Pipeline Step DDL | `sql/ddl/create_PIPELINE_STEP.sql` | Table creation script |
| Step 1 Seed | `sql/inserts/pipeline_steps/STEP_001_register_source.sql` | Step definition seed |
