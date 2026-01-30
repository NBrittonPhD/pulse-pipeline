# Governance — Step 4
## Metadata Synchronization

---

## Governance Objectives

Step 4 ensures that all variable definitions in the database are traceable, versioned, and auditable. Every change to the metadata dictionary is recorded at the field level, enabling full reproducibility and compliance review.

---

## Data Quality Controls

### Version Management

- Every sync operation increments `version_number` by 1
- All active rows in `reference.metadata` share the same `version_number` after a sync
- Previous versions are preserved via `reference.metadata_history`
- Version number is queryable: `SELECT MAX(version_number) FROM reference.metadata`

### Soft Deletes

- Variables removed from the Excel dictionary are **not** physically deleted
- They are marked `is_active = FALSE` with `updated_at` set to the sync timestamp
- Soft-deleted variables retain their `version_number` (set to the version that removed them)
- Downstream queries must filter on `is_active = TRUE`

### Duplicate Key Prevention

- Composite PK: (`lake_table_name`, `lake_variable_name`, `source_type`)
- `load_metadata_dictionary()` warns on duplicate keys in Excel before sync
- Database enforces uniqueness via `ON CONFLICT` clause

---

## Audit Trail

### `reference.metadata`

- `version_number`: Current metadata version
- `is_active`: TRUE = active variable, FALSE = soft-deleted
- `created_at`: Row creation timestamp
- `updated_at`: Last modified timestamp

### `reference.metadata_history`

- `version_number`: Version in which this change occurred
- `lake_table_name`, `lake_variable_name`, `source_type`: Variable identity
- `field_changed`: Which column was modified
- `old_value`, `new_value`: Before and after values
- `change_type`: INITIAL / ADD / UPDATE / REMOVE
- `changed_at`: When the change was recorded
- `changed_by`: Database user who performed the sync

### `governance.audit_log`

- One event per sync operation
- `event_type`: `metadata_sync`
- `details`: JSON with version_number, dict_path, total_variables, adds, updates, removes

---

## Reproducibility

### Deterministic Sync

1. Same Excel file always produces the same dictionary output
2. Same `version_number` always maps to the same set of changes in history
3. Change detection is field-level — only actual value differences are recorded

### Re-sync

To re-sync from Excel:
1. Update `CURRENT_core_metadata_dictionary.xlsx` with desired changes
2. Run `source("r/scripts/4_sync_metadata.R")`
3. New version is created; previous version preserved in history

---

## Compliance Checklist

- [ ] `reference.metadata` table has all dictionary columns populated
- [ ] Version number matches expected release
- [ ] `reference.metadata_history` has change records for current version
- [ ] All active variables have `is_active = TRUE`
- [ ] Removed variables have `is_active = FALSE` (not physically deleted)
- [ ] Audit log event exists for each sync operation
- [ ] No duplicate composite keys in metadata table

---

## Related Governance Artifacts

| Artifact | Location | Purpose |
|----------|----------|---------|
| Core Metadata Dictionary | `reference/CURRENT_core_metadata_dictionary.xlsx` | Source of truth for variable definitions |
| Metadata Table | `reference.metadata` | Database-queryable dictionary (synced by Step 4) |
| Metadata History | `reference.metadata_history` | Field-level change audit trail |
| Metadata DDL | `sql/ddl/recreate_METADATA_v2.sql` | Table creation script |
| History DDL | `sql/ddl/create_METADATA_HISTORY.sql` | History table creation script |
| Audit Log | `governance.audit_log` | Sync event records |
