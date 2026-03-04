# Developer Onboarding — Step 1
## Source Registration

---

## Quick Start

### Run Step 1 (Source Registration)

```r
# 1. Edit the USER INPUT SECTION in the script
# 2. Run:
source("r/scripts/1_onboard_new_source.R")
```

---

## Prerequisites

Before running Step 1:

1. **PostgreSQL running**: The PULSE database must be accessible
2. **Environment variables set**: `PULSE_DB`, `PULSE_HOST`, `PULSE_USER`, `PULSE_PW`
3. **Core tables bootstrapped**: Run `source("pulse-init-all.R")` once to create schemas and governance tables

---

## Configuration Options

In `r/scripts/1_onboard_new_source.R`:

```r
# A unique identifier for this onboarding run
ingest_id <- "cisir2026_toy_260128"

# Core metadata for the source
source_params <- list(
  source_id              = "cisir2026_toy",
  source_name            = "CISIR Toy Data",
  system_type            = "CSV",          # CSV, XLSX, SQL, API, FHIR, Other
  update_frequency       = "monthly",      # daily, weekly, biweekly, monthly, quarterly, annually, ad_hoc
  data_owner             = "Data Owner Name",
  ingest_method          = "pull",         # push, pull, api, sftp, manual
  expected_schema_version = "1.0.0",
  retention_policy       = "Raw indefinite; staging 30 days",  # or NULL
  pii_classification     = "PHI",          # PHI, Limited, NonPHI
  active                 = TRUE
)
```

In `config/pipeline_settings.yml`:

```yaml
allowed_system_type: [CSV, XLSX, SQL, API, FHIR, Other]
allowed_update_frequency: [daily, weekly, biweekly, monthly, quarterly, annually, ad_hoc]
allowed_ingest_method: [push, pull, api, sftp, manual]
allowed_pii_classification: [PHI, Limited, NonPHI]
```

---

## Common Tasks

### Register a New Source

```r
source("r/scripts/1_onboard_new_source.R")
```

### Register Programmatically

```r
source("pulse-init-all.R")
source("pulse-launch.R")

pulse_launch(
  ingest_id     = "my_source_260128",
  source_params = list(
    source_id              = "my_source",
    source_name            = "My Source",
    system_type            = "CSV",
    update_frequency       = "monthly",
    data_owner             = "Data Team",
    ingest_method          = "manual",
    expected_schema_version = "1.0.0",
    pii_classification     = "PHI",
    active                 = TRUE
  )
)
```

### Update an Existing Source

Run the same script with the same `source_id` but updated field values. `register_source()` uses insert-or-update logic based on `source_id`.

### Check Registered Sources

```sql
SELECT source_id, source_name, system_type, update_frequency,
       pii_classification, active, created_at_utc
FROM governance.source_registry
ORDER BY source_id;
```

### View Registration Audit Trail

```sql
SELECT audit_id, action, details, executed_at_utc
FROM governance.audit_log
WHERE action LIKE '%source_registration%'
ORDER BY executed_at_utc DESC;
```

---

## Understanding Results

### What Gets Created

| Artifact | Location |
|----------|----------|
| Source registry row | `governance.source_registry` |
| Audit log entry | `governance.audit_log` |
| Pipeline step record | `governance.pipeline_step` |
| Raw incoming folder | `raw/<source_id>/incoming/` |
| Raw archive folder | `raw/<source_id>/archive/` |
| Staging incoming folder | `staging/<source_id>/incoming/` |
| Staging archive folder | `staging/<source_id>/archive/` |
| Validated folder | `validated/<source_id>/` |
| Governance folders | `governance/logs/`, `governance/qc/`, `governance/reports/` |

### Console Output

On success, the script prints:
```
>> Onboarding completed successfully!
   - Source registered in governance.source_registry
   - Folders created under raw/, staging/, validated/
   - Audit log entry written to governance.audit_log
   - STEP_001 present/enabled in governance.pipeline_step
   - You may now proceed to ingestion (Step 2: batch logging).
```

---

## Troubleshooting

### "Invalid system_type '...'. Allowed: ..."

The `system_type` value doesn't match the allowed vocabulary in `config/pipeline_settings.yml`. Check spelling and case — values are case-sensitive.

### "Missing required fields: ..."

One or more required fields are missing from `source_params`. Required fields (defined in `pipeline_settings.yml`): `source_id`, `source_name`, `system_type`, `update_frequency`, `data_owner`, `ingest_method`, `expected_schema_version`, `pii_classification`, `active`.

### "Required fields cannot be NULL: ..."

A required field was provided but set to `NULL`. All required fields must have non-NULL values.

### "Database connection failed"

Check that environment variables are set:
```r
Sys.getenv("PULSE_DB")    # should be "primeai_lake"
Sys.getenv("PULSE_HOST")  # should be "localhost"
Sys.getenv("PULSE_USER")  # your username
Sys.getenv("PULSE_PW")    # your password
```

### "Folder creation failed"

Check that you have write permissions to the project root. The folder template is defined in `directory_structure.yml`.

---

## File Locations

| Purpose | Path |
|---------|------|
| User script | `r/scripts/1_onboard_new_source.R` |
| Registration logic | `r/steps/register_source.R` |
| Step wrapper | `r/steps/run_step1_register_source.R` |
| Validation | `r/utilities/validate_source_entry.R` |
| Folder creation | `r/utilities/create_source_folders.R` |
| Audit logging | `r/steps/write_audit_event.R` |
| Pipeline step writer | `r/utilities/write_pipeline_step.R` |
| Pipeline runner | `r/runner.R` |
| Pipeline launcher | `pulse-launch.R` |
| Bootstrap | `pulse-init-all.R` |
| Pipeline settings | `config/pipeline_settings.yml` |
| Folder template | `directory_structure.yml` |
| Source Registry DDL | `sql/ddl/create_SOURCE_REGISTRY.sql` |
| Audit Log DDL | `sql/ddl/create_AUDIT_LOG.sql` |
| Pipeline Step DDL | `sql/ddl/create_PIPELINE_STEP.sql` |
| Step 1 seed data | `sql/inserts/pipeline_steps/STEP_001_register_source.sql` |
| Unit tests | `tests/testthat/test_step1_register_source.R` |
| Integration tests | `tests/testthat/test_step1_integration.R` |
