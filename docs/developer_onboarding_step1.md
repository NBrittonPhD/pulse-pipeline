# Developer Onboarding — Step 1: Source Registration

## Overview
Step 1 establishes a new data source in the PULSE system. It validates metadata, writes governance entries, creates the folder structure, and produces lineage needed for downstream processing. This onboarding document explains how Step 1 works and how developers should interact with it.

---

## 1. What Step 1 Does
- Validates all source metadata against controlled vocabularies
- Creates or updates a row in `governance.source_registry`
- Builds the required directory structure for the source
- Writes an audit log entry describing the registration event
- Records the successful execution of Step 1 in `governance.pipeline_step`
- Executes automatically inside `run_pipeline()`

Step 1 is the foundation for ingestion, schema validation, profiling, harmonization, releases, and governance documentation.

---

## 2. Files and Components Involved

### Configuration Files
- `config/pipeline_settings.yml` — vocabularies and global settings
- `config/source_params.yml` — parameters for onboarding a specific source
- `directory_structure.yml` — folder creation template

### SQL DDL Files
- `sql/ddl/create_schemas.sql`
- `sql/ddl/create_source_registry.sql`
- `sql/ddl/create_audit_log.sql`
- `sql/ddl/create_pipeline_step.sql`

### R Code
- `r/steps/register_source.R`
- `r/steps/run_step1_register_source.R`
- `r/utilities/validate_source_entry.R`
- `r/utilities/create_source_folders.R`
- `r/steps/write_audit_event.R`
- `r/runner.R`
- `pulse-init-all.R`

### Tests
- `tests/testthat/test_step1_register_source.R`
- `tests/testthat/test_step1_integration.R`

---

## 3. End-to-End Execution Flow
1. Developer or onboarding script writes `config/source_params.yml`.
2. `run_pipeline()` is executed.
3. Pipeline loads settings and determines enabled steps.
4. `execute_step()` detects Step 1 and passes control to `run_step1_register_source()`.
5. `register_source()`:
   - calls `validate_source_entry()`  
   - writes to `source_registry`  
   - calls `create_source_folders()`  
   - calls `write_audit_event()`  
6. `write_pipeline_step()` records completion of Step 1.
7. Pipeline moves on to Step 2.

---

## 4. How to Run Step 1 Manually

source("pulse-init-all.R")
run_pipeline("my_ingest_id")

or
source("r/scripts/1_onboard_new_source.R")

## 5. When Step 1 Is Considered Complete
- Source registry entry created or updated
- Folder structure created successfully
- Audit log entry written
- Unit tests passing
- Integration test verifies pipeline execution
- Row written to governance.pipeline_step

