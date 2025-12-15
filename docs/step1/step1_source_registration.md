Step 1 — Source Registration System

Step 1 is the entry point for all data sources entering the PULSE pipeline.

Its purpose is to:
  --validate source metadata against controlled vocabularies,
  --create folder structures for ingest + governance,
  --register the source in the governance database,
  --write an auditable event log, and
  --record the pipeline step completion in governance.pipeline_step.

Step 1 is automatically executed when running:
  --run_pipeline(ingest_id)
or interactively via the onboarding script:
  --source("r/scripts/1_onboard_new_source.R")

Files Involved
      Component	                           Path
Source registration function	    r/steps/register_source.R
Vocabulary + required fields	    config/pipeline_settings.yml
Folder structure template	        directory_structure.yml
Validation helper	                r/utilities/validate_source_entry.R
Folder creation utility	          r/utilities/create_source_folders.R
Audit logging	                    r/steps/write_audit_event.R
Step wrapper	                    r/steps/run_step1_register_source.R
Pipeline runner	                  r/runner.R
Unit tests	                      tests/testthat/test_step1_register_source.R
                                  tests/testthat/test_step1_integration.R
Execution Flow

flowchart TD
    A[source_params.yml] --> B(run_pipeline)
    B --> C(load_pipeline_settings)
    C --> D(run_step1_register_source)
    D --> E(validate_source_entry)
    E --> F(register_source)
    F --> F1[create_or_update source_registry]
    F --> F2[create_source_folders]
    F --> F3[write_audit_event]
    F --> F4[write_pipeline_step]
    F4 --> G[Step 1 complete]
    
    
What Step 1 Produces:
  --A new row in governance.source_registry
  --A folder tree under /raw/<source_id>/, /staging/…, /validated/…, /governance/...
  --A governance audit log entry with event_type = "source_registration"
  --A pipeline step log in governance.pipeline_step

Step 1 must pass all unit tests before downstream steps (batch logging, schema validation, harmonization) can run.