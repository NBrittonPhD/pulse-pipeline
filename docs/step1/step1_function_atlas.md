Function Atlas — Step 1: Source Registration

1. validate_source_entry()
File: r/utilities/validate_source_entry.R
Purpose:
  --Ensures all required fields are present.
  --Confirms values comply with controlled vocabularies in pipeline_settings.yml.
Inputs: named list or YAML-derived parameters
Returns: TRUE or raises an error
Throws:
  --Missing required field
  --Invalid vocabulary value
  --Incorrect data type
  
  
2. create_source_folders()
File: r/utilities/create_source_folders.R
Purpose: Given a source_id, builds folder tree defined in directory_structure.yml.
Side effects: Creates directories under the project root.
Returns: vector of created paths


3. write_audit_event()
File: r/steps/write_audit_event.R
Purpose:Insert a single, governed event record into governance.audit_log.
Inputs:
  --con postgres connection
  --event_type
  --event_details list (auto JSON-encoded)
Outputs: audit_log row


4. register_source()
File: r/steps/register_source.R
Purpose:
Core Step 1 action: validate, insert/update registry entry, create folders, log audit.
Inputs: Source metadata fields
Side effects:
  --Writes DB rows
  --Writes folder structure
  --Generates audit log event
Returns: list with status, source_id, metadata


5. run_step1_register_source()
File: r/steps/run_step1_register_source.R
Purpose: Step wrapper executed by the pipeline runner.
Inputs:
  --con
  --Source params (from YAML)
Side effects: Calls register_source()
Records step completion in pipeline_step


6. load_source_params()
File: r/utilities/load_source_params.R
Loads config/source_params.yml.

7. load_pipeline_settings()
File: r/runner.R
Loads vocab + folder template file references.

8. execute_step() / run_pipeline()
File: r/runner.R
Determines which step function to call.
Coordinates sequencing and logging.