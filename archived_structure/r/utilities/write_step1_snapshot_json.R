# =============================================================================
# write_step1_snapshot_json.R
# -----------------------------------------------------------------------------
# Writes a static JSON snapshot of Step 1 × Cluster 1 to disk.
# This is mainly for portability and documentation, not for auto-discovery.
# =============================================================================

write_step1_snapshot_json <- function(
    output_path = "~/Documents/PULSE/pulse-pipeline/docs/governance/step1_cluster1_snapshot.json"
) {
  # Make sure required packages are loaded
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required. Please install it first.")
  }
  if (!requireNamespace("fs", quietly = TRUE)) {
    stop("Package 'fs' is required. Please install it first.")
  }
  
  # Create parent directory if needed
  fs::dir_create(fs::path_dir(output_path))
  
  # The JSON structure below MUST match the one in the SOP / chat snapshot
  snapshot <- list(
    version = "step1_cluster1_snapshot_v1",
    description = "PULSE pipeline Step 1 × Cluster 1 (Source Registration) — code + structure snapshot.",
    schemas = list(
      governance = "governance",
      raw        = "raw",
      staging    = "staging",
      validated  = "validated",
      reference  = "reference"
    ),
    db_objects = list(
      tables = list(
        list(
          name = "governance.source_registry",
          purpose = "Canonical registry of data sources and governance metadata.",
          ddl_file = "sql/ddl/create_SOURCE_REGISTRY.sql",
          key_columns = c(
            "source_id",
            "source_name",
            "system_type",
            "update_frequency",
            "data_owner",
            "ingest_method",
            "expected_schema_version",
            "retention_policy",
            "pii_classification",
            "active",
            "created_at_utc",
            "last_modified_utc",
            "created_by"
          )
        ),
        list(
          name = "governance.audit_log",
          purpose = "Governance audit log for events such as source_registration and source_update.",
          ddl_file = "sql/ddl/create_AUDIT_LOG.sql",
          key_columns = c(
            "audit_id",
            "ingest_id",
            "action",
            "details",
            "executed_by",
            "executed_at_utc"
          )
        ),
        list(
          name = "governance.pipeline_step",
          purpose = "Registry of all pipeline steps and their definitions.",
          ddl_file = "sql/ddl/create_PIPELINE_STEP.sql",
          key_columns = c(
            "step_id",
            "step_order",
            "step_name",
            "step_description",
            "step_type",
            "code_snippet",
            "enabled",
            "created_at_utc",
            "last_modified_utc"
          )
        )
      ),
      seed_data = list(
        list(
          file = "sql/inserts/insert_PIPELINE_STEP.sql",
          notes = "Inserts STEP_001–STEP_010 into governance.pipeline_step."
        )
      )
    ),
    configs = list(
      pipeline_settings = list(
        file = "config/pipeline_settings.yml",
        contents_summary = list(
          schemas = c("governance", "raw", "staging", "validated", "reference"),
          allowed_system_type = c("CSV", "XLSX", "SQL", "API", "FHIR", "Other"),
          allowed_update_frequency = c("daily", "weekly", "biweekly", "monthly", "quarterly", "annually", "ad_hoc"),
          allowed_ingest_method = c("push", "pull", "api", "sftp", "manual"),
          allowed_pii_classification = c("PHI", "Limited", "NonPHI")
        )
      ),
      source_params = list(
        file = "config/source_params.yml",
        usage = "Holds one source definition at a time for onboarding.",
        fields = c(
          "source_id",
          "source_name",
          "system_type",
          "update_frequency",
          "data_owner",
          "ingest_method",
          "expected_schema_version",
          "retention_policy",
          "pii_classification",
          "active"
        )
      ),
      directory_structure = list(
        file = "directory_structure.yml",
        usage = "Template used by create_source_folders() to build folder trees.",
        zones_example = c(
          "raw/{source_id}/incoming/",
          "raw/{source_id}/archive/",
          "staging/{source_id}/incoming/",
          "staging/{source_id}/archive/",
          "validated/{source_id}/",
          "governance/logs/",
          "governance/qc/",
          "governance/reports/"
        )
      )
    ),
    functions = list(
      list(
        name = "connect_to_pulse",
        file = "r/connect_to_pulse.R",
        category = "db_connection",
        description = "Connects to Postgres using env vars PULSE_DB, PULSE_HOST, PULSE_USER, PULSE_PW.",
        calls = c("DBI::dbConnect", "RPostgres::Postgres"),
        called_by = c("run_pipeline", "tests", "manual_use")
      ),
      list(
        name = "load_pipeline_settings",
        file = "r/runner.R",
        category = "config_loader",
        description = "Loads global pipeline settings and vocab lists from pipeline_settings.yml.",
        calls = "yaml::read_yaml",
        called_by = c("run_pipeline", "register_source", "validate_source_entry (indirect)")
      ),
      list(
        name = "get_pipeline_steps",
        file = "r/runner.R",
        category = "runner_helper",
        description = "Fetches enabled steps from governance.pipeline_step and sorts by step_order.",
        calls = c("DBI::dbReadTable", "dplyr::filter", "dplyr::arrange"),
        called_by = "run_pipeline"
      ),
      list(
        name = "execute_step",
        file = "r/runner.R",
        category = "runner_helper",
        description = "Executes individual steps; special handling for STEP_001, SQL, R, and RMD types.",
        calls = c("load_source_params", "run_step1_register_source", "do.call", "glue::glue", "DBI::dbExecute", "rmarkdown::render"),
        called_by = "run_pipeline"
      ),
      list(
        name = "run_pipeline",
        file = "r/runner.R",
        category = "runner_main",
        description = "Main pipeline orchestration function for a given ingest_id.",
        calls = c("connect_to_pulse", "load_pipeline_settings", "get_pipeline_steps", "execute_step"),
        called_by = c("pulse_launch", "integration_tests", "manual_use")
      ),
      list(
        name = "validate_source_entry",
        file = "r/utilities/validate_source_entry.R",
        category = "validation",
        description = "Validates source candidate list against required fields and allowed vocab.",
        calls = character(),
        called_by = c("register_source", "unit_tests")
      ),
      list(
        name = "create_source_folders",
        file = "r/utilities/create_source_folders.R",
        category = "filesystem",
        description = "Creates folder tree for source_id using directory_structure.yml.",
        calls = c("yaml::read_yaml", "fs::dir_create", "normalizePath", "glue::glue"),
        called_by = "register_source"
      ),
      list(
        name = "write_audit_event",
        file = "r/steps/write_audit_event.R",
        category = "governance",
        description = "Writes JSON-encoded audit events to governance.audit_log.",
        calls = c("uuid::UUIDgenerate", "jsonlite::toJSON", "DBI::dbGetQuery", "DBI::dbExecute"),
        called_by = c("register_source", "future_steps")
      ),
      list(
        name = "register_source",
        file = "r/steps/register_source.R",
        category = "step_logic",
        description = "Step 1 core logic; insert/update registry, folder creation, audit logging.",
        calls = c("yaml::read_yaml", "validate_source_entry", "DBI::dbGetQuery", "DBI::dbExecute", "create_source_folders", "write_audit_event"),
        called_by = c("run_step1_register_source", "unit_tests", "manual_console")
      ),
      list(
        name = "write_pipeline_step",
        file = "r/utilities/write_pipeline_step.R",
        category = "governance",
        description = "Writes pipeline step metadata into governance.pipeline_step.",
        calls = "DBI::dbExecute",
        called_by = c("run_step1_register_source", "future_step_wrappers")
      ),
      list(
        name = "run_step1_register_source",
        file = "r/steps/run_step1_register_source.R",
        category = "step_wrapper",
        description = "Wrapper for STEP_001 that calls register_source() and writes pipeline_step entry.",
        calls = c("register_source", "write_pipeline_step"),
        called_by = c("execute_step", "unit_tests")
      ),
      list(
        name = "load_source_params",
        file = "r/utilities/load_source_params.R",
        category = "config_loader",
        description = "Loads YAML source parameters from config/source_params.yml.",
        calls = "yaml::read_yaml",
        called_by = c("execute_step", "integration_tests", "manual_use")
      ),
      list(
        name = "pulse_launch",
        file = "pulse-launch.R",
        category = "entrypoint",
        description = "High-level launcher for pipeline runs; can write source_params.yml and then call run_pipeline().",
        calls = c("yaml::write_yaml", "run_pipeline"),
        called_by = c("r/scripts/1_onboard_new_source.R", "integration_tests", "future_CLI")
      )
    ),
    scripts = list(
      list(
        path = "r/scripts/1_onboard_new_source.R",
        purpose = "Human-friendly onboarding script guiding user inputs and calling pulse_launch()."
      ),
      list(
        path = "pulse-init-all.R",
        purpose = "Project bootstrap script that loads R scripts and packages."
      )
    ),
    tests = list(
      unit = "tests/testthat/test_step1_register_source.R",
      integration = "tests/testthat/test_step1_integration.R",
      status = "all_pass"
    ),
    step1_status = list(
      ddl_created = TRUE,
      functions_implemented = TRUE,
      folder_creation_working = TRUE,
      audit_logging_working = TRUE,
      unit_tests_pass = TRUE,
      integration_tests_pass = TRUE,
      runner_integration_complete = TRUE
    )
  )
  
  jsonlite::write_json(
    snapshot,
    path = output_path,
    pretty = TRUE,
    auto_unbox = TRUE
  )
  
  message("Step 1 snapshot JSON written to: ", output_path)
}