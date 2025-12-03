# =============================================================================
# pulse-launch.R
# -----------------------------------------------------------------------------
# High-level launcher for the PULSE pipeline.
#
# This is a thin convenience wrapper used by scripts like:
#   - r/scripts/1_onboard_new_source.R
#
# It is designed to:
#   1. Optionally write config/source_params.yml from a provided list.
#   2. Source the main runner (r/runner.R).
#   3. Call run_pipeline(ingest_id) to execute the configured steps.
#
# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
# ingest_id : character
#   A unique identifier for this pipeline run / ingest attempt.
#   - Example: "tr2026_test_id"
#   - Used for logging and lineage in later steps (batch logging, QC, etc.).
#
# source_params : list
#   A named list of source registration parameters used in Step 1
#   (register_source). These fields are written into config/source_params.yml
#   when auto_write_params = TRUE.
#
#   Expected names:
#     - source_id               : Unique, stable ID for the source.
#     - source_name             : Human-readable name.
#     - system_type             : One of allowed vocabulary 
#                                 (CSV, XLSX, SQL, API, FHIR, Other).
#     - update_frequency        : Expected cadence (daily, weekly, monthly, etc.).
#     - data_owner              : Name/role of upstream data owner.
#     - ingest_method           : One of allowed vocab (push, pull, api, sftp, manual).
#     - expected_schema_version : Semantic version string, e.g. "1.0.0".
#     - retention_policy        : Text description, or NULL.
#     - pii_classification      : PHI / Limited / NonPHI.
#     - active                  : TRUE/FALSE flag.
#
# auto_write_params : logical (default = TRUE)
#   If TRUE:
#     - Writes source_params to "config/source_params.yml" before running.
#   If FALSE:
#     - Assumes config/source_params.yml already exists and is correct.
#
# params_path : character
#   Path to the YAML file that will hold source parameters.
#   Default: "config/source_params.yml"
#
# ---------------------------------------------------------------------------
# Behavior
# ---------------------------------------------------------------------------
# - Ensures the config/ directory exists.
# - Optionally writes source_params into config/source_params.yml.
# - Sources r/runner.R (which loads all step functions and utilities).
# - Calls run_pipeline(ingest_id), which:
#     * Reads PIPELINE_STEP from governance.pipeline_step
#     * Executes enabled steps in order, including STEP_001
#       via run_step1_register_source().
#
# This function does NOT:
#   - Manipulate the database directly.
#   - Decide which steps are enabled (that’s controlled in PIPELINE_STEP).
#
# =============================================================================

pulse_launch <- function(
    ingest_id,
    source_params,
    auto_write_params = TRUE,
    params_path = "config/source_params.yml"
) {
  # Basic argument checks (lightweight)
  if (missing(ingest_id) || !nzchar(ingest_id)) {
    stop("ingest_id must be a non-empty character string.")
  }
  
  if (!is.list(source_params)) {
    stop("source_params must be a named list.")
  }
  
  # ---------------------------------------------------------------------------
  # 1. Ensure config directory exists
  # ---------------------------------------------------------------------------
  if (!dir.exists("config")) {
    dir.create("config", recursive = TRUE)
  }
  
  # ---------------------------------------------------------------------------
  # 2. Optionally write source_params to YAML
  # ---------------------------------------------------------------------------
  if (isTRUE(auto_write_params)) {
    yaml::write_yaml(source_params, params_path)
    message(sprintf(">> Wrote source parameters to %s", params_path))
  } else {
    message(">> auto_write_params = FALSE; using existing source_params.yml")
  }
  
  # ---------------------------------------------------------------------------
  # 3. Source the pipeline runner
  # ---------------------------------------------------------------------------
  if (!file.exists("r/runner.R")) {
    stop("r/runner.R not found. Are you in the project root?")
  }
  
  source("r/runner.R")
  
  # ---------------------------------------------------------------------------
  # 4. Run the pipeline
  # ---------------------------------------------------------------------------
  message(sprintf(">> Launching PULSE pipeline with ingest_id = '%s' ...", ingest_id))
  run_pipeline(ingest_id = ingest_id)
  message(">> PULSE pipeline completed.")
  
  invisible(TRUE)
}