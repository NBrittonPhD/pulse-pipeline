# =============================================================================
# run_step2_batch_logging.R
# Pipeline Step 2 Wrapper
# =============================================================================
# This function executes Step 2 of the PULSE pipeline:
#   - Logs a batch into governance.batch_log
#   - Logs all files into governance.ingest_file_log
#   - Appends data into raw.<lake_table_name> tables via ingest()
#   - Writes pipeline_step entry for STEP_002
#
# It is orchestrated by execute_step() inside r/runner.R.
# =============================================================================

run_step2_batch_logging <- function(con, ingest_id, settings = NULL) {
  
  # ------------------------------------------------------------
  # Load source_id from source_params.yml (same convention as Step 1)
  # ------------------------------------------------------------
  source_params <- load_source_params()
  source_id     <- source_params$source_id
  
  raw_path <- fs::path("raw", source_id, "incoming")
  
  # ------------------------------------------------------------
  # Identify all incoming files
  # ------------------------------------------------------------
  file_paths <- fs::dir_ls(raw_path, regexp = "\\.csv$", recurse = FALSE)
  
  # ------------------------------------------------------------
  # Run batch logging (file-level lineage + batch-level metadata)
  # ------------------------------------------------------------
  lineage <- log_batch_ingest(
    con        = con,
    ingest_id  = ingest_id,
    file_paths = file_paths,
    source_id  = source_id
  )
  
  # ------------------------------------------------------------
  # Append files to RAW zone tables
  # ------------------------------------------------------------
  ingest(source_id, raw_path, con)
  
  # ------------------------------------------------------------
  # Write pipeline_step row for STEP_002 (parallel to STEP_001 logic)
  # ------------------------------------------------------------
  write_pipeline_step(
    con         = con,
    step_id     = "STEP_002",
    step_name   = "batch_logging_and_ingestion",
    status      = lineage$status,
    metadata    = list(
      file_count  = lineage$n_files,
      success     = lineage$n_success,
      error       = lineage$n_error
    )
  )
  
  return(lineage)
}
