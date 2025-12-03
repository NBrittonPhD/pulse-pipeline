# =============================================================================
# Step 2 wrapper for pipeline runner
# Called by runner.R using step_id = 'STEP_002'
# =============================================================================

run_step2_log_batch_ingest <- function(ingest_id, con) {
  
  message("========================================================")
  message("Running STEP_002: Batch Logging & Ingestion Recording")
  message("Ingest_id: ", ingest_id)
  message("--------------------------------------------------------")
  
  # Derive source_id from ingest_id (pattern: BATCH_<SOURCEID_UPPER>_YYYYMMDD_...)
  source_id_upper <- sub("^BATCH_", "", ingest_id)
  source_id_upper <- sub("_.*$", "", source_id_upper)
  source_id       <- tolower(source_id_upper)
  
  message("Derived source_id: ", source_id)
  
  # Delegate to main orchestrator
  res <- log_batch_ingest(
    source_id = source_id,
    con       = con
    # incoming_dir and archive_dir default based on source_id
  )
  
  invisible(res)
}
