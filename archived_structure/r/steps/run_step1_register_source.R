# =============================================================================
# STEP 1 Wrapper — correct version for pipeline runner
# =============================================================================

run_step1_register_source <- function(con, source_params, settings) {
  
  message("--------------------------------------------------------")
  message("Running STEP_001: register_source (wrapper)")
  message("--------------------------------------------------------")
  
  # Call your actual Step 1 code
  # This uses the updated version from register_source.R
  res <- run_step1_register_source_impl(
    con          = con,
    source_params = source_params,
    settings     = settings
  )
  
  invisible(res)
}
