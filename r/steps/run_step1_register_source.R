# =============================================================================
# run_step1_register_source.R
# -----------------------------------------------------------------------------
# Wrapper for executing Step 1 of the PULSE pipeline.
#
# Responsible for:
#   - Calling register_source() with user-specified source_params
#   - Creating folders, validating vocab, writing audit events
#   - Recording step completion in governance.pipeline_step
#
# This wrapper is called automatically by execute_step() when step_id = "STEP_001"
# and manually by onboarding scripts via pulse_launch().
# =============================================================================

run_step1_register_source <- function(con, source_params, settings = NULL) {
  
  # ---------------------------
  # 1. Execute core Step 1 logic
  # ---------------------------
  do.call(
    register_source,
    c(list(con = con), source_params)
  )
  
  # ---------------------------
  # 2. Record pipeline step completion
  # ---------------------------
  write_pipeline_step(
    con              = con,
    step_id          = "STEP_001",
    step_order       = 1,
    step_name        = "register_source",
    step_description = "Register source, validate vocab, create folders, write audit logs.",
    step_type        = "R",
    code_snippet     = "run_step1_register_source()"
  )
  
  invisible(TRUE)
}