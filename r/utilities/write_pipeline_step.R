# =============================================================================
# write_pipeline_step.R
# -----------------------------------------------------------------------------
# Utility to upsert (insert or update) a row in governance.pipeline_step.
#
# This is *configuration* for the pipeline, not per-run history. It ensures
# that the PIPELINE_STEP table always reflects the current definition of each
# step (order, name, type, code snippet, etc.).
#
# Used by:
#   - run_step1_register_source()  (to ensure STEP_001 is correctly defined)
#   - (optionally) by future step wrapper functions STEP_002–STEP_010
#
# Behavior:
#   - If step_id does NOT exist → INSERT a new row.
#   - If step_id already exists → UPDATE its metadata (order, name, etc.)
#     and bump last_modified_utc.
# =============================================================================

write_pipeline_step <- function(
    con,
    step_id,
    step_order,
    step_name,
    step_description,
    step_type,
    code_snippet,
    enabled = TRUE
) {
  # Parameterized UPSERT into governance.pipeline_step
  sql <- "
    INSERT INTO governance.pipeline_step (
      step_id,
      step_order,
      step_name,
      step_description,
      step_type,
      code_snippet,
      enabled
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (step_id) DO UPDATE
    SET
      step_order        = EXCLUDED.step_order,
      step_name         = EXCLUDED.step_name,
      step_description  = EXCLUDED.step_description,
      step_type         = EXCLUDED.step_type,
      code_snippet      = EXCLUDED.code_snippet,
      enabled           = EXCLUDED.enabled,
      last_modified_utc = CURRENT_TIMESTAMP;
  "
  
  DBI::dbExecute(
    con,
    sql,
    params = list(
      step_id,
      step_order,
      step_name,
      step_description,
      step_type,
      code_snippet,
      enabled
    )
  )
  
  invisible(TRUE)
}