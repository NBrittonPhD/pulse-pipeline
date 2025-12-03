# =====================================================================
# write_pipeline_step()
# Records the completion of a pipeline step in governance.pipeline_step
# =====================================================================

write_pipeline_step <- function(con,
                                step_id,
                                step_order,
                                step_name,
                                step_description = NULL,
                                step_type = "operational",
                                code_snippet = NULL) {
  
  DBI::dbExecute(
    con,
    "
    INSERT INTO governance.pipeline_step (
      step_id, step_order, step_name, step_description,
      step_type, code_snippet
    )
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (step_id) DO UPDATE SET
      step_description = EXCLUDED.step_description,
      last_modified_utc = CURRENT_TIMESTAMP;
    ",
    params = list(
      step_id,
      step_order,
      step_name,
      step_description,
      step_type,
      code_snippet
    )
  )
  
  invisible(TRUE)
}
