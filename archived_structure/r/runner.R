# =============================================================================
# PULSE Pipeline Runner
# Executes governed pipeline steps in correct order, using pipeline_step table.
# =============================================================================

suppressPackageStartupMessages({
  library(DBI)
  library(glue)
  library(dplyr)
  library(purrr)
  library(rmarkdown)
  library(yaml)
})

# =============================================================================
# Load connection
# =============================================================================
source("r/connect_to_pulse.R")
con <- connect_to_pulse()

# =============================================================================
# Load all step functions (STEP_001 ... STEP_010)
# =============================================================================
step_files <- list.files(
  path = "r/steps",
  pattern = "\\.R$",
  full.names = TRUE
)
invisible(lapply(step_files, source))

# =============================================================================
# Load utilities
# =============================================================================
utility_files <- list.files(
  path = "r/utilities",
  pattern = "\\.R$",
  full.names = TRUE
)
invisible(lapply(utility_files, source))

# =============================================================================
# Load global pipeline settings
# =============================================================================
load_pipeline_settings <- function() {
  yaml::read_yaml("config/pipeline_settings.yml")
}

# =============================================================================
# Helper: schema-qualified table references
# =============================================================================
qualify_table <- function(schema, table) {
  DBI::Id(schema = schema, table = table)
}

# =============================================================================
# Fetch enabled pipeline steps, in correct order
# =============================================================================
get_pipeline_steps <- function(con, settings) {
  dbReadTable(
    con,
    DBI::Id(schema = settings$schemas$governance, table = "pipeline_step")
  ) %>%
    filter(enabled == TRUE) %>%
    arrange(step_order)
}

# =============================================================================
# Execute a single pipeline step
# =============================================================================
execute_step <- function(step, con, ingest_id = NULL, settings) {
  
  message("--------------------------------------------------------")
  message(
    sprintf(
      "Running step %s (%s): %s",
      step$step_order,
      step$step_id,
      step$step_name
    )
  )
  message("--------------------------------------------------------")
  
  step_type <- step$step_type
  
  # ------------------------------------------------------------
  # SPECIAL CASE: STEP_001 (register_source)
  # ------------------------------------------------------------
  if (step$step_id == "STEP_001") {
    
    source_params <- load_source_params()
    
    run_step1_register_source(
      con           = con,
      source_params = source_params,
      settings      = settings
    )
    
    return(invisible(TRUE))
  }
  
  # ------------------------------------------------------------
  # SQL step
  # ------------------------------------------------------------
  if (step_type == "SQL") {
    sql <- glue(step$code_snippet)
    dbExecute(con, sql)
    return(invisible(TRUE))
  }
  
  # ------------------------------------------------------------
  # R step (generic)
  # ------------------------------------------------------------
  if (step_type == "R") {
    
    # Extract function name from code_snippet, e.g. 'run_step2_log_batch_ingest()'
    fn <- sub("\\(.*", "", step$code_snippet)
    
    # Call the function with con + ingest_id + settings
    do.call(
      what = fn,
      args = list(
        con       = con,
        ingest_id = ingest_id,
        settings  = settings
      )
    )
    
    return(invisible(TRUE))
  }
  
  # ------------------------------------------------------------
  # R Markdown step
  # ------------------------------------------------------------
  if (step_type == "RMD") {
    rmarkdown::render(
      input      = step$code_snippet,
      params     = list(
        ingest_id = ingest_id,
        settings  = settings
      ),
      output_dir = "docs/SOP_rendered/"
    )
    return(invisible(TRUE))
  }
  
  stop(glue("Unknown step_type '{step_type}' for step_id {step$step_id}"))
}

# =============================================================================
# Run full PULSE pipeline
# =============================================================================
run_pipeline <- function(ingest_id) {
  
  message("========================================================")
  message("               PULSE PIPELINE RUNNER                    ")
  message("========================================================")
  
  con      <- connect_to_pulse()
  settings <- load_pipeline_settings()
  
  steps <- get_pipeline_steps(con, settings)
  
  for (i in seq_len(nrow(steps))) {
    
    step <- steps[i, ]
    
    step_result <- try(
      execute_step(
        step      = step,
        con       = con,
        ingest_id = ingest_id,
        settings  = settings
      ),
      silent = TRUE
    )
    
    # --------------------------------------------------------
    # Error handling + pipeline governance
    # --------------------------------------------------------
    if (inherits(step_result, "try-error")) {
      
      error_msg <- as.character(step_result)
      
      DBI::dbExecute(con, "
        UPDATE governance.pipeline_step
        SET last_modified_utc = CURRENT_TIMESTAMP,
            step_description   = CONCAT(step_description, ' | FAILED: ', $1)
        WHERE step_id = $2;
      ",
                     params = list(error_msg, step$step_id))
      
      stop(
        sprintf(
          "Pipeline failed at %s (%s):\n%s",
          step$step_id,
          step$step_name,
          error_msg
        )
      )
    }
    
    # Mark success for step
    DBI::dbExecute(con, "
      UPDATE governance.pipeline_step
      SET last_modified_utc = CURRENT_TIMESTAMP
      WHERE step_id = $1;
    ",
                   params = list(step$step_id))
  }
  
  message("--------------------------------------------------------")
  message("Pipeline completed successfully.")
  message("--------------------------------------------------------")
  
  invisible(TRUE)
}
