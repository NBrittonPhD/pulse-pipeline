# =============================================================================
# r/runner.R
# -----------------------------------------------------------------------------
# Core pipeline runner for PULSE.
#
# Responsibilities:
#   - Load DB connection wrapper
#   - Load all step and utility functions
#   - Read pipeline configuration from governance.pipeline_step
#   - Execute enabled steps in order
#   - Special-case STEP_001 to call run_step1_register_source()
# =============================================================================

library(DBI)
library(glue)
library(dplyr)
library(purrr)
library(rmarkdown)
library(yaml)

# ---------------------------------------
# Resolve project root (works in tests + interactively)
# ---------------------------------------
proj_root <- getOption("pulse.proj_root", default = ".")

# ---------------------------------------
# Load connection wrapper
# ---------------------------------------
source(file.path(proj_root, "r", "connect_to_pulse.R"))

# ---------------------------------------
# Load step functions
# ---------------------------------------
step_files <- list.files(
  path       = file.path(proj_root, "r", "steps"),
  pattern    = "\\.R$",
  full.names = TRUE
)
invisible(lapply(step_files, source))

# ---------------------------------------
# Load utility functions
# ---------------------------------------
utility_files <- list.files(
  path       = file.path(proj_root, "r", "utilities"),
  pattern    = "\\.R$",
  full.names = TRUE
)
invisible(lapply(utility_files, source))

# ---------------------------------------
# Load global pipeline settings (schemas, vocab, etc.)
# ---------------------------------------
load_pipeline_settings <- function() {
  # Use project root recorded by helper_pulse_step1, or fallback to getwd()
  proj_root <- getOption("pulse.proj_root", default = getwd())
  
  yaml::read_yaml(
    file.path(proj_root, "config", "pipeline_settings.yml")
  )
}

# ---------------------------------------
# Helper: get schema-qualified table identifier
# ---------------------------------------
qualify_table <- function(schema, table) {
  DBI::Id(schema = schema, table = table)
}

# ---------------------------------------
# Fetch pipeline steps (schema-safe)
# ---------------------------------------
get_pipeline_steps <- function(con, settings) {
  dbReadTable(
    con,
    qualify_table(settings$schemas$governance, "pipeline_step")
  ) %>%
    dplyr::filter(enabled == TRUE) %>%
    dplyr::arrange(step_order)
}

# ---------------------------------------
# Execute a single pipeline step
# ---------------------------------------
execute_step <- function(step, con, ingest_id = NULL, settings) {
  
  message(paste0("Running step ", step$step_order, ": ", step$step_name))
  
  step_type <- step$step_type
  
  # ---------------------------------------------------------------------------
  # SPECIAL CASE: Step 1 (register_source)
  # ---------------------------------------------------------------------------
  if (step$step_id == "STEP_001") {
    
    # Source parameters are loaded from config/source_params.yml
    source_params <- load_source_params()
    
    run_step1_register_source(
      con           = con,
      source_params = source_params,
      settings      = settings
    )
    
    return(invisible(TRUE))
  }
  
  # ---------------------------------------------------------------------------
  # SQL step
  # ---------------------------------------------------------------------------
  if (step_type == "SQL") {
    sql <- glue(step$code_snippet)
    dbExecute(con, sql)
    return(invisible(TRUE))
  }
  
  # ---------------------------------------------------------------------------
  # R step
  # ---------------------------------------------------------------------------
  if (step_type == "R") {
    
    fn <- sub("\\(.*", "", step$code_snippet)  # strip any trailing "()" just in case
    
    do.call(
      fn,
      list(
        con       = con,
        ingest_id = ingest_id,
        settings  = settings
      )
    )
    
    return(invisible(TRUE))
  }
  
  # ---------------------------------------------------------------------------
  # R Markdown step
  # ---------------------------------------------------------------------------
  if (step_type == "RMD") {
    rmarkdown::render(
      input  = step$code_snippet,
      params = list(
        ingest_id = ingest_id,
        settings  = settings
      ),
      output_dir = "docs/SOP_rendered/"
    )
    return(invisible(TRUE))
  }
  
  stop(glue("Unknown step_type '{step_type}' for step_id {step$step_id}"))
}

# ---------------------------------------
# Run full pipeline
# ---------------------------------------
run_pipeline <- function(ingest_id) {
  
  # Open DB connection
  con <- connect_to_pulse()
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # Load settings (schemas, vocab, etc.)
  settings <- load_pipeline_settings()
  
  # Fetch enabled steps
  steps <- get_pipeline_steps(con, settings)
  
  # Execute each step in order
  for (i in seq_len(nrow(steps))) {
    execute_step(
      step      = steps[i, ],
      con       = con,
      ingest_id = ingest_id,
      settings  = settings
    )
  }
  
  invisible(TRUE)
}