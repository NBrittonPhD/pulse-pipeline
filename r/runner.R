
library(DBI)
library(glue)
library(dplyr)
library(purrr)
library(rmarkdown)

# Load connection wrapper
source("r/connect_to_pulse.R")

# --------------------------
# Load pipeline step functions
# --------------------------
step_files <- list.files(
  path = "r/steps",
  pattern = "\\.R$",
  full.names = TRUE
)

invisible(lapply(step_files, source))


# --------------------------
# Helper: fetch pipeline steps
# --------------------------
get_pipeline_steps <- function(con) {
  dbReadTable(con, "pipeline_step") %>%
    filter(enabled == TRUE) %>%
    arrange(step_order)
}

# --------------------------
# Execute a single step
# --------------------------
execute_step <- function(step, con, ingest_id = NULL) {
  message(paste0("Running step ", step$step_order, ": ", step$step_name))
  
  if (step$step_type == "SQL") {
    sql <- glue(step$code_snippet)
    dbExecute(con, sql)
    
  } else if (step$step_type == "R") {
    fn <- sub("\\(.*", "", step$code_snippet)
    do.call(fn, list(ingest_id = ingest_id, con = con))
    
  } else if (step$step_type == "RMD") {
    rmarkdown::render(
      input = step$code_snippet,
      params = list(ingest_id = ingest_id),
      output_dir = "docs/SOP_rendered/"
    )
  }
}

# --------------------------
# Main pipeline runner
# --------------------------
run_pipeline <- function(ingest_id) {
  
  # create connection only here
  con <- connect_to_pulse()
  
  steps <- get_pipeline_steps(con)
  
  for (i in seq_len(nrow(steps))) {
    execute_step(steps[i, ], con = con, ingest_id = ingest_id)
  }
}