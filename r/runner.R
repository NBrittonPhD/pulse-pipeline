
library(DBI)
library(glue)
library(dplyr)
library(purrr)

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  dbname   = Sys.getenv("PULSE_DB"),
  host     = Sys.getenv("PULSE_HOST"),
  user     = Sys.getenv("PULSE_USER"),
  password = Sys.getenv("PULSE_PW")
)

get_pipeline_steps <- function(con) {
  dbReadTable(con, "PIPELINE_STEP") %>%
    filter(enabled == TRUE) %>%
    arrange(step_order)
}

execute_step <- function(step, con, ingest_id = NULL) {
  message(paste0("Running step ", step$step_order, ": ", step$step_name))

  if (step$step_type == "SQL") {
    sql <- glue(step$code_snippet)
    DBI::dbExecute(con, sql)

  } else if (step$step_type == "R") {
    fn <- sub("\(.*", "", step$code_snippet)
    do.call(fn, list(ingest_id = ingest_id, con = con))

  } else if (step$step_type == "RMD") {
    rmarkdown::render(
      input = step$code_snippet,
      params = list(ingest_id = ingest_id),
      output_dir = "docs/SOP_rendered/"
    )
  }
}

run_pipeline <- function(ingest_id) {
  steps <- get_pipeline_steps(con)
  for (i in seq_len(nrow(steps))) {
    execute_step(steps[i, ], con = con, ingest_id = ingest_id)
  }
}

