# ==============================================================
# pulse_launch()
# One-command startup → loads packages → sources runner → runs pipeline
# ==============================================================

pulse_launch <- function(
    ingest_id = NULL,
    source_params = NULL,
    auto_write_params = TRUE
) {
  message("Loading PULSE pipeline...")
  
  # Load required packages
  suppressPackageStartupMessages({
    library(DBI)
    library(RPostgres)
    library(yaml)
  })
  
  # Source the runner
  source("r/runner.R")
  
  # Optionally write source parameters
  if (!is.null(source_params) && auto_write_params) {
    do.call(write_source_params, c(source_params, list(file = "config/source_params.yml")))
  }
  
  # Default ingest_id if missing
  if (is.null(ingest_id)) {
    ingest_id <- paste0("ingest_", format(Sys.time(), "%Y%m%d_%H%M%S"))
  }
  
  message("Running pipeline for ingest_id = ", ingest_id)
  run_pipeline(ingest_id)
  
  message("Pipeline complete.")
}
