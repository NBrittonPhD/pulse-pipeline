# =============================================================================
# run_step2_batch_logging.R
# Pipeline Step 2 Wrapper
# =============================================================================
# This function executes Step 2 of the PULSE pipeline:
#   - Logs a batch into governance.batch_log
#   - Logs all files into governance.ingest_file_log
#   - Appends data into raw.<lake_table_name> tables via ingest_batch()
#   - Writes pipeline_step entry for STEP_002
#
# It is orchestrated by execute_step() inside r/runner.R.
#
# source_type Derivation:
#   source_type (e.g. "CISIR") is derived automatically from
#   reference.ingest_dictionary by matching incoming file names against
#   source_table_name. This keeps the logic metadata-driven.
# =============================================================================

run_step2_batch_logging <- function(con, ingest_id, settings = NULL) {

  # ------------------------------------------------------------
  # Load source_id from source_params.yml (same convention as Step 1)
  # ------------------------------------------------------------
  source_params <- load_source_params()
  source_id     <- source_params$source_id

  raw_path <- fs::path("raw", source_id, "incoming")

  # ------------------------------------------------------------
  # Identify all incoming files
  # ------------------------------------------------------------
  file_paths <- fs::dir_ls(raw_path, regexp = "\\.csv$", recurse = FALSE)

  if (length(file_paths) == 0) {
    stop(glue::glue(
      "run_step2_batch_logging(): No CSV files found in {raw_path}"
    ))
  }

  # ------------------------------------------------------------
  # Derive source_type from reference.ingest_dictionary
  # Match incoming file names against source_table_name mappings
  # ------------------------------------------------------------
  dict <- DBI::dbReadTable(
    con,
    DBI::Id(schema = "reference", table = "ingest_dictionary")
  )
  names(dict) <- tolower(names(dict))

  file_bases <- tolower(sub("\\.csv$", "", basename(file_paths)))
  dict$source_table_name_norm <- tolower(trimws(dict$source_table_name))
  matched_types <- unique(dict$source_type[dict$source_table_name_norm %in% file_bases])

  if (length(matched_types) == 0) {
    stop(glue::glue(
      "run_step2_batch_logging(): Could not derive source_type from ",
      "reference.ingest_dictionary. No source_table_name entries match ",
      "the incoming files: {paste(basename(file_paths), collapse = ', ')}"
    ))
  }

  if (length(matched_types) > 1) {
    warning(glue::glue(
      "run_step2_batch_logging(): Multiple source_types detected: ",
      "{paste(matched_types, collapse = ', ')}. Using first: {matched_types[1]}"
    ))
  }

  source_type <- matched_types[1]
  message(">> Derived source_type = ", source_type)

  # ------------------------------------------------------------
  # Run batch logging (file-level lineage + batch-level metadata)
  # ------------------------------------------------------------
  log_batch_ingest(
    con         = con,
    ingest_id   = ingest_id,
    source_id   = source_id,
    source_type = source_type,
    file_paths  = file_paths
  )

  # ------------------------------------------------------------
  # Append files to RAW zone tables via ingest_batch()
  # ------------------------------------------------------------
  result <- ingest_batch(
    con         = con,
    ingest_id   = ingest_id,
    raw_path    = raw_path,
    source_id   = source_id,
    source_type = source_type
  )

  # ------------------------------------------------------------
  # Write pipeline_step row for STEP_002 (parallel to STEP_001 logic)
  # ------------------------------------------------------------
  write_pipeline_step(
    con              = con,
    step_id          = "STEP_002",
    step_order       = 2,
    step_name        = "batch_logging_and_ingestion",
    step_description = "Log batch, create file lineage, and ingest files to raw zone.",
    step_type        = "R",
    code_snippet     = "run_step2_batch_logging()"
  )

  return(result)
}
