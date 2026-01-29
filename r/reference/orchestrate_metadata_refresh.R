# =============================================================================
# orchestrate_metadata_refresh
# =============================================================================
# Purpose:      Master orchestrator that chains the full metadata propagation
#               pipeline from CURRENT_core_metadata_dictionary.xlsx through
#               to all downstream files and database tables. This makes the
#               core metadata dictionary the single source of truth.
#
#               The chain executes 5 steps in sequence:
#                 Step A: derive_ingest_dictionary()         — Core → xlsx
#                 Step B: sync_ingest_dictionary()           — xlsx → DB
#                 Step C: update_type_decision_table()       — Core + old → xlsx
#                 Step D: build_expected_schema_dictionary()  — DB + xlsx → xlsx
#                 Step E: sync_metadata()                    — xlsx → DB
#
# Inputs:
#   - con:                 DBI connection object
#   - core_dict_path:      character path to core metadata dictionary
#   - ingest_dict_path:    character output path for derived ingest dictionary
#   - type_decision_path:  character path to type_decision_table.xlsx
#   - expected_schema_path: character output path for expected schema dictionary
#   - schema_version:      character schema version for expected schema
#   - effective_from:      Date, version effective date
#   - effective_to:        Date or NA, version end date
#   - sync_mode:           character mode for DB syncs ("replace" or "append")
#   - archive_existing:    logical, archive old files before overwriting
#   - created_by:          character audit trail identifier
#
# Outputs:      list with (status, steps_completed, derive_ingest_result,
#               sync_ingest_result, update_type_result, build_schema_rows,
#               sync_metadata_result, duration_seconds)
#
# Side Effects: Writes 3 xlsx files, syncs 2 DB tables
#
# Dependencies: DBI, readxl, writexl, dplyr, tibble, glue,
#               derive_ingest_dictionary, sync_ingest_dictionary,
#               update_type_decision_table, build_expected_schema_dictionary,
#               sync_metadata
#
# Author:       Noel
# Last Updated: 2026-01-29
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(DBI)
library(readxl)
library(writexl)
library(dplyr)
library(tibble)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
orchestrate_metadata_refresh <- function(con,
                                          core_dict_path       = NULL,
                                          ingest_dict_path     = NULL,
                                          type_decision_path   = NULL,
                                          expected_schema_path = NULL,
                                          schema_version       = "2025.0",
                                          effective_from       = Sys.Date(),
                                          effective_to         = NA,
                                          sync_mode            = "replace",
                                          archive_existing     = TRUE,
                                          created_by           = "orchestrate_metadata_refresh") {

  # =========================================================================
  # RESOLVE PATHS
  # =========================================================================
  proj_root <- getOption("pulse.proj_root", default = ".")

  if (is.null(core_dict_path)) {
    core_dict_path <- file.path(proj_root, "reference",
                                "CURRENT_core_metadata_dictionary.xlsx")
  }

  if (is.null(ingest_dict_path)) {
    ingest_dict_path <- file.path(proj_root, "reference",
                                  "ingest_dictionary.xlsx")
  }

  if (is.null(type_decision_path)) {
    type_decision_path <- file.path(proj_root, "reference", "type_decisions",
                                    "type_decision_table.xlsx")
  }

  if (is.null(expected_schema_path)) {
    expected_schema_path <- file.path(proj_root, "reference",
                                      "expected_schema_dictionary.xlsx")
  }

  # =========================================================================
  # BANNER
  # =========================================================================
  message("")
  message("=================================================================")
  message("  PULSE PIPELINE — METADATA REFRESH ORCHESTRATOR")
  message("=================================================================")
  message("  Core Dict:         ", core_dict_path)
  message("  Ingest Dict:       ", ingest_dict_path)
  message("  Type Decisions:    ", type_decision_path)
  message("  Expected Schema:   ", expected_schema_path)
  message("  Schema Version:    ", schema_version)
  message("  Sync Mode:         ", sync_mode)
  message("  Archive Existing:  ", archive_existing)
  message("  Created By:        ", created_by)
  message("=================================================================")
  message("")

  # =========================================================================
  # INPUT VALIDATION
  # =========================================================================
  if (!inherits(con, "DBIConnection")) {
    stop("[orchestrate] ERROR: 'con' must be a valid DBI connection object.")
  }

  if (!DBI::dbIsValid(con)) {
    stop("[orchestrate] ERROR: Database connection is not valid.")
  }

  if (!file.exists(core_dict_path)) {
    stop("[orchestrate] ERROR: Core metadata dictionary not found at: ",
         core_dict_path)
  }

  # Track which steps complete successfully
  orchestration_start <- Sys.time()
  steps_completed <- character(0)

  # =========================================================================
  # STEP A: DERIVE INGEST DICTIONARY FROM CORE DICT
  # =========================================================================
  message("")
  message(">> ============================================================")
  message(">> STEP A: DERIVE INGEST DICTIONARY")
  message(">> ============================================================")

  derive_result <- tryCatch({
    derive_ingest_dictionary(
      core_dict_path = core_dict_path,
      output_path    = ingest_dict_path,
      overwrite      = TRUE
    )
  }, error = function(e) {
    message("[orchestrate] STEP A FAILED: ", e$message)
    message("[orchestrate] Steps completed before failure: ",
            paste(steps_completed, collapse = " -> "))
    stop("[orchestrate] Halting orchestration at Step A. ", e$message)
  })

  steps_completed <- c(steps_completed, "A:derive_ingest")
  message("[orchestrate] Step A complete. Rows derived: ",
          derive_result$rows_derived)

  # =========================================================================
  # STEP B: SYNC INGEST DICTIONARY TO DATABASE
  # =========================================================================
  message("")
  message(">> ============================================================")
  message(">> STEP B: SYNC INGEST DICTIONARY TO DATABASE")
  message(">> ============================================================")

  sync_ingest_result <- tryCatch({
    sync_ingest_dictionary(
      con        = con,
      xlsx_path  = ingest_dict_path,
      mode       = sync_mode,
      created_by = created_by
    )
  }, error = function(e) {
    message("[orchestrate] STEP B FAILED: ", e$message)
    message("[orchestrate] Steps completed before failure: ",
            paste(steps_completed, collapse = " -> "))
    stop("[orchestrate] Halting orchestration at Step B. ", e$message)
  })

  steps_completed <- c(steps_completed, "B:sync_ingest")
  message("[orchestrate] Step B complete. Rows synced: ",
          sync_ingest_result$rows_synced)

  # =========================================================================
  # STEP C: UPDATE TYPE DECISION TABLE
  # =========================================================================
  message("")
  message(">> ============================================================")
  message(">> STEP C: UPDATE TYPE DECISION TABLE")
  message(">> ============================================================")

  update_type_result <- tryCatch({
    update_type_decision_table(
      core_dict_path     = core_dict_path,
      type_decision_path = type_decision_path,
      archive_existing   = archive_existing
    )
  }, error = function(e) {
    message("[orchestrate] STEP C FAILED: ", e$message)
    message("[orchestrate] Steps completed before failure: ",
            paste(steps_completed, collapse = " -> "))
    stop("[orchestrate] Halting orchestration at Step C. ", e$message)
  })

  steps_completed <- c(steps_completed, "C:update_types")
  message("[orchestrate] Step C complete. Total rows: ",
          update_type_result$total_rows,
          " (new: ", update_type_result$new_rows,
          ", preserved: ", update_type_result$preserved_rows, ")")

  # =========================================================================
  # STEP D: BUILD EXPECTED SCHEMA DICTIONARY (EXISTING FUNCTION)
  # =========================================================================
  # This calls the existing build_expected_schema_dictionary() function,
  # which reads from reference.ingest_dictionary (DB, freshly synced in
  # Step B) and type_decision_table.xlsx (freshly updated in Step C).
  # =========================================================================
  message("")
  message(">> ============================================================")
  message(">> STEP D: BUILD EXPECTED SCHEMA DICTIONARY")
  message(">> ============================================================")

  build_schema_result <- tryCatch({
    expected_schema <- build_expected_schema_dictionary(
      con                = con,
      schema_version     = schema_version,
      effective_from     = effective_from,
      effective_to       = effective_to,
      type_decision_path = type_decision_path
    )

    # Write the result to xlsx
    dir.create(dirname(expected_schema_path), recursive = TRUE,
               showWarnings = FALSE)
    writexl::write_xlsx(expected_schema, expected_schema_path)

    message("[orchestrate] Expected schema dictionary written to: ",
            expected_schema_path)
    message("[orchestrate] ", nrow(expected_schema), " rows, ",
            dplyr::n_distinct(expected_schema$lake_table_name), " tables.")

    expected_schema
  }, error = function(e) {
    message("[orchestrate] STEP D FAILED: ", e$message)
    message("[orchestrate] Steps completed before failure: ",
            paste(steps_completed, collapse = " -> "))
    stop("[orchestrate] Halting orchestration at Step D. ", e$message)
  })

  steps_completed <- c(steps_completed, "D:build_schema")
  message("[orchestrate] Step D complete. Schema rows: ",
          nrow(build_schema_result))

  # =========================================================================
  # STEP E: SYNC METADATA TO DATABASE (EXISTING FUNCTION)
  # =========================================================================
  # This calls the existing sync_metadata() function, which reads
  # expected_schema_dictionary.xlsx and loads it into reference.metadata.
  # =========================================================================
  message("")
  message(">> ============================================================")
  message(">> STEP E: SYNC METADATA TO DATABASE")
  message(">> ============================================================")

  sync_metadata_result <- tryCatch({
    sync_metadata(
      con        = con,
      xlsx_path  = expected_schema_path,
      mode       = sync_mode,
      created_by = created_by
    )
  }, error = function(e) {
    message("[orchestrate] STEP E FAILED: ", e$message)
    message("[orchestrate] Steps completed before failure: ",
            paste(steps_completed, collapse = " -> "))
    stop("[orchestrate] Halting orchestration at Step E. ", e$message)
  })

  steps_completed <- c(steps_completed, "E:sync_metadata")
  message("[orchestrate] Step E complete. Rows synced: ",
          sync_metadata_result$rows_synced)

  # =========================================================================
  # FINAL SUMMARY
  # =========================================================================
  orchestration_end <- Sys.time()
  duration_secs <- as.numeric(difftime(orchestration_end, orchestration_start,
                                       units = "secs"))

  message("")
  message("=================================================================")
  message("  METADATA REFRESH COMPLETE")
  message("=================================================================")
  message("  Steps completed:    ",
          paste(steps_completed, collapse = " -> "))
  message("  Duration:           ", round(duration_secs, 2), " seconds")
  message("")
  message("  Step A (ingest dict):     ", derive_result$rows_derived,
          " rows derived")
  message("  Step B (ingest DB sync):  ", sync_ingest_result$rows_synced,
          " rows synced")
  message("  Step C (type decisions):  ", update_type_result$total_rows,
          " total (", update_type_result$new_rows, " new)")
  message("  Step D (expected schema): ", nrow(build_schema_result),
          " rows built")
  message("  Step E (metadata sync):   ", sync_metadata_result$rows_synced,
          " rows synced")
  message("=================================================================")

  if (update_type_result$new_rows > 0) {
    message("")
    message("  ACTION REQUIRED: ", update_type_result$new_rows,
            " new variables in type_decision_table.xlsx")
    message("  need human review. Open the file and set final_type for")
    message("  rows marked 'PENDING REVIEW'.")
  }

  # =========================================================================
  # RETURN RESULTS
  # =========================================================================
  return(list(
    status               = "success",
    steps_completed      = steps_completed,
    derive_ingest_result = derive_result,
    sync_ingest_result   = sync_ingest_result,
    update_type_result   = update_type_result,
    build_schema_rows    = nrow(build_schema_result),
    sync_metadata_result = sync_metadata_result,
    duration_seconds     = round(duration_secs, 2)
  ))
}
