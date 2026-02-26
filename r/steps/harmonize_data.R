# =============================================================================
# harmonize_data
# =============================================================================
# Purpose:      Step 6 orchestrator. Harmonizes staging tables into validated
#               tables using mappings derived from the metadata dictionary.
#
#               This function:
#                 1. Validates inputs and verifies the ingest batch exists
#                 2. Determines which validated tables to process (all with
#                    active mappings, or a user-specified subset)
#                 3. Calls harmonize_table() for each target table
#                 4. Writes an audit event to governance.audit_log
#                 5. Returns a summary of the harmonization run
#
#               Each target table is processed independently with tryCatch,
#               so a failure in one table does not abort the others.
#
#               The harmonization mappings come from reference.harmonization_map,
#               which is populated by sync_harmonization_map() from the metadata
#               dictionary (CURRENT_core_metadata_dictionary.xlsx → reference.metadata
#               → reference.harmonization_map).
#
# Inputs:
#   - con:                DBI connection object (required)
#   - ingest_id:          character: batch identifier (required)
#   - target_tables:      character vector: validated table names to process,
#                         or NULL for all tables with active mappings
#   - source_type_filter: character: optional, limit to one source type
#
# Outputs:      Named list with:
#                 tables_processed, total_rows, sources_processed, by_table
#
# Side Effects:
#   - Writes to validated.* tables (via harmonize_table)
#   - Writes to governance.transform_log (via harmonize_table)
#   - Writes to governance.audit_log (via write_audit_event)
#
# Dependencies:
#   - DBI, dplyr, glue
#   - harmonize_table() from r/harmonization/harmonize_table.R
#   - write_audit_event() from r/steps/write_audit_event.R
#
# Author:       Noel
# Last Updated: 2026-02-04
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(DBI)
library(dplyr)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
harmonize_data <- function(con, ingest_id, target_tables = NULL,
                            source_type_filter = NULL) {

    message("=================================================================")
    message("[harmonize_data] STEP 6: HARMONIZATION")
    message("=================================================================")
    message(glue("[harmonize_data] ingest_id:          {ingest_id}"))
    message(glue("[harmonize_data] target_tables:      {ifelse(is.null(target_tables), 'ALL', paste(target_tables, collapse = ', '))}"))
    message(glue("[harmonize_data] source_type_filter: {ifelse(is.null(source_type_filter), 'ALL', source_type_filter)}"))

    # =========================================================================
    # INPUT VALIDATION
    # =========================================================================
    if (!inherits(con, "DBIConnection")) {
        stop("[harmonize_data] ERROR: 'con' must be a valid DBI connection object.")
    }
    if (!DBI::dbIsValid(con)) {
        stop("[harmonize_data] ERROR: Database connection is not valid.")
    }
    if (is.null(ingest_id) || !nzchar(ingest_id)) {
        stop("[harmonize_data] ERROR: ingest_id must be a non-empty string.")
    }

    # =========================================================================
    # SOURCE DEPENDENCIES
    # =========================================================================
    proj_root <- getOption("pulse.proj_root", default = ".")

    source(file.path(proj_root, "r", "harmonization", "harmonize_table.R"))
    source(file.path(proj_root, "r", "steps", "write_audit_event.R"))

    # =========================================================================
    # VERIFY INGEST EXISTS
    # =========================================================================
    batch_check <- DBI::dbGetQuery(con, glue(
        "SELECT ingest_id FROM governance.batch_log WHERE ingest_id = '{ingest_id}'"
    ))
    if (nrow(batch_check) == 0) {
        stop(glue("[harmonize_data] ERROR: ingest_id '{ingest_id}' not found in governance.batch_log."))
    }

    # =========================================================================
    # DETERMINE TARGET TABLES
    # =========================================================================
    # If the user did not specify target tables, find all validated tables
    # that have active mappings in reference.harmonization_map.
    # =========================================================================
    if (is.null(target_tables)) {
        target_tables <- DBI::dbGetQuery(con, "
            SELECT DISTINCT target_table
            FROM reference.harmonization_map
            WHERE is_active = TRUE
            ORDER BY target_table
        ")$target_table

        if (length(target_tables) == 0) {
            stop("[harmonize_data] ERROR: No active mappings found in reference.harmonization_map. ",
                 "Run sync_harmonization_map() first.")
        }
    }

    message(glue("[harmonize_data] Tables to harmonize: {length(target_tables)}"))
    for (i in seq_along(target_tables)) {
        message(glue("[harmonize_data]   {i}. {target_tables[i]}"))
    }

    # =========================================================================
    # PROCESS EACH TARGET TABLE
    # =========================================================================
    step_start   <- Sys.time()
    by_table     <- list()
    total_rows   <- 0L
    total_sources <- 0L
    tables_ok    <- 0L
    tables_failed <- 0L

    for (i in seq_along(target_tables)) {
        tbl <- target_tables[i]

        message("")
        message(glue("[harmonize_data] ─── Table {i}/{length(target_tables)}: {tbl} ───"))

        result <- tryCatch({
            harmonize_table(
                con                = con,
                target_table       = tbl,
                ingest_id          = ingest_id,
                source_type_filter = source_type_filter
            )
        }, error = function(e) {
            message(glue("[harmonize_data] ERROR on {tbl}: {e$message}"))
            list(
                target_table      = tbl,
                rows_inserted     = 0L,
                sources_processed = 0L,
                sources_skipped   = 0L,
                sources_failed    = 1L
            )
        })

        by_table[[tbl]]  <- result$rows_inserted
        total_rows       <- total_rows + result$rows_inserted
        total_sources    <- total_sources + result$sources_processed

        if (result$rows_inserted > 0 || result$sources_processed > 0) {
            tables_ok <- tables_ok + 1L
        } else if (result$sources_failed > 0) {
            tables_failed <- tables_failed + 1L
        }
    }

    step_duration <- round(as.numeric(difftime(Sys.time(), step_start, units = "secs")), 1)

    # =========================================================================
    # WRITE AUDIT EVENT
    # =========================================================================
    write_audit_event(
        con         = con,
        ingest_id   = ingest_id,
        event_type  = "harmonization",
        object_type = "schema",
        object_name = "validated.*",
        status      = ifelse(tables_failed == 0, "success", "partial"),
        details     = list(
            tables_processed   = length(target_tables),
            tables_ok          = tables_ok,
            tables_failed      = tables_failed,
            total_rows         = total_rows,
            sources_processed  = total_sources,
            source_type_filter = ifelse(is.null(source_type_filter), "ALL", source_type_filter),
            duration_seconds   = step_duration
        )
    )

    # =========================================================================
    # SUMMARY
    # =========================================================================
    message("")
    message("=================================================================")
    message("[harmonize_data] STEP 6 COMPLETE")
    message("=================================================================")
    message(glue("[harmonize_data] Tables processed:   {length(target_tables)}"))
    message(glue("[harmonize_data] Tables OK:          {tables_ok}"))
    message(glue("[harmonize_data] Tables failed:      {tables_failed}"))
    message(glue("[harmonize_data] Total rows:         {total_rows}"))
    message(glue("[harmonize_data] Total sources:      {total_sources}"))
    message(glue("[harmonize_data] Duration:           {step_duration}s"))
    message("=================================================================")

    # =========================================================================
    # RETURN
    # =========================================================================
    list(
        tables_processed  = length(target_tables),
        total_rows        = total_rows,
        sources_processed = total_sources,
        by_table          = by_table,
        status            = ifelse(tables_failed == 0, "success", "partial")
    )
}
