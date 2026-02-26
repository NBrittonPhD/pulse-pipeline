# =============================================================================
# harmonize_table
# =============================================================================
# Purpose:      Harmonize all source staging tables into one validated table.
#
#               For a given validated target table (e.g., "admission"), this
#               function:
#                 1. Loads active mappings from reference.harmonization_map
#                 2. Groups them by (source_type, source_table)
#                 3. Deletes prior data for this ingest_id (idempotency)
#                 4. For each source group:
#                    a. Verifies the staging table exists
#                    b. Builds a SQL SELECT via build_harmonization_query()
#                    c. Executes INSERT INTO validated.{target_table} SELECT ...
#                    d. Logs the operation to governance.transform_log
#                 5. Returns a summary of rows inserted and sources processed
#
#               Each source group is wrapped in tryCatch so that a failure in
#               one staging table does not abort the entire target table.
#
# Inputs:
#   - con:                DBI connection object (required)
#   - target_table:       character: validated table name (required)
#   - ingest_id:          character: batch identifier (required)
#   - source_type_filter: character: optional, limit to one source type
#
# Outputs:      Named list with:
#                 target_table, rows_inserted, sources_processed,
#                 sources_skipped, sources_failed
#
# Side Effects:
#   - Writes to validated.{target_table} (insert)
#   - Writes to governance.transform_log (append)
#   - Deletes prior rows in validated.{target_table} for this ingest_id
#
# Dependencies:
#   - DBI, dplyr, glue
#   - load_harmonization_map() from r/harmonization/load_harmonization_map.R
#   - build_harmonization_query() from r/harmonization/build_harmonization_query.R
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
harmonize_table <- function(con, target_table, ingest_id, source_type_filter = NULL) {

    # =========================================================================
    # INPUT VALIDATION
    # =========================================================================
    if (!inherits(con, "DBIConnection")) {
        stop("[harmonize_table] ERROR: 'con' must be a valid DBI connection object.")
    }
    if (!DBI::dbIsValid(con)) {
        stop("[harmonize_table] ERROR: Database connection is not valid.")
    }
    if (is.null(target_table) || !nzchar(target_table)) {
        stop("[harmonize_table] ERROR: target_table must be a non-empty string.")
    }
    if (is.null(ingest_id) || !nzchar(ingest_id)) {
        stop("[harmonize_table] ERROR: ingest_id must be a non-empty string.")
    }

    # =========================================================================
    # SOURCE DEPENDENCIES
    # =========================================================================
    proj_root <- getOption("pulse.proj_root", default = ".")

    source(file.path(proj_root, "r", "harmonization", "load_harmonization_map.R"))
    source(file.path(proj_root, "r", "harmonization", "build_harmonization_query.R"))

    # =========================================================================
    # VERIFY VALIDATED TABLE EXISTS
    # =========================================================================
    table_exists <- DBI::dbExistsTable(con, DBI::Id(schema = "validated", table = target_table))
    if (!table_exists) {
        stop(glue("[harmonize_table] ERROR: validated.{target_table} does not exist. ",
                  "Run the DDL (sql/ddl/create_VALIDATED_{toupper(target_table)}.sql) first."))
    }

    # =========================================================================
    # LOAD MAPPINGS
    # =========================================================================
    mappings <- load_harmonization_map(con, target_table, source_type_filter)

    if (nrow(mappings) == 0) {
        message(glue("[harmonize_table] No active mappings found for validated.{target_table}. Skipping."))
        return(list(
            target_table      = target_table,
            rows_inserted     = 0L,
            sources_processed = 0L,
            sources_skipped   = 0L,
            sources_failed    = 0L
        ))
    }

    # =========================================================================
    # GROUP BY (source_type, source_table)
    # =========================================================================
    # Each unique (source_type, source_table) pair represents one staging table
    # that contributes data to this validated table.
    # =========================================================================
    source_groups <- mappings %>%
        distinct(source_type, source_table) %>%
        arrange(source_type, source_table)

    message(glue("[harmonize_table] validated.{target_table}: ",
                 "{nrow(source_groups)} source table(s) to process"))

    # =========================================================================
    # IDEMPOTENT DELETE — remove prior data for this ingest_id
    # =========================================================================
    # This ensures re-running harmonization for the same ingest_id produces
    # identical results without duplication.
    # =========================================================================
    target_quoted <- DBI::dbQuoteIdentifier(con, DBI::Id(schema = "validated", table = target_table))

    prior_rows <- DBI::dbGetQuery(con, glue(
        "SELECT COUNT(*) AS n FROM {target_quoted} WHERE ingest_id = '{ingest_id}'"
    ))$n

    if (prior_rows > 0) {
        DBI::dbExecute(con, glue(
            "DELETE FROM {target_quoted} WHERE ingest_id = '{ingest_id}'"
        ))
        message(glue("[harmonize_table] Deleted {prior_rows} prior rows for ingest_id '{ingest_id}'"))
    }

    # Also clean prior transform_log entries for this ingest + target
    DBI::dbExecute(con, glue(
        "DELETE FROM governance.transform_log
         WHERE ingest_id = '{ingest_id}'
           AND target_schema = 'validated'
           AND target_table = '{target_table}'"
    ))

    # =========================================================================
    # PROCESS EACH SOURCE GROUP
    # =========================================================================
    total_rows       <- 0L
    sources_ok       <- 0L
    sources_skipped  <- 0L
    sources_failed   <- 0L

    for (g in seq_len(nrow(source_groups))) {
        st   <- source_groups$source_type[g]
        stbl <- source_groups$source_table[g]

        message(glue("[harmonize_table]   ({g}/{nrow(source_groups)}) ",
                     "{st} / {stbl}"))

        # -----------------------------------------------------------------
        # Check that the staging table exists
        # -----------------------------------------------------------------
        if (!DBI::dbExistsTable(con, DBI::Id(schema = "staging", table = stbl))) {
            message(glue("[harmonize_table]   WARNING: staging.{stbl} not found. Skipping."))
            sources_skipped <- sources_skipped + 1L

            # Log the skip to transform_log
            DBI::dbExecute(con, glue("
                INSERT INTO governance.transform_log (
                    ingest_id, source_schema, source_table, source_row_count,
                    target_schema, target_table, target_row_count,
                    operation_type, columns_mapped, status, error_message,
                    started_at, completed_at, duration_seconds
                ) VALUES (
                    '{ingest_id}', 'staging', '{stbl}', 0,
                    'validated', '{target_table}', 0,
                    'insert', 0, 'failed', 'Staging table not found',
                    NOW(), NOW(), 0
                )
            "))
            next
        }

        # -----------------------------------------------------------------
        # Filter mappings to this source group
        # -----------------------------------------------------------------
        source_mappings <- mappings %>%
            filter(source_type == st, source_table == stbl)

        # -----------------------------------------------------------------
        # Get source row count (for logging)
        # -----------------------------------------------------------------
        source_row_count <- DBI::dbGetQuery(con, glue(
            "SELECT COUNT(*) AS n FROM staging.\"{stbl}\""
        ))$n

        started_at <- Sys.time()

        # -----------------------------------------------------------------
        # Build and execute the INSERT INTO ... SELECT query
        # -----------------------------------------------------------------
        result <- tryCatch({

            # Build the SELECT statement
            select_query <- build_harmonization_query(
                con          = con,
                mappings     = source_mappings,
                source_table = stbl,
                source_type  = st,
                ingest_id    = ingest_id
            )

            # Build the target column list: governance cols + mapped domain cols
            target_cols <- c("source_type", "source_table", "ingest_id",
                             source_mappings$target_column)
            target_col_list <- paste(
                vapply(target_cols, function(c) as.character(DBI::dbQuoteIdentifier(con, c)), character(1)),
                collapse = ", "
            )

            # Assemble and execute the full INSERT
            insert_sql <- glue("INSERT INTO {target_quoted} ({target_col_list})\n{select_query}")
            rows_inserted <- DBI::dbExecute(con, insert_sql)

            list(rows = rows_inserted, status = "success", error = NULL)

        }, error = function(e) {
            message(glue("[harmonize_table]   ERROR: {e$message}"))
            list(rows = 0L, status = "failed", error = e$message)
        })

        completed_at <- Sys.time()
        duration <- round(as.numeric(difftime(completed_at, started_at, units = "secs")), 2)

        # -----------------------------------------------------------------
        # Log to governance.transform_log
        # -----------------------------------------------------------------
        error_msg_sql <- if (is.null(result$error)) "NULL" else glue("'{gsub(\"'\", \"''\", result$error)}'")

        DBI::dbExecute(con, glue("
            INSERT INTO governance.transform_log (
                ingest_id, source_schema, source_table, source_row_count,
                target_schema, target_table, target_row_count,
                operation_type, columns_mapped, status, error_message,
                started_at, completed_at, duration_seconds
            ) VALUES (
                '{ingest_id}', 'staging', '{stbl}', {source_row_count},
                'validated', '{target_table}', {result$rows},
                'insert', {nrow(source_mappings)}, '{result$status}', {error_msg_sql},
                '{format(started_at, \"%Y-%m-%d %H:%M:%S\")}',
                '{format(completed_at, \"%Y-%m-%d %H:%M:%S\")}',
                {duration}
            )
        "))

        # Accumulate totals
        if (result$status == "success") {
            total_rows  <- total_rows + result$rows
            sources_ok  <- sources_ok + 1L
            message(glue("[harmonize_table]   -> {result$rows} rows inserted ",
                         "({nrow(source_mappings)} columns, {duration}s)"))
        } else {
            sources_failed <- sources_failed + 1L
        }
    }

    # =========================================================================
    # RETURN SUMMARY
    # =========================================================================
    message(glue("[harmonize_table] validated.{target_table}: ",
                 "{total_rows} total rows, ",
                 "{sources_ok} sources OK, ",
                 "{sources_skipped} skipped, ",
                 "{sources_failed} failed"))

    list(
        target_table      = target_table,
        rows_inserted     = total_rows,
        sources_processed = sources_ok,
        sources_skipped   = sources_skipped,
        sources_failed    = sources_failed
    )
}
