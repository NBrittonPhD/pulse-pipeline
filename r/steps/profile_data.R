# =============================================================================
# profile_data
# =============================================================================
# Purpose:      Step 5 orchestrator. Profiles all tables from a given ingest
#               batch to assess data quality before harmonization.
#
#               For each table in the ingest:
#                 1. Infers column types (numeric, categorical, date, identifier)
#                 2. Detects sentinel/placeholder values
#                 3. Profiles missingness (NA, empty, whitespace, sentinel)
#                 4. Computes distribution statistics
#                 5. Generates quality issues
#                 6. Calculates per-table quality scores
#
#               Results are written to 5 governance tables. Prior profiling
#               data for the same (ingest_id, schema_name) is deleted first
#               to ensure idempotent re-runs.
#
# Inputs:
#   - con:               DBIConnection (required)
#   - ingest_id:         character: batch identifier (required)
#   - schema_to_profile: character: "raw" or "staging" (default "raw")
#   - config_path:       character: path to profiling_settings.yml (optional)
#
# Outputs:      Named list with:
#                 tables_profiled, variables_profiled, sentinels_detected,
#                 critical_issues, warning_issues, info_issues, overall_score
#
# Side Effects:
#   - Writes to governance.data_profile (append)
#   - Writes to governance.data_profile_distribution (append)
#   - Writes to governance.data_profile_sentinel (append)
#   - Writes to governance.data_profile_issue (append)
#   - Writes to governance.data_profile_summary (append)
#   - Writes to governance.audit_log (append)
#
# Dependencies:
#   - DBI, dplyr, glue, tibble
#   - load_profiling_config, infer_column_type
#   - detect_sentinels, profile_missingness, profile_distribution
#   - generate_issues, calculate_quality_score, profile_table
#   - write_audit_event
#
# Author:       Noel
# Last Updated: 2026-01-30
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(DBI)
library(dplyr)
library(glue)
library(tibble)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
profile_data <- function(con, ingest_id, schema_to_profile = "raw",
                          config_path = NULL) {

    message("=================================================================")
    message("[profile_data] STEP 5: DATA PROFILING")
    message("=================================================================")
    message(glue("[profile_data] ingest_id: {ingest_id}"))
    message(glue("[profile_data] schema:    {schema_to_profile}"))

    # =========================================================================
    # INPUT VALIDATION
    # =========================================================================
    if (!inherits(con, "DBIConnection")) {
        stop("[profile_data] ERROR: 'con' must be a valid DBI connection object.")
    }
    if (!DBI::dbIsValid(con)) {
        stop("[profile_data] ERROR: Database connection is not valid.")
    }
    if (is.null(ingest_id) || !nzchar(ingest_id)) {
        stop("[profile_data] ERROR: ingest_id must be a non-empty string.")
    }

    # =========================================================================
    # SOURCE DEPENDENCIES
    # =========================================================================
    proj_root <- getOption("pulse.proj_root", default = ".")

    source(file.path(proj_root, "r", "utilities", "load_profiling_config.R"))
    source(file.path(proj_root, "r", "utilities", "infer_column_type.R"))
    source(file.path(proj_root, "r", "profiling", "detect_sentinels.R"))
    source(file.path(proj_root, "r", "profiling", "profile_missingness.R"))
    source(file.path(proj_root, "r", "profiling", "profile_distribution.R"))
    source(file.path(proj_root, "r", "profiling", "generate_issues.R"))
    source(file.path(proj_root, "r", "profiling", "calculate_quality_score.R"))
    source(file.path(proj_root, "r", "profiling", "profile_table.R"))
    source(file.path(proj_root, "r", "steps", "write_audit_event.R"))

    # =========================================================================
    # LOAD CONFIG
    # =========================================================================
    config <- load_profiling_config(config_path)

    # =========================================================================
    # VERIFY INGEST EXISTS
    # =========================================================================
    batch_check <- DBI::dbGetQuery(con, glue(
        "SELECT ingest_id FROM governance.batch_log WHERE ingest_id = '{ingest_id}'"
    ))
    if (nrow(batch_check) == 0) {
        stop(glue("[profile_data] ERROR: ingest_id '{ingest_id}' not found in governance.batch_log."))
    }

    # =========================================================================
    # GET TABLES TO PROFILE
    # =========================================================================
    # For raw/staging: use ingest_file_log (lake_table_name).
    # For validated: use transform_log (target_table) since validated table
    # names differ from staging table names.
    # =========================================================================
    if (schema_to_profile == "validated") {
        raw_tables <- DBI::dbGetQuery(con, glue(
            "SELECT DISTINCT target_table AS lake_table_name
             FROM governance.transform_log
             WHERE ingest_id = '{ingest_id}'
               AND target_schema = 'validated'
               AND status = 'success'
             ORDER BY target_table"
        ))
    } else {
        raw_tables <- DBI::dbGetQuery(con, glue(
            "SELECT DISTINCT lake_table_name
             FROM governance.ingest_file_log
             WHERE ingest_id = '{ingest_id}'
               AND load_status = 'success'
             ORDER BY lake_table_name"
        ))
    }

    if (nrow(raw_tables) == 0) {
        stop(glue("[profile_data] ERROR: No successfully loaded tables found for ingest_id '{ingest_id}' ",
                  "in schema '{schema_to_profile}'."))
    }

    n_tables <- nrow(raw_tables)
    message(glue("[profile_data] Found {n_tables} tables to profile."))

    # =========================================================================
    # CLEAR PRIOR PROFILING DATA (IDEMPOTENCY)
    # =========================================================================
    # Delete any existing profiling rows for this (ingest_id, schema_name)
    # so re-running does not duplicate results.
    # =========================================================================
    message("[profile_data] Clearing prior profiling data...")

    profiling_tables <- c(
        "data_profile", "data_profile_distribution", "data_profile_sentinel",
        "data_profile_issue", "data_profile_summary"
    )

    for (tbl in profiling_tables) {
        DBI::dbExecute(con, glue(
            "DELETE FROM governance.{tbl}
             WHERE ingest_id = '{ingest_id}'
               AND schema_name = '{schema_to_profile}'"
        ))
    }

    # =========================================================================
    # PROFILE EACH TABLE
    # =========================================================================
    total_variables  <- 0L
    total_sentinels  <- 0L
    total_critical   <- 0L
    total_warning    <- 0L
    total_info       <- 0L
    table_scores     <- character()

    for (i in seq_len(n_tables)) {
        tbl_name <- raw_tables$lake_table_name[i]
        message(glue("[profile_data] Profiling table {i}/{n_tables}: {tbl_name}"))

        table_result <- profile_table(con, schema_to_profile, tbl_name, ingest_id, config)

        # --- Write profile rows ---
        if (nrow(table_result$profile) > 0) {
            DBI::dbWriteTable(
                con,
                DBI::Id(schema = "governance", table = "data_profile"),
                table_result$profile,
                append = TRUE, row.names = FALSE
            )
            total_variables <- total_variables + nrow(table_result$profile)
        }

        # --- Write distribution rows ---
        if (nrow(table_result$distributions) > 0) {
            DBI::dbWriteTable(
                con,
                DBI::Id(schema = "governance", table = "data_profile_distribution"),
                table_result$distributions,
                append = TRUE, row.names = FALSE
            )
        }

        # --- Write sentinel rows ---
        if (nrow(table_result$sentinels) > 0) {
            DBI::dbWriteTable(
                con,
                DBI::Id(schema = "governance", table = "data_profile_sentinel"),
                table_result$sentinels,
                append = TRUE, row.names = FALSE
            )
            total_sentinels <- total_sentinels + nrow(table_result$sentinels)
        }

        # --- Write issue rows ---
        if (nrow(table_result$issues) > 0) {
            DBI::dbWriteTable(
                con,
                DBI::Id(schema = "governance", table = "data_profile_issue"),
                table_result$issues,
                append = TRUE, row.names = FALSE
            )
            total_critical <- total_critical + sum(table_result$issues$severity == "critical")
            total_warning  <- total_warning  + sum(table_result$issues$severity == "warning")
            total_info     <- total_info     + sum(table_result$issues$severity == "info")
        }

        # --- Write summary row ---
        if (nrow(table_result$summary) > 0) {
            DBI::dbWriteTable(
                con,
                DBI::Id(schema = "governance", table = "data_profile_summary"),
                table_result$summary,
                append = TRUE, row.names = FALSE
            )
            table_scores <- c(table_scores, table_result$summary$quality_score)
        }
    }

    # =========================================================================
    # COMPUTE OVERALL SCORE
    # =========================================================================
    # Overall score is the worst per-table score.
    # =========================================================================
    score_levels <- c("Excellent", "Good", "Fair", "Needs Review")
    if (length(table_scores) > 0) {
        worst_idx <- max(match(table_scores, score_levels), na.rm = TRUE)
        overall_score <- score_levels[worst_idx]
    } else {
        overall_score <- "Needs Review"
    }

    # =========================================================================
    # WRITE AUDIT EVENT
    # =========================================================================
    message("[profile_data] Writing audit log event...")

    write_audit_event(
        con         = con,
        ingest_id   = ingest_id,
        event_type  = "data_profiling",
        object_type = "schema",
        object_name = paste0(schema_to_profile, ".*"),
        status      = "success",
        details     = list(
            tables_profiled    = n_tables,
            variables_profiled = total_variables,
            sentinels_detected = total_sentinels,
            critical_issues    = total_critical,
            warning_issues     = total_warning,
            info_issues        = total_info,
            overall_score      = overall_score
        )
    )

    # =========================================================================
    # PRINT SUMMARY
    # =========================================================================
    message("=================================================================")
    message(glue("[profile_data] PROFILING COMPLETE"))
    message("=================================================================")
    message(glue("  Tables profiled:    {n_tables}"))
    message(glue("  Variables profiled: {total_variables}"))
    message(glue("  Sentinels detected: {total_sentinels}"))
    message(glue("  Issues: {total_critical}C / {total_warning}W / {total_info}I"))
    message(glue("  Overall score:      {overall_score}"))
    message("=================================================================")

    # =========================================================================
    # RETURN
    # =========================================================================
    return(list(
        tables_profiled    = n_tables,
        variables_profiled = total_variables,
        sentinels_detected = total_sentinels,
        critical_issues    = total_critical,
        warning_issues     = total_warning,
        info_issues        = total_info,
        overall_score      = overall_score
    ))
}
