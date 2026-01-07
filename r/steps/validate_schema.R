# =============================================================================
# validate_schema
# =============================================================================
# Purpose:
#   Main schema validation step function for Step 3 of the PULSE pipeline.
#   Compares all raw tables against expected metadata definitions from
#   reference.metadata, identifies structural mismatches, and writes
#   issues to governance.structure_qc_table.
#
# Inputs:
#   con (DBIConnection)
#       Active database connection to PULSE.
#
#   ingest_id (character)
#       Identifier for the current ingest batch. Must exist in governance.batch_log.
#
#   source_id (character, optional)
#       Source identifier. If NULL, derived from batch_log.
#
#   source_type (character, optional)
#       Source type (e.g., "CISIR", "CLARITY"). If NULL, derived from ingest tables.
#
#   halt_on_error (logical)
#       If TRUE, stop execution when ERROR/critical severity issues are found.
#       Default: TRUE (recommended for production)
#
# Outputs:
#   A list with:
#     - success: logical (TRUE if no critical errors)
#     - issues_count: integer total issues
#     - critical_count: integer critical issues
#     - warning_count: integer warning issues
#     - info_count: integer info issues
#     - tables_validated: integer count of tables checked
#     - issues: data.frame of all issues
#
# Side Effects:
#   - Writes rows to governance.structure_qc_table
#
# Dependencies:
#   - DBI, dplyr, glue, tibble
#   - r/utilities/compare_fields.R
#   - reference.metadata table (synced from expected_schema_dictionary.xlsx)
#   - governance.structure_qc_table
#   - governance.batch_log
#   - governance.ingest_file_log
#
# Author: Noel
# Last Updated: 2026-01-07
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(DBI)
library(dplyr)
library(glue)
library(tibble)

# =============================================================================
# SOURCE DEPENDENCIES
# =============================================================================
proj_root <- getOption("pulse.proj_root", default = ".")
source(file.path(proj_root, "r/utilities/compare_fields.R"))

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
validate_schema <- function(con,
                            ingest_id,
                            source_id = NULL,
                            source_type = NULL,
                            halt_on_error = TRUE) {

    # =========================================================================
    # STEP 3 HEADER
    # =========================================================================
    message("=================================================================")
    message("[validate_schema] STEP 3: SCHEMA VALIDATION ENGINE")
    message("=================================================================")
    message(glue("[validate_schema] Ingest ID:      {ingest_id}"))
    message(glue("[validate_schema] Halt on Error:  {halt_on_error}"))

    # =========================================================================
    # INPUT VALIDATION
    # =========================================================================
    if (!inherits(con, "DBIConnection")) {
        stop("[validate_schema] ERROR: 'con' must be a valid DBI connection object.")
    }

    if (is.null(ingest_id) || nchar(trimws(ingest_id)) == 0) {
        stop("[validate_schema] ERROR: 'ingest_id' must be a non-empty string.")
    }

    # =========================================================================
    # VERIFY INGEST_ID EXISTS IN BATCH_LOG
    # =========================================================================
    message("[validate_schema] Verifying ingest_id exists in batch_log...")

    batch_check <- dbGetQuery(con, glue("
        SELECT ingest_id, source_id, status
        FROM governance.batch_log
        WHERE ingest_id = '{ingest_id}'
    "))

    if (nrow(batch_check) == 0) {
        stop(glue("[validate_schema] ERROR: ingest_id '{ingest_id}' not found in governance.batch_log."))
    }

    # Derive source_id from batch_log if not provided
    if (is.null(source_id)) {
        source_id <- batch_check$source_id[1]
        message(glue("[validate_schema] Derived source_id from batch_log: {source_id}"))
    }

    # =========================================================================
    # LOAD EXPECTED SCHEMA FROM REFERENCE.METADATA
    # =========================================================================
    message("[validate_schema] Loading expected schema from reference.metadata...")

    expected_schema <- dbGetQuery(con, "
        SELECT
            schema_version,
            lake_table_name,
            lake_variable_name,
            data_type,
            udt_name,
            COALESCE(is_nullable, TRUE) as is_nullable,
            COALESCE(is_required, FALSE) as is_required,
            COALESCE(is_primary_key, FALSE) as is_primary_key,
            ordinal_position
        FROM reference.metadata
        WHERE is_active = TRUE
    ")

    if (nrow(expected_schema) == 0) {
        stop("[validate_schema] ERROR: No active metadata found in reference.metadata. Run sync_metadata() first.")
    }

    schema_version <- unique(expected_schema$schema_version)[1]
    message(glue("[validate_schema] Loaded {nrow(expected_schema)} expected field definitions."))
    message(glue("[validate_schema] Schema version: {schema_version}"))

    # =========================================================================
    # IDENTIFY RAW TABLES FOR THIS INGEST
    # =========================================================================
    message("[validate_schema] Identifying raw tables for this ingest...")

    # Get tables from ingest_file_log for this ingest_id
    raw_tables <- dbGetQuery(con, glue("
        SELECT DISTINCT lake_table_name
        FROM governance.ingest_file_log
        WHERE ingest_id = '{ingest_id}'
          AND lake_table_name IS NOT NULL
          AND load_status = 'success'
    "))

    if (nrow(raw_tables) == 0) {
        warning("[validate_schema] WARNING: No raw tables found for this ingest. Returning empty result.")
        return(list(
            success = TRUE,
            issues_count = 0L,
            critical_count = 0L,
            warning_count = 0L,
            info_count = 0L,
            tables_validated = 0L,
            issues = tibble()
        ))
    }

    message(glue("[validate_schema] Found {nrow(raw_tables)} raw tables to validate."))

    # =========================================================================
    # VALIDATE EACH TABLE
    # =========================================================================
    all_issues <- tibble()

    for (i in seq_len(nrow(raw_tables))) {
        table_name <- raw_tables$lake_table_name[i]
        message(glue("[validate_schema] Validating table {i}/{nrow(raw_tables)}: {table_name}"))

        # ---------------------------------------------------------------------
        # Get expected fields for this table
        # ---------------------------------------------------------------------
        expected_fields <- expected_schema %>%
            filter(lake_table_name == !!table_name)

        if (nrow(expected_fields) == 0) {
            message(glue("[validate_schema]   WARNING: No expected schema for '{table_name}'. Skipping."))
            next
        }

        # ---------------------------------------------------------------------
        # Get observed fields from information_schema
        # ---------------------------------------------------------------------
        observed_fields <- dbGetQuery(con, glue("
            SELECT
                '{table_name}' as lake_table_name,
                column_name as lake_variable_name,
                data_type,
                udt_name,
                CASE WHEN is_nullable = 'YES' THEN TRUE ELSE FALSE END as is_nullable,
                FALSE as is_primary_key,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'raw'
              AND table_name = '{table_name}'
            ORDER BY ordinal_position
        "))

        if (nrow(observed_fields) == 0) {
            message(glue("[validate_schema]   WARNING: Table 'raw.{table_name}' not found in database."))
            next
        }

        # ---------------------------------------------------------------------
        # Compare fields using compare_fields()
        # ---------------------------------------------------------------------
        comparison <- compare_fields(
            expected_schema = expected_fields,
            observed_schema = observed_fields,
            lake_table_name = table_name,
            schema_version = schema_version
        )

        message(glue("[validate_schema]   Issues found: {comparison$n_issues}"))

        if (comparison$n_issues > 0) {
            all_issues <- bind_rows(all_issues, comparison$issues)
        }
    }

    # =========================================================================
    # WRITE ISSUES TO STRUCTURE_QC_TABLE
    # =========================================================================
    if (nrow(all_issues) > 0) {
        message(glue("[validate_schema] Writing {nrow(all_issues)} issues to structure_qc_table..."))

        # Prepare data for insert
        issues_to_write <- all_issues %>%
            mutate(
                ingest_id = !!ingest_id,
                source_id = !!source_id,
                source_type = !!source_type,
                issue_message = paste0(issue_type, ": ", lake_variable_name),
                check_run_at = Sys.time(),
                created_at = Sys.time(),
                created_by = "validate_schema"
            ) %>%
            select(
                ingest_id,
                source_id,
                source_type,
                schema_version,
                lake_table_name,
                lake_variable_name,
                issue_code,
                issue_type,
                issue_group,
                severity,
                is_blocking,
                issue_message,
                expected_value,
                observed_value,
                check_context,
                check_run_at,
                created_at,
                created_by
            )

        # Write to database
        dbWriteTable(
            con,
            DBI::Id(schema = "governance", table = "structure_qc_table"),
            issues_to_write,
            append = TRUE,
            row.names = FALSE
        )

        message("[validate_schema] Issues written successfully.")
    }

    # =========================================================================
    # CALCULATE SUMMARY STATISTICS
    # =========================================================================
    critical_count <- sum(all_issues$severity == "critical", na.rm = TRUE)
    warning_count <- sum(all_issues$severity == "warning", na.rm = TRUE)
    info_count <- sum(all_issues$severity == "info", na.rm = TRUE)

    # =========================================================================
    # PRINT VALIDATION SUMMARY
    # =========================================================================
    message("=================================================================")
    message("[validate_schema] VALIDATION SUMMARY")
    message("=================================================================")
    message(glue("  Tables Validated: {nrow(raw_tables)}"))
    message(glue("  Total Issues:     {nrow(all_issues)}"))
    message(glue("  Critical:         {critical_count}"))
    message(glue("  Warnings:         {warning_count}"))
    message(glue("  Info:             {info_count}"))
    message("=================================================================")

    # =========================================================================
    # HALT ON CRITICAL ERRORS IF REQUESTED
    # =========================================================================
    if (critical_count > 0 && halt_on_error) {
        stop(glue(
            "[validate_schema] CRITICAL: {critical_count} critical issues detected. ",
            "Execution halted. Review governance.structure_qc_table for details:\n",
            "  SELECT * FROM governance.structure_qc_table WHERE ingest_id = '{ingest_id}';"
        ))
    }

    # =========================================================================
    # RETURN RESULTS
    # =========================================================================
    message("[validate_schema] Schema validation complete.")

    list(
        success = (critical_count == 0),
        issues_count = nrow(all_issues),
        critical_count = critical_count,
        warning_count = warning_count,
        info_count = info_count,
        tables_validated = nrow(raw_tables),
        issues = all_issues
    )
}
