# =============================================================================
# explore_structure_qc
# =============================================================================
# Purpose:
#   Inspect the governance.structure_qc_table to review schema validation
#   issues detected during Step 3. Shows missing fields, extra fields,
#   type mismatches, and their severity. Useful for diagnosing why a
#   schema validation run failed or for reviewing warnings before
#   proceeding to downstream steps.
#
# Usage:
#   source("r/explore/explore_structure_qc.R")
#   con <- connect_to_pulse()
#   explore_structure_qc(con, ingest_id = "ING_tr2026_test_20251209_120000")
#   explore_structure_qc(con, severity = "critical")            # Critical only
#   explore_structure_qc(con, issue_type = "MISSING_FIELD")     # Missing fields
#   explore_structure_qc(con, lake_table = "cisir_vitals")      # By table
#   explore_structure_qc(con, blocking_only = TRUE)             # Blocking issues
#
# Author: Noel
# Last Updated: 2026-01-29
# =============================================================================

library(DBI)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
explore_structure_qc <- function(con,
                                 ingest_id = NULL,
                                 severity = NULL,
                                 issue_type = NULL,
                                 lake_table = NULL,
                                 blocking_only = FALSE,
                                 n = 100) {

    # -------------------------------------------------------------------------
    # INPUT VALIDATION
    # -------------------------------------------------------------------------
    if (!inherits(con, "DBIConnection")) {
        stop("[explore_structure_qc] ERROR: 'con' must be a valid DBI connection.")
    }

    # -------------------------------------------------------------------------
    # BUILD QUERY WITH OPTIONAL FILTERS
    # -------------------------------------------------------------------------
    where_clauses <- c()

    if (!is.null(ingest_id)) {
        where_clauses <- c(where_clauses, glue("ingest_id ILIKE '%{ingest_id}%'"))
    }

    if (!is.null(severity)) {
        where_clauses <- c(where_clauses, glue("severity = '{severity}'"))
    }

    if (!is.null(issue_type)) {
        where_clauses <- c(where_clauses, glue("issue_type ILIKE '%{issue_type}%'"))
    }

    if (!is.null(lake_table)) {
        where_clauses <- c(where_clauses, glue("lake_table_name ILIKE '%{lake_table}%'"))
    }

    if (blocking_only) {
        where_clauses <- c(where_clauses, "is_blocking = TRUE")
    }

    where_sql <- ""
    if (length(where_clauses) > 0) {
        where_sql <- paste("WHERE", paste(where_clauses, collapse = " AND "))
    }

    query <- glue("
        SELECT
            qc_issue_id,
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
            notes
        FROM governance.structure_qc_table
        {where_sql}
        ORDER BY
            CASE severity
                WHEN 'critical' THEN 1
                WHEN 'warning'  THEN 2
                WHEN 'info'     THEN 3
            END,
            lake_table_name,
            lake_variable_name
        LIMIT {n}
    ")

    # -------------------------------------------------------------------------
    # EXECUTE AND RETURN
    # -------------------------------------------------------------------------
    result <- DBI::dbGetQuery(con, query)

    if (nrow(result) == 0) {
        message("[explore_structure_qc] No matching QC issues found.")
    } else {
        # Print a quick severity breakdown
        severity_counts <- table(result$severity)
        breakdown <- paste(
            names(severity_counts), severity_counts,
            sep = ": ", collapse = " | "
        )
        message(glue("[explore_structure_qc] Returned {nrow(result)} issue(s). [{breakdown}]"))
    }

    result
}
