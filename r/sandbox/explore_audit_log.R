# =============================================================================
# explore_audit_log
# =============================================================================
# Purpose:
#   Inspect the governance.audit_log table to view the trail of all
#   governance-relevant actions taken across pipeline steps. Useful for
#   debugging, confirming step execution, and governance audits.
#
# Usage:
#   source("r/sandbox/explore_audit_log.R")
#   con <- connect_to_pulse()
#   explore_audit_log(con)                                     # Most recent 20
#   explore_audit_log(con, n = 50)                             # More rows
#   explore_audit_log(con, ingest_id = "ING_tr2026_test")      # By ingest
#   explore_audit_log(con, action = "source_registration")     # By action keyword
#
# Author: Noel
# Last Updated: 2026-01-29
# =============================================================================

library(DBI)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
explore_audit_log <- function(con,
                              ingest_id = NULL,
                              action = NULL,
                              executed_by = NULL,
                              n = 20) {

    # -------------------------------------------------------------------------
    # INPUT VALIDATION
    # -------------------------------------------------------------------------
    if (!inherits(con, "DBIConnection")) {
        stop("[explore_audit_log] ERROR: 'con' must be a valid DBI connection.")
    }

    # -------------------------------------------------------------------------
    # BUILD QUERY WITH OPTIONAL FILTERS
    # -------------------------------------------------------------------------
    where_clauses <- c()

    if (!is.null(ingest_id)) {
        where_clauses <- c(where_clauses, glue("ingest_id ILIKE '%{ingest_id}%'"))
    }

    if (!is.null(action)) {
        where_clauses <- c(where_clauses, glue("action ILIKE '%{action}%'"))
    }

    if (!is.null(executed_by)) {
        where_clauses <- c(where_clauses, glue("executed_by ILIKE '%{executed_by}%'"))
    }

    where_sql <- ""
    if (length(where_clauses) > 0) {
        where_sql <- paste("WHERE", paste(where_clauses, collapse = " AND "))
    }

    query <- glue("
        SELECT
            audit_id,
            ingest_id,
            action,
            details,
            executed_by,
            executed_at_utc
        FROM governance.audit_log
        {where_sql}
        ORDER BY executed_at_utc DESC
        LIMIT {n}
    ")

    # -------------------------------------------------------------------------
    # EXECUTE AND RETURN
    # -------------------------------------------------------------------------
    result <- DBI::dbGetQuery(con, query)

    if (nrow(result) == 0) {
        message("[explore_audit_log] No matching audit entries found.")
    } else {
        message(glue("[explore_audit_log] Returned {nrow(result)} audit entry(ies)."))
    }

    result
}
