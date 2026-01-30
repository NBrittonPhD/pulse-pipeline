# =============================================================================
# explore_batch_log
# =============================================================================
# Purpose:
#   Inspect the governance.batch_log table to view ingestion batch history.
#   Shows all columns including timing, file counts, and error messages.
#   Useful for reviewing Step 2 output, diagnosing failed ingests, and
#   finding ingest_id values for downstream steps.
#
# Usage:
#   source("r/sandbox/explore_batch_log.R")
#   con <- connect_to_pulse()
#   explore_batch_log(con)                                  # Most recent 20
#   explore_batch_log(con, source_id = "tr2026")            # Filter by source
#   explore_batch_log(con, status = "success")              # Only successful
#   explore_batch_log(con, status = "error")                # Only failures
#   explore_batch_log(con, ingest_id = "ING_tr2026_test")   # Specific ingest
#
# Note:
#   See also list_ingests() for a lighter-weight lookup of ingest_id values.
#
# Author: Noel
# Last Updated: 2026-01-29
# =============================================================================

library(DBI)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
explore_batch_log <- function(con,
                              ingest_id = NULL,
                              source_id = NULL,
                              status = NULL,
                              n = 20) {

    # -------------------------------------------------------------------------
    # INPUT VALIDATION
    # -------------------------------------------------------------------------
    if (!inherits(con, "DBIConnection")) {
        stop("[explore_batch_log] ERROR: 'con' must be a valid DBI connection.")
    }

    # -------------------------------------------------------------------------
    # BUILD QUERY WITH OPTIONAL FILTERS
    # -------------------------------------------------------------------------
    where_clauses <- c()

    if (!is.null(ingest_id)) {
        where_clauses <- c(where_clauses, glue("ingest_id ILIKE '%{ingest_id}%'"))
    }

    if (!is.null(source_id)) {
        where_clauses <- c(where_clauses, glue("source_id ILIKE '%{source_id}%'"))
    }

    if (!is.null(status)) {
        where_clauses <- c(where_clauses, glue("status = '{status}'"))
    }

    where_sql <- ""
    if (length(where_clauses) > 0) {
        where_sql <- paste("WHERE", paste(where_clauses, collapse = " AND "))
    }

    query <- glue("
        SELECT
            ingest_id,
            source_id,
            status,
            error_message,
            file_count,
            files_success,
            files_error,
            batch_started_at_utc,
            batch_completed_at_utc,
            ingest_timestamp
        FROM governance.batch_log
        {where_sql}
        ORDER BY ingest_timestamp DESC
        LIMIT {n}
    ")

    # -------------------------------------------------------------------------
    # EXECUTE AND RETURN
    # -------------------------------------------------------------------------
    result <- DBI::dbGetQuery(con, query)

    if (nrow(result) == 0) {
        message("[explore_batch_log] No matching batch entries found.")
    } else {
        message(glue("[explore_batch_log] Returned {nrow(result)} batch(es)."))
    }

    result
}
