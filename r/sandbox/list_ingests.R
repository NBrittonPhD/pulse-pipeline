# =============================================================================
# list_ingests
# =============================================================================
# Purpose:
#   Utility function to quickly list recent ingest_ids from governance.batch_log.
#   Useful for finding the ingest_id to use in Step 3 and beyond.
#
# Usage:
#   source("r/sandbox/list_ingests.R")
#   con <- connect_to_pulse()
#   list_ingests(con)                        # Most recent 10
#   list_ingests(con, n = 20)                # Most recent 20
#   list_ingests(con, source_id = "cisir")   # Filter by source
#   list_ingests(con, status = "success")    # Only successful
#
# Author: Noel
# Last Updated: 2026-01-07
# =============================================================================

library(DBI)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
list_ingests <- function(con,
                         n = 10,
                         source_id = NULL,
                         status = NULL) {

    # -------------------------------------------------------------------------
    # INPUT VALIDATION
    # -------------------------------------------------------------------------
    if (!inherits(con, "DBIConnection")) {
        stop("[list_ingests] ERROR: 'con' must be a valid DBI connection.")
    }

    # -------------------------------------------------------------------------
    # BUILD QUERY
    # -------------------------------------------------------------------------
    where_clauses <- c()

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
            files_success
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
        message("[list_ingests] No matching ingests found.")
    }

    result
}
