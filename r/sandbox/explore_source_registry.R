# =============================================================================
# explore_source_registry
# =============================================================================
# Purpose:
#   Inspect the governance.source_registry table to view all registered data
#   sources. Useful for verifying Step 1 output, checking which sources are
#   active, and confirming source_id values before running downstream steps.
#
# Usage:
#   source("r/sandbox/explore_source_registry.R")
#   con <- connect_to_pulse()
#   explore_source_registry(con)                          # All sources
#   explore_source_registry(con, active_only = TRUE)      # Active only
#   explore_source_registry(con, system_type = "CISIR")   # Filter by type
#   explore_source_registry(con, source_id = "tr2026")    # Search by id
#
# Author: Noel
# Last Updated: 2026-01-29
# =============================================================================

library(DBI)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
explore_source_registry <- function(con,
                                    source_id = NULL,
                                    system_type = NULL,
                                    active_only = FALSE,
                                    n = 50) {

    # -------------------------------------------------------------------------
    # INPUT VALIDATION
    # -------------------------------------------------------------------------
    if (!inherits(con, "DBIConnection")) {
        stop("[explore_source_registry] ERROR: 'con' must be a valid DBI connection.")
    }

    # -------------------------------------------------------------------------
    # BUILD QUERY WITH OPTIONAL FILTERS
    # -------------------------------------------------------------------------
    where_clauses <- c()

    if (!is.null(source_id)) {
        where_clauses <- c(where_clauses, glue("source_id ILIKE '%{source_id}%'"))
    }

    if (!is.null(system_type)) {
        where_clauses <- c(where_clauses, glue("system_type ILIKE '%{system_type}%'"))
    }

    if (active_only) {
        where_clauses <- c(where_clauses, "active = TRUE")
    }

    where_sql <- ""
    if (length(where_clauses) > 0) {
        where_sql <- paste("WHERE", paste(where_clauses, collapse = " AND "))
    }

    query <- glue("
        SELECT
            source_id,
            source_name,
            system_type,
            update_frequency,
            data_owner,
            ingest_method,
            expected_schema_version,
            pii_classification,
            active,
            created_at_utc,
            last_modified_utc,
            created_by
        FROM governance.source_registry
        {where_sql}
        ORDER BY created_at_utc DESC
        LIMIT {n}
    ")

    # -------------------------------------------------------------------------
    # EXECUTE AND RETURN
    # -------------------------------------------------------------------------
    result <- DBI::dbGetQuery(con, query)

    if (nrow(result) == 0) {
        message("[explore_source_registry] No matching sources found.")
    } else {
        message(glue("[explore_source_registry] Returned {nrow(result)} source(s)."))
    }

    result
}
