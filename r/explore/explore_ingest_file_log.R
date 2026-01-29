# =============================================================================
# explore_ingest_file_log
# =============================================================================
# Purpose:
#   Inspect the governance.ingest_file_log table to view file-level lineage
#   for ingestion batches. Shows individual file metadata including checksums,
#   row counts, load status, and destination table names. Useful for verifying
#   Step 2 output at the file level and tracing data provenance.
#
# Usage:
#   source("r/explore/explore_ingest_file_log.R")
#   con <- connect_to_pulse()
#   explore_ingest_file_log(con, ingest_id = "ING_tr2026_test_20251209_120000")
#   explore_ingest_file_log(con, load_status = "error")       # Failed files
#   explore_ingest_file_log(con, file_name = "vitals")         # Search by name
#   explore_ingest_file_log(con, lake_table = "cisir_vitals")  # By destination
#
# Author: Noel
# Last Updated: 2026-01-29
# =============================================================================

library(DBI)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
explore_ingest_file_log <- function(con,
                                    ingest_id = NULL,
                                    file_name = NULL,
                                    lake_table = NULL,
                                    load_status = NULL,
                                    n = 50) {

    # -------------------------------------------------------------------------
    # INPUT VALIDATION
    # -------------------------------------------------------------------------
    if (!inherits(con, "DBIConnection")) {
        stop("[explore_ingest_file_log] ERROR: 'con' must be a valid DBI connection.")
    }

    # -------------------------------------------------------------------------
    # BUILD QUERY WITH OPTIONAL FILTERS
    # -------------------------------------------------------------------------
    where_clauses <- c()

    if (!is.null(ingest_id)) {
        where_clauses <- c(where_clauses, glue("ingest_id ILIKE '%{ingest_id}%'"))
    }

    if (!is.null(file_name)) {
        where_clauses <- c(where_clauses, glue("file_name ILIKE '%{file_name}%'"))
    }

    if (!is.null(lake_table)) {
        where_clauses <- c(where_clauses, glue("lake_table_name ILIKE '%{lake_table}%'"))
    }

    if (!is.null(load_status)) {
        where_clauses <- c(where_clauses, glue("load_status = '{load_status}'"))
    }

    where_sql <- ""
    if (length(where_clauses) > 0) {
        where_sql <- paste("WHERE", paste(where_clauses, collapse = " AND "))
    }

    query <- glue("
        SELECT
            ingest_file_id,
            ingest_id,
            file_name,
            file_path,
            lake_table_name,
            file_size_bytes,
            row_count,
            checksum,
            load_status,
            logged_at_utc,
            completed_at_utc
        FROM governance.ingest_file_log
        {where_sql}
        ORDER BY logged_at_utc DESC
        LIMIT {n}
    ")

    # -------------------------------------------------------------------------
    # EXECUTE AND RETURN
    # -------------------------------------------------------------------------
    result <- DBI::dbGetQuery(con, query)

    if (nrow(result) == 0) {
        message("[explore_ingest_file_log] No matching file log entries found.")
    } else {
        message(glue("[explore_ingest_file_log] Returned {nrow(result)} file entry(ies)."))
    }

    result
}
