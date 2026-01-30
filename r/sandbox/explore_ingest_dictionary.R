# =============================================================================
# explore_ingest_dictionary
# =============================================================================
# Purpose:
#   Inspect the reference.ingest_dictionary table to view the mappings
#   between source tables/variables and lake tables/variables. This is the
#   authoritative mapping that drives column renaming during ingestion and
#   expected schema construction. Useful for verifying that the dictionary
#   was loaded correctly and understanding source-to-lake field mappings.
#
# Usage:
#   source("r/sandbox/explore_ingest_dictionary.R")
#   con <- connect_to_pulse()
#   explore_ingest_dictionary(con)                                # All mappings
#   explore_ingest_dictionary(con, lake_table = "cisir_vitals")   # One table
#   explore_ingest_dictionary(con, source_type = "cisir")         # By source
#   explore_ingest_dictionary(con, source_variable = "admit")     # Search vars
#
# Author: Noel
# Last Updated: 2026-01-29
# =============================================================================

library(DBI)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
explore_ingest_dictionary <- function(con,
                                      source_type = NULL,
                                      source_table = NULL,
                                      source_variable = NULL,
                                      lake_table = NULL,
                                      lake_variable = NULL,
                                      n = 200) {

    # -------------------------------------------------------------------------
    # INPUT VALIDATION
    # -------------------------------------------------------------------------
    if (!inherits(con, "DBIConnection")) {
        stop("[explore_ingest_dictionary] ERROR: 'con' must be a valid DBI connection.")
    }

    # -------------------------------------------------------------------------
    # BUILD QUERY WITH OPTIONAL FILTERS
    # -------------------------------------------------------------------------
    where_clauses <- c()

    if (!is.null(source_type)) {
        where_clauses <- c(where_clauses, glue("source_type ILIKE '%{source_type}%'"))
    }

    if (!is.null(source_table)) {
        where_clauses <- c(where_clauses, glue("source_table_name ILIKE '%{source_table}%'"))
    }

    if (!is.null(source_variable)) {
        where_clauses <- c(where_clauses, glue("source_variable_name ILIKE '%{source_variable}%'"))
    }

    if (!is.null(lake_table)) {
        where_clauses <- c(where_clauses, glue("lake_table_name ILIKE '%{lake_table}%'"))
    }

    if (!is.null(lake_variable)) {
        where_clauses <- c(where_clauses, glue("lake_variable_name ILIKE '%{lake_variable}%'"))
    }

    where_sql <- ""
    if (length(where_clauses) > 0) {
        where_sql <- paste("WHERE", paste(where_clauses, collapse = " AND "))
    }

    query <- glue("
        SELECT *
        FROM reference.ingest_dictionary
        {where_sql}
        ORDER BY source_type, lake_table_name, lake_variable_name
        LIMIT {n}
    ")

    # -------------------------------------------------------------------------
    # EXECUTE AND RETURN
    # -------------------------------------------------------------------------
    result <- DBI::dbGetQuery(con, query)

    if (nrow(result) == 0) {
        message("[explore_ingest_dictionary] No matching dictionary entries found.")
    } else {
        table_count <- length(unique(result$lake_table_name))
        source_count <- length(unique(result$source_type))
        message(glue(
            "[explore_ingest_dictionary] Returned {nrow(result)} mapping(s) ",
            "across {table_count} table(s) from {source_count} source type(s)."
        ))
    }

    result
}
