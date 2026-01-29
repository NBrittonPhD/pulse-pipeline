# =============================================================================
# explore_metadata
# =============================================================================
# Purpose:
#   Inspect the reference.metadata table to view the expected schema
#   definitions used by Step 3 schema validation. Shows governed column
#   definitions, data types, nullability, primary keys, and schema version
#   info. Useful for verifying that metadata sync completed correctly and
#   understanding what Step 3 validates against.
#
# Usage:
#   source("r/explore/explore_metadata.R")
#   con <- connect_to_pulse()
#   explore_metadata(con)                                       # All active
#   explore_metadata(con, lake_table = "cisir_vitals")          # One table
#   explore_metadata(con, source_type = "CISIR")                # By source
#   explore_metadata(con, schema_version = "2025.0")            # By version
#   explore_metadata(con, required_only = TRUE)                 # Required cols
#   explore_metadata(con, active_only = FALSE)                  # Include inactive
#
# Author: Noel
# Last Updated: 2026-01-29
# =============================================================================

library(DBI)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
explore_metadata <- function(con,
                             lake_table = NULL,
                             lake_variable = NULL,
                             source_type = NULL,
                             schema_version = NULL,
                             required_only = FALSE,
                             active_only = TRUE,
                             n = 200) {

    # -------------------------------------------------------------------------
    # INPUT VALIDATION
    # -------------------------------------------------------------------------
    if (!inherits(con, "DBIConnection")) {
        stop("[explore_metadata] ERROR: 'con' must be a valid DBI connection.")
    }

    # -------------------------------------------------------------------------
    # BUILD QUERY WITH OPTIONAL FILTERS
    # -------------------------------------------------------------------------
    where_clauses <- c()

    if (active_only) {
        where_clauses <- c(where_clauses, "is_active = TRUE")
    }

    if (!is.null(lake_table)) {
        where_clauses <- c(where_clauses, glue("lake_table_name ILIKE '%{lake_table}%'"))
    }

    if (!is.null(lake_variable)) {
        where_clauses <- c(where_clauses, glue("lake_variable_name ILIKE '%{lake_variable}%'"))
    }

    if (!is.null(source_type)) {
        where_clauses <- c(where_clauses, glue("source_type ILIKE '%{source_type}%'"))
    }

    if (!is.null(schema_version)) {
        where_clauses <- c(where_clauses, glue("schema_version = '{schema_version}'"))
    }

    if (required_only) {
        where_clauses <- c(where_clauses, "is_required = TRUE")
    }

    where_sql <- ""
    if (length(where_clauses) > 0) {
        where_sql <- paste("WHERE", paste(where_clauses, collapse = " AND "))
    }

    query <- glue("
        SELECT
            metadata_id,
            schema_version,
            effective_from,
            effective_to,
            table_schema,
            lake_table_name,
            lake_variable_name,
            data_type,
            udt_name,
            is_nullable,
            is_required,
            is_primary_key,
            ordinal_position,
            type_descriptor,
            source_type,
            source_table_name,
            source_variable_name,
            column_schema_hash,
            is_active,
            synced_at
        FROM reference.metadata
        {where_sql}
        ORDER BY lake_table_name, ordinal_position
        LIMIT {n}
    ")

    # -------------------------------------------------------------------------
    # EXECUTE AND RETURN
    # -------------------------------------------------------------------------
    result <- DBI::dbGetQuery(con, query)

    if (nrow(result) == 0) {
        message("[explore_metadata] No matching metadata entries found.")
    } else {
        table_count <- length(unique(result$lake_table_name))
        message(glue(
            "[explore_metadata] Returned {nrow(result)} variable(s) ",
            "across {table_count} table(s)."
        ))
    }

    result
}
