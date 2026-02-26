# =============================================================================
# load_harmonization_map
# =============================================================================
# Purpose:      Load active harmonization mappings from
#               reference.harmonization_map for a specific validated target
#               table, with optional source_type filtering.
#
#               Returns a tibble of column-level mappings that tell the
#               harmonization engine how to SELECT from staging and INSERT
#               into validated tables.
#
# Inputs:
#   - con:          DBI connection object (required)
#   - target_table: character: validated table name, e.g. "admission" (required)
#   - source_type:  character: optional filter for a single source type
#                   (e.g. "CISIR"). NULL means all sources.
#
# Outputs:      Tibble with columns:
#                 map_id, source_type, source_table, source_column,
#                 target_table, target_column, transform_type,
#                 transform_expression, priority
#
# Dependencies:
#   - DBI, dplyr, glue, tibble
#
# Author:       Noel
# Last Updated: 2026-02-04
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
load_harmonization_map <- function(con, target_table, source_type = NULL) {

    # =========================================================================
    # INPUT VALIDATION
    # =========================================================================
    if (!inherits(con, "DBIConnection")) {
        stop("[load_harmonization_map] ERROR: 'con' must be a valid DBI connection object.")
    }
    if (!DBI::dbIsValid(con)) {
        stop("[load_harmonization_map] ERROR: Database connection is not valid.")
    }
    if (is.null(target_table) || !nzchar(target_table)) {
        stop("[load_harmonization_map] ERROR: target_table must be a non-empty string.")
    }

    # =========================================================================
    # QUERY ACTIVE MAPPINGS
    # =========================================================================
    # Build query with optional source_type filter. Only active mappings
    # (is_active = TRUE) are returned, ordered by source_type, source_table,
    # and priority (lower = higher priority).
    # =========================================================================

    if (is.null(source_type)) {

        mappings <- DBI::dbGetQuery(con, glue("
            SELECT map_id, source_type, source_table, source_column,
                   target_table, target_column, transform_type,
                   transform_expression, priority
            FROM reference.harmonization_map
            WHERE target_table = '{target_table}'
              AND is_active = TRUE
            ORDER BY source_type, source_table, priority
        "))

    } else {

        mappings <- DBI::dbGetQuery(con, glue("
            SELECT map_id, source_type, source_table, source_column,
                   target_table, target_column, transform_type,
                   transform_expression, priority
            FROM reference.harmonization_map
            WHERE target_table = '{target_table}'
              AND source_type = '{source_type}'
              AND is_active = TRUE
            ORDER BY source_table, priority
        "))

    }

    message(glue("[load_harmonization_map] Loaded {nrow(mappings)} active mappings ",
                 "for validated.{target_table}",
                 "{ifelse(is.null(source_type), '', paste0(' (', source_type, ')'))}"))

    return(tibble::as_tibble(mappings))
}
