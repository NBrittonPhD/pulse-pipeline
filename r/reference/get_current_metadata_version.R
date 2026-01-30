# =============================================================================
# get_current_metadata_version
# =============================================================================
# Purpose:      Return the current (maximum) version number from
#               reference.metadata. Returns 0 if the table is empty.
#
# Inputs:
#   - con: DBI connection to PULSE database
#
# Outputs:      Integer version number
#
# Side Effects: None (read-only query)
#
# Dependencies: DBI
#
# Author:       Noel
# Last Updated: 2026-01-30
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(DBI)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
get_current_metadata_version <- function(con) {

    if (!inherits(con, "DBIConnection") || !DBI::dbIsValid(con)) {
        stop("[get_current_metadata_version] ERROR: Invalid database connection.")
    }

    result <- DBI::dbGetQuery(con, "
        SELECT COALESCE(MAX(version_number), 0) as version
        FROM reference.metadata
    ")

    return(result$version[1])
}
