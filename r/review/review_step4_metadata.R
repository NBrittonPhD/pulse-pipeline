# =============================================================================
# review_step4_metadata.R — Review Metadata Synchronization
# =============================================================================
# Purpose: Inspect the current state of reference.metadata and the change
#          history in reference.metadata_history. Shows version info, variable
#          counts by source, and recent changes.
#
# HOW TO USE:
#   1. Optionally set source_type_filter to narrow by source
#   2. Run: source("r/review/review_step4_metadata.R")
#
# Author: Noel
# =============================================================================


# =============================================================================
# USER INPUT SECTION — EDIT BELOW
# =============================================================================

# Filter by source type, or NULL for all
source_type_filter <- NULL
# source_type_filter <- "TRAUMA_REGISTRY"

# How many recent history rows to show
history_limit <- 30

# =============================================================================
# END USER INPUT SECTION
# =============================================================================


# =============================================================================
# INITIALIZE
# =============================================================================
source("pulse-init-all.R")
con <- connect_to_pulse()


# =============================================================================
# QUERY 1: METADATA OVERVIEW
# =============================================================================

cat("\n")
cat("===================================================================\n")
cat("           STEP 4 REVIEW: METADATA SYNCHRONIZATION                 \n")
cat("===================================================================\n\n")

overview <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS total_active_vars,
           COUNT(DISTINCT lake_table_name) AS tables,
           COUNT(DISTINCT source_type) AS source_types,
           MAX(version_number) AS latest_version
    FROM reference.metadata
    WHERE is_active = TRUE
")

cat("--- Metadata Overview ---\n\n")
cat(glue::glue("  Active variables: {overview$total_active_vars}"), "\n")
cat(glue::glue("  Tables:           {overview$tables}"), "\n")
cat(glue::glue("  Source types:     {overview$source_types}"), "\n")
cat(glue::glue("  Latest version:   {overview$latest_version}"), "\n")


# =============================================================================
# QUERY 2: VARIABLE COUNTS BY SOURCE TYPE
# =============================================================================

by_source <- DBI::dbGetQuery(con, "
    SELECT source_type,
           COUNT(*) AS variables,
           COUNT(DISTINCT lake_table_name) AS tables
    FROM reference.metadata
    WHERE is_active = TRUE
    GROUP BY source_type
    ORDER BY source_type
")

cat("\n\n--- Variables by Source Type ---\n\n")
print(by_source, row.names = FALSE)


# =============================================================================
# QUERY 3: VARIABLE COUNTS BY TABLE (filtered if requested)
# =============================================================================

source_where <- if (!is.null(source_type_filter)) {
    glue::glue("AND source_type = '{source_type_filter}'")
} else {
    ""
}

by_table <- DBI::dbGetQuery(con, glue::glue("
    SELECT lake_table_name,
           source_type,
           COUNT(*) AS variables,
           SUM(CASE WHEN is_identifier THEN 1 ELSE 0 END) AS identifiers,
           SUM(CASE WHEN is_phi THEN 1 ELSE 0 END) AS phi_fields,
           SUM(CASE WHEN is_required THEN 1 ELSE 0 END) AS required_fields
    FROM reference.metadata
    WHERE is_active = TRUE
    {source_where}
    GROUP BY lake_table_name, source_type
    ORDER BY source_type, lake_table_name
"))

cat("\n\n--- Variables by Table ---\n\n")
print(by_table, row.names = FALSE)


# =============================================================================
# QUERY 4: RECENT CHANGE HISTORY
# =============================================================================

history <- DBI::dbGetQuery(con, glue::glue("
    SELECT version_number,
           change_type,
           COUNT(*) AS changes
    FROM reference.metadata_history
    GROUP BY version_number, change_type
    ORDER BY version_number DESC, change_type
    LIMIT {history_limit}
"))

if (nrow(history) > 0) {
    cat("\n\n--- Change History (by version) ---\n\n")
    print(history, row.names = FALSE)
}


# =============================================================================
# QUERY 5: INACTIVE (REMOVED) VARIABLES
# =============================================================================

removed <- DBI::dbGetQuery(con, "
    SELECT lake_table_name,
           lake_variable_name,
           source_type,
           version_number,
           updated_at
    FROM reference.metadata
    WHERE is_active = FALSE
    ORDER BY updated_at DESC
    LIMIT 20
")

if (nrow(removed) > 0) {
    cat("\n\n--- Recently Removed Variables (soft-deleted) ---\n\n")
    print(removed, row.names = FALSE)
} else {
    cat("\n\n  No removed variables.\n")
}


# =============================================================================
# CLEANUP
# =============================================================================
cat("\n===================================================================\n")
if (DBI::dbIsValid(con)) DBI::dbDisconnect(con)
