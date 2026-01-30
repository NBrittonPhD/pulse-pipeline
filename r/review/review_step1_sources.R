# =============================================================================
# review_step1_sources.R — Review Registered Sources
# =============================================================================
# Purpose: Inspect all data sources registered in governance.source_registry.
#          Run after Step 1 to verify source registration.
#
# HOW TO USE:
#   1. No user input needed — shows all sources
#   2. Run: source("r/review/review_step1_sources.R")
#
# Author: Noel
# =============================================================================


# =============================================================================
# INITIALIZE
# =============================================================================
source("pulse-init-all.R")
con <- connect_to_pulse()


# =============================================================================
# QUERY: ALL REGISTERED SOURCES
# =============================================================================

cat("\n")
cat("===================================================================\n")
cat("           STEP 1 REVIEW: REGISTERED DATA SOURCES                 \n")
cat("===================================================================\n\n")

sources <- DBI::dbGetQuery(con, "
    SELECT source_id,
           source_name,
           system_type,
           update_frequency,
           data_owner,
           ingest_method,
           is_active,
           created_at_utc
    FROM governance.source_registry
    ORDER BY source_id
")

if (nrow(sources) == 0) {
    cat("  No sources registered yet.\n")
} else {
    cat(glue::glue("  Total sources: {nrow(sources)}"), "\n")
    cat(glue::glue("  Active:        {sum(sources$is_active)}"), "\n")
    cat(glue::glue("  Inactive:      {sum(!sources$is_active)}"), "\n\n")
    print(sources, row.names = FALSE)
}


# =============================================================================
# CLEANUP
# =============================================================================
cat("\n===================================================================\n")
if (DBI::dbIsValid(con)) DBI::dbDisconnect(con)
