# =============================================================================
# review_audit_trail.R — Review Audit Trail Across All Steps
# =============================================================================
# Purpose: Inspect the governance.audit_log for pipeline events across all
#          steps. Can filter by ingest_id or show everything.
#
# HOW TO USE:
#   1. Optionally set ingest_filter below
#   2. Run: source("r/review/review_audit_trail.R")
#
# Author: Noel
# =============================================================================


# =============================================================================
# USER INPUT SECTION — EDIT BELOW
# =============================================================================

# Filter by ingest_id (partial match with LIKE), or NULL for all
ingest_filter <- NULL
# ingest_filter <- "trauma_registry2026_toy"

# How many recent events to show
max_events <- 50

# =============================================================================
# END USER INPUT SECTION
# =============================================================================


# =============================================================================
# INITIALIZE
# =============================================================================
source("pulse-init-all.R")
con <- connect_to_pulse()


# =============================================================================
# QUERY 1: AUDIT EVENT SUMMARY
# =============================================================================

cat("\n")
cat("===================================================================\n")
cat("           AUDIT TRAIL REVIEW                                      \n")
cat("===================================================================\n\n")

# Count by action type
action_summary <- DBI::dbGetQuery(con, "
    SELECT action,
           COUNT(*) AS events
    FROM governance.audit_log
    GROUP BY action
    ORDER BY action
")

cat("--- Events by Action ---\n\n")
if (nrow(action_summary) > 0) {
    print(action_summary, row.names = FALSE)
} else {
    cat("  No audit events recorded.\n")
}


# =============================================================================
# QUERY 2: RECENT EVENTS (optionally filtered)
# =============================================================================

audit_where <- if (!is.null(ingest_filter)) {
    glue::glue("WHERE ingest_id LIKE '%{ingest_filter}%'")
} else {
    ""
}

events <- DBI::dbGetQuery(con, glue::glue("
    SELECT audit_id,
           action,
           ingest_id,
           details,
           executed_by,
           executed_at_utc
    FROM governance.audit_log
    {audit_where}
    ORDER BY executed_at_utc DESC
    LIMIT {max_events}
"))

filter_label <- if (!is.null(ingest_filter)) {
    glue::glue(" (filter: {ingest_filter})")
} else {
    ""
}

cat(glue::glue("\n\n--- Recent Events{filter_label} ---\n\n"))
if (nrow(events) == 0) {
    cat("  No events found.\n")
} else {
    cat(glue::glue("  Showing {nrow(events)} events"), "\n\n")
    print(events, row.names = FALSE)
}


# =============================================================================
# QUERY 3: EVENTS BY INGEST
# =============================================================================

by_ingest <- DBI::dbGetQuery(con, glue::glue("
    SELECT ingest_id,
           COUNT(*) AS events,
           MIN(executed_at_utc) AS first_event,
           MAX(executed_at_utc) AS last_event
    FROM governance.audit_log
    {audit_where}
    GROUP BY ingest_id
    ORDER BY MAX(executed_at_utc) DESC
    LIMIT 20
"))

if (nrow(by_ingest) > 0) {
    cat("\n\n--- Events by Ingest ---\n\n")
    print(by_ingest, row.names = FALSE)
}


# =============================================================================
# CLEANUP
# =============================================================================
cat("\n===================================================================\n")
if (DBI::dbIsValid(con)) DBI::dbDisconnect(con)
