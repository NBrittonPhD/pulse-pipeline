# =============================================================================
# review_step3_validation.R — Review Schema Validation Results
# =============================================================================
# Purpose: Inspect schema validation issues from governance.structure_qc_table.
#          Shows issue counts by table, severity breakdown, and full issue
#          detail for a selected table.
#
# HOW TO USE:
#   1. Set ingest_id below
#   2. Optionally set detail_table to drill into a specific table
#   3. Run: source("r/review/review_step3_validation.R")
#
# Author: Noel
# =============================================================================


# =============================================================================
# USER INPUT SECTION — EDIT BELOW
# =============================================================================

# The ingest_id to review (from governance.batch_log)
ingest_id <- "ING_trauma_registry2026_toy_20260128_170308"

# Set to a table name to see full issue detail, or NULL for summary only
detail_table <- NULL
# detail_table <- "trauma_registry_blood"

# =============================================================================
# END USER INPUT SECTION
# =============================================================================


# =============================================================================
# INITIALIZE
# =============================================================================
source("pulse-init-all.R")
con <- connect_to_pulse()


# =============================================================================
# QUERY 1: ISSUE SUMMARY BY TABLE AND SEVERITY
# =============================================================================

cat("\n")
cat("===================================================================\n")
cat("           STEP 3 REVIEW: SCHEMA VALIDATION                       \n")
cat("===================================================================\n")
cat(glue::glue("  Ingest: {ingest_id}"), "\n\n")

summary_by_table <- DBI::dbGetQuery(con, glue::glue("
    SELECT lake_table_name,
           severity,
           COUNT(*) AS issue_count
    FROM governance.structure_qc_table
    WHERE ingest_id = '{ingest_id}'
    GROUP BY lake_table_name, severity
    ORDER BY lake_table_name, severity
"))

if (nrow(summary_by_table) == 0) {
    cat("  No schema validation issues found for this ingest.\n")
} else {
    cat("--- Issues by Table and Severity ---\n\n")
    print(summary_by_table, row.names = FALSE)
}


# =============================================================================
# QUERY 2: SEVERITY TOTALS
# =============================================================================

severity_totals <- DBI::dbGetQuery(con, glue::glue("
    SELECT severity,
           COUNT(*) AS total_issues,
           COUNT(DISTINCT lake_table_name) AS tables_affected
    FROM governance.structure_qc_table
    WHERE ingest_id = '{ingest_id}'
    GROUP BY severity
    ORDER BY severity
"))

if (nrow(severity_totals) > 0) {
    cat("\n\n--- Severity Totals ---\n\n")
    print(severity_totals, row.names = FALSE)
}


# =============================================================================
# QUERY 3: ISSUE TYPE BREAKDOWN
# =============================================================================

issue_types <- DBI::dbGetQuery(con, glue::glue("
    SELECT issue_type,
           severity,
           COUNT(*) AS occurrences
    FROM governance.structure_qc_table
    WHERE ingest_id = '{ingest_id}'
    GROUP BY issue_type, severity
    ORDER BY COUNT(*) DESC
"))

if (nrow(issue_types) > 0) {
    cat("\n\n--- Issue Types ---\n\n")
    print(issue_types, row.names = FALSE)
}


# =============================================================================
# QUERY 4: DETAIL FOR A SPECIFIC TABLE (if requested)
# =============================================================================

if (!is.null(detail_table)) {
    cat(glue::glue("\n\n--- Detail: {detail_table} ---\n\n"))

    detail <- DBI::dbGetQuery(con, glue::glue("
        SELECT lake_variable_name,
               issue_type,
               severity,
               is_blocking,
               issue_message,
               expected_value,
               observed_value
        FROM governance.structure_qc_table
        WHERE ingest_id = '{ingest_id}'
          AND lake_table_name = '{detail_table}'
        ORDER BY severity, lake_variable_name
    "))

    if (nrow(detail) == 0) {
        cat("  No issues found for this table.\n")
    } else {
        cat(glue::glue("  Issues: {nrow(detail)}"), "\n\n")
        print(detail, row.names = FALSE)
    }
}


# =============================================================================
# CLEANUP
# =============================================================================
cat("\n===================================================================\n")
if (DBI::dbIsValid(con)) DBI::dbDisconnect(con)
