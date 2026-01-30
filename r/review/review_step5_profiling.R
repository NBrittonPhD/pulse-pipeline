# =============================================================================
# review_step5_profiling.R — Review Data Profiling Results
# =============================================================================
# Purpose: Inspect data quality profiling results from Step 5. Shows per-table
#          quality scores, issues by severity, missingness breakdown, sentinel
#          detections, and distribution statistics.
#
# HOW TO USE:
#   1. Set ingest_id below
#   2. Optionally set detail_table to drill into a specific table
#   3. Run: source("r/review/review_step5_profiling.R")
#
# Author: Noel
# =============================================================================


# =============================================================================
# USER INPUT SECTION — EDIT BELOW
# =============================================================================

# The ingest_id to review (from governance.batch_log)
ingest_id <- "ING_trauma_registry2026_toy_20260128_170308"

# Which schema was profiled
schema_name <- "raw"

# Set to a table name for detailed column-level output, or NULL for summary
detail_table <- NULL
# detail_table <- "trauma_registry_blood"

# Show top N worst variables by missingness
top_n_worst <- 20

# =============================================================================
# END USER INPUT SECTION
# =============================================================================


# =============================================================================
# INITIALIZE
# =============================================================================
source("pulse-init-all.R")
con <- connect_to_pulse()


# =============================================================================
# QUERY 1: TABLE-LEVEL QUALITY SCORES
# =============================================================================

cat("\n")
cat("===================================================================\n")
cat("           STEP 5 REVIEW: DATA PROFILING                          \n")
cat("===================================================================\n")
cat(glue::glue("  Ingest: {ingest_id}"), "\n")
cat(glue::glue("  Schema: {schema_name}"), "\n\n")

summary <- DBI::dbGetQuery(con, glue::glue("
    SELECT table_name,
           row_count,
           variable_count,
           quality_score,
           critical_issue_count AS critical,
           warning_issue_count AS warnings,
           info_issue_count AS info,
           ROUND(avg_valid_pct, 1) AS avg_valid_pct,
           worst_variable,
           ROUND(worst_variable_missing_pct, 1) AS worst_miss_pct
    FROM governance.data_profile_summary
    WHERE ingest_id = '{ingest_id}'
      AND schema_name = '{schema_name}'
    ORDER BY quality_score, table_name
"))

cat("--- Quality Scores by Table ---\n\n")
if (nrow(summary) == 0) {
    cat("  No profiling results found for this ingest.\n")
} else {
    # Print score distribution
    score_counts <- table(summary$quality_score)
    for (s in names(score_counts)) {
        cat(glue::glue("  {s}: {score_counts[s]} tables"), "\n")
    }
    cat("\n")
    print(summary, row.names = FALSE)
}


# =============================================================================
# QUERY 2: CRITICAL AND WARNING ISSUES
# =============================================================================

issues <- DBI::dbGetQuery(con, glue::glue("
    SELECT table_name,
           variable_name,
           issue_type,
           severity,
           description,
           ROUND(value, 1) AS value,
           recommendation
    FROM governance.data_profile_issue
    WHERE ingest_id = '{ingest_id}'
      AND schema_name = '{schema_name}'
      AND severity IN ('critical', 'warning')
    ORDER BY
        CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 END,
        table_name,
        variable_name
"))

cat("\n\n--- Critical & Warning Issues ---\n\n")
if (nrow(issues) == 0) {
    cat("  No critical or warning issues.\n")
} else {
    critical <- issues[issues$severity == "critical", ]
    warnings <- issues[issues$severity == "warning", ]
    cat(glue::glue("  Critical: {nrow(critical)}   Warnings: {nrow(warnings)}"), "\n\n")

    if (nrow(critical) > 0) {
        cat("  CRITICAL:\n")
        print(critical[, c("table_name", "variable_name", "issue_type", "description")],
              row.names = FALSE)
        cat("\n")
    }

    # Show first 30 warnings to avoid flooding the console
    if (nrow(warnings) > 0) {
        show_n <- min(30, nrow(warnings))
        cat(glue::glue("  WARNINGS (showing {show_n} of {nrow(warnings)}):"), "\n")
        print(warnings[1:show_n, c("table_name", "variable_name", "issue_type", "value")],
              row.names = FALSE)
    }
}


# =============================================================================
# QUERY 3: TOP N WORST VARIABLES BY MISSINGNESS
# =============================================================================

worst <- DBI::dbGetQuery(con, glue::glue("
    SELECT table_name,
           variable_name,
           inferred_type,
           ROUND(total_missing_pct, 1) AS missing_pct,
           ROUND(na_pct, 1) AS na_pct,
           ROUND(empty_pct, 1) AS empty_pct,
           ROUND(whitespace_pct, 1) AS ws_pct,
           ROUND(sentinel_pct, 1) AS sentinel_pct,
           ROUND(valid_pct, 1) AS valid_pct
    FROM governance.data_profile
    WHERE ingest_id = '{ingest_id}'
      AND schema_name = '{schema_name}'
    ORDER BY total_missing_pct DESC
    LIMIT {top_n_worst}
"))

cat(glue::glue("\n\n--- Top {top_n_worst} Worst Variables (by missingness) ---\n\n"))
if (nrow(worst) > 0) {
    print(worst, row.names = FALSE)
} else {
    cat("  No profile data found.\n")
}


# =============================================================================
# QUERY 4: DETECTED SENTINELS
# =============================================================================

sentinels <- DBI::dbGetQuery(con, glue::glue("
    SELECT table_name,
           variable_name,
           sentinel_value,
           sentinel_count,
           ROUND(sentinel_pct, 1) AS sentinel_pct,
           detection_method,
           confidence
    FROM governance.data_profile_sentinel
    WHERE ingest_id = '{ingest_id}'
      AND schema_name = '{schema_name}'
    ORDER BY sentinel_count DESC
"))

cat("\n\n--- Detected Sentinel Values ---\n\n")
if (nrow(sentinels) == 0) {
    cat("  No sentinels detected.\n")
} else {
    cat(glue::glue("  Total sentinel detections: {nrow(sentinels)}"), "\n\n")
    print(sentinels, row.names = FALSE)
}


# =============================================================================
# QUERY 5: DETAIL FOR A SPECIFIC TABLE (if requested)
# =============================================================================

if (!is.null(detail_table)) {
    cat(glue::glue("\n\n--- Column Detail: {detail_table} ---\n\n"))

    # Missingness per column
    cols <- DBI::dbGetQuery(con, glue::glue("
        SELECT variable_name,
               inferred_type,
               total_count,
               valid_count,
               ROUND(valid_pct, 1) AS valid_pct,
               ROUND(total_missing_pct, 1) AS missing_pct,
               ROUND(na_pct, 1) AS na_pct,
               ROUND(empty_pct, 1) AS empty_pct,
               ROUND(sentinel_pct, 1) AS sentinel_pct,
               unique_count
        FROM governance.data_profile
        WHERE ingest_id = '{ingest_id}'
          AND schema_name = '{schema_name}'
          AND table_name = '{detail_table}'
        ORDER BY total_missing_pct DESC
    "))

    if (nrow(cols) > 0) {
        cat(glue::glue("  Columns: {nrow(cols)}"), "\n\n")
        print(cols, row.names = FALSE)
    } else {
        cat("  No profile data for this table.\n")
    }

    # Distribution stats
    cat(glue::glue("\n\n--- Distribution Stats: {detail_table} ---\n\n"))

    dist_numeric <- DBI::dbGetQuery(con, glue::glue("
        SELECT variable_name,
               ROUND(stat_min, 2) AS min,
               ROUND(stat_max, 2) AS max,
               ROUND(stat_mean, 2) AS mean,
               ROUND(stat_median, 2) AS median,
               ROUND(stat_sd, 2) AS sd,
               ROUND(stat_q25, 2) AS q25,
               ROUND(stat_q75, 2) AS q75
        FROM governance.data_profile_distribution
        WHERE ingest_id = '{ingest_id}'
          AND schema_name = '{schema_name}'
          AND table_name = '{detail_table}'
          AND distribution_type = 'numeric'
        ORDER BY variable_name
    "))

    if (nrow(dist_numeric) > 0) {
        cat("  Numeric distributions:\n\n")
        print(dist_numeric, row.names = FALSE)
    }

    dist_cat <- DBI::dbGetQuery(con, glue::glue("
        SELECT variable_name,
               mode_value,
               mode_count,
               ROUND(mode_pct, 1) AS mode_pct
        FROM governance.data_profile_distribution
        WHERE ingest_id = '{ingest_id}'
          AND schema_name = '{schema_name}'
          AND table_name = '{detail_table}'
          AND distribution_type = 'categorical'
        ORDER BY variable_name
    "))

    if (nrow(dist_cat) > 0) {
        cat("\n\n  Categorical distributions (mode):\n\n")
        print(dist_cat, row.names = FALSE)
    }

    # Issues for this table
    cat(glue::glue("\n\n--- Issues: {detail_table} ---\n\n"))

    tbl_issues <- DBI::dbGetQuery(con, glue::glue("
        SELECT variable_name,
               issue_type,
               severity,
               description,
               recommendation
        FROM governance.data_profile_issue
        WHERE ingest_id = '{ingest_id}'
          AND schema_name = '{schema_name}'
          AND table_name = '{detail_table}'
        ORDER BY
            CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
            variable_name
    "))

    if (nrow(tbl_issues) > 0) {
        print(tbl_issues, row.names = FALSE)
    } else {
        cat("  No issues for this table.\n")
    }
}


# =============================================================================
# CLEANUP
# =============================================================================
cat("\n===================================================================\n")
if (DBI::dbIsValid(con)) DBI::dbDisconnect(con)
