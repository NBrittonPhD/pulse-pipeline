# =============================================================================
# profile_table
# =============================================================================
# Purpose:      Profile all columns of a single table. Calls leaf functions
#               for each column (infer type, detect sentinels, profile
#               missingness, profile distribution, generate issues), then
#               aggregates into a per-table summary with quality score.
#
#               This function does NOT write to the database. It returns
#               structured tibbles that the orchestrator (profile_data)
#               writes to the 5 governance tables.
#
# Inputs:
#   - con:          DBIConnection (for reading the table data)
#   - schema_name:  character, "raw" or "staging"
#   - table_name:   character, table name without schema prefix
#   - ingest_id:    character, batch identifier
#   - config:       list from load_profiling_config()
#
# Outputs:      Named list with 5 tibbles:
#                 profile       → governance.data_profile rows
#                 distributions → governance.data_profile_distribution rows
#                 sentinels     → governance.data_profile_sentinel rows
#                 issues        → governance.data_profile_issue rows
#                 summary       → governance.data_profile_summary row (1 row)
#
# Side Effects: Reads from database (SELECT only)
#
# Dependencies: DBI, dplyr, tibble, glue
#               infer_column_type, detect_sentinels, profile_missingness,
#               profile_distribution, generate_issues, calculate_quality_score
#
# Author:       Noel
# Last Updated: 2026-01-30
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(DBI)
library(dplyr)
library(tibble)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
profile_table <- function(con, schema_name, table_name, ingest_id, config) {

    profiled_at <- Sys.time()

    # =========================================================================
    # EMPTY RESULT TEMPLATES
    # =========================================================================
    empty_profile <- tibble::tibble(
        ingest_id = character(), schema_name = character(),
        table_name = character(), variable_name = character(),
        inferred_type = character(), total_count = integer(),
        valid_count = integer(), na_count = integer(),
        empty_count = integer(), whitespace_count = integer(),
        sentinel_count = integer(), na_pct = numeric(),
        empty_pct = numeric(), whitespace_pct = numeric(),
        sentinel_pct = numeric(), total_missing_count = integer(),
        total_missing_pct = numeric(), valid_pct = numeric(),
        unique_count = integer(), unique_pct = numeric(),
        profiled_at = as.POSIXct(character())
    )
    empty_dist <- tibble::tibble(
        ingest_id = character(), schema_name = character(),
        table_name = character(), variable_name = character(),
        distribution_type = character(), stat_min = numeric(),
        stat_max = numeric(), stat_mean = numeric(),
        stat_median = numeric(), stat_sd = numeric(),
        stat_q25 = numeric(), stat_q75 = numeric(),
        stat_iqr = numeric(), top_values_json = character(),
        mode_value = character(), mode_count = integer(),
        mode_pct = numeric()
    )
    empty_sentinel <- tibble::tibble(
        ingest_id = character(), schema_name = character(),
        table_name = character(), variable_name = character(),
        sentinel_value = character(), sentinel_count = integer(),
        sentinel_pct = numeric(), detection_method = character(),
        confidence = character()
    )
    empty_issue <- tibble::tibble(
        ingest_id = character(), schema_name = character(),
        table_name = character(), variable_name = character(),
        issue_type = character(), severity = character(),
        description = character(), value = numeric(),
        recommendation = character()
    )

    # =========================================================================
    # LOAD TABLE DATA
    # =========================================================================
    table_data <- DBI::dbGetQuery(
        con,
        glue::glue("SELECT * FROM {schema_name}.{table_name}")
    )

    row_count <- nrow(table_data)

    if (row_count == 0) {
        message(glue("  [profile_table] {table_name}: 0 rows — skipping"))
        summary_row <- tibble::tibble(
            ingest_id = ingest_id, schema_name = schema_name,
            table_name = table_name, row_count = 0L,
            variable_count = ncol(table_data), avg_valid_pct = NA_real_,
            min_valid_pct = NA_real_, max_missing_pct = NA_real_,
            critical_issue_count = 0L, warning_issue_count = 0L,
            info_issue_count = 0L, quality_score = "Excellent",
            worst_variable = NA_character_,
            worst_variable_missing_pct = NA_real_
        )
        return(list(
            profile = empty_profile, distributions = empty_dist,
            sentinels = empty_sentinel, issues = empty_issue,
            summary = summary_row
        ))
    }

    column_names <- names(table_data)
    n_cols <- length(column_names)

    message(glue("  [profile_table] {table_name}: {row_count} rows, {n_cols} columns"))

    # =========================================================================
    # PROFILE EACH COLUMN
    # =========================================================================
    all_profiles      <- vector("list", n_cols)
    all_distributions <- vector("list", n_cols)
    all_sentinels     <- list()
    all_issues        <- list()

    for (i in seq_along(column_names)) {
        col_name <- column_names[i]
        values   <- as.character(table_data[[col_name]])

        # --- Infer type ---
        col_type <- infer_column_type(values, col_name, config)

        # --- Detect sentinels ---
        sentinel_result <- detect_sentinels(values, col_name, col_type, config)
        sentinel_vals   <- sentinel_result$sentinel_value

        # --- Profile missingness ---
        miss_result <- profile_missingness(values, sentinel_vals)

        # --- Profile distribution ---
        dist_result <- profile_distribution(values, col_type, sentinel_vals, config)

        # --- Generate issues ---
        issue_result <- generate_issues(
            col_name, table_name, miss_result, col_type,
            miss_result$unique_count, miss_result$total_count, config
        )

        # --- Build profile row ---
        all_profiles[[i]] <- tibble::tibble(
            ingest_id           = ingest_id,
            schema_name         = schema_name,
            table_name          = table_name,
            variable_name       = col_name,
            inferred_type       = col_type,
            total_count         = as.integer(miss_result$total_count),
            valid_count         = as.integer(miss_result$valid_count),
            na_count            = as.integer(miss_result$na_count),
            empty_count         = as.integer(miss_result$empty_count),
            whitespace_count    = as.integer(miss_result$whitespace_count),
            sentinel_count      = as.integer(miss_result$sentinel_count),
            na_pct              = miss_result$na_pct,
            empty_pct           = miss_result$empty_pct,
            whitespace_pct      = miss_result$whitespace_pct,
            sentinel_pct        = miss_result$sentinel_pct,
            total_missing_count = as.integer(miss_result$total_missing_count),
            total_missing_pct   = miss_result$total_missing_pct,
            valid_pct           = miss_result$valid_pct,
            unique_count        = as.integer(miss_result$unique_count),
            unique_pct          = miss_result$unique_pct,
            profiled_at         = profiled_at
        )

        # --- Build distribution row ---
        all_distributions[[i]] <- tibble::tibble(
            ingest_id         = ingest_id,
            schema_name       = schema_name,
            table_name        = table_name,
            variable_name     = col_name,
            distribution_type = dist_result$distribution_type,
            stat_min          = dist_result$stat_min,
            stat_max          = dist_result$stat_max,
            stat_mean         = dist_result$stat_mean,
            stat_median       = dist_result$stat_median,
            stat_sd           = dist_result$stat_sd,
            stat_q25          = dist_result$stat_q25,
            stat_q75          = dist_result$stat_q75,
            stat_iqr          = dist_result$stat_iqr,
            top_values_json   = dist_result$top_values_json,
            mode_value        = dist_result$mode_value,
            mode_count        = as.integer(dist_result$mode_count),
            mode_pct          = dist_result$mode_pct
        )

        # --- Accumulate sentinels (if any) ---
        if (nrow(sentinel_result) > 0) {
            sentinel_result$ingest_id   <- ingest_id
            sentinel_result$schema_name <- schema_name
            sentinel_result$table_name  <- table_name
            sentinel_result$variable_name <- col_name
            all_sentinels[[length(all_sentinels) + 1]] <- sentinel_result
        }

        # --- Accumulate issues (if any) ---
        if (nrow(issue_result) > 0) {
            issue_result$ingest_id   <- ingest_id
            issue_result$schema_name <- schema_name
            all_issues[[length(all_issues) + 1]] <- issue_result
        }
    }

    # =========================================================================
    # BIND RESULTS
    # =========================================================================
    profile_tbl <- dplyr::bind_rows(all_profiles)
    dist_tbl    <- dplyr::bind_rows(all_distributions)

    sentinel_tbl <- if (length(all_sentinels) > 0) {
        dplyr::bind_rows(all_sentinels)
    } else {
        empty_sentinel
    }

    issues_tbl <- if (length(all_issues) > 0) {
        dplyr::bind_rows(all_issues)
    } else {
        empty_issue
    }

    # =========================================================================
    # CALCULATE TABLE-LEVEL SUMMARY
    # =========================================================================
    avg_valid_pct <- round(mean(profile_tbl$valid_pct, na.rm = TRUE), 2)
    min_valid_pct <- round(min(profile_tbl$valid_pct, na.rm = TRUE), 2)
    max_missing_pct <- round(max(profile_tbl$total_missing_pct, na.rm = TRUE), 2)

    critical_count <- sum(issues_tbl$severity == "critical", na.rm = TRUE)
    warning_count  <- sum(issues_tbl$severity == "warning", na.rm = TRUE)
    info_count     <- sum(issues_tbl$severity == "info", na.rm = TRUE)

    quality_score <- calculate_quality_score(max_missing_pct, critical_count, config)

    # Worst variable: highest total_missing_pct
    worst_idx <- which.max(profile_tbl$total_missing_pct)
    worst_var <- if (length(worst_idx) > 0) profile_tbl$variable_name[worst_idx] else NA_character_
    worst_pct <- if (length(worst_idx) > 0) profile_tbl$total_missing_pct[worst_idx] else NA_real_

    summary_tbl <- tibble::tibble(
        ingest_id              = ingest_id,
        schema_name            = schema_name,
        table_name             = table_name,
        row_count              = as.integer(row_count),
        variable_count         = as.integer(n_cols),
        avg_valid_pct          = avg_valid_pct,
        min_valid_pct          = min_valid_pct,
        max_missing_pct        = max_missing_pct,
        critical_issue_count   = as.integer(critical_count),
        warning_issue_count    = as.integer(warning_count),
        info_issue_count       = as.integer(info_count),
        quality_score          = quality_score,
        worst_variable         = worst_var,
        worst_variable_missing_pct = worst_pct
    )

    message(glue("  [profile_table] {table_name}: score={quality_score}, issues={critical_count}C/{warning_count}W/{info_count}I"))

    # =========================================================================
    # RETURN
    # =========================================================================
    return(list(
        profile       = profile_tbl,
        distributions = dist_tbl,
        sentinels     = sentinel_tbl,
        issues        = issues_tbl,
        summary       = summary_tbl
    ))
}
