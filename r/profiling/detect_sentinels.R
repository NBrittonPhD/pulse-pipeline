# =============================================================================
# detect_sentinels
# =============================================================================
# Purpose:      Detect sentinel/placeholder values in a column using both
#               config-based matching and frequency analysis.
#
#               Config-based detection compares values against known sentinel
#               lists (e.g., 999, UNKNOWN). Frequency analysis looks for
#               suspicious repeat-digit patterns in numeric columns.
#
# Inputs:
#   - values:       character vector of column values
#   - column_name:  character scalar
#   - column_type:  character scalar from infer_column_type()
#   - config:       list from load_profiling_config()
#
# Outputs:      Tibble with columns: sentinel_value, sentinel_count,
#               sentinel_pct, detection_method, confidence
#               (zero rows if none detected)
#
# Side Effects: None (pure function)
#
# Dependencies: dplyr, tibble
#
# Author:       Noel
# Last Updated: 2026-01-30
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(dplyr)
library(tibble)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
detect_sentinels <- function(values, column_name, column_type, config) {

    total_count <- length(values)
    results <- list()

    # =========================================================================
    # EXTRACT VALID (NON-NA, NON-EMPTY) VALUES FOR COMPARISON
    # =========================================================================
    trimmed <- trimws(values)
    valid_mask <- !is.na(values) & trimmed != ""
    valid_values <- trimmed[valid_mask]

    if (length(valid_values) == 0) {
        return(tibble::tibble(
            sentinel_value   = character(),
            sentinel_count   = integer(),
            sentinel_pct     = numeric(),
            detection_method = character(),
            confidence       = character()
        ))
    }

    # =========================================================================
    # CONFIG-BASED DETECTION (HIGH CONFIDENCE)
    # =========================================================================
    # Check against configured numeric and string sentinel lists.
    # Numeric sentinels are compared as strings (since raw is all TEXT).
    # String sentinels are compared case-insensitively.
    # =========================================================================
    found_config <- character()

    # Numeric sentinels
    if (column_type == "numeric") {
        for (sentinel in config$sentinel_detection$numeric_sentinels) {
            sentinel_str <- as.character(sentinel)
            count <- sum(valid_values == sentinel_str, na.rm = TRUE)
            if (count > 0) {
                results[[length(results) + 1]] <- tibble::tibble(
                    sentinel_value   = sentinel_str,
                    sentinel_count   = as.integer(count),
                    sentinel_pct     = round(count / total_count * 100, 2),
                    detection_method = "config_list",
                    confidence       = "high"
                )
                found_config <- c(found_config, sentinel_str)
            }
        }
    }

    # String sentinels (all column types)
    upper_values <- toupper(valid_values)
    for (sentinel in config$sentinel_detection$string_sentinels) {
        sentinel_upper <- toupper(sentinel)
        count <- sum(upper_values == sentinel_upper, na.rm = TRUE)
        if (count > 0 && !(sentinel_upper %in% toupper(found_config))) {
            results[[length(results) + 1]] <- tibble::tibble(
                sentinel_value   = sentinel,
                sentinel_count   = as.integer(count),
                sentinel_pct     = round(count / total_count * 100, 2),
                detection_method = "config_list",
                confidence       = "high"
            )
            found_config <- c(found_config, sentinel)
        }
    }

    # =========================================================================
    # FREQUENCY ANALYSIS (MEDIUM CONFIDENCE) — NUMERIC COLUMNS ONLY
    # =========================================================================
    # Look for repeat-digit patterns (e.g., 99, 999, 9999, 88, 77) that are
    # common sentinel values. Only flag values that appear above the minimum
    # frequency threshold and are not already found by config detection.
    # =========================================================================
    if (column_type == "numeric") {
        n_unique <- length(unique(valid_values))
        max_unique <- config$sentinel_detection$max_unique_for_detection %||% 50
        min_freq_pct <- config$sentinel_detection$min_frequency_pct %||% 1.0

        if (n_unique <= max_unique) {
            freq_table <- as.data.frame(table(valid_values), stringsAsFactors = FALSE)
            names(freq_table) <- c("value", "count")
            freq_table$pct <- freq_table$count / total_count * 100

            # Repeat-digit pattern: 99, 999, 9999, 88, 77, or negative versions
            repeat_pattern <- "^-?(\\d)\\1+$"

            for (i in seq_len(nrow(freq_table))) {
                val <- freq_table$value[i]
                pct <- freq_table$pct[i]

                if (pct >= min_freq_pct &&
                    grepl(repeat_pattern, val) &&
                    !(val %in% found_config) &&
                    !(toupper(val) %in% toupper(found_config))) {

                    results[[length(results) + 1]] <- tibble::tibble(
                        sentinel_value   = val,
                        sentinel_count   = as.integer(freq_table$count[i]),
                        sentinel_pct     = round(pct, 2),
                        detection_method = "frequency_analysis",
                        confidence       = "medium"
                    )
                }
            }
        }
    }

    # =========================================================================
    # COMBINE AND RETURN
    # =========================================================================
    if (length(results) > 0) {
        return(dplyr::bind_rows(results))
    }

    return(tibble::tibble(
        sentinel_value   = character(),
        sentinel_count   = integer(),
        sentinel_pct     = numeric(),
        detection_method = character(),
        confidence       = character()
    ))
}
