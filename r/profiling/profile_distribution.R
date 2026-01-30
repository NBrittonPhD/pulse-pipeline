# =============================================================================
# profile_distribution
# =============================================================================
# Purpose:      Compute distribution statistics appropriate to the inferred
#               column type. Numeric columns get min/max/mean/median/sd/
#               quartiles. All other types get a frequency table with top-N
#               values as JSON and mode statistics.
#
#               Values classified as NA, empty, whitespace, or sentinel are
#               excluded before computing statistics.
#
# Inputs:
#   - values:           character vector of column values
#   - column_type:      character scalar from infer_column_type()
#   - sentinel_values:  character vector of sentinel strings (may be empty)
#   - config:           list from load_profiling_config()
#
# Outputs:      Named list with: distribution_type, stat_min, stat_max,
#               stat_mean, stat_median, stat_sd, stat_q25, stat_q75,
#               stat_iqr, top_values_json, mode_value, mode_count, mode_pct
#
# Side Effects: None (pure function)
#
# Dependencies: jsonlite
#
# Author:       Noel
# Last Updated: 2026-01-30
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(jsonlite)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
profile_distribution <- function(values, column_type,
                                  sentinel_values = character(0),
                                  config = list()) {

    dp <- config$display$decimal_places %||% 2
    top_n <- config$display$top_n_categories %||% 15

    # =========================================================================
    # EMPTY RESULT TEMPLATE
    # =========================================================================
    empty_result <- list(
        distribution_type = NA_character_,
        stat_min          = NA_real_,
        stat_max          = NA_real_,
        stat_mean         = NA_real_,
        stat_median       = NA_real_,
        stat_sd           = NA_real_,
        stat_q25          = NA_real_,
        stat_q75          = NA_real_,
        stat_iqr          = NA_real_,
        top_values_json   = NA_character_,
        mode_value        = NA_character_,
        mode_count        = NA_integer_,
        mode_pct          = NA_real_
    )

    # =========================================================================
    # FILTER TO VALID VALUES
    # =========================================================================
    # Remove NA, empty, whitespace, and sentinel values — same logic as
    # profile_missingness's "valid" category.
    # =========================================================================
    is_na         <- is.na(values)
    is_empty      <- !is_na & (values == "")
    is_whitespace <- !is_na & !is_empty & grepl("^\\s+$", values)

    if (length(sentinel_values) > 0) {
        sentinel_upper <- toupper(sentinel_values)
        is_sentinel <- !is_na & !is_empty & !is_whitespace &
            (toupper(trimws(values)) %in% sentinel_upper)
    } else {
        is_sentinel <- rep(FALSE, length(values))
    }

    valid_values <- values[!is_na & !is_empty & !is_whitespace & !is_sentinel]

    if (length(valid_values) == 0) {
        return(empty_result)
    }

    # =========================================================================
    # NUMERIC PATH
    # =========================================================================
    if (column_type == "numeric") {
        parsed <- suppressWarnings(as.numeric(valid_values))
        numeric_vals <- parsed[!is.na(parsed)]

        if (length(numeric_vals) == 0) {
            return(empty_result)
        }

        q <- quantile(numeric_vals, probs = c(0.25, 0.75), na.rm = TRUE)

        # Mode: most frequent value (as string for consistency)
        freq <- sort(table(valid_values), decreasing = TRUE)
        mode_val  <- names(freq)[1]
        mode_cnt  <- as.integer(freq[1])
        mode_p    <- round(mode_cnt / length(valid_values) * 100, dp)

        return(list(
            distribution_type = "numeric",
            stat_min          = round(min(numeric_vals, na.rm = TRUE), dp),
            stat_max          = round(max(numeric_vals, na.rm = TRUE), dp),
            stat_mean         = round(mean(numeric_vals, na.rm = TRUE), dp),
            stat_median       = round(median(numeric_vals, na.rm = TRUE), dp),
            stat_sd           = round(sd(numeric_vals, na.rm = TRUE), dp),
            stat_q25          = round(as.numeric(q[1]), dp),
            stat_q75          = round(as.numeric(q[2]), dp),
            stat_iqr          = round(as.numeric(q[2] - q[1]), dp),
            top_values_json   = NA_character_,
            mode_value        = mode_val,
            mode_count        = mode_cnt,
            mode_pct          = mode_p
        ))
    }

    # =========================================================================
    # CATEGORICAL / DATE / IDENTIFIER PATH
    # =========================================================================
    freq <- sort(table(valid_values), decreasing = TRUE)

    # Top N
    top_entries <- head(freq, top_n)
    top_df <- data.frame(
        value = names(top_entries),
        count = as.integer(top_entries),
        pct   = round(as.integer(top_entries) / length(valid_values) * 100, dp),
        stringsAsFactors = FALSE
    )
    top_json <- jsonlite::toJSON(top_df, auto_unbox = TRUE)

    # Mode
    mode_val  <- names(freq)[1]
    mode_cnt  <- as.integer(freq[1])
    mode_p    <- round(mode_cnt / length(valid_values) * 100, dp)

    return(list(
        distribution_type = "categorical",
        stat_min          = NA_real_,
        stat_max          = NA_real_,
        stat_mean         = NA_real_,
        stat_median       = NA_real_,
        stat_sd           = NA_real_,
        stat_q25          = NA_real_,
        stat_q75          = NA_real_,
        stat_iqr          = NA_real_,
        top_values_json   = as.character(top_json),
        mode_value        = mode_val,
        mode_count        = mode_cnt,
        mode_pct          = mode_p
    ))
}
