# =============================================================================
# profile_missingness
# =============================================================================
# Purpose:      Classify every value in a column into exactly one of five
#               mutually exclusive categories:
#                 1. NA (R NA)
#                 2. Empty string ("")
#                 3. Whitespace-only (e.g., "  ", "\t")
#                 4. Sentinel (matches known placeholder values)
#                 5. Valid (everything else)
#
#               Returns counts and percentages for each category plus
#               unique value count among valid values.
#
# Inputs:
#   - values:           character vector of column values
#   - sentinel_values:  character vector of sentinel strings to match
#                       (from detect_sentinels output; may be empty)
#
# Outputs:      Named list with: total_count, valid_count, na_count,
#               empty_count, whitespace_count, sentinel_count,
#               na_pct, empty_pct, whitespace_pct, sentinel_pct,
#               total_missing_count, total_missing_pct, valid_pct,
#               unique_count, unique_pct
#
# Side Effects: None (pure function)
#
# Dependencies: None (base R only)
#
# Author:       Noel
# Last Updated: 2026-01-30
# =============================================================================

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
profile_missingness <- function(values, sentinel_values = character(0)) {

    total_count <- length(values)

    # =========================================================================
    # MUTUALLY EXCLUSIVE CLASSIFICATION
    # =========================================================================
    # Each value is assigned to exactly one category. The order matters:
    # NA check first, then empty, then whitespace, then sentinel, then valid.
    # =========================================================================
    is_na         <- is.na(values)
    is_empty      <- !is_na & (values == "")
    is_whitespace <- !is_na & !is_empty & grepl("^\\s+$", values)

    # Sentinel comparison: case-insensitive, trimmed
    if (length(sentinel_values) > 0) {
        sentinel_upper <- toupper(sentinel_values)
        is_sentinel <- !is_na & !is_empty & !is_whitespace &
            (toupper(trimws(values)) %in% sentinel_upper)
    } else {
        is_sentinel <- rep(FALSE, total_count)
    }

    is_valid <- !is_na & !is_empty & !is_whitespace & !is_sentinel

    # =========================================================================
    # COUNT EACH CATEGORY
    # =========================================================================
    na_count         <- sum(is_na)
    empty_count      <- sum(is_empty)
    whitespace_count <- sum(is_whitespace)
    sentinel_count   <- sum(is_sentinel)
    valid_count      <- sum(is_valid)

    # =========================================================================
    # COMPUTE PERCENTAGES
    # =========================================================================
    safe_pct <- function(count) {
        if (total_count == 0) return(0)
        round(count / total_count * 100, 2)
    }

    na_pct            <- safe_pct(na_count)
    empty_pct         <- safe_pct(empty_count)
    whitespace_pct    <- safe_pct(whitespace_count)
    sentinel_pct      <- safe_pct(sentinel_count)
    valid_pct         <- safe_pct(valid_count)

    total_missing_count <- na_count + empty_count + whitespace_count + sentinel_count
    total_missing_pct   <- safe_pct(total_missing_count)

    # =========================================================================
    # UNIQUE COUNT AMONG VALID VALUES
    # =========================================================================
    unique_count <- length(unique(values[is_valid]))
    unique_pct   <- if (valid_count > 0) {
        round(unique_count / valid_count * 100, 2)
    } else {
        0
    }

    # =========================================================================
    # RETURN
    # =========================================================================
    return(list(
        total_count         = total_count,
        valid_count         = valid_count,
        na_count            = na_count,
        empty_count         = empty_count,
        whitespace_count    = whitespace_count,
        sentinel_count      = sentinel_count,
        na_pct              = na_pct,
        empty_pct           = empty_pct,
        whitespace_pct      = whitespace_pct,
        sentinel_pct        = sentinel_pct,
        total_missing_count = total_missing_count,
        total_missing_pct   = total_missing_pct,
        valid_pct           = valid_pct,
        unique_count        = unique_count,
        unique_pct          = unique_pct
    ))
}
