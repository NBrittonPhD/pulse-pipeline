# =============================================================================
# calculate_quality_score
# =============================================================================
# Purpose:      Rate a table's overall quality based on its worst missingness
#               percentage and count of critical issues. Returns one of four
#               quality levels: Excellent, Good, Fair, or Needs Review.
#
# Inputs:
#   - max_missing_pct: numeric, highest total_missing_pct among all variables
#   - critical_count:  integer, number of critical-severity issues
#   - config:          list from load_profiling_config()
#
# Outputs:      Character scalar: "Excellent", "Good", "Fair", or "Needs Review"
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
calculate_quality_score <- function(max_missing_pct, critical_count, config) {

    # =========================================================================
    # DEFENSIVE HANDLING OF NULL/NA INPUTS
    # =========================================================================
    if (is.null(max_missing_pct) || is.na(max_missing_pct)) {
        max_missing_pct <- 100
    }
    if (is.null(critical_count) || is.na(critical_count)) {
        critical_count <- 999
    }

    # =========================================================================
    # EXTRACT THRESHOLDS FROM CONFIG
    # =========================================================================
    thresholds <- config$quality_score_thresholds

    excellent_miss     <- thresholds$excellent$max_missing_pct %||% 5
    excellent_critical <- thresholds$excellent$max_critical_issues %||% 0

    good_miss          <- thresholds$good$max_missing_pct %||% 10
    good_critical      <- thresholds$good$max_critical_issues %||% 2

    fair_miss          <- thresholds$fair$max_missing_pct %||% 20
    fair_critical      <- thresholds$fair$max_critical_issues %||% 5

    # =========================================================================
    # EVALUATE IN ORDER (first match wins)
    # =========================================================================
    if (max_missing_pct <= excellent_miss && critical_count <= excellent_critical) {
        return("Excellent")
    }

    if (max_missing_pct <= good_miss && critical_count <= good_critical) {
        return("Good")
    }

    if (max_missing_pct <= fair_miss && critical_count <= fair_critical) {
        return("Fair")
    }

    return("Needs Review")
}
