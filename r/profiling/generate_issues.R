# =============================================================================
# generate_issues
# =============================================================================
# Purpose:      Evaluate profiling results for a single variable and flag
#               quality issues at the appropriate severity level.
#
#               Issue types:
#                 - identifier_missing (critical): identifier has missing values
#                 - high_missingness   (warning):  >20% missing
#                 - moderate_missingness (info):   10-20% missing
#                 - constant_value     (info):     only 1 unique value
#                 - high_cardinality   (info):     >90% unique (non-identifier)
#
# Inputs:
#   - variable_name:      character scalar
#   - table_name:         character scalar
#   - missingness_result: list from profile_missingness()
#   - column_type:        character scalar from infer_column_type()
#   - unique_count:       integer unique values among valid data
#   - total_count:        integer total row count
#   - config:             list from load_profiling_config()
#
# Outputs:      Tibble with columns: variable_name, table_name, issue_type,
#               severity, description, value, recommendation
#               (zero rows if no issues detected)
#
# Side Effects: None (pure function)
#
# Dependencies: tibble, glue
#
# Author:       Noel
# Last Updated: 2026-01-30
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(tibble)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
generate_issues <- function(variable_name, table_name, missingness_result,
                             column_type, unique_count, total_count, config) {

    issues <- list()
    miss_pct <- missingness_result$total_missing_pct

    thresholds <- config$missingness_thresholds
    critical_threshold <- thresholds$critical %||% 0
    high_threshold     <- thresholds$high %||% 20
    moderate_threshold <- thresholds$moderate %||% 10

    # =========================================================================
    # CHECK 1: IDENTIFIER MISSING (CRITICAL)
    # =========================================================================
    # Identifier columns must have zero missing values. Any missingness in
    # an identifier is a critical issue because it breaks record linkage.
    # =========================================================================
    if (column_type == "identifier" && miss_pct > critical_threshold) {
        issues[[length(issues) + 1]] <- tibble::tibble(
            variable_name  = variable_name,
            table_name     = table_name,
            issue_type     = "identifier_missing",
            severity       = "critical",
            description    = glue("Identifier column {variable_name} has {miss_pct}% missing values"),
            value          = miss_pct,
            recommendation = "Investigate source data -- identifier fields must be complete."
        )
    }

    # =========================================================================
    # CHECK 2: HIGH MISSINGNESS (WARNING)
    # =========================================================================
    if (column_type != "identifier" && miss_pct > high_threshold) {
        issues[[length(issues) + 1]] <- tibble::tibble(
            variable_name  = variable_name,
            table_name     = table_name,
            issue_type     = "high_missingness",
            severity       = "warning",
            description    = glue("{variable_name} has {miss_pct}% missing values (threshold: {high_threshold}%)"),
            value          = miss_pct,
            recommendation = "Review data source; consider imputation or exclusion."
        )
    }

    # =========================================================================
    # CHECK 3: MODERATE MISSINGNESS (INFO)
    # =========================================================================
    if (column_type != "identifier" &&
        miss_pct > moderate_threshold &&
        miss_pct <= high_threshold) {
        issues[[length(issues) + 1]] <- tibble::tibble(
            variable_name  = variable_name,
            table_name     = table_name,
            issue_type     = "moderate_missingness",
            severity       = "info",
            description    = glue("{variable_name} has {miss_pct}% missing values"),
            value          = miss_pct,
            recommendation = "Monitor; may need review before use."
        )
    }

    # =========================================================================
    # CHECK 4: CONSTANT VALUE (INFO)
    # =========================================================================
    # A column with only one distinct valid value provides no discriminating
    # information. This is not an error but worth noting.
    # =========================================================================
    if (unique_count == 1 && total_count > 0) {
        issues[[length(issues) + 1]] <- tibble::tibble(
            variable_name  = variable_name,
            table_name     = table_name,
            issue_type     = "constant_value",
            severity       = "info",
            description    = glue("{variable_name} has only one unique value across {total_count} rows"),
            value          = 1,
            recommendation = "Column provides no discriminating information."
        )
    }

    # =========================================================================
    # CHECK 5: HIGH CARDINALITY (INFO)
    # =========================================================================
    # If >90% of values are unique and the column is not an identifier, it
    # may be free-text or poorly coded. Skip for small tables (<= 10 rows).
    # =========================================================================
    if (column_type != "identifier" &&
        total_count > 10 &&
        unique_count > 0 &&
        (unique_count / total_count) > 0.90) {

        card_pct <- round(unique_count / total_count * 100, 2)
        issues[[length(issues) + 1]] <- tibble::tibble(
            variable_name  = variable_name,
            table_name     = table_name,
            issue_type     = "high_cardinality",
            severity       = "info",
            description    = glue("{variable_name} has {card_pct}% unique values ({unique_count} of {total_count})"),
            value          = card_pct,
            recommendation = "Verify this is expected; high cardinality may indicate free-text."
        )
    }

    # =========================================================================
    # COMBINE AND RETURN
    # =========================================================================
    if (length(issues) > 0) {
        return(dplyr::bind_rows(issues))
    }

    return(tibble::tibble(
        variable_name  = character(),
        table_name     = character(),
        issue_type     = character(),
        severity       = character(),
        description    = character(),
        value          = numeric(),
        recommendation = character()
    ))
}
