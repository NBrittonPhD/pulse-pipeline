# =============================================================================
# infer_column_type
# =============================================================================
# Purpose:      Classify a TEXT column as numeric, categorical, date, or
#               identifier based on column name patterns and value content.
#
#               Because all raw.* tables store data as TEXT, this function
#               inspects actual values to determine the semantic type. The
#               priority order is:
#                 1. Identifier (name match first, then pattern match)
#                 2. Numeric (>90% of non-missing values parse as numbers)
#                 3. Date (>80% parse in common date formats)
#                 4. Categorical (default fallback)
#
# Inputs:
#   - values:       character vector of column values
#   - column_name:  character scalar, the column name
#   - config:       list from load_profiling_config() (optional)
#
# Outputs:      One of: "identifier", "numeric", "date", "categorical"
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
infer_column_type <- function(values, column_name, config = NULL) {

    # =========================================================================
    # USE DEFAULT CONFIG IF NOT PROVIDED
    # =========================================================================
    if (is.null(config)) {
        config <- list(
            identifier_columns  = c("ACCOUNTNO", "MEDRECNO", "TRAUMANO",
                                     "account_number", "mrn", "trauma_no",
                                     "cisir_id"),
            identifier_patterns = c("_id$", "_no$", "^id_",
                                     "^accountno", "^medrecno", "^traumano")
        )
    }

    col_lower <- tolower(column_name)

    # =========================================================================
    # CHECK 1: IDENTIFIER BY EXACT NAME MATCH (case-insensitive)
    # =========================================================================
    id_cols <- tolower(config$identifier_columns)
    if (col_lower %in% id_cols) {
        return("identifier")
    }

    # =========================================================================
    # CHECK 2: IDENTIFIER BY REGEX PATTERN MATCH
    # =========================================================================
    for (pattern in config$identifier_patterns) {
        if (grepl(pattern, col_lower, ignore.case = TRUE)) {
            return("identifier")
        }
    }

    # =========================================================================
    # STRIP MISSING VALUES FOR CONTENT ANALYSIS
    # =========================================================================
    # Remove NA, empty string, and whitespace-only values before attempting
    # to parse content. If nothing remains, default to categorical.
    # =========================================================================
    valid_values <- values[!is.na(values) & trimws(values) != ""]

    if (length(valid_values) == 0) {
        return("categorical")
    }

    # =========================================================================
    # CHECK 3: NUMERIC (>90% parse success)
    # =========================================================================
    parsed_numeric <- suppressWarnings(as.numeric(valid_values))
    numeric_success_rate <- sum(!is.na(parsed_numeric)) / length(valid_values)

    if (numeric_success_rate > 0.90) {
        return("numeric")
    }

    # =========================================================================
    # CHECK 4: DATE (>80% parse in any common format)
    # =========================================================================
    # Sample up to 100 values for efficiency on large columns.
    # =========================================================================
    date_formats <- c("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y",
                      "%Y/%m/%d", "%d-%b-%Y", "%Y%m%d")

    sample_values <- if (length(valid_values) > 100) {
        sample(valid_values, 100)
    } else {
        valid_values
    }

    for (fmt in date_formats) {
        parsed_dates <- suppressWarnings(as.Date(sample_values, format = fmt))
        date_success_rate <- sum(!is.na(parsed_dates)) / length(sample_values)
        if (date_success_rate > 0.80) {
            return("date")
        }
    }

    # =========================================================================
    # FALLBACK: CATEGORICAL
    # =========================================================================
    return("categorical")
}
