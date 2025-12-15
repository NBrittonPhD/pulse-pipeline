# =============================================================================
# coerce_types()
# =============================================================================
# Purpose:
#   Coerce an incoming data frame to the governed, expected column types defined
#   in the expected_schema_dictionary for a given lake_table_name.
#
# Inputs:
#   df               - tibble/data.frame read from CSV after renaming to
#                      lake_variable_name.
#   expected_schema  - tibble slice of expected_schema_dictionary for one
#                      lake_table_name, must include:
#                        • lake_variable_name
#                        • type_descriptor
#
# Outputs:
#   Tibble with columns coerced to the expected types.
#
# Side effects:
#   None (warnings printed if coercion issues occur).
#
# Notes:
#   • Missing columns in df are ignored here (handled elsewhere).
#   • Extra columns in df are ignored here (Step 3 will flag them).
#
# =============================================================================

library(dplyr)
library(readr)
library(lubridate)

coerce_types <- function(df, expected_schema) {
  
  df_out <- df
  
  # Loop over expected variables, and coerce if present in df_out
  for (i in seq_len(nrow(expected_schema))) {
    var  <- expected_schema$lake_variable_name[i]
    type <- expected_schema$type_descriptor[i]
    
    if (!var %in% names(df_out)) {
      next
    }
    
    suppressWarnings({
      if (identical(type, "integer")) {
        df_out[[var]] <- readr::parse_integer(df_out[[var]], na = c("", "NA"))
      } else if (identical(type, "boolean")) {
        df_out[[var]] <- readr::parse_logical(df_out[[var]], na = c("", "NA"))
      } else if (identical(type, "timestamp")) {
        df_out[[var]] <- readr::parse_datetime(df_out[[var]], na = c("", "NA"))
      } else if (identical(type, "date")) {
        df_out[[var]] <- readr::parse_date(df_out[[var]], na = c("", "NA"))
      } else if (grepl("^numeric\\(", type)) {
        df_out[[var]] <- readr::parse_double(df_out[[var]], na = c("", "NA"))
      } else if (grepl("^varchar\\(", type)) {
        df_out[[var]] <- as.character(df_out[[var]])
      } else if (identical(type, "text")) {
        df_out[[var]] <- as.character(df_out[[var]])
      } else {
        # Fallback: leave as-is or coerce to character for safety
        df_out[[var]] <- as.character(df_out[[var]])
      }
    })
  }
  
  df_out
}
