# =============================================================================
# FILE: r/utilities/scalar_helpers.R
# HELPERS: scalar-level utility functions
# =============================================================================

`%||%` <- function(x, y) {
  if (is.null(x) || is.na(x)) y else x
}

safe_pick_first_non_missing <- function(...) {
  vals <- list(...)
  for (v in vals) {
    if (!is.null(v) && !all(is.na(v))) return(v)
  }
  return(NA)
}
