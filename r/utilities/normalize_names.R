# =============================================================================
# normalize_name()
#
# Canonical name normalization for ALL name matching in PULSE.
#
# Used for:
#   - ingest_dictionary fields (source_table_name, source_variable_name)
#   - CSV column names
#   - schema validation comparisons (Step 3)
#   - QC rule targeting
#
# Any code that MATCHES names MUST use this function.
# Canonical lake table and variable names should already be normalized.
#
# =============================================================================

normalize_name <- function(x) {
  x %>%
    tolower() %>%
    stringr::str_trim() %>%
    
    # ---- semantic unit normalization (BEFORE generic cleanup) ----
  stringr::str_replace_all("\\(y/n\\)", "yn") %>%
    stringr::str_replace_all("\\(minutes\\)", "minutes") %>%
    stringr::str_replace_all("\\(min\\)", "min") %>%
    stringr::str_replace_all("\\(cm\\)", "cm") %>%
    stringr::str_replace_all("\\(kg\\)", "kg") %>%
    
    # ---- generic normalization ----
  stringr::str_replace_all("[^a-z0-9]+", "_") %>%
    stringr::str_replace_all("^_+|_+$", "") %>%
    stringr::str_replace_all("_+", "_")
}
