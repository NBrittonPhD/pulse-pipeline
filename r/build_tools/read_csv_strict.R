# =============================================================================
# r/build_tools/read_csv_strict.R
# Utility — robust CSV reader with header fallback
# =============================================================================
# Purpose:
#   Read CSVs safely when header rows are malformed (wrapped, merged, quoted chunks).
#
# Strategy:
#   1) Try normal header read.
#   2) If headers don't match expected dictionary variables, re-read with col_names=FALSE
#      and find a skip value that yields the expected column count.
#   3) Assign expected source-variable column names (normalized) in dictionary order.
# =============================================================================

read_csv_strict <- function(file_path, expected_source_vars_norm, max_skip = 10) {
  
  # --- Attempt 1: normal read (with header) ---
  df <- tryCatch(
    {
      vroom::vroom(
        file_path,
        col_types = vroom::cols(.default = "c"),
        .name_repair = "minimal",
        progress = FALSE
      )
    },
    error = function(e) NULL
  )
  
  if (!is.null(df)) {
    hdr_norm <- normalize_name(names(df))
    names(df) <- hdr_norm
    
    # If we have overlap with expected headers, accept this parse
    if (any(expected_source_vars_norm %in% hdr_norm)) {
      return(df)
    }
  }
  
  # --- Attempt 2: brute-force skip search with col_names=FALSE ---
  target_n <- length(expected_source_vars_norm)
  
  for (s in 0:max_skip) {
    df2 <- tryCatch(
      {
        readr::read_csv(
          file_path,
          col_names = FALSE,
          skip = s,
          col_types = readr::cols(.default = readr::col_character()),
          n_max = Inf,
          show_col_types = FALSE,
          progress = FALSE
        )
      },
      error = function(e) NULL
    )
    
    if (!is.null(df2) && ncol(df2) == target_n) {
      names(df2) <- expected_source_vars_norm
      return(df2)
    }
  }
  
  # If we got here, we failed to parse deterministically
  NULL
}
