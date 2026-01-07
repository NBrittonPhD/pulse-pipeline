# =============================================================================
# r/action/ingest.R
# Strict Type-Enforced Single-File Ingestion for Step 2 (Robust Append Mode)
# =============================================================================
# Responsibilities:
#   - Ingest ONE file into raw.<lake_table>
#   - STRICT: source_type filter must be enforced
#   - Normalize source variable names symmetrically (file headers + dictionary)
#   - Harmonize into lake variables per ingest_dictionary
#   - Append to raw tables defensively:
#       * if table missing: create
#       * if table exists: align columns and auto-add missing table columns (TEXT)
#
# Returns (always a list):
#   status ("success" | "error")
#   lake_table
#   row_count
#   file_size_bytes
#   checksum
#   error_message (NULL or character)
# =============================================================================

library(DBI)
library(dplyr)
library(vroom)
library(glue)
library(fs)
library(readr)
library(digest)
library(stringr)

# -----------------------------------------------------------------------------
# Utility contract:
# normalize_name() must exist and be sourced by pulse-init-all.R
# Expected location:
#   r/utilities/normalize_names.R
# -----------------------------------------------------------------------------

# -------------------------------------------------------------------------
# Load ingest dictionary (reference.ingest_dictionary)
# -------------------------------------------------------------------------
get_ingest_dict <- function(con) {
  dict <- DBI::dbReadTable(
    con,
    DBI::Id(schema = "reference", table = "ingest_dictionary")
  )
  names(dict) <- tolower(names(dict))
  dict
}

# -------------------------------------------------------------------------
# Ensure lake_table is a single scalar
# -------------------------------------------------------------------------
pick_one <- function(x) {
  x <- unique(as.character(x))
  x <- x[!is.na(x) & x != ""]
  if (length(x) == 0) return(NA_character_)
  x[[1]]
}

# -------------------------------------------------------------------------
# Align df to raw table:
#  - add missing cols in df as NA
#  - if df has cols not in table, add them to table as TEXT (pragmatic mode)
#  - reorder df to match table column order (then append)
# -------------------------------------------------------------------------
align_df_to_raw_table <- function(con, lake_table, df) {
  
  tbl <- DBI::Id(schema = "raw", table = lake_table)
  
  # Create table if missing
  if (!DBI::dbExistsTable(con, tbl)) {
    DBI::dbWriteTable(con, tbl, df, overwrite = TRUE, row.names = FALSE)
    return(df)
  }
  
  table_cols <- DBI::dbListFields(con, tbl)
  df_cols    <- names(df)
  
  # Columns present in df but not in table -> add to table (TEXT)
  extra_in_df <- setdiff(df_cols, table_cols)
  if (length(extra_in_df) > 0) {
    for (col in extra_in_df) {
      DBI::dbExecute(
        con,
        glue(
          "ALTER TABLE raw.{DBI::dbQuoteIdentifier(con, lake_table)} ",
          "ADD COLUMN IF NOT EXISTS {DBI::dbQuoteIdentifier(con, col)} TEXT"
        )
      )
    }
    # refresh table cols after ALTERs
    table_cols <- DBI::dbListFields(con, tbl)
  }
  
  # Columns present in table but missing in df -> add NA columns
  missing_in_df <- setdiff(table_cols, df_cols)
  if (length(missing_in_df) > 0) {
    df[missing_in_df] <- NA_character_
  }
  
  # Drop anything still not in table (should be none, but defensive)
  df <- df[, intersect(names(df), table_cols), drop = FALSE]
  
  # Reorder to table column order
  df <- df[, table_cols, drop = FALSE]
  
  df
}

# =============================================================================
# ingest_one_file()
# =============================================================================
ingest_one_file <- function(con, file_path, source_type) {
  
  # -----------------------------
  # Identify file
  # -----------------------------
  fname      <- basename(file_path)
  fname_base <- tolower(sub("\\.csv$", "", fname))
  st         <- tolower(trimws(source_type))
  
  # -----------------------------
  # Load dictionary
  # -----------------------------
  dict <- get_ingest_dict(con)
  
  # Normalize dictionary fields we match on
  dict <- dict %>%
    mutate(
      source_type          = tolower(trimws(source_type)),
      source_table_name    = tolower(trimws(source_table_name)),
      source_variable_name = as.character(source_variable_name),
      lake_table_name      = tolower(trimws(lake_table_name)),
      lake_variable_name   = tolower(trimws(lake_variable_name))
    )
  
  # STRICT filter by source_type
  dict_st <- dict %>% filter(source_type == st)
  
  if (nrow(dict_st) == 0) {
    return(list(
      status          = "error",
      lake_table      = NA_character_,
      row_count       = NA_integer_,
      file_size_bytes = NA_integer_,
      checksum        = NA_character_,
      error_message   = glue("No ingest_dictionary rows found for source_type='{source_type}'.")
    ))
  }
  
  # -----------------------------
  # Determine lake_table
  # -----------------------------
  lk <- dict_st %>%
    filter(source_table_name == fname_base) %>%
    pull(lake_table_name)
  
  lake_table <- pick_one(lk)
  
  # Optional wildcard for TR labs_YYYY pattern (kept, but only if you still use it)
  lab_year <- NA_integer_
  if (is.na(lake_table) && startsWith(fname_base, "labs_")) {
    lake_table <- "labs"
    lab_year <- suppressWarnings(as.integer(sub("^labs_", "", fname_base)))
  }
  
  if (is.na(lake_table)) {
    return(list(
      status          = "error",
      lake_table      = NA_character_,
      row_count       = NA_integer_,
      file_size_bytes = NA_integer_,
      checksum        = NA_character_,
      error_message   = glue("No lake_table match for file '{fname}' under source_type='{source_type}'.")
    ))
  }
  
  # -----------------------------
  # Load file
  # -----------------------------
  df_raw <- tryCatch(
    {
      vroom::vroom(
        file_path,
        delim        = ",",
        col_types    = vroom::cols(.default = "c"),
        .name_repair = "minimal",
        progress     = FALSE
      )
      # delim = "," is REQUIRED to prevent vroom's automatic delimiter detection
      # from incorrectly guessing space as the delimiter for CSVs with spaces in headers
    },
    error = function(e) e
  )
  
  if (inherits(df_raw, "error")) {
    return(list(
      status          = "error",
      lake_table      = lake_table,
      row_count       = NA_integer_,
      file_size_bytes = NA_integer_,
      checksum        = NA_character_,
      error_message   = paste0("vroom failed: ", conditionMessage(df_raw))
    ))
  }
  
  # -----------------------------
  # Normalize headers for safe matching
  # -----------------------------
  # normalize_name must be applied symmetrically:
  #  - to file headers
  #  - to dictionary source_variable_name
  names(df_raw) <- normalize_name(names(df_raw))
  
  # -----------------------------
  # Build mapping for THIS lake table
  # -----------------------------
  m <- dict_st %>%
    filter(lake_table_name == lake_table) %>%
    mutate(
      src_norm  = normalize_name(source_variable_name),
      lake_norm = normalize_name(lake_variable_name)
    ) %>%
    select(src_norm, lake_variable_name) %>%
    distinct()
  
  if (nrow(m) == 0) {
    return(list(
      status          = "error",
      lake_table      = lake_table,
      row_count       = NA_integer_,
      file_size_bytes = NA_integer_,
      checksum        = NA_character_,
      error_message   = glue("No variable mappings found for lake_table='{lake_table}' and source_type='{source_type}'.")
    ))
  }
  
  # Map normalized source headers -> lake variable names
  rename_map <- setNames(m$lake_variable_name, m$src_norm)
  
  # Harmonize
  df <- df_raw %>%
    rename(any_of(rename_map))
  
  keep_cols <- unique(m$lake_variable_name)
  
  # Add any missing lake variables
  missing_cols <- setdiff(keep_cols, names(df))
  if (length(missing_cols) > 0) df[missing_cols] <- NA_character_
  
  # Drop extras + order columns to keep_cols (later we align to raw table order)
  df <- df %>% select(all_of(keep_cols))
  
  # Add lab_year if needed
  if (lake_table == "labs") {
    df$lab_year <- as.character(lab_year)
  }
  
  # -----------------------------
  # Append to raw.<lake_table> (robust)
  # -----------------------------
  df_aligned <- tryCatch(
    {
      align_df_to_raw_table(con, lake_table, df)
    },
    error = function(e) e
  )
  
  if (inherits(df_aligned, "error")) {
    return(list(
      status          = "error",
      lake_table      = lake_table,
      row_count       = NA_integer_,
      file_size_bytes = NA_integer_,
      checksum        = NA_character_,
      error_message   = paste0("align_df_to_raw_table failed: ", conditionMessage(df_aligned))
    ))
  }
  
  ok <- tryCatch(
    {
      DBI::dbWriteTable(
        con,
        DBI::Id(schema = "raw", table = lake_table),
        df_aligned,
        append    = TRUE,
        row.names = FALSE
      )
      TRUE
    },
    error = function(e) {
      message("!! dbWriteTable failed for raw.", lake_table, ": ", conditionMessage(e))
      FALSE
    }
  )
  
  if (!ok) {
    return(list(
      status          = "error",
      lake_table      = lake_table,
      row_count       = NA_integer_,
      file_size_bytes = NA_integer_,
      checksum        = NA_character_,
      error_message   = glue("dbWriteTable append failed for raw.{lake_table} (see console for DB error).")
    ))
  }
  
  # -----------------------------
  # Success metadata
  # -----------------------------
  size_bytes <- file.info(file_path)$size
  checksum   <- digest(file = file_path, algo = "md5")
  row_count  <- nrow(df_aligned)
  
  list(
    status          = "success",
    lake_table      = lake_table,
    row_count       = row_count,
    file_size_bytes = size_bytes,
    checksum        = checksum,
    error_message   = NULL
  )
}
