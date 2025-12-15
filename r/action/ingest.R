# =============================================================================
# r/action/ingest.R
# Strict, Type-Enforced Single-File Ingestion for Step 2
# =============================================================================
# INGEST ONE FILE ONLY.
# No logging. No governance. No batch logic.
# Step 2 handles logging; this performs raw ingestion + type enforcement.
#
# Returns a list:
#   status            ("success" | "error")
#   lake_table        character
#   row_count         integer
#   file_size_bytes   integer
#   checksum          sha256 string
#
# =============================================================================

library(DBI)
library(dplyr)
library(vroom)
library(glue)
library(fs)
library(readr)
library(digest)
library(stringr)
library(readxl)

# Load utilities
source("r/utilities/scalar_helpers.R")
source("r/utilities/coerce_types.R")

# -------------------------------------------------------------------------
# Load ingest dictionary (from reference schema)
# -------------------------------------------------------------------------
get_ingest_dict <- function(con) {
  dict <- DBI::dbReadTable(con, DBI::Id(schema = "reference", table = "ingest_dictionary"))
  names(dict) <- tolower(names(dict))
  
  dict$source_type         <- tolower(dict$source_type)
  dict$source_table_name   <- tolower(dict$source_table_name)
  dict$lake_table_name     <- tolower(dict$lake_table_name)
  dict$lake_variable_name  <- tolower(dict$lake_variable_name)
  dict$source_variable_name <- tolower(dict$source_variable_name)
  
  dict
}

# -------------------------------------------------------------------------
# Resolve lake_table_name with strict source_type enforcement
# -------------------------------------------------------------------------
infer_lake_table <- function(fname_base, dict_st) {
  
  # Direct matches
  lk <- dict_st %>%
    filter(source_table_name == fname_base) %>%
    distinct(lake_table_name) %>%
    pull(lake_table_name)
  
  # Ambiguous mapping
  if (length(lk) > 1) {
    stop(glue(
      "ERROR: Multiple lake_table_name values found for '{fname_base}' under this source_type.\n",
      "Values: {paste(lk, collapse = ', ')}"
    ))
  }
  
  # Wildcard: labs_YYYY → labs
  lab_year <- NA_integer_
  if (length(lk) == 0 && startsWith(fname_base, "labs_")) {
    lk <- "labs"
    lab_year <- suppressWarnings(as.integer(sub("^labs_", "", fname_base)))
  }
  
  # No match found
  if (length(lk) == 0) lk <- NA_character_
  
  list(
    lake_table = lk,
    lab_year   = lab_year
  )
}

# =============================================================================
# ingest_one_file()
# =============================================================================
ingest_one_file <- function(con, file_path, source_type) {
  
  fname       <- basename(file_path)
  fname_base  <- tolower(sub("\\.csv$", "", fname))
  st          <- tolower(source_type)
  
  # ---------------------------------------------------------------------------
  # Load ingest_dictionary subset for this source_type
  # ---------------------------------------------------------------------------
  dict <- get_ingest_dict(con)
  dict_st <- dict %>% filter(source_type == st)
  
  # ---------------------------------------------------------------------------
  # Determine lake table (strict)
  # ---------------------------------------------------------------------------
  mapping <- infer_lake_table(fname_base, dict_st)
  lake_table <- mapping$lake_table
  lab_year   <- mapping$lab_year
  
  if (is.na(lake_table)) {
    return(list(
      status           = "error",
      lake_table       = NA_character_,
      row_count        = NA_integer_,
      file_size_bytes  = NA_integer_,
      checksum         = NA_character_
    ))
  }
  
  # ---------------------------------------------------------------------------
  # Read CSV safely (all columns as character for deterministic typing)
  # ---------------------------------------------------------------------------
  df <- tryCatch(
    vroom::vroom(
      file_path,
      col_types = vroom::cols(.default = "c"),
      .name_repair = "minimal",
      progress = FALSE
    ),
    error = function(e) NULL
  )
  
  if (is.null(df)) {
    return(list(
      status           = "error",
      lake_table       = lake_table,
      row_count        = NA_integer_,
      file_size_bytes  = NA_integer_,
      checksum         = NA_character_
    ))
  }
  
  names(df) <- tolower(names(df))
  
  # ---------------------------------------------------------------------------
  # Harmonize to lake_variable_name using dictionary subset
  # ---------------------------------------------------------------------------
  m <- dict_st %>% filter(lake_table_name == lake_table)
  
  rename_map <- setNames(m$lake_variable_name, m$source_variable_name)
  keep_cols  <- unique(m$lake_variable_name)
  
  df <- df %>% rename(any_of(rename_map))
  
  # Missing expected columns → NA
  missing_cols <- setdiff(keep_cols, names(df))
  if (length(missing_cols) > 0) {
    df[missing_cols] <- NA_character_
  }
  
  df <- df %>% select(all_of(keep_cols))
  
  # ---------------------------------------------------------------------------
  # Add lab_year if applicable
  # ---------------------------------------------------------------------------
  if (identical(lake_table, "labs")) {
    df$lab_year <- lab_year
  }
  
  # ---------------------------------------------------------------------------
  # Load expected schema for type enforcement
  # ---------------------------------------------------------------------------
  expected_schema <- readxl::read_excel("reference/expected_schema_dictionary.xlsx") %>%
    as_tibble() %>%
    filter(lake_table_name == lake_table) %>%
    select(lake_variable_name, type_descriptor)
  
  # ---------------------------------------------------------------------------
  # Coerce column types according to expected_schema_dictionary
  # ---------------------------------------------------------------------------
  df <- coerce_types(df, expected_schema)
  
  row_count <- nrow(df)
  file_size_bytes <- file.info(file_path)$size %||% 0L
  checksum <- digest(file = file_path, algo = "sha256")
  
  # ---------------------------------------------------------------------------
  # Write into RAW zone (create table with correct structure if needed)
  # ---------------------------------------------------------------------------
  full_table_name <- DBI::Id(schema = "raw", table = lake_table)
  
  if (!DBI::dbExistsTable(con, full_table_name)) {
    # Create typed table based on the first record’s structure
    DBI::dbCreateTable(con, full_table_name, df[0, , drop = FALSE])
  }
  
  ok <- tryCatch(
    {
      DBI::dbWriteTable(
        con,
        full_table_name,
        df,
        append = TRUE,
        row.names = FALSE
      )
      TRUE
    },
    error = function(e) FALSE
  )
  
  if (!ok) {
    return(list(
      status           = "error",
      lake_table       = lake_table,
      row_count        = NA_integer_,
      file_size_bytes  = file_size_bytes,
      checksum         = checksum
    ))
  }
  
  # ---------------------------------------------------------------------------
  # Success
  # ---------------------------------------------------------------------------
  list(
    status           = "success",
    lake_table       = lake_table,
    row_count        = row_count,
    file_size_bytes  = file_size_bytes,
    checksum         = checksum
  )
}
