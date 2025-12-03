# r/action/ingest.R

library(DBI)
library(dplyr)
library(glue)
library(vroom)
library(fs)
library(stringr)
library(purrr)
library(readr)

# -------------------------------------------------------------------
# Helper: load the ingest dictionary (reference.ingest_dictionary)
# -------------------------------------------------------------------
get_ingest_dict <- function(con) {
  dbReadTable(con, DBI::Id(schema = "reference", table = "ingest_dictionary"))
}

# -------------------------------------------------------------------
# Main ingestion function
# -------------------------------------------------------------------
ingest <- function(source_id, raw_path, con) {
  
  if (!dir_exists(raw_path)) {
    stop(glue("The raw_path does not exist: {raw_path}"))
  }
  
  # Build an ingest_id
  ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
  ingest_id <- glue("ING_{source_id}_{ts}")
  message("Starting ingestion: ", ingest_id)
  
  # Load ingest dictionary
  dict <- get_ingest_dict(con) %>%
    mutate(across(everything(), tolower))
  
  # All files in the raw drop
  files <- dir_ls(raw_path, regexp = "\\.csv$", recurse = FALSE)
  
  # Map source_table_name → lake_table_name
  file_lookup <- dict %>%
    distinct(source_table_name, lake_table_name)
  
  # -------------------------------------------------------------------
  # Loop through files
  # -------------------------------------------------------------------
  walk(files, function(fpath) {
    
    fname <- tolower(path_file(fpath))           # e.g. labs_2019.csv
    base  <- str_remove(fname, "\\.csv$")        # labs_2019
    
    # --------------------------------------------------------------
    # Determine lake_table via dictionary OR wildcard
    # --------------------------------------------------------------
    lake_table <- file_lookup %>%
      filter(source_table_name == base) %>%
      pull(lake_table_name)
    
    # Wildcard for labs_YYYY
    if (length(lake_table) == 0 && startsWith(base, "labs_")) {
      lake_table <- "labs"
    }
    
    # If still nothing matched, skip
    if (length(lake_table) == 0) {
      message("Skipping unrecognized file: ", fname)
      return(NULL)
    }
    
    message("Appending ", fname, " → raw.", lake_table)
    
    # --------------------------------------------------------------
    # Load data as character (safe for ingestion)
    # --------------------------------------------------------------
    df <- vroom::vroom(
      fpath,
      col_types = vroom::cols(.default = "c"),
      .name_repair = "minimal",
      progress = FALSE
    )
    
    names(df) <- tolower(names(df))
    
    # --------------------------------------------------------------
    # Harmonize column names based on ingest_dictionary
    # --------------------------------------------------------------
    m <- dict %>% filter(lake_table_name == lake_table)
    
    rename_map <- setNames(m$lake_variable_name, m$source_variable_name)
    
    df <- df %>% rename(any_of(rename_map))
    
    # Keep only the lake-defined variables
    keep_cols <- unique(m$lake_variable_name)
    df <- df %>% select(any_of(keep_cols))
    
    # Fill missing lake-vars with NA
    missing <- setdiff(keep_cols, names(df))
    if (length(missing) > 0) {
      df[missing] <- NA
    }
    
    df <- df %>% select(all_of(keep_cols))
    
    # --------------------------------------------------------------
    # Append to raw.<lake_table>
    # --------------------------------------------------------------
    DBI::dbWriteTable(
      con,
      DBI::Id(schema = "raw", table = lake_table),
      df,
      append = TRUE,
      row.names = FALSE
    )
  })
  
  message("Ingestion completed: ", ingest_id)
  return(ingest_id)
}