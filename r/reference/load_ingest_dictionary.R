# r/reference/load_ingest_dictionary.R
library(DBI)
library(readxl)
library(janitor)
library(dplyr)
library(readr)

load_ingest_dictionary <- function(con) {
  
  message("Refreshing reference.ingest_dictionary from ingest_dictionary.xlsx …")
  
  dict_path <- "~/Documents/PULSE/pulse-pipeline/ingest_dictionary.xlsx"
  
  # -------------------------------------------------------------------
  # 1. Read Excel, normalize, append metadata
  # -------------------------------------------------------------------
  df <- readxl::read_excel(dict_path) %>%
    janitor::clean_names() %>%
    mutate(across(everything(), ~ tolower(as.character(.)))) %>%
    mutate(
      created_at_utc = Sys.time(),
      last_modified_utc = Sys.time(),
      created_by = Sys.getenv("USER")
    )
  
  # -------------------------------------------------------------------
  # 2. Remove duplicate key pairs BEFORE loading
  # -------------------------------------------------------------------
  df_dedup <- df %>%
    distinct(source_table_name, source_variable_name, .keep_all = TRUE)
  
  dup_count <- nrow(df) - nrow(df_dedup)
  
  if (dup_count > 0) {
    message("Deduplication: Removed ", dup_count,
            " duplicate rows based on (source_table_name, source_variable_name).")
  }
  
  # -------------------------------------------------------------------
  # 3. Replace table contents in a transaction
  # -------------------------------------------------------------------
  DBI::dbWithTransaction(con, {
    
    # Clear existing terms
    DBI::dbExecute(con, "TRUNCATE TABLE reference.ingest_dictionary;")
    
    # Upload safely without COPY
    DBI::dbWriteTable(
      con,
      DBI::Id(schema = "reference", table = "ingest_dictionary"),
      df_dedup,
      append = TRUE,
      row.names = FALSE
    )
  })
  
  message("reference.ingest_dictionary updated successfully.")
  
  return(invisible(TRUE))
}