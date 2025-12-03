library(readxl)
library(dplyr)
library(glue)
library(purrr)
library(DBI)

create_raw_tables <- function(con, metadata_path = "CORE_METADATA_DICTIONARY.xlsx") {
  
  meta <- readxl::read_excel(metadata_path)
  
  tables <- unique(meta$table_name)
  
  walk(tables, function(tbl) {
    
    cols <- meta %>%
      filter(table_name == tbl) %>%
      pull(variable_name)
    
    # Construct column definitions as TEXT
    col_defs <- paste0(cols, " TEXT", collapse = ",\n    ")
    
    sql <- glue("
      CREATE TABLE IF NOT EXISTS raw.{tbl} (
        {col_defs}
      );
    ")
    
    message("Creating raw table: raw.", tbl)
    dbExecute(con, sql)
  })
  
  invisible(TRUE)
}
