# r/reference/create_raw_tables_from_ingest_dict.R

create_raw_tables_from_ingest_dict <- function(con) {
  
  dict <- dbReadTable(con, DBI::Id(schema = "reference", table = "ingest_dictionary"))
  
  dict <- dict %>%
    mutate(
      lake_table_name = tolower(lake_table_name),
      lake_variable_name = tolower(lake_variable_name)
    )
  
  # Group by table
  tables <- dict %>%
    group_by(lake_table_name) %>%
    summarise(cols = list(lake_variable_name))
  
  walk(seq_len(nrow(tables)), function(i) {
    
    tbl <- tables$lake_table_name[i]
    cols <- tables$cols[[i]]
    
    message("Rebuilding raw.", tbl)
    
    # Drop if exists
    dbExecute(con, glue::glue("DROP TABLE IF EXISTS raw.{tbl} CASCADE;"))
    
    col_defs <- paste0(cols, " TEXT", collapse = ",\n  ")
    
    create_sql <- glue::glue("
      CREATE TABLE raw.{tbl} (
        {col_defs}
      );
    ")
    
    dbExecute(con, create_sql)
  })
  
  message("All raw tables rebuilt from ingest_dictionary.")
}