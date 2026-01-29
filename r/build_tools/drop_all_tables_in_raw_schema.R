# =============================================================================
# DROP ALL TABLES IN RAW SCHEMA
# =============================================================================
# Purpose:
#   Completely remove all tables from the 'raw' schema so that the pipeline can
#   start fresh with clean ingestion tests and properly governed naming.
#
# Inputs:
#   con - active DB connection to PULSE Postgres
#
# Outputs:
#   All raw.<table> objects will be dropped
#
# Side Effects:
#   Raw data removed permanently
#
# =============================================================================

library(DBI)
library(dplyr)

drop_raw_schema_tables <- function(con) {
  
  # Get all raw schema tables
  tbls <- dbGetQuery(
    con,
    "
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'raw';
    "
  ) %>% dplyr::pull(table_name)
  
  if (length(tbls) == 0) {
    message(">> No tables found in raw schema.")
    return(invisible(TRUE))
  }
  
  message(">> Dropping raw tables:")
  print(tbls)
  
  # Drop each table
  for (tbl in tbls) {
    sql <- sprintf('DROP TABLE IF EXISTS raw.%s CASCADE;', tbl)
    message("   - ", sql)
    dbExecute(con, sql)
  }
  
  message(">> Raw schema successfully cleared.")
  invisible(TRUE)
}

# =============================================================================
# Example use:
# =============================================================================
# con <- connect_to_pulse()
# drop_raw_schema_tables(con)
