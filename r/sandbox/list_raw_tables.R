# =============================================================================
# list_raw_tables()
# =============================================================================
# Purpose:
#   Retrieve all tables under the 'raw' schema and map them to the
#   expected source_type based on the ingest_dictionary metadata.
#
#   This is essential for verifying naming conventions, ensuring tables
#   align with governed lake_table_name patterns, and identifying tables
#   that require renaming before schema validation (Step 3).
#
# Inputs:
#   con  - A valid Postgres DBI connection (connect_to_pulse()).
#
# Outputs:
#   tibble with:
#       table_schema
#       table_name
#       full_name (schema.table)
#       lake_table_name (same as table_name)
#       source_type (from ingest_dictionary)
#       source_match (TRUE/FALSE)
#
# Side effects:
#   None.
#
# Author:
#   Noel / PRIME-AI PULSE
#
# Revision:
#   2025-12-11
# =============================================================================

library(DBI)
library(dplyr)

list_raw_tables <- function(con) {

  # ---------------------------------------------------------------------------
  # Load ingest dictionary from database (authoritative mapping: lake_table_name → source_type)
  # ---------------------------------------------------------------------------
  ingest_dict <- DBI::dbReadTable(
    con,
    DBI::Id(schema = "reference", table = "ingest_dictionary")
  )
  names(ingest_dict) <- tolower(names(ingest_dict))

  ingest_dict <- ingest_dict %>%
    dplyr::select(
      lake_table_name,
      source_type
    ) %>%
    dplyr::distinct() %>%
    dplyr::mutate(
      lake_table_name = tolower(lake_table_name)
    )
  
  # ---------------------------------------------------------------------------
  # Get raw schema tables from Postgres
  # ---------------------------------------------------------------------------
  raw_tables <- dbGetQuery(
    con,
    "
    SELECT 
      table_schema,
      table_name
    FROM information_schema.tables
    WHERE table_schema = 'raw'
    ORDER BY table_name;
    "
  ) %>%
    dplyr::as_tibble() %>%
    dplyr::mutate(
      lake_table_name = tolower(table_name),
      full_name = paste0(table_schema, '.', table_name)
    )
  
  # ---------------------------------------------------------------------------
  # Join raw table list to ingest metadata to identify source_type
  # ---------------------------------------------------------------------------
  result <- raw_tables %>%
    dplyr::left_join(ingest_dict, by = "lake_table_name") %>%
    dplyr::mutate(
      source_match = !is.na(source_type)
    ) %>%
    dplyr::arrange(lake_table_name)
  
  return(result)
}
