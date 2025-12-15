# =============================================================================
# create_raw_tables_from_ingest_dictionary.R
# =============================================================================
# Rebuild RAW zone tables based on the ingest_dictionary.
#
# - Reads ingest_dictionary (reference.ingest_dictionary)
# - Groups by lake_table_name
# - Creates one RAW.<lake_table_name> table per group
# - All columns default to TEXT unless overridden
# - Special engineered fields (e.g., lab_year) added here when needed
#
# NOTE:
# This script is part of the RAW zone infrastructure and should be run whenever
#   - ingest_dictionary is updated
#   - new lake tables are added
#   - RAW schema needs to be reconstructed
#
# =============================================================================

create_raw_tables_from_ingest_dictionary <- function(con) {
  
  # ---------------------------------------------------------------------------
  # Load & normalize ingest_dictionary
  # ---------------------------------------------------------------------------
  dict <- dbReadTable(con, DBI::Id(schema = "reference", table = "ingest_dictionary"))
  
  dict <- dict %>%
    mutate(
      lake_table_name   = tolower(lake_table_name),
      lake_variable_name = tolower(lake_variable_name)
    )
  
  # ---------------------------------------------------------------------------
  # Group by lake_table_name → list of variables
  # ---------------------------------------------------------------------------
  tables <- dict %>%
    group_by(lake_table_name) %>%
    summarise(cols = list(lake_variable_name), .groups = "drop")
  
  # ---------------------------------------------------------------------------
  # Iterate over lake tables and create RAW tables
  # ---------------------------------------------------------------------------
  purrr::walk(seq_len(nrow(tables)), function(i) {
    
    tbl  <- tables$lake_table_name[i]
    cols <- tables$cols[[i]]
    
    message(">> Rebuilding raw.", tbl)
    
    # -------------------------------------------------------------------------
    # Drop existing RAW table (CASCADE ensures clean rebuild)
    # -------------------------------------------------------------------------
    dbExecute(con, glue::glue("DROP TABLE IF EXISTS raw.{tbl} CASCADE;"))
    
    # -------------------------------------------------------------------------
    # Build column definitions (all TEXT by default)
    # -------------------------------------------------------------------------
    col_defs <- paste0(cols, " TEXT", collapse = ",\n  ")
    
    # -------------------------------------------------------------------------
    # SPECIAL CASE: labs_YYYY files
    # Add engineered field `lab_year`
    # -------------------------------------------------------------------------
    if (tbl == "labs") {
      col_defs <- paste0(
        col_defs,
        ",\n  lab_year INTEGER"
      )
    }
    
    # -------------------------------------------------------------------------
    # Construct CREATE TABLE statement
    # -------------------------------------------------------------------------
    create_sql <- glue::glue("
      CREATE TABLE raw.{tbl} (
        {col_defs}
      );
    ")
    
    dbExecute(con, create_sql)
  })
  
  message(">> All RAW tables rebuilt from ingest_dictionary.")
  invisible(TRUE)
}
