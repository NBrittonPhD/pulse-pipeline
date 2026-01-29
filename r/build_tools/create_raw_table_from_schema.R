# =============================================================================
# create_raw_table_from_schema()
# =============================================================================
# STEP 2 / STEP 3 SUPPORT UTILITY
#
# Purpose:
#   Create a fully typed raw.<lake_table_name> table in Postgres
#   based entirely on the expected_schema_dictionary (versioned).
#
# Inputs:
#   con                   Active DBI/Postgres connection
#   expected_schema_tbl   The FULL expected_schema_dictionary (tibble)
#   lake_table_name       Character, the table to create ("ems_times", "labs", etc.)
#
# Behavior:
#   • Filters expected_schema_dictionary for the table.
#   • Generates a strict, typed CREATE TABLE statement.
#   • Creates raw.<lake_table_name> only if it does NOT already exist.
#   • Fails loudly if the table already exists (to avoid accidental overwrites).
#   • Returns a structured result list.
#
# Outputs:
#   list(
#     status = "created" | "exists" | "error",
#     lake_table_name = <chr>,
#     n_columns = <int>,
#     ddl = <chr>
#   )
#
# Side effects:
#   Writes a raw schema table into Postgres if it does not exist.
#
# =============================================================================

library(DBI)
library(glue)
library(dplyr)
library(stringr)

create_raw_table_from_schema <- function(con, expected_schema_tbl, lake_table_name) {
  
  lake_table_name <- tolower(lake_table_name)
  
  # ----------------------------------------------------------------------------
  # Validate expected_schema_tbl
  # ----------------------------------------------------------------------------
  req_cols <- c("lake_variable_name", "type_descriptor", "lake_table_name")
  if (!all(req_cols %in% names(expected_schema_tbl))) {
    stop("expected_schema_tbl is missing required columns.")
  }
  
  # ----------------------------------------------------------------------------
  # Filter schema dictionary for this table
  # ----------------------------------------------------------------------------
  es <- expected_schema_tbl %>%
    filter(tolower(lake_table_name) == !!lake_table_name)
  
  if (nrow(es) == 0) {
    stop(glue("No expected schema definitions found for lake_table_name = '{lake_table_name}'"))
  }
  
  # ----------------------------------------------------------------------------
  # Check if raw table already exists
  # ----------------------------------------------------------------------------
  tbl_id <- DBI::Id(schema = "raw", table = lake_table_name)
  
  if (DBI::dbExistsTable(con, tbl_id)) {
    return(list(
      status = "exists",
      lake_table_name = lake_table_name,
      n_columns = nrow(es),
      ddl = NA_character_
    ))
  }
  
  # ----------------------------------------------------------------------------
  # Build column DDL components
  # ----------------------------------------------------------------------------
  col_expr <- purrr::map_chr(seq_len(nrow(es)), function(i) {
    var  <- es$lake_variable_name[i]
    type <- es$type_descriptor[i]
    
    # Fallback typing safety
    if (is.na(type) || type == "") {
      type <- "text"
    }
    
    glue("{DBI::dbQuoteIdentifier(con, var)} {type}")
  })
  
  # ----------------------------------------------------------------------------
  # Compose full CREATE TABLE statement
  # ----------------------------------------------------------------------------
  ddl <- glue(
    "CREATE TABLE raw.{DBI::dbQuoteIdentifier(con, lake_table_name)} (\n  ",
    paste(col_expr, collapse = ",\n  "),
    "\n);"
  )
  
  # ----------------------------------------------------------------------------
  # Execute DDL
  # ----------------------------------------------------------------------------
  ok <- tryCatch(
    {
      DBI::dbExecute(con, ddl)
      TRUE
    },
    error = function(e) {
      message("ERROR creating raw table: ", e$message)
      FALSE
    }
  )
  
  if (!ok) {
    return(list(
      status = "error",
      lake_table_name = lake_table_name,
      n_columns = nrow(es),
      ddl = ddl
    ))
  }
  
  # ----------------------------------------------------------------------------
  # Success
  # ----------------------------------------------------------------------------
  list(
    status = "created",
    lake_table_name = lake_table_name,
    n_columns = nrow(es),
    ddl = ddl
  )
}
