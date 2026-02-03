# =============================================================================
# promote_to_staging()
# =============================================================================
# Purpose:
#   Promote a single raw.<lake_table> to staging.<lake_table> by type-casting
#   every column according to the type_decision_table. Uses pure SQL
#   CREATE TABLE ... AS SELECT CAST(...) to handle special column names,
#   TIME types, and avoid R-to-Postgres type mapping issues.
#
# Inputs:
#   con              Active DBI/Postgres connection
#   lake_table_name  Character, the table to promote ("ems_times", "labs", etc.)
#   type_decisions   Data frame from type_decision_table.xlsx with columns:
#                      lake_table_name, lake_variable_name, final_type,
#                      suggested_type (fallbacks in that order)
#
# Behavior:
#   - Verifies raw.<table> exists
#   - Gets raw column names from information_schema.columns
#   - Looks up each column's target type from type_decisions
#     (final_type -> suggested_type -> TEXT fallback)
#   - Drops staging.<table> if it exists, then creates via CAST
#   - Executes within a transaction (BEGIN / DROP / CREATE / COMMIT)
#   - Rolls back on error
#
# Returns:
#   list(
#     status     = "promoted" | "error",
#     lake_table = <chr>,
#     n_rows     = <int>,
#     n_columns  = <int>,
#     n_typed    = <int>,   # columns that got a non-TEXT type
#     ddl        = <chr>,   # the CREATE TABLE AS SELECT statement
#     error_message = NULL | <chr>
#   )
#
# =============================================================================

library(DBI)
library(glue)
library(dplyr)
library(rlang)

promote_to_staging <- function(con, lake_table_name, type_decisions) {

  lake_table_name <- tolower(trimws(lake_table_name))

  # --------------------------------------------------------------------------
  # 1. Verify raw.<table> exists
  # --------------------------------------------------------------------------
  raw_id <- DBI::Id(schema = "raw", table = lake_table_name)

  if (!DBI::dbExistsTable(con, raw_id)) {
    return(list(
      status        = "error",
      lake_table    = lake_table_name,
      n_rows        = NA_integer_,
      n_columns     = NA_integer_,
      n_typed       = NA_integer_,
      ddl           = NA_character_,
      error_message = glue("raw.{lake_table_name} does not exist.")
    ))
  }

  # --------------------------------------------------------------------------
  # 2. Get raw column names from information_schema
  #    (preserves exact column names including special characters)
  # --------------------------------------------------------------------------
  raw_cols <- DBI::dbGetQuery(con, glue("
    SELECT column_name
      FROM information_schema.columns
     WHERE table_schema = 'raw'
       AND table_name   = '{lake_table_name}'
     ORDER BY ordinal_position
  "))$column_name

  if (length(raw_cols) == 0) {
    return(list(
      status        = "error",
      lake_table    = lake_table_name,
      n_rows        = NA_integer_,
      n_columns     = NA_integer_,
      n_typed       = NA_integer_,
      ddl           = NA_character_,
      error_message = glue("raw.{lake_table_name} has no columns in information_schema.")
    ))
  }

  # --------------------------------------------------------------------------
  # 3. Look up target type for each column from type_decisions
  #    Priority: final_type -> suggested_type -> TEXT
  # --------------------------------------------------------------------------
  # Normalize type_decisions for matching
  # Handle both naming conventions: lake_table_name/lake_variable_name OR table_name/variable
  td <- type_decisions

  # Determine which column names are present
  has_lake_names <- "lake_table_name" %in% names(td)
  tbl_col <- if (has_lake_names) "lake_table_name" else "table_name"
  var_col <- if (has_lake_names) "lake_variable_name" else "variable"

  # Normalize to standard internal names
 td <- td %>%
    rename(
      .tbl = !!sym(tbl_col),
      .var = !!sym(var_col)
    ) %>%
    mutate(
      .tbl = tolower(trimws(.tbl)),
      .var = tolower(trimws(.var))
    )

  # Filter to this table
 td_tbl <- td %>%
    filter(.tbl == !!lake_table_name)

  n_typed <- 0L

  cast_exprs <- vapply(raw_cols, function(col) {
    quoted_col <- DBI::dbQuoteIdentifier(con, col)

    # Find matching type decision row
    match_row <- td_tbl %>%
      filter(.var == tolower(trimws(col)))

    target_type <- "TEXT"

    if (nrow(match_row) > 0) {
      row <- match_row[1, ]

      # Try final_type first
      if ("final_type" %in% names(row) &&
          !is.na(row$final_type) &&
          nchar(trimws(row$final_type)) > 0) {
        target_type <- trimws(row$final_type)
      }
      # Fall back to suggested_type
      else if ("suggested_type" %in% names(row) &&
               !is.na(row$suggested_type) &&
               nchar(trimws(row$suggested_type)) > 0) {
        target_type <- trimws(row$suggested_type)
      }
    }

    # Track how many columns get a non-TEXT type
    if (toupper(target_type) != "TEXT") {
      n_typed <<- n_typed + 1L
    }

    # Build the CAST expression
    glue("CAST({quoted_col} AS {target_type}) AS {quoted_col}")
  }, character(1))

  # --------------------------------------------------------------------------
  # 4. Build the CREATE TABLE AS SELECT statement
  # --------------------------------------------------------------------------
  quoted_staging_table <- glue(
    "staging.{DBI::dbQuoteIdentifier(con, lake_table_name)}"
  )
  quoted_raw_table <- glue(
    "raw.{DBI::dbQuoteIdentifier(con, lake_table_name)}"
  )

  select_clause <- paste(cast_exprs, collapse = ",\n       ")

  create_sql <- glue("
CREATE TABLE {quoted_staging_table} AS
SELECT {select_clause}
  FROM {quoted_raw_table}
  ")

  drop_sql <- glue("DROP TABLE IF EXISTS {quoted_staging_table}")

  # --------------------------------------------------------------------------
  # 5. Execute within a transaction
  # --------------------------------------------------------------------------
  result <- tryCatch({
    DBI::dbExecute(con, "BEGIN")
    DBI::dbExecute(con, drop_sql)
    DBI::dbExecute(con, create_sql)
    DBI::dbExecute(con, "COMMIT")

    # Get row count after successful promotion
    n_rows <- DBI::dbGetQuery(
      con,
      glue("SELECT COUNT(*) AS n FROM {quoted_staging_table}")
    )$n

    list(
      status        = "promoted",
      lake_table    = lake_table_name,
      n_rows        = as.integer(n_rows),
      n_columns     = length(raw_cols),
      n_typed       = n_typed,
      ddl           = create_sql,
      error_message = NULL
    )
  },
  error = function(e) {
    # Rollback on any error
    tryCatch(DBI::dbExecute(con, "ROLLBACK"), error = function(e2) NULL)

    list(
      status        = "error",
      lake_table    = lake_table_name,
      n_rows        = NA_integer_,
      n_columns     = length(raw_cols),
      n_typed       = n_typed,
      ddl           = create_sql,
      error_message = conditionMessage(e)
    )
  })

  result
}
