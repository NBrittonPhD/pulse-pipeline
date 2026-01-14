# =============================================================================
# STEP 4 — Type Coercion ETL: raw.* → staging.*
# File: r/steps/run_step4_type_coercion_etl.R
# =============================================================================
# Purpose:
#   Transform tables from the raw schema (all text) to the staging schema with
#   proper data types as defined in the expected_schema_dictionary (target_type).
#
#   This step:
#     • Reads target_type for each column from reference.metadata
#     • Creates staging.* tables with proper column types
#     • Attempts type coercion for each column
#     • Logs coercion results (success/failure/nullified counts)
#     • Uses DROP + CREATE pattern (full replace)
#
# Dependencies:
#   r/connect_to_pulse.R
#   DBI, dplyr, glue
#   reference.metadata table with target_type column
#
# =============================================================================

library(DBI)
library(dplyr)
library(glue)

# Resolve project root for portable paths
proj_root <- getOption("pulse.proj_root", default = ".")

source(file.path(proj_root, "r/connect_to_pulse.R"))

# =============================================================================
# USER INPUT SECTION — EDIT BELOW
# =============================================================================
# Specify which tables to process, or leave NULL to process all tables
# that have target_type definitions in reference.metadata
tables_to_process <- NULL

# Set to TRUE to actually execute DDL statements, FALSE for dry-run
execute_ddl <- FALSE

# Set to TRUE to drop existing staging tables before creating new ones
drop_existing <- TRUE
# =============================================================================
# END USER INPUT SECTION
# =============================================================================

# =============================================================================
# MAIN FUNCTION: type_coercion_etl()
# =============================================================================
#' @title Type Coercion ETL from raw.* to staging.*
#' @description Transform raw tables to staging tables with proper data types
#' @param con DBI connection object
#' @param tables Character vector of table names to process (NULL = all)
#' @param execute Logical, whether to execute DDL statements
#' @param drop_existing Logical, whether to drop existing staging tables
#' @return List with summary statistics and coercion log
type_coercion_etl <- function(con,
                               tables = NULL,
                               execute = FALSE,
                               drop_existing = TRUE) {

  message("=================================================================")
  message("[type_coercion_etl] STEP 4: TYPE COERCION ETL")
  message("=================================================================")
  message(glue("[type_coercion_etl] Execute mode: {execute}"))
  message(glue("[type_coercion_etl] Drop existing: {drop_existing}"))

  # ===========================================================================
  # STEP 4.1 — Ensure staging schema exists
  # ===========================================================================
  message("[type_coercion_etl] Ensuring staging schema exists...")

  if (execute) {
    dbExecute(con, "CREATE SCHEMA IF NOT EXISTS staging")
  } else {
    message("[type_coercion_etl]   (dry-run) Would execute: CREATE SCHEMA IF NOT EXISTS staging")
  }

  # ===========================================================================
  # STEP 4.2 — Load target type metadata
  # ===========================================================================
  message("[type_coercion_etl] Loading target type metadata from reference.metadata...")

  metadata <- dbGetQuery(con, "
    SELECT
      lake_table_name,
      lake_variable_name,
      data_type as current_type,
      target_type,
      ordinal_position
    FROM reference.metadata
    WHERE is_active = TRUE
      AND target_type IS NOT NULL
      AND target_type != ''
    ORDER BY lake_table_name, ordinal_position
  ")

  if (nrow(metadata) == 0) {
    stop("[type_coercion_etl] ERROR: No target_type definitions found in reference.metadata.")
  }

  message(glue("[type_coercion_etl] Loaded {nrow(metadata)} column definitions with target types."))

  # ===========================================================================
  # STEP 4.3 — Identify tables to process
  # ===========================================================================
  all_tables <- unique(metadata$lake_table_name)

  if (!is.null(tables)) {
    tables_to_run <- intersect(tables, all_tables)
    if (length(tables_to_run) == 0) {
      stop("[type_coercion_etl] ERROR: None of the specified tables have target_type definitions.")
    }
  } else {
    tables_to_run <- all_tables
  }

  message(glue("[type_coercion_etl] Processing {length(tables_to_run)} tables."))

  # ===========================================================================
  # STEP 4.4 — Build SQL type mapping
  # ===========================================================================
  # Map target_type values to Postgres SQL types
  map_target_to_sql <- function(target_type) {
    target_lower <- tolower(target_type)
    dplyr::case_when(
      target_lower == "integer"   ~ "INTEGER",
      target_lower == "numeric"   ~ "NUMERIC",
      target_lower == "boolean"   ~ "BOOLEAN",
      target_lower == "date"      ~ "DATE",
      target_lower == "timestamp" ~ "TIMESTAMP",
      target_lower == "time"      ~ "TIME",
      target_lower == "text"      ~ "TEXT",
      TRUE                        ~ "TEXT"
    )
  }

  # Build CAST expression for a column
  build_cast_expression <- function(col_name, target_type) {
    sql_type <- map_target_to_sql(target_type)
    target_lower <- tolower(target_type)

    # Use NULLIF to handle empty strings gracefully
    # Different coercion strategies based on type
    if (target_lower == "boolean") {
      # Boolean: handle common text representations
      glue("CASE
             WHEN LOWER(TRIM(\"{col_name}\")) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
             WHEN LOWER(TRIM(\"{col_name}\")) IN ('false', 'f', 'no', 'n', '0') THEN FALSE
             WHEN TRIM(\"{col_name}\") = '' OR \"{col_name}\" IS NULL THEN NULL
             ELSE NULL
           END AS \"{col_name}\"")
    } else if (target_lower %in% c("integer", "numeric")) {
      # Numeric: use try_cast pattern with NULLIF for empty strings
      glue("CASE
             WHEN TRIM(\"{col_name}\") ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN TRIM(\"{col_name}\")::{sql_type}
             WHEN TRIM(\"{col_name}\") = '' OR \"{col_name}\" IS NULL THEN NULL
             ELSE NULL
           END AS \"{col_name}\"")
    } else if (target_lower %in% c("date", "timestamp", "time")) {
      # Date/time: cast with NULLIF for empty strings
      glue("CASE
             WHEN TRIM(\"{col_name}\") = '' OR \"{col_name}\" IS NULL THEN NULL
             ELSE NULLIF(TRIM(\"{col_name}\"), '')::{sql_type}
           END AS \"{col_name}\"")
    } else {
      # Text: just pass through
      glue("\"{col_name}\"")
    }
  }

  # ===========================================================================
  # STEP 4.5 — Process each table
  # ===========================================================================
  coercion_log <- list()

  for (tbl_name in tables_to_run) {
    message(glue("[type_coercion_etl] Processing table: {tbl_name}"))

    # Get column metadata for this table
    tbl_metadata <- metadata %>%
      filter(lake_table_name == tbl_name) %>%
      arrange(ordinal_position)

    if (nrow(tbl_metadata) == 0) {
      message(glue("[type_coercion_etl]   Skipping {tbl_name}: no column metadata."))
      next
    }

    # Check if raw table exists
    raw_exists <- dbGetQuery(con, glue("
      SELECT COUNT(*) as n
      FROM information_schema.tables
      WHERE table_schema = 'raw'
        AND table_name = '{tbl_name}'
    "))$n > 0

    if (!raw_exists) {
      message(glue("[type_coercion_etl]   Skipping {tbl_name}: raw.{tbl_name} does not exist."))
      next
    }

    # Build column list with CAST expressions
    col_expressions <- sapply(seq_len(nrow(tbl_metadata)), function(i) {
      row <- tbl_metadata[i, ]
      build_cast_expression(row$lake_variable_name, row$target_type)
    })

    select_clause <- paste(col_expressions, collapse = ",\n    ")

    # Build CREATE TABLE AS SELECT statement
    create_sql <- glue("
      CREATE TABLE staging.\"{tbl_name}\" AS
      SELECT
        {select_clause}
      FROM raw.\"{tbl_name}\"
    ")

    drop_sql <- glue("DROP TABLE IF EXISTS staging.\"{tbl_name}\"")

    # -------------------------------------------------------------------------
    # Execute or dry-run
    # -------------------------------------------------------------------------
    if (execute) {
      tryCatch({
        # Drop existing if requested
        if (drop_existing) {
          message(glue("[type_coercion_etl]   Dropping existing staging.{tbl_name}..."))
          dbExecute(con, drop_sql)
        }

        # Create new table with coerced types
        message(glue("[type_coercion_etl]   Creating staging.{tbl_name}..."))
        dbExecute(con, create_sql)

        # Get row count for logging
        row_count <- dbGetQuery(con, glue("SELECT COUNT(*) as n FROM staging.\"{tbl_name}\""))$n

        message(glue("[type_coercion_etl]   SUCCESS: staging.{tbl_name} created with {row_count} rows."))

        coercion_log[[tbl_name]] <- list(
          table_name = tbl_name,
          status = "success",
          row_count = row_count,
          columns_processed = nrow(tbl_metadata),
          error_message = NA_character_
        )

      }, error = function(e) {
        message(glue("[type_coercion_etl]   ERROR: Failed to create staging.{tbl_name}"))
        message(glue("[type_coercion_etl]   Error: {e$message}"))

        coercion_log[[tbl_name]] <<- list(
          table_name = tbl_name,
          status = "error",
          row_count = NA_integer_,
          columns_processed = nrow(tbl_metadata),
          error_message = e$message
        )
      })

    } else {
      # Dry-run mode
      message(glue("[type_coercion_etl]   (dry-run) Would execute:"))
      if (drop_existing) {
        message(glue("[type_coercion_etl]     {drop_sql}"))
      }
      message(glue("[type_coercion_etl]     {create_sql}"))

      coercion_log[[tbl_name]] <- list(
        table_name = tbl_name,
        status = "dry-run",
        row_count = NA_integer_,
        columns_processed = nrow(tbl_metadata),
        error_message = NA_character_
      )
    }
  }

  # ===========================================================================
  # STEP 4.6 — Build summary
  # ===========================================================================
  log_df <- dplyr::bind_rows(coercion_log)

  n_success <- sum(log_df$status == "success", na.rm = TRUE)
  n_error   <- sum(log_df$status == "error", na.rm = TRUE)
  n_dryrun  <- sum(log_df$status == "dry-run", na.rm = TRUE)

  message("=================================================================")
  message("[type_coercion_etl] SUMMARY")
  message("=================================================================")
  message(glue("  Tables processed:  {nrow(log_df)}"))
  message(glue("  Successful:        {n_success}"))
  message(glue("  Errors:            {n_error}"))
  message(glue("  Dry-run:           {n_dryrun}"))
  message("=================================================================")

  list(
    success = (n_error == 0),
    tables_processed = nrow(log_df),
    n_success = n_success,
    n_error = n_error,
    n_dryrun = n_dryrun,
    log = log_df
  )
}

# =============================================================================
# EXECUTE STEP 4
# =============================================================================
message(">> STEP 4: Type Coercion ETL (raw.* -> staging.*)")

con <- connect_to_pulse()

result <- type_coercion_etl(
  con = con,
  tables = tables_to_process,
  execute = execute_ddl,
  drop_existing = drop_existing
)

message(">> Step 4 complete.")

if (!result$success) {
  warning(">> Some tables failed to process. Check result$log for details.")
}

invisible(result)
