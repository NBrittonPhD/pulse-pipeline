# =============================================================================
# sync_ingest_dictionary
# =============================================================================
# Purpose:      Synchronize ingest_dictionary.xlsx into the
#               reference.ingest_dictionary database table. This function
#               bridges the governed Excel file to the database table used
#               by build_expected_schema_dictionary() and the Step 2
#               ingestion workflow.
#
#               This mirrors the pattern established by sync_metadata.R
#               for the reference.metadata table.
#
# Inputs:
#   - con:        DBI connection object to the PULSE database
#   - xlsx_path:  character path to ingest_dictionary.xlsx
#   - mode:       character sync mode ("replace" or "append")
#   - created_by: character identifier for audit trail
#
# Outputs:      list with (status, rows_synced, tables_synced,
#               source_types, error_message)
#
# Side Effects: Writes/overwrites rows in reference.ingest_dictionary
#
# Dependencies: DBI, readxl, dplyr, glue
#
# Author:       Noel
# Last Updated: 2026-01-29
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(DBI)
library(readxl)
library(dplyr)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
sync_ingest_dictionary <- function(con,
                                    xlsx_path  = NULL,
                                    mode       = "replace",
                                    created_by = "sync_ingest_dictionary") {

  # =========================================================================
  # RESOLVE PATH
  # =========================================================================
  proj_root <- getOption("pulse.proj_root", default = ".")

  if (is.null(xlsx_path)) {
    xlsx_path <- file.path(proj_root, "reference", "ingest_dictionary.xlsx")
  }

  message("=================================================================")
  message("[sync_ingest_dictionary] STEP B: SYNC INGEST DICTIONARY TO DB")
  message("=================================================================")

  # =========================================================================
  # INPUT VALIDATION
  # =========================================================================
  if (!inherits(con, "DBIConnection")) {
    stop("[sync_ingest_dictionary] ERROR: 'con' must be a valid DBI ",
         "connection object.")
  }

  if (!file.exists(xlsx_path)) {
    stop("[sync_ingest_dictionary] ERROR: Excel file not found at: ",
         xlsx_path)
  }

  valid_modes <- c("replace", "append")
  if (!mode %in% valid_modes) {
    stop("[sync_ingest_dictionary] ERROR: Invalid mode '", mode,
         "'. Must be one of: ", paste(valid_modes, collapse = ", "))
  }

  message("[sync_ingest_dictionary] Excel path: ", xlsx_path)
  message("[sync_ingest_dictionary] Sync mode:  ", mode)

  # =========================================================================
  # LOAD EXCEL DATA
  # =========================================================================
  message("[sync_ingest_dictionary] Loading Excel file...")

  excel_data <- tryCatch(
    readxl::read_excel(xlsx_path),
    error = function(e) {
      stop("[sync_ingest_dictionary] ERROR: Failed to read Excel file: ",
           e$message)
    }
  )

  message("[sync_ingest_dictionary] Loaded ", nrow(excel_data),
          " rows from Excel.")

  # =========================================================================
  # VALIDATE REQUIRED COLUMNS
  # =========================================================================
  required_cols <- c("source_type", "source_table_name",
                     "source_variable_name", "lake_table_name",
                     "lake_variable_name")
  missing_cols <- setdiff(required_cols, names(excel_data))
  if (length(missing_cols) > 0) {
    stop("[sync_ingest_dictionary] ERROR: Excel file missing required ",
         "columns: ", paste(missing_cols, collapse = ", "))
  }

  # =========================================================================
  # NORMALIZE TEXT COLUMNS
  # =========================================================================
  # All text values are normalized to lowercase + trimmed to match the
  # convention in build_expected_schema_dictionary.R (lines 57-63) and
  # the ingest workflow (run_step2_batch_logging.R line 51).
  # =========================================================================
  message("[sync_ingest_dictionary] Normalizing text columns...")

  sync_data <- excel_data %>%
    dplyr::mutate(
      source_type          = tolower(trimws(source_type)),
      source_table_name    = tolower(trimws(source_table_name)),
      source_variable_name = tolower(trimws(source_variable_name)),
      lake_table_name      = tolower(trimws(lake_table_name)),
      lake_variable_name   = tolower(trimws(lake_variable_name))
    )

  # Summary stats for reporting
  n_tables <- dplyr::n_distinct(sync_data$lake_table_name)
  source_types <- sort(unique(sync_data$source_type))

  message("[sync_ingest_dictionary] Tables:       ", n_tables)
  message("[sync_ingest_dictionary] Source types:  ",
          paste(source_types, collapse = ", "))
  message("[sync_ingest_dictionary] Variables:     ", nrow(sync_data))

  # =========================================================================
  # EXECUTE SYNC BASED ON MODE
  # =========================================================================
  tbl_id <- DBI::Id(schema = "reference", table = "ingest_dictionary")

  if (mode == "replace") {
    # -------------------------------------------------------------------
    # REPLACE MODE: Delete all existing rows, insert fresh
    # -------------------------------------------------------------------
    message("[sync_ingest_dictionary] Mode 'replace': Deleting existing ",
            "rows...")

    tryCatch({
      DBI::dbExecute(con, "DELETE FROM reference.ingest_dictionary")
      message("[sync_ingest_dictionary] Existing rows deleted.")
    }, error = function(e) {
      message("[sync_ingest_dictionary] Note: Delete returned: ", e$message)
    })

    message("[sync_ingest_dictionary] Inserting new rows...")

    DBI::dbWriteTable(
      con,
      tbl_id,
      sync_data,
      append = TRUE,
      row.names = FALSE
    )

    rows_synced <- nrow(sync_data)

  } else if (mode == "append") {
    # -------------------------------------------------------------------
    # APPEND MODE: Insert only, may fail on duplicates
    # -------------------------------------------------------------------
    message("[sync_ingest_dictionary] Mode 'append': Inserting new rows...")

    DBI::dbWriteTable(
      con,
      tbl_id,
      sync_data,
      append = TRUE,
      row.names = FALSE
    )

    rows_synced <- nrow(sync_data)
  }

  # =========================================================================
  # VERIFY SYNC
  # =========================================================================
  message("[sync_ingest_dictionary] Verifying sync...")

  count_check <- DBI::dbGetQuery(
    con, "SELECT COUNT(*) as n FROM reference.ingest_dictionary"
  )

  message("[sync_ingest_dictionary] Rows in reference.ingest_dictionary: ",
          count_check$n)

  if (count_check$n != rows_synced) {
    warning("[sync_ingest_dictionary] WARNING: Row count mismatch. ",
            "Expected ", rows_synced, " but found ", count_check$n,
            " in database.")
  }

  # =========================================================================
  # RETURN RESULTS
  # =========================================================================
  message("=================================================================")
  message("[sync_ingest_dictionary] SYNC COMPLETE")
  message("=================================================================")
  message("  Tables synced:   ", n_tables)
  message("  Rows synced:     ", rows_synced)
  message("  Source types:    ", paste(source_types, collapse = ", "))
  message("=================================================================")

  return(list(
    status        = "success",
    rows_synced   = rows_synced,
    tables_synced = n_tables,
    source_types  = source_types,
    error_message = NULL
  ))
}
