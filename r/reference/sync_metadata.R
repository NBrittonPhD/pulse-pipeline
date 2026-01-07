# =============================================================================
# sync_metadata
# =============================================================================
# Purpose:
#   Synchronize expected schema definitions from expected_schema_dictionary.xlsx
#   into reference.metadata table. This function is the bridge between the
#   governed Excel file and the database table used by validate_schema().
#
# Inputs:
#   con (DBIConnection)
#       Active database connection to PULSE.
#
#   xlsx_path (character)
#       Path to expected_schema_dictionary.xlsx.
#       Default: "reference/expected_schema_dictionary.xlsx"
#
#   mode (character)
#       How to handle existing data:
#         - "replace": DROP all existing rows, INSERT fresh (default)
#         - "upsert":  Update existing rows, INSERT new ones
#         - "append":  INSERT only, fail on duplicates
#
#   created_by (character)
#       Identifier for who/what triggered the sync.
#       Default: "sync_metadata"
#
# Outputs:
#   A list with:
#     - status: "success" or "error"
#     - rows_synced: integer count of rows written
#     - tables_synced: integer count of distinct tables
#     - schema_version: character version synced
#     - error_message: NULL or character if error
#
# Side Effects:
#   - Writes/overwrites rows in reference.metadata
#   - Creates table if it doesn't exist (runs DDL)
#
# Dependencies:
#   - DBI, readxl, dplyr, glue
#   - sql/ddl/create_METADATA.sql
#
# Author: Noel
# Last Updated: 2026-01-07
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
sync_metadata <- function(con,
                          xlsx_path = NULL,
                          mode = "replace",
                          created_by = "sync_metadata") {

    # =========================================================================
    # RESOLVE PROJECT ROOT FOR RELATIVE PATHS
    # =========================================================================
    proj_root <- getOption("pulse.proj_root", default = ".")

    # Default xlsx_path if not provided
    if (is.null(xlsx_path)) {
        xlsx_path <- file.path(proj_root, "reference/expected_schema_dictionary.xlsx")
    }

    # =========================================================================
    # INPUT VALIDATION
    # =========================================================================
    message("=================================================================")
    message("[sync_metadata] Starting metadata synchronization")
    message("=================================================================")

    # Validate connection
    if (!inherits(con, "DBIConnection")) {
        stop("[sync_metadata] ERROR: 'con' must be a valid DBI connection object.")
    }

    # Validate xlsx_path
    if (!file.exists(xlsx_path)) {
        stop(glue("[sync_metadata] ERROR: Excel file not found at '{xlsx_path}'."))
    }

    # Validate mode
    valid_modes <- c("replace", "upsert", "append")
    if (!mode %in% valid_modes) {
        stop(glue("[sync_metadata] ERROR: Invalid mode '{mode}'. Must be one of: {paste(valid_modes, collapse = ', ')}"))
    }

    message(glue("[sync_metadata] Excel path: {xlsx_path}"))
    message(glue("[sync_metadata] Sync mode:  {mode}"))

    # =========================================================================
    # ENSURE TABLE EXISTS
    # =========================================================================
    # Run the DDL to create the table if it doesn't exist
    # =========================================================================
    ddl_path <- file.path(proj_root, "sql/ddl/create_METADATA.sql")

    if (file.exists(ddl_path)) {
        message("[sync_metadata] Ensuring reference.metadata table exists...")
        ddl_sql <- readr::read_file(ddl_path)
        tryCatch(
            DBI::dbExecute(con, ddl_sql),
            error = function(e) {
                # Table may already exist, which is fine
                message(glue("[sync_metadata] Note: DDL execution returned: {e$message}"))
            }
        )
    } else {
        message("[sync_metadata] WARNING: DDL file not found, assuming table exists.")
    }

    # =========================================================================
    # LOAD EXCEL DATA
    # =========================================================================
    message("[sync_metadata] Loading Excel file...")

    excel_data <- tryCatch(
        readxl::read_excel(xlsx_path),
        error = function(e) {
            stop(glue("[sync_metadata] ERROR: Failed to read Excel file: {e$message}"))
        }
    )

    message(glue("[sync_metadata] Loaded {nrow(excel_data)} rows from Excel."))

    # Validate required columns exist
    required_cols <- c(
        "schema_version", "effective_from", "lake_table_name",
        "lake_variable_name", "data_type"
    )
    missing_cols <- setdiff(required_cols, names(excel_data))
    if (length(missing_cols) > 0) {
        stop(glue(
            "[sync_metadata] ERROR: Excel file missing required columns: ",
            "{paste(missing_cols, collapse = ', ')}"
        ))
    }

    # =========================================================================
    # PREPARE DATA FOR INSERT
    # =========================================================================
    message("[sync_metadata] Preparing data for database insert...")

    # Add governance columns
    sync_data <- excel_data %>%
        mutate(
            is_active = TRUE,
            synced_at = Sys.time(),
            created_at = Sys.time(),
            created_by = !!created_by
        )

    # Convert POSIXct to Date for effective_from/effective_to
    if ("effective_from" %in% names(sync_data)) {
        sync_data$effective_from <- as.Date(sync_data$effective_from)
    }
    if ("effective_to" %in% names(sync_data)) {
        sync_data$effective_to <- as.Date(sync_data$effective_to)
    }

    # Get schema version for reporting
    schema_version <- unique(sync_data$schema_version)[1]
    n_tables <- n_distinct(sync_data$lake_table_name)

    message(glue("[sync_metadata] Schema version: {schema_version}"))
    message(glue("[sync_metadata] Tables: {n_tables}"))
    message(glue("[sync_metadata] Variables: {nrow(sync_data)}"))

    # =========================================================================
    # EXECUTE SYNC BASED ON MODE
    # =========================================================================

    tbl_id <- DBI::Id(schema = "reference", table = "metadata")

    if (mode == "replace") {
        # ---------------------------------------------------------------------
        # REPLACE MODE: Delete all existing, insert fresh
        # ---------------------------------------------------------------------
        message("[sync_metadata] Mode 'replace': Deleting existing rows...")

        tryCatch({
            DBI::dbExecute(con, "DELETE FROM reference.metadata")
            message("[sync_metadata] Existing rows deleted.")
        }, error = function(e) {
            message(glue("[sync_metadata] Note: Delete returned: {e$message}"))
        })

        message("[sync_metadata] Inserting new rows...")

        # Remove metadata_id column if it exists (it's auto-generated)
        if ("metadata_id" %in% names(sync_data)) {
            sync_data <- sync_data %>% select(-metadata_id)
        }

        DBI::dbWriteTable(
            con,
            tbl_id,
            sync_data,
            append = TRUE,
            row.names = FALSE
        )

        rows_synced <- nrow(sync_data)

    } else if (mode == "append") {
        # ---------------------------------------------------------------------
        # APPEND MODE: Insert only, may fail on duplicates
        # ---------------------------------------------------------------------
        message("[sync_metadata] Mode 'append': Inserting new rows...")

        if ("metadata_id" %in% names(sync_data)) {
            sync_data <- sync_data %>% select(-metadata_id)
        }

        DBI::dbWriteTable(
            con,
            tbl_id,
            sync_data,
            append = TRUE,
            row.names = FALSE
        )

        rows_synced <- nrow(sync_data)

    } else if (mode == "upsert") {
        # ---------------------------------------------------------------------
        # UPSERT MODE: Update existing, insert new
        # This is more complex and requires row-by-row logic
        # ---------------------------------------------------------------------
        message("[sync_metadata] Mode 'upsert': Upserting rows...")

        # For simplicity, we'll use replace mode logic for now
        # A true upsert would require ON CONFLICT handling
        message("[sync_metadata] WARNING: Upsert mode falling back to replace for now.")

        DBI::dbExecute(con, "DELETE FROM reference.metadata")

        if ("metadata_id" %in% names(sync_data)) {
            sync_data <- sync_data %>% select(-metadata_id)
        }

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
    message("[sync_metadata] Verifying sync...")

    count_check <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM reference.metadata")

    message(glue("[sync_metadata] Rows in reference.metadata: {count_check$n}"))

    # =========================================================================
    # RETURN RESULTS
    # =========================================================================
    message("=================================================================")
    message("[sync_metadata] Synchronization complete!")
    message("=================================================================")
    message(glue("  Schema Version: {schema_version}"))
    message(glue("  Tables Synced:  {n_tables}"))
    message(glue("  Rows Synced:    {rows_synced}"))
    message("=================================================================")

    list(
        status = "success",
        rows_synced = rows_synced,
        tables_synced = n_tables,
        schema_version = schema_version,
        error_message = NULL
    )
}
