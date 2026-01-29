# =============================================================================
# run_sync_metadata.R — METADATA SYNC WRAPPER SCRIPT
# =============================================================================
# Purpose:      Synchronize expected_schema_dictionary.xlsx to reference.metadata
#               table in the PULSE database. This script bridges the governed
#               Excel file to the database table used by Step 3 schema validation.
# Usage:        1. Ensure DB environment variables are set (see below)
#               2. Source this script: source("r/scripts/run_sync_metadata.R")
# Author:       Noel
# Last Updated: 2026-01-14
# =============================================================================

# =============================================================================
# USER INPUT SECTION — MODIFY THESE VALUES
# =============================================================================

# Sync mode options:
#   "replace" - Delete all existing rows, insert fresh (default, recommended)
#   "upsert"  - Update existing rows, insert new ones
#   "append"  - Insert only, will fail on duplicates
sync_mode <- "replace"

# Path to the Excel file (relative to project root)
# Default: "reference/expected_schema_dictionary.xlsx"
xlsx_path <- NULL  # NULL uses default path

# Identifier for who/what triggered the sync (for audit trail)
created_by <- "run_sync_metadata"

# =============================================================================
# DATABASE CONNECTION
# =============================================================================
# Ensure these environment variables are set before running:
#   Sys.setenv(PULSE_DB   = "primeai_lake")
#   Sys.setenv(PULSE_HOST = "your-host")
#   Sys.setenv(PULSE_USER = "your-username")
#   Sys.setenv(PULSE_PW   = "your-password")
#
# Or set them in your .Renviron file.
# =============================================================================

# =============================================================================
# END USER INPUT SECTION
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
message("=================================================================")
message("[run_sync_metadata] Starting metadata sync process")
message("=================================================================")

library(DBI)
library(RPostgres)
library(readxl)
library(dplyr)
library(glue)
library(readr)

# =============================================================================
# SET PROJECT ROOT
# =============================================================================
# Determine project root from script location or current working directory
if (!exists("proj_root")) {
    proj_root <- getOption("pulse.proj_root", default = getwd())
}
options(pulse.proj_root = proj_root)

message(glue("[run_sync_metadata] Project root: {proj_root}"))

# =============================================================================
# SOURCE DEPENDENCIES
# =============================================================================
message("[run_sync_metadata] Sourcing dependencies...")

source(file.path(proj_root, "r", "connect_to_pulse.R"))
source(file.path(proj_root, "r", "reference", "sync_metadata.R"))

# =============================================================================
# ESTABLISH DATABASE CONNECTION
# =============================================================================
message("[run_sync_metadata] Establishing database connection...")

con <- connect_to_pulse()

# Verify connection
if (!dbIsValid(con)) {
    stop("[run_sync_metadata] ERROR: Failed to establish database connection. Check environment variables.")
}

message("[run_sync_metadata] Database connection established successfully.")

# =============================================================================
# EXECUTE SYNC
# =============================================================================
message("=================================================================")
message("[run_sync_metadata] EXECUTING METADATA SYNC")
message("=================================================================")
message(glue("[run_sync_metadata] Sync mode:   {sync_mode}"))
message(glue("[run_sync_metadata] Created by:  {created_by}"))
message("=================================================================")

sync_start_time <- Sys.time()

result <- tryCatch({
    sync_metadata(
        con        = con,
        xlsx_path  = xlsx_path,
        mode       = sync_mode,
        created_by = created_by
    )
}, error = function(e) {
    message("[run_sync_metadata] ERROR: Metadata sync failed.")
    message(glue("[run_sync_metadata] Error message: {e$message}"))
    dbDisconnect(con)
    stop(e)
})

sync_end_time <- Sys.time()
sync_duration <- difftime(sync_end_time, sync_start_time, units = "secs")

# =============================================================================
# PRINT FINAL SUMMARY
# =============================================================================
message("=================================================================")
message("[run_sync_metadata] SYNC COMPLETE")
message("=================================================================")
message(glue("  Status:         {result$status}"))
message(glue("  Schema Version: {result$schema_version}"))
message(glue("  Tables Synced:  {result$tables_synced}"))
message(glue("  Rows Synced:    {result$rows_synced}"))
message(glue("  Duration:       {round(as.numeric(sync_duration), 2)} seconds"))
message("=================================================================")

# Quick verification query
message("[run_sync_metadata] Verification query:")
message("  SELECT lake_table_name, COUNT(*) FROM reference.metadata GROUP BY lake_table_name;")

verify_query <- DBI::dbGetQuery(con, "
    SELECT lake_table_name, COUNT(*) as variable_count
    FROM reference.metadata
    WHERE is_active = TRUE
    GROUP BY lake_table_name
    ORDER BY lake_table_name
")

if (nrow(verify_query) > 0) {
    message("[run_sync_metadata] Tables in reference.metadata:")
    for (i in seq_len(nrow(verify_query))) {
        message(glue("    - {verify_query$lake_table_name[i]}: {verify_query$variable_count[i]} variables"))
    }
}

# =============================================================================
# CLEANUP
# =============================================================================
message("[run_sync_metadata] Closing database connection...")
dbDisconnect(con)

message("[run_sync_metadata] Metadata sync complete.")
