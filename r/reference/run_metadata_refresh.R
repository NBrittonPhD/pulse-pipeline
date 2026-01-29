# =============================================================================
# run_metadata_refresh.R — METADATA REFRESH ORCHESTRATOR WRAPPER SCRIPT
# =============================================================================
# Purpose:      Execute the full metadata refresh chain starting from
#               CURRENT_core_metadata_dictionary.xlsx as the single source
#               of truth. This script:
#
#               Step A: Derives ingest_dictionary.xlsx from core dict
#               Step B: Syncs ingest_dictionary to reference.ingest_dictionary DB
#               Step C: Updates type_decision_table.xlsx (preserving human
#                       decisions, flagging new variables as PENDING REVIEW)
#               Step D: Builds expected_schema_dictionary.xlsx (existing function)
#               Step E: Syncs expected schema to reference.metadata DB
#
# Usage:        1. Ensure DB environment variables are set (see below)
#               2. Review and set values in the USER INPUT SECTION
#               3. Source this script:
#                  source("r/reference/run_metadata_refresh.R")
#
# Prerequisites:
#               - The core metadata dictionary must have the source_table_name
#                 column. If not, run the migration first:
#                 source("r/reference/run_migrate_source_table_name.R")
#
# Author:       Noel
# Last Updated: 2026-01-29
# =============================================================================

# =============================================================================
# USER INPUT SECTION — MODIFY THESE VALUES
# =============================================================================

# Path to the core metadata dictionary (single source of truth)
# NULL uses default: reference/CURRENT_core_metadata_dictionary.xlsx
core_dict_path <- NULL

# Schema version for the expected schema dictionary
schema_version <- "2025.0"

# Effective date range for the schema version
effective_from <- Sys.Date()
effective_to   <- NA

# Sync mode for database operations
#   "replace" - Delete all existing rows, insert fresh (default, recommended)
#   "append"  - Insert only, may fail on duplicates
sync_mode <- "replace"

# Archive existing files before overwriting?
# TRUE (recommended): saves timestamped copies to archive/ directories
archive_existing <- TRUE

# Identifier for audit trail
created_by <- "run_metadata_refresh"

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
# SET PROJECT ROOT
# =============================================================================
if (!exists("proj_root")) {
  proj_root <- getOption("pulse.proj_root", default = getwd())
}
options(pulse.proj_root = proj_root)

message("")
message("=================================================================")
message("[run_metadata_refresh] Starting metadata refresh process")
message("=================================================================")
message("[run_metadata_refresh] Project root: ", proj_root)

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
message("[run_metadata_refresh] Loading required packages...")

library(DBI)
library(RPostgres)
library(readxl)
library(writexl)
library(dplyr)
library(tibble)
library(glue)
library(readr)
library(digest)
library(stringr)

# =============================================================================
# SOURCE DEPENDENCIES
# =============================================================================
# Source all function files needed by the orchestrator. This includes
# both the new propagation functions and the existing build/sync functions.
# =============================================================================
message("[run_metadata_refresh] Sourcing dependencies...")

source(file.path(proj_root, "r", "connect_to_pulse.R"))
source(file.path(proj_root, "r", "reference", "derive_ingest_dictionary.R"))
source(file.path(proj_root, "r", "reference", "sync_ingest_dictionary.R"))
source(file.path(proj_root, "r", "reference", "update_type_decision_table.R"))
source(file.path(proj_root, "r", "reference",
                 "build_expected_schema_dictionary.R"))
source(file.path(proj_root, "r", "reference", "sync_metadata.R"))
source(file.path(proj_root, "r", "reference",
                 "orchestrate_metadata_refresh.R"))

# =============================================================================
# ESTABLISH DATABASE CONNECTION
# =============================================================================
message("[run_metadata_refresh] Establishing database connection...")

con <- connect_to_pulse()

# Verify connection
if (!DBI::dbIsValid(con)) {
  stop("[run_metadata_refresh] ERROR: Failed to establish database ",
       "connection. Check environment variables.")
}

message("[run_metadata_refresh] Database connection established ",
        "successfully.")

# Ensure cleanup on exit
on.exit({
  if (DBI::dbIsValid(con)) {
    DBI::dbDisconnect(con)
    message("[run_metadata_refresh] Database connection closed.")
  }
}, add = TRUE)

# =============================================================================
# EXECUTE METADATA REFRESH
# =============================================================================
message("=================================================================")
message("[run_metadata_refresh] EXECUTING METADATA REFRESH")
message("=================================================================")
message("[run_metadata_refresh] Schema version:   ", schema_version)
message("[run_metadata_refresh] Sync mode:        ", sync_mode)
message("[run_metadata_refresh] Archive existing: ", archive_existing)
message("[run_metadata_refresh] Created by:       ", created_by)
message("=================================================================")

refresh_start_time <- Sys.time()

result <- tryCatch({
  orchestrate_metadata_refresh(
    con                = con,
    core_dict_path     = core_dict_path,
    schema_version     = schema_version,
    effective_from     = effective_from,
    effective_to       = effective_to,
    sync_mode          = sync_mode,
    archive_existing   = archive_existing,
    created_by         = created_by
  )
}, error = function(e) {
  message("[run_metadata_refresh] ERROR: Metadata refresh failed.")
  message("[run_metadata_refresh] Error message: ", e$message)
  stop(e)
})

refresh_end_time <- Sys.time()
refresh_duration <- difftime(refresh_end_time, refresh_start_time,
                             units = "secs")

# =============================================================================
# VERIFICATION QUERIES
# =============================================================================
message("")
message("[run_metadata_refresh] Running verification queries...")

# Verify ingest dictionary
ingest_count <- DBI::dbGetQuery(
  con, "SELECT COUNT(*) as n FROM reference.ingest_dictionary"
)
message("[run_metadata_refresh] reference.ingest_dictionary: ",
        ingest_count$n, " rows")

# Verify metadata
meta_count <- DBI::dbGetQuery(
  con, "SELECT COUNT(*) as n FROM reference.metadata"
)
message("[run_metadata_refresh] reference.metadata: ",
        meta_count$n, " rows")

# Show table breakdown
table_breakdown <- DBI::dbGetQuery(con, "
  SELECT lake_table_name, COUNT(*) as variable_count
  FROM reference.metadata
  WHERE is_active = TRUE
  GROUP BY lake_table_name
  ORDER BY lake_table_name
")

if (nrow(table_breakdown) > 0) {
  message("[run_metadata_refresh] Tables in reference.metadata:")
  for (i in seq_len(nrow(table_breakdown))) {
    message("    - ", table_breakdown$lake_table_name[i], ": ",
            table_breakdown$variable_count[i], " variables")
  }
}

# =============================================================================
# PRINT FINAL SUMMARY
# =============================================================================
message("")
message("=================================================================")
message("[run_metadata_refresh] METADATA REFRESH COMPLETE")
message("=================================================================")
message("  Status:           ", result$status)
message("  Steps completed:  ",
        paste(result$steps_completed, collapse = " -> "))
message("  Duration:         ", round(as.numeric(refresh_duration), 2),
        " seconds")
message("")
message("  Files updated:")
message("    - reference/ingest_dictionary.xlsx")
message("    - reference/type_decisions/type_decision_table.xlsx")
message("    - reference/expected_schema_dictionary.xlsx")
message("")
message("  Database tables synced:")
message("    - reference.ingest_dictionary: ", ingest_count$n, " rows")
message("    - reference.metadata:          ", meta_count$n, " rows")
message("=================================================================")

if (result$update_type_result$new_rows > 0) {
  message("")
  message("  ACTION REQUIRED: ", result$update_type_result$new_rows,
          " new variables need type review.")
  message("  Open reference/type_decisions/type_decision_table.xlsx")
  message("  and set final_type for rows marked 'PENDING REVIEW'.")
}

message("")
message("[run_metadata_refresh] Metadata refresh script complete.")
