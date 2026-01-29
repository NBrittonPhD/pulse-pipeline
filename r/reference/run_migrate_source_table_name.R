# =============================================================================
# run_migrate_source_table_name.R — ONE-TIME MIGRATION WRAPPER SCRIPT
# =============================================================================
# Purpose:      Add the `source_table_name` column to the core metadata
#               dictionary by joining against the existing ingest dictionary.
#               This is a one-time migration that must be run BEFORE the
#               automatic metadata refresh workflow can be used.
#
#               After running this script, the core metadata dictionary will
#               have 20 columns (19 original + source_table_name) and can
#               serve as the single source of truth for all downstream files.
#
# Usage:        1. Review the USER INPUT SECTION below
#               2. Source this script:
#                  source("r/reference/run_migrate_source_table_name.R")
#
# Author:       Noel
# Last Updated: 2026-01-29
# =============================================================================

# =============================================================================
# USER INPUT SECTION — MODIFY THESE VALUES
# =============================================================================

# Path to the core metadata dictionary (the master file to be updated)
# NULL uses default: reference/CURRENT_core_metadata_dictionary.xlsx
core_dict_path <- NULL

# Path to the existing ingest dictionary (source of source_table_name values)
# NULL uses default: reference/ingest_dictionary.xlsx
ingest_dict_path <- NULL

# Archive the existing core dict before overwriting?
# TRUE (recommended): saves a timestamped copy to reference/archive/
# FALSE: overwrites in place without backup
archive_before <- TRUE

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

message("=================================================================")
message("[run_migrate] Starting source_table_name migration")
message("=================================================================")
message("[run_migrate] Project root: ", proj_root)

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
message("[run_migrate] Loading required packages...")

library(readxl)
library(writexl)
library(dplyr)
library(tibble)

# =============================================================================
# SOURCE MIGRATION FUNCTION
# =============================================================================
message("[run_migrate] Sourcing migration function...")

source(file.path(proj_root, "r", "reference", "migrate_source_table_name.R"))

# =============================================================================
# EXECUTE MIGRATION
# =============================================================================
message("=================================================================")
message("[run_migrate] EXECUTING MIGRATION")
message("=================================================================")

migration_start_time <- Sys.time()

result <- tryCatch({
  migrate_source_table_name(
    core_dict_path   = core_dict_path,
    ingest_dict_path = ingest_dict_path,
    archive_before   = archive_before
  )
}, error = function(e) {
  message("[run_migrate] ERROR: Migration failed.")
  message("[run_migrate] Error message: ", e$message)
  stop(e)
})

migration_end_time <- Sys.time()
migration_duration <- difftime(migration_end_time, migration_start_time,
                               units = "secs")

# =============================================================================
# PRINT FINAL SUMMARY
# =============================================================================
message("=================================================================")
message("[run_migrate] MIGRATION COMPLETE")
message("=================================================================")
message("  Status:         ", result$status)
message("  Rows migrated:  ", result$rows_migrated)
message("  Rows unmatched: ", result$rows_unmatched)
message("  Output file:    ", result$output_path)
message("  Duration:       ", round(as.numeric(migration_duration), 2),
        " seconds")
message("=================================================================")

if (result$rows_unmatched > 0) {
  message("")
  message("[run_migrate] ACTION REQUIRED: ", result$rows_unmatched,
          " rows have source_table_name = NA.")
  message("[run_migrate] Open the core dict in Excel and fill in the missing")
  message("[run_migrate] source_table_name values before running the metadata")
  message("[run_migrate] refresh workflow.")
}

if (result$status == "already_migrated") {
  message("")
  message("[run_migrate] The core dict already has source_table_name.")
  message("[run_migrate] No changes were made. You can proceed to run")
  message("[run_migrate] the metadata refresh workflow directly:")
  message("[run_migrate]   source(\"r/reference/run_metadata_refresh.R\")")
}

if (result$status == "success") {
  message("")
  message("[run_migrate] NEXT STEP: Verify the updated core dict in Excel,")
  message("[run_migrate] then run the metadata refresh workflow:")
  message("[run_migrate]   source(\"r/reference/run_metadata_refresh.R\")")
}

message("[run_migrate] Migration script complete.")
