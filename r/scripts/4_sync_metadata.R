# =============================================================================
# 4_sync_metadata.R
# Step 4 — Metadata Synchronization
# =============================================================================
# This script synchronizes the core metadata dictionary (Excel) with the
# database, tracking all field-level changes with version history.
#
# HOW TO USE:
#   1. Open this file: r/scripts/4_sync_metadata.R
#   2. Edit the fields in the USER INPUT SECTION.
#   3. Save the file.
#   4. Run:
#        source("r/scripts/4_sync_metadata.R")
#
# This will:
#   - Connect to Postgres
#   - Load CURRENT_core_metadata_dictionary.xlsx
#   - Compare it to the current reference.metadata table
#   - Detect field-level changes (adds, updates, removes)
#   - Write changes to reference.metadata_history
#   - Upsert reference.metadata with a new version number
#   - Write audit log event
#
# PREREQUISITES:
#   - Steps 1-3 must have been run
#   - CURRENT_core_metadata_dictionary.xlsx must exist in reference/
#   - Database environment variables must be set
#
# =============================================================================


# =============================================================================
# USER INPUT SECTION — EDIT BELOW
# =============================================================================

# Path to the metadata dictionary Excel file
dict_path <- "reference/CURRENT_core_metadata_dictionary.xlsx"

# Optional: Filter to a specific source type (set to NULL for all sources)
# Options: "CISIR", "CLARITY", "TRAUMA_REGISTRY", or NULL
source_type_filter <- NULL


# =============================================================================
# END USER INPUT — DO NOT MODIFY BELOW THIS LINE
# =============================================================================


# =============================================================================
# INITIALIZE PIPELINE
# =============================================================================
source("pulse-init-all.R")

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
message("=================================================================")
message("[run_step4] Loading required packages...")
message("=================================================================")

library(DBI)
library(RPostgres)
library(dplyr)
library(tidyr)
library(glue)
library(tibble)
library(readxl)
library(jsonlite)
library(uuid)

# =============================================================================
# SOURCE STEP FUNCTION
# =============================================================================
message("[run_step4] Sourcing step function...")

source("r/reference/sync_metadata.R")

# =============================================================================
# ESTABLISH DATABASE CONNECTION
# =============================================================================
message("[run_step4] Establishing database connection...")

con <- connect_to_pulse()

if (!DBI::dbIsValid(con)) {
    stop("[run_step4] ERROR: Failed to establish database connection. ",
         "Check environment variables.")
}

message("[run_step4] Database connection established successfully.")

# =============================================================================
# EXECUTE STEP 4: METADATA SYNCHRONIZATION
# =============================================================================
message("=================================================================")
message("[run_step4] EXECUTING STEP 4: METADATA SYNCHRONIZATION")
message("=================================================================")
message(paste0("[run_step4] Dictionary:      ", dict_path))
message(paste0("[run_step4] Source filter:   ", source_type_filter %||% "ALL"))
message("=================================================================")

step_start_time <- Sys.time()

result <- tryCatch({
    sync_metadata(
        con = con,
        dict_path = dict_path,
        source_type_filter = source_type_filter
    )
}, error = function(e) {
    message("[run_step4] ERROR: Metadata sync failed.")
    message(paste0("[run_step4] Error message: ", e$message))
    if (DBI::dbIsValid(con)) {
        DBI::dbDisconnect(con)
        message("[run_step4] Database connection closed.")
    }
    stop(e)
})

step_end_time <- Sys.time()
step_duration <- difftime(step_end_time, step_start_time, units = "secs")

# =============================================================================
# PRINT FINAL SUMMARY
# =============================================================================
message("")
message("=================================================================")
message("[run_step4] STEP 4 COMPLETE")
message("=================================================================")
message(paste0("  Version:         ", result$version_number))
message(paste0("  Total Variables: ", result$total_variables))
message(paste0("  Adds:            ", result$adds))
message(paste0("  Updates:         ", result$updates))
message(paste0("  Removes:         ", result$removes))
message(paste0("  Total Changes:   ", result$total_changes))
message(paste0("  Duration:        ", round(as.numeric(step_duration), 2), " seconds"))
message("=================================================================")

if (result$total_changes == 0) {
    message("[run_step4] No changes detected. Database is up to date.")
} else {
    message("[run_step4] Review changes in reference.metadata_history:")
    message(paste0("  SELECT * FROM reference.metadata_history WHERE version_number = ",
                   result$version_number, ";"))
}

message("")
message("[run_step4] Next: Run Step 5 to profile data.")

# =============================================================================
# CLEANUP
# =============================================================================
if (DBI::dbIsValid(con)) {
    DBI::dbDisconnect(con)
    message("[run_step4] Database connection closed.")
}

message("[run_step4] Step 4 execution complete.")
