# =============================================================================
# 3_validate_schema.R
# Step 3 — Schema Validation Engine
# =============================================================================
# This script provides a human-friendly interface for running Step 3 of the
# PULSE pipeline: schema validation against expected metadata definitions.
#
# HOW TO USE:
#   1. Open this file: r/scripts/3_validate_schema.R
#   2. Edit the fields in the USER INPUT SECTION.
#   3. Save the file.
#   4. Run:
#        source("r/scripts/3_validate_schema.R")
#
# This will:
#   - Connect to Postgres
#   - Optionally sync metadata from CURRENT_core_metadata_dictionary.xlsx
#   - Load expected schema from reference.metadata
#   - Identify raw tables from the specified ingest batch
#   - Compare each table against expected schema
#   - Write any issues to governance.structure_qc_table
#   - Print a validation summary
#   - Optionally halt on critical errors
#
# PREREQUISITES:
#   - Step 1 must have been run (source registered)
#   - Step 2 must have been run (files ingested, ingest_id created)
#   - The ingest_id must exist in governance.batch_log
#
# =============================================================================


# =============================================================================
# USER INPUT SECTION — EDIT BELOW
# =============================================================================


#ING_cisir2026_toy_20260128_170418           
#ING_clarity2026_toy_20260128_170320
#ING_trauma_registry2026_toy_20260128_170308
# 1. The ingest_id from Step 2 that you want to validate
#    This must match an existing ingest_id in governance.batch_log
ingest_id <- "ING_trauma_registry2026_toy_20260128_170308"   # EDIT ME

# 2. Source type (for logging purposes; optional if derivable from batch_log)
source_type <- "CISIR"   # EDIT ME

# =============================================================================
# END USER INPUT SECTION
# =============================================================================


# =============================================================================
# CONFIGURATION DEFAULTS
# =============================================================================
# These settings control Step 3 behavior. Change if needed, but typically
# these defaults are appropriate for most runs.
# -----------------------------------------------------------------------------

# Should validation halt if critical schema errors are found?
#   TRUE  = Stop immediately on critical errors (recommended for production)
#   FALSE = Continue and log all issues (useful for debugging/exploration)
halt_on_error <- FALSE

# Sync metadata before validation?
#   TRUE  = Re-sync reference.metadata from CURRENT_core_metadata_dictionary.xlsx
#   FALSE = Use existing reference.metadata (faster if already synced)
sync_metadata_first <- FALSE


# =============================================================================
# INITIALIZE PULSE SYSTEM
# =============================================================================
message("=================================================================")
message("[Step 3] Initializing PULSE system...")
message("=================================================================")

source("pulse-init-all.R")

# Load Step 3 logic
source("r/steps/validate_schema.R")

# Connect to database
con <- connect_to_pulse()


# =============================================================================
# OPTIONAL: SYNC METADATA FROM EXCEL
# =============================================================================
if (sync_metadata_first) {
    message("[Step 3] Syncing metadata from CURRENT_core_metadata_dictionary.xlsx...")
    source("r/reference/sync_metadata.R")
    sync_result <- sync_metadata(con, dict_path = "reference/CURRENT_core_metadata_dictionary.xlsx")
    message(glue::glue("[Step 3] Synced {sync_result$rows_synced} metadata rows."))
}


# =============================================================================
# EXECUTE STEP 3: SCHEMA VALIDATION
# =============================================================================
message("=================================================================")
message("[Step 3] EXECUTING SCHEMA VALIDATION")
message("=================================================================")
message(glue::glue("[Step 3] Ingest ID:       {ingest_id}"))
message(glue::glue("[Step 3] Source Type:     {source_type}"))
message(glue::glue("[Step 3] Halt on Error:   {halt_on_error}"))
message("=================================================================")

step_start_time <- Sys.time()

result <- tryCatch({
    validate_schema(
        con = con,
        ingest_id = ingest_id,
        source_type = source_type,
        halt_on_error = halt_on_error
    )
}, error = function(e) {
    message("[Step 3] ERROR: Schema validation failed.")
    message(glue::glue("[Step 3] Error message: {e$message}"))

    # Return error result
    list(
        success = FALSE,
        issues_count = NA_integer_,
        critical_count = NA_integer_,
        warning_count = NA_integer_,
        info_count = NA_integer_,
        tables_validated = NA_integer_,
        error_message = e$message
    )
})

step_end_time <- Sys.time()
step_duration <- difftime(step_end_time, step_start_time, units = "secs")


# =============================================================================
# PRINT FINAL SUMMARY
# =============================================================================
message("\n=================================================================")
message("  STEP 3 SUMMARY")
message("=================================================================")

if (isTRUE(result$success) || isFALSE(result$success)) {
    status_text <- if (result$success) "SUCCESS" else "COMPLETED WITH ISSUES"
    message(glue::glue("  Status:           {status_text}"))
    message(glue::glue("  Tables Validated: {result$tables_validated}"))
    message(glue::glue("  Total Issues:     {result$issues_count}"))
    message(glue::glue("  Critical:         {result$critical_count}"))
    message(glue::glue("  Warnings:         {result$warning_count}"))
    message(glue::glue("  Info:             {result$info_count}"))
} else {
    message("  Status: ERROR")
    message(glue::glue("  Error:  {result$error_message}"))
}

message(glue::glue("  Duration:         {round(as.numeric(step_duration), 2)} seconds"))
message("=================================================================")

if (!is.null(result$issues_count) && result$issues_count > 0) {
    message("\n[Step 3] Review issues in governance.structure_qc_table:")
    message(glue::glue("  SELECT * FROM governance.structure_qc_table WHERE ingest_id = '{ingest_id}';"))
}


# =============================================================================
# CLEANUP
# =============================================================================
message("\n[Step 3] Closing database connection...")
DBI::dbDisconnect(con)

message("[Step 3] Step 3 execution complete.")

if (result$success) {
    message("[Step 3] You may now proceed to data profiling (Step 5).")
} else {
    message("[Step 3] Address schema issues before proceeding to Step 5.")
}
