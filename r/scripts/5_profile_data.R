# =============================================================================
# 5_profile_data.R — Profile Data Quality
# =============================================================================
# Purpose: Profile all raw tables from a given ingest batch to assess data
#          quality before harmonization (Step 6).
#
# HOW TO USE:
#   1. Edit the USER INPUT SECTION below with your ingest_id
#   2. Save and run: source("r/scripts/5_profile_data.R")
#
# PREREQUISITES:
#   - Steps 1-4 complete (source registered, data ingested, schema validated,
#     metadata synced)
#   - Database tables exist (run DDLs for governance.data_profile* if needed)
#
# WHAT THIS SCRIPT DOES:
#   1. Loads profiling configuration from YAML
#   2. Finds all tables from the ingest batch
#   3. For each table, profiles every column:
#      - Infers column type (numeric, categorical, date, identifier)
#      - Detects sentinel/placeholder values (999, UNKNOWN, etc.)
#      - Profiles missingness (NA, empty, whitespace, sentinel, valid)
#      - Computes distribution statistics
#      - Generates quality issues (critical/warning/info)
#   4. Calculates per-table quality scores (Excellent/Good/Fair/Needs Review)
#   5. Writes all results to 5 governance tables
#   6. Logs an audit event
#
# Author: Noel
# =============================================================================

# =============================================================================
# USER INPUT SECTION — EDIT BELOW
# =============================================================================

# The ingest_id from Step 2 (check governance.batch_log for available IDs)
ingest_id <- "ING_cisir2026_toy_20260128_170000"

# Which schema to profile: "raw" (before harmonization) or "staging" (after)
schema_to_profile <- "raw"

# Path to profiling config (uses defaults if file missing)
config_path <- "config/profiling_settings.yml"

# =============================================================================
# END USER INPUT SECTION
# =============================================================================


# =============================================================================
# INITIALIZE
# =============================================================================
source("pulse-init-all.R")
source("r/steps/profile_data.R")

con <- connect_to_pulse()

# =============================================================================
# EXECUTE
# =============================================================================
step_start <- Sys.time()

result <- tryCatch({
    profile_data(
        con              = con,
        ingest_id        = ingest_id,
        schema_to_profile = schema_to_profile,
        config_path      = config_path
    )
}, error = function(e) {
    message("=================================================================")
    message("[Step 5] ERROR: ", e$message)
    message("=================================================================")
    list(
        tables_profiled    = 0,
        variables_profiled = 0,
        sentinels_detected = 0,
        critical_issues    = 0,
        warning_issues     = 0,
        info_issues        = 0,
        overall_score      = "ERROR"
    )
})

step_duration <- round(difftime(Sys.time(), step_start, units = "secs"), 1)

# =============================================================================
# PRINT SUMMARY
# =============================================================================
cat("\n")
cat("===================================================================\n")
cat("                    STEP 5: PROFILING SUMMARY                      \n")
cat("===================================================================\n")
cat(glue::glue("
  Ingest ID:        {ingest_id}
  Schema:           {schema_to_profile}
  Duration:         {step_duration}s

  Tables profiled:  {result$tables_profiled}
  Variables:        {result$variables_profiled}
  Sentinels found:  {result$sentinels_detected}

  Issues:
    Critical:       {result$critical_issues}
    Warning:        {result$warning_issues}
    Info:           {result$info_issues}

  Overall Score:    {result$overall_score}
"))
cat("\n===================================================================\n")

# =============================================================================
# CLEANUP
# =============================================================================
if (DBI::dbIsValid(con)) {
    DBI::dbDisconnect(con)
}

cat("\nStep 5 complete. You may now proceed to Step 6: Harmonization.\n")
