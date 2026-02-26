# =============================================================================
# 6_harmonize_data.R — Harmonize Staging to Validated
# =============================================================================
# Purpose: Map staging tables to validated tables for cross-source analysis.
#          Combines data from CISIR, CLARITY, and TRAUMA_REGISTRY into unified
#          validated.* tables using column mappings from the metadata dictionary.
#
# HOW TO USE:
#   1. Edit the USER INPUT SECTION below with your ingest_id
#   2. Save and run: source("r/scripts/6_harmonize_data.R")
#
# PREREQUISITES:
#   - Steps 1-5 complete (source registered, data ingested, schema validated,
#     metadata synced, raw data profiled)
#   - Validated table DDLs executed (sql/ddl/create_VALIDATED_*.sql)
#   - Harmonization map DDL executed (sql/ddl/create_HARMONIZATION_MAP.sql)
#   - Transform log DDL executed (sql/ddl/create_TRANSFORM_LOG.sql)
#   - Staging tables exist and are populated
#
# WHAT THIS SCRIPT DOES:
#   1. Optionally syncs harmonization mappings from reference.metadata
#   2. For each validated target table with active mappings:
#      - Loads column mappings (source_column → target_column)
#      - Builds SQL SELECT to transform staging data
#      - Inserts into validated.{table} with source_type provenance
#      - Logs each operation to governance.transform_log
#   3. Optionally re-profiles validated tables using Step 5
#   4. Writes an audit event to governance.audit_log
#
# Author: Noel
# =============================================================================

# =============================================================================
# USER INPUT SECTION — EDIT BELOW
# =============================================================================

# Ingest ID to harmonize (check governance.batch_log for available IDs)
ingest_id <- "ING_cisir2026_toy_20260128_170418"

# Which validated tables to populate (NULL = all with active mappings)
target_tables <- NULL
# Or specify: target_tables <- c("demographics", "admission", "labs")

# Filter to specific source type (NULL = all sources)
source_type_filter <- NULL
# Or specify: source_type_filter <- "CISIR"

# Sync mappings from metadata dictionary before harmonizing?
sync_mappings_first <- TRUE

# Profile validated tables after harmonization?
profile_after <- TRUE

# =============================================================================
# END USER INPUT SECTION
# =============================================================================


# =============================================================================
# INITIALIZE
# =============================================================================
source("pulse-init-all.R")

con <- connect_to_pulse()

# =============================================================================
# SYNC MAPPINGS (OPTIONAL)
# =============================================================================
if (sync_mappings_first) {
    source("r/harmonization/sync_harmonization_map.R")
    sync_result <- sync_harmonization_map(con, source = "metadata")
    cat(glue::glue("\nSynced {sync_result$count} mappings ",
                   "({sync_result$tables_mapped} tables, ",
                   "{sync_result$sources_mapped} sources)\n\n"))
}

# =============================================================================
# EXECUTE HARMONIZATION
# =============================================================================
source("r/steps/harmonize_data.R")

step_start <- Sys.time()

result <- tryCatch({
    harmonize_data(
        con                = con,
        ingest_id          = ingest_id,
        target_tables      = target_tables,
        source_type_filter = source_type_filter
    )
}, error = function(e) {
    message("=================================================================")
    message("[Step 6] ERROR: ", e$message)
    message("=================================================================")
    list(
        tables_processed  = 0,
        total_rows        = 0,
        sources_processed = 0,
        by_table          = list(),
        status            = "error"
    )
})

step_duration <- round(difftime(Sys.time(), step_start, units = "secs"), 1)

# =============================================================================
# PRINT SUMMARY
# =============================================================================
cat("\n")
cat("===================================================================\n")
cat("                 STEP 6: HARMONIZATION SUMMARY                     \n")
cat("===================================================================\n")
cat(glue::glue("
  Ingest ID:          {ingest_id}
  Source Filter:       {ifelse(is.null(source_type_filter), 'ALL', source_type_filter)}
  Duration:            {step_duration}s
  Status:              {result$status}

  Tables Harmonized:   {result$tables_processed}
  Total Rows Inserted: {result$total_rows}
  Sources Processed:   {result$sources_processed}
"))
cat("\n")

if (length(result$by_table) > 0) {
    cat("\n  By Table:\n")
    for (t in names(result$by_table)) {
        cat(glue::glue("    {t}: {result$by_table[[t]]} rows\n"))
    }
}
cat("===================================================================\n")

# =============================================================================
# PROFILE VALIDATED TABLES (OPTIONAL)
# =============================================================================
if (profile_after && result$total_rows > 0) {
    cat("\nProfiling validated tables...\n")
    source("r/steps/profile_data.R")

    profile_result <- tryCatch({
        profile_data(con, ingest_id, schema_to_profile = "validated")
    }, error = function(e) {
        message("[Step 6] Profiling error: ", e$message)
        list(overall_score = "ERROR")
    })

    cat(glue::glue("Validated profiling complete. Overall: {profile_result$overall_score}\n"))
}

# =============================================================================
# CLEANUP
# =============================================================================
if (DBI::dbIsValid(con)) {
    DBI::dbDisconnect(con)
}

cat("\nStep 6 complete. View validated.* tables or proceed to Step 7: QC Rules.\n")
