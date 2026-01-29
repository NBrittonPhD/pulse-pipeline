# =============================================================================
# CLEAR BATCH LOGS (STEP 2+ ONLY)
# =============================================================================
# Purpose:
#   Remove all batch-level ingestion data from Steps 2 and 3 while keeping
#   source_registry, audit_log, and pipeline_step intact. Use this when you
#   want to re-run batch ingestion without losing source definitions or the
#   audit trail.
#
# Tables Cleared:
#   - governance.structure_qc_table  (Step 3 QC issues)
#   - governance.ingest_file_log     (Step 2 file lineage)
#   - governance.batch_log           (Step 2 batch metadata)
#
# Tables Preserved:
#   - governance.source_registry     (Step 1 source definitions)
#   - governance.audit_log           (governance audit trail)
#   - governance.pipeline_step       (pipeline step registry)
#
# Inputs:
#   con - active DBI connection to PULSE Postgres
#
# Outputs:
#   Batch-related governance tables will be truncated (rows removed,
#   structure kept)
#
# Side Effects:
#   Batch tracking data removed permanently. Tables remain intact.
#
# Deletion Order (respects foreign key constraints):
#   1. governance.structure_qc_table  (FK → batch_log)
#   2. governance.ingest_file_log     (FK → batch_log)
#   3. governance.batch_log           (FK → source_registry — parent kept)
#
# Dependencies:
#   DBI
#
# =============================================================================

library(DBI)

clear_batch_logs <- function(con) {

  # =========================================================================
  # TABLES TO CLEAR — ordered children-first to respect foreign keys
  # =========================================================================
  tables <- c(
    "governance.structure_qc_table",
    "governance.ingest_file_log",
    "governance.batch_log"
  )

  message(">> Clearing batch logs (keeping source_registry, audit_log, and pipeline_step)...")

  for (tbl in tables) {

    # Check if table exists before attempting to truncate
    schema_tbl <- strsplit(tbl, "\\.")[[1]]
    exists <- dbGetQuery(con, sprintf(
      "SELECT COUNT(*) AS n FROM information_schema.tables
       WHERE table_schema = '%s' AND table_name = '%s';",
      schema_tbl[1], schema_tbl[2]
    ))$n > 0

    if (!exists) {
      message("   - SKIP (not found): ", tbl)
      next
    }

    row_count <- dbGetQuery(con, sprintf("SELECT COUNT(*) AS n FROM %s;", tbl))$n
    sql <- sprintf("TRUNCATE %s CASCADE;", tbl)
    message("   - ", sql, "  (", row_count, " rows removed)")
    dbExecute(con, sql)
  }

  message(">> Batch logs cleared. source_registry, audit_log, and pipeline_step unchanged.")
  invisible(TRUE)
}

# =============================================================================
# Example use:
# =============================================================================
# con <- connect_to_pulse()
# clear_batch_logs(con)
