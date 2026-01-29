# =============================================================================
# CLEAR INGEST LOGS
# =============================================================================
# Purpose:
#   Remove all ingest-tracking rows from the governance schema while keeping
#   source_registry and pipeline_step intact. Use this when you want to
#   re-run Steps 2+ without re-registering sources.
#
# Tables Cleared:
#   - governance.structure_qc_table  (Step 3 QC issues)
#   - governance.ingest_file_log     (Step 2 file lineage)
#   - governance.batch_log           (Step 2 batch metadata)
#   - governance.audit_log           (all audit trail entries)
#
# Tables Preserved:
#   - governance.source_registry     (Step 1 source definitions)
#   - governance.pipeline_step       (pipeline step registry)
#
# Inputs:
#   con - active DBI connection to PULSE Postgres
#
# Outputs:
#   Ingest-related governance tables will be truncated (rows removed,
#   structure kept)
#
# Side Effects:
#   Ingest tracking data removed permanently. Tables remain intact.
#
# Deletion Order (respects foreign key constraints):
#   1. governance.structure_qc_table  (FK → batch_log)
#   2. governance.ingest_file_log     (FK → batch_log)
#   3. governance.batch_log           (FK → source_registry — parent kept)
#   4. governance.audit_log           (no FK constraints)
#
# Dependencies:
#   DBI
#
# =============================================================================

library(DBI)

clear_ingest_logs <- function(con) {

  # =========================================================================
  # TABLES TO CLEAR — ordered children-first to respect foreign keys
  # =========================================================================
  tables <- c(
    "governance.structure_qc_table",
    "governance.ingest_file_log",
    "governance.batch_log",
    "governance.audit_log"
  )

  message(">> Clearing ingest logs (keeping source_registry and pipeline_step)...")

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

  message(">> Ingest logs cleared. source_registry and pipeline_step unchanged.")
  invisible(TRUE)
}

# =============================================================================
# Example use:
# =============================================================================
# con <- connect_to_pulse()
# clear_ingest_logs(con)
