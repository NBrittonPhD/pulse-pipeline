# =============================================================================
# CLEAR ALL GOVERNANCE LOGS
# =============================================================================
# Purpose:
#   Completely remove all rows from every governance schema table so the
#   pipeline can start fresh from Step 1. This is the nuclear option — it
#   wipes source_registry, audit_log, batch_log, ingest_file_log,
#   structure_qc_table, and pipeline_step.
#
# Inputs:
#   con - active DBI connection to PULSE Postgres
#
# Outputs:
#   All governance tables will be truncated (rows removed, structure kept)
#
# Side Effects:
#   All governance data removed permanently. Tables remain intact.
#
# Deletion Order (respects foreign key constraints):
#   1. governance.structure_qc_table  (FK → batch_log)
#   2. governance.ingest_file_log     (FK → batch_log)
#   3. governance.batch_log           (FK → source_registry)
#   4. governance.audit_log           (no FK constraints)
#   5. governance.pipeline_step       (no FK constraints)
#   6. governance.source_registry     (referenced by batch_log — cleared above)
#
# Dependencies:
#   DBI
#
# =============================================================================

library(DBI)

clear_all_governance_logs <- function(con) {

  # =========================================================================
  # TABLES TO CLEAR — ordered children-first to respect foreign keys
  # =========================================================================
  tables <- c(
    "governance.structure_qc_table",
    "governance.ingest_file_log",
    "governance.batch_log",
    "governance.audit_log",
    "governance.pipeline_step",
    "governance.source_registry"
  )

  message(">> Clearing ALL governance tables...")

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

  message(">> All governance tables cleared.")
  invisible(TRUE)
}

# =============================================================================
# Example use:
# =============================================================================
# con <- connect_to_pulse()
# clear_all_governance_logs(con)
