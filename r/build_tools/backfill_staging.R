# =============================================================================
# 2b_backfill_staging.R
# Step 2B — One-Time Backfill: Promote raw.* Tables to staging.*
# =============================================================================
# This script promotes raw tables to staging using SQL-based type casting.
# It reads target types from the type_decision_table and builds
# CREATE TABLE staging.<table> AS SELECT CAST(...) FROM raw.<table>.
#
# HOW TO USE:
#   1. Open this file: r/scripts/2b_backfill_staging.R
#   2. Set mode below ("missing_only" or "all")
#   3. Save the file.
#   4. Run:
#        source("r/scripts/2b_backfill_staging.R")
#
# This will:
#   - Connect to Postgres
#   - Identify raw tables missing from staging (or all, depending on mode)
#   - Load type_decision_table.xlsx for target types
#   - Promote each table using SQL CAST
#   - Write an audit event
#   - Print summary
#
# =============================================================================


# ------------------------------
# USER INPUT SECTION — EDIT BELOW
# ------------------------------

# Mode:
#   "missing_only" = only promote raw tables that do NOT yet exist in staging
#   "all"          = re-promote ALL raw tables (drops + recreates staging tables)
mode <- "missing_only"   # EDIT ME

# ------------------------------
# END USER INPUT SECTION
# ------------------------------


# ------------------------------
# Initialize PULSE system
# ------------------------------
source("pulse-init-all.R")

# Load promotion utility
source("r/build_tools/promote_to_staging.R")

# Load audit writer
source("r/steps/write_audit_event.R")

# Connect to DB
con <- connect_to_pulse()

# --------------------------------------------------------------------------
# 1. Discover raw and staging tables
# --------------------------------------------------------------------------
raw_tables <- DBI::dbGetQuery(con, "
  SELECT table_name
    FROM information_schema.tables
   WHERE table_schema = 'raw'
     AND table_type   = 'BASE TABLE'
   ORDER BY table_name
")$table_name

staging_tables <- DBI::dbGetQuery(con, "
  SELECT table_name
    FROM information_schema.tables
   WHERE table_schema = 'staging'
     AND table_type   = 'BASE TABLE'
   ORDER BY table_name
")$table_name

message(">> Raw tables found:     ", length(raw_tables))
message(">> Staging tables found: ", length(staging_tables))

# --------------------------------------------------------------------------
# 2. Determine which tables to promote
# --------------------------------------------------------------------------
if (mode == "missing_only") {
  tables_to_promote <- setdiff(raw_tables, staging_tables)
  message(">> Mode: missing_only")
  message(">> Tables to promote:   ", length(tables_to_promote))
} else if (mode == "all") {
  tables_to_promote <- raw_tables
  message(">> Mode: all (re-promoting every raw table)")
  message(">> Tables to promote:   ", length(tables_to_promote))
} else {
  stop(glue::glue("Invalid mode: '{mode}'. Use 'missing_only' or 'all'."))
}

if (length(tables_to_promote) == 0) {
  message(">> Nothing to promote. All raw tables already exist in staging.")
  DBI::dbDisconnect(con)
  stop("No tables to promote — exiting.", call. = FALSE)
}

message("\n>> Tables queued for promotion:")
for (t in tables_to_promote) {
  message("   - ", t)
}

# --------------------------------------------------------------------------
# 3. Load type_decision_table from Excel
# --------------------------------------------------------------------------
td_path <- "reference/type_decisions/type_decision_table.xlsx"

if (!file.exists(td_path)) {
  DBI::dbDisconnect(con)
  stop(glue::glue("Type decision table not found: {td_path}"))
}

type_decisions <- readxl::read_excel(td_path)
names(type_decisions) <- tolower(names(type_decisions))

# Determine table column name (handle both naming conventions)
tbl_col_name <- if ("lake_table_name" %in% names(type_decisions)) "lake_table_name" else "table_name"
message(">> Type decisions loaded: ", nrow(type_decisions), " rows across ",
        length(unique(type_decisions[[tbl_col_name]])), " tables")

# --------------------------------------------------------------------------
# 4. Promote each table
# --------------------------------------------------------------------------
results <- list()

for (tbl in tables_to_promote) {
  message("\n>> Promoting raw.", tbl, " -> staging.", tbl, " ...")

  res <- promote_to_staging(
    con             = con,
    lake_table_name = tbl,
    type_decisions  = type_decisions
  )

  results[[tbl]] <- res

  if (res$status == "promoted") {
    message("   OK: ", res$n_rows, " rows, ",
            res$n_columns, " columns (",
            res$n_typed, " typed, ",
            res$n_columns - res$n_typed, " TEXT)")
  } else {
    message("   ERROR: ", res$error_message)
  }
}

# --------------------------------------------------------------------------
# 5. Write audit event
# --------------------------------------------------------------------------
n_ok    <- sum(vapply(results, function(r) r$status == "promoted", logical(1)))
n_err   <- sum(vapply(results, function(r) r$status == "error",    logical(1)))

tryCatch({
  write_audit_event(
    con         = con,
    event_type  = "staging_backfill",
    object_type = "schema",
    object_name = "staging",
    details     = list(
      mode              = mode,
      tables_attempted  = length(tables_to_promote),
      tables_promoted   = n_ok,
      tables_errored    = n_err,
      tables            = names(results)
    ),
    status = if (n_err == 0) "success" else "partial"
  )
  message("\n>> Audit event written.")
},
error = function(e) {
  message(">> WARNING: Could not write audit event: ", conditionMessage(e))
})

# --------------------------------------------------------------------------
# 6. Summary
# --------------------------------------------------------------------------
message("\n==============================")
message("  BACKFILL STAGING SUMMARY")
message("==============================")
message("Mode:               ", mode)
message("Tables attempted:   ", length(tables_to_promote))
message(" - Promoted:        ", n_ok)
message(" - Errors:          ", n_err)

if (n_err > 0) {
  message("\nFailed tables:")
  for (tbl in names(results)) {
    if (results[[tbl]]$status == "error") {
      message("  - ", tbl, ": ", results[[tbl]]$error_message)
    }
  }
}

# Final staging count
final_staging <- DBI::dbGetQuery(con, "
  SELECT COUNT(*) AS n
    FROM information_schema.tables
   WHERE table_schema = 'staging'
     AND table_type   = 'BASE TABLE'
")$n
message("\nStaging tables now:  ", final_staging)
message("==============================\n")

DBI::dbDisconnect(con)
message(">> Backfill complete.")
