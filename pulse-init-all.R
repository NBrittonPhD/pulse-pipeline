# =============================================================================
# pulse-init-all.R  (Minimal Step 1 Version)
# -----------------------------------------------------------------------------
# Purpose:
#   Initialize ONLY the schemas + governance tables required for Step 1:
#     - SCHEMAS
#     - SOURCE_REGISTRY
#     - AUDIT_LOG
#     - PIPELINE_STEP
#
# This stripped-down initializer excludes all later-step infrastructure:
#   - QC rule tables (RULE_LIBRARY, RULE_EXECUTION_LOG, etc.)
#   - Metadata structures (METADATA, METADATA_HISTORY)
#   - Profiling tables (DATA_PROFILE)
#   - Ingest/batch structures (INGEST_DICTIONARY, BATCH_LOG)
#   - Release/log lineage structures
#
# This keeps the project clean while Step 1 develops.
# =============================================================================

library(DBI)
library(readr)

# -------------------------------------------------------------------------
# 1. Load DB connection wrapper
# -------------------------------------------------------------------------
source("r/connect_to_pulse.R")

# -------------------------------------------------------------------------
# 2. Helper to run a SQL file
# -------------------------------------------------------------------------
run_sql_file <- function(con, path, label = NULL) {
  if (!file.exists(path)) {
    warning(sprintf("SQL file not found, skipping: %s", path))
    return(invisible(FALSE))
  }
  
  if (is.null(label)) label <- basename(path)
  message(sprintf(">> Running SQL: %s", label))
  
  sql <- readr::read_file(path)
  DBI::dbExecute(con, sql)
  
  invisible(TRUE)
}

# -------------------------------------------------------------------------
# 3. Minimal initializer: ONLY Step 1 dependencies
# -------------------------------------------------------------------------
pulse_init_all <- function() {
  
  message("=== PULSE INIT (Minimal Step 1): Starting initialization ===")
  
  con <- connect_to_pulse()
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # -----------------------------------------------------------------------
  # Create core schemas
  # -----------------------------------------------------------------------
  run_sql_file(con, "sql/ddl/create_SCHEMAS.sql")
  
  # -----------------------------------------------------------------------
  # Create ONLY Step-1 tables
  # -----------------------------------------------------------------------
  ddl_order <- c(
    "sql/ddl/create_SOURCE_REGISTRY.sql",
    "sql/ddl/create_AUDIT_LOG.sql",
    "sql/ddl/create_PIPELINE_STEP.sql"
  )
  
  for (path in ddl_order) {
    if (file.exists(path)) {
      run_sql_file(con, path)
    }
  }
  
  # -----------------------------------------------------------------------
  # Seed Step-1–only INSERT scripts
  #   - RULE_LIBRARY removed
  #   - RULE_EXECUTION_MAP removed
  #   - Only pipeline steps remain
  # -----------------------------------------------------------------------
  
  insert_order <- c(
    "sql/inserts/insert_PIPELINE_STEP.sql"
  )
  
  for (path in insert_order) {
    if (file.exists(path)) {
      run_sql_file(con, path)
    }
  }
  
  message("=== PULSE INIT (Minimal Step 1): Initialization complete ===")
  invisible(TRUE)
}

# -------------------------------------------------------------------------
# 4. Auto-run when sourced interactively
# -------------------------------------------------------------------------
if (sys.nframe() == 0) {
  pulse_init_all()
}
