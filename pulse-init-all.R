# =============================================================================
# pulse-init-all.R
# -----------------------------------------------------------------------------
# Purpose:
#   One-stop initializer for the PULSE database objects used in Step 1 (and
#   the core governance layer for later steps).
#
# What this script does:
#   1. Connects to the PULSE Postgres database (via r/connect_to_pulse.R).
#   2. Creates core schemas (raw, staging, validated, governance, reference).
#   3. Creates all core DDL tables in a sensible order:
#        - SOURCE_REGISTRY
#        - AUDIT_LOG
#        - PIPELINE_STEP
#        - RULE_LIBRARY / RULE_EXECUTION_MAP / RULE_EXECUTION_LOG
#        - METADATA / METADATA_HISTORY / DATA_PROFILE
#        - INGEST_DICTIONARY / INGEST_FILE_LOG / BATCH_LOG
#        - RELEASE_LOG / TRANSFORM_LOG / STRUCTURE_QC_TABLE
#   4. Runs the INSERT scripts for:
#        - RULE_LIBRARY
#        - PIPELINE_STEP
#        - RULE_EXECUTION_MAP
#
# How to use:
#   - From a fresh R session, set working directory to the project root
#     (where pulse-init-all.R lives), then run:
#
#       source("pulse-init-all.R")
#       pulse_init_all()
#
#   - Or just source the file; at the bottom it will auto-run if sourced
#     interactively.
#
# Requirements:
#   - r/connect_to_pulse.R with connect_to_pulse() defined.
#   - sql/ddl/*.sql files present as listed below.
#   - sql/inserts/*.sql files present.
# =============================================================================

library(DBI)
library(readr)

# -------------------------------------------------------------------------
# 1. Load DB connection wrapper
# -------------------------------------------------------------------------
source("r/connect_to_pulse.R")

# -------------------------------------------------------------------------
# 2. Helper to run a single SQL file
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
# 3. Main initializer
# -------------------------------------------------------------------------
pulse_init_all <- function() {
  
  message("=== PULSE INIT: Starting full database initialization ===")
  
  con <- connect_to_pulse()
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # -----------------------------------------------------------------------
  # 3a. Create schemas first
  # -----------------------------------------------------------------------
  run_sql_file(con, "sql/ddl/create_SCHEMAS.sql", "create_SCHEMAS.sql")
  
  # -----------------------------------------------------------------------
  # 3b. Create core tables in a stable, explicit order
  #      (only runs those that actually exist on disk)
  # -----------------------------------------------------------------------
  ddl_order <- c(
    # governance core
    "sql/ddl/create_SOURCE_REGISTRY.sql",
    "sql/ddl/create_AUDIT_LOG.sql",
    "sql/ddl/create_PIPELINE_STEP.sql",
    
    # rule + QC infrastructure
    "sql/ddl/create_RULE_LIBRARY.sql",
    "sql/ddl/create_RULE_EXECUTION_MAP.sql",
    "sql/ddl/create_RULE_EXECUTION_LOG.sql",
    "sql/ddl/create_STRUCTURE_QC_TABLE.sql",
    
    # metadata + profiling
    "sql/ddl/create_METADATA.sql",
    "sql/ddl/create_METADATA_HISTORY.sql",
    "sql/ddl/create_DATA_PROFILE.sql",
    
    # ingest + batch tracking
    "sql/ddl/create_INGEST_DICTIONARY.sql",
    "sql/ddl/create_INGEST_FILE_LOG.sql",
    "sql/ddl/create_BATCH_LOG.sql",
    
    # release + transform lineage
    "sql/ddl/create_RELEASE_LOG.sql",
    "sql/ddl/create_TRANSFORM_LOG.sql"
  )
  
  for (path in ddl_order) {
    if (file.exists(path)) {
      run_sql_file(con, path)
    }
  }
  
  # -----------------------------------------------------------------------
  # 3c. Seed reference / governance tables with INSERT scripts
  #      Order matters for FK relationships:
  #        - RULE_LIBRARY first
  #        - PIPELINE_STEP so maps can refer to step_ids
  #        - RULE_EXECUTION_MAP last
  # -----------------------------------------------------------------------
  insert_order <- c(
    "sql/inserts/insert_RULE_LIBRARY.sql",
    "sql/inserts/insert_PIPELINE_STEP.sql",
    "sql/inserts/insert_RULE_EXECUTION_MAP.sql"
  )
  
  for (path in insert_order) {
    if (file.exists(path)) {
      run_sql_file(con, path)
    }
  }
  
  message("=== PULSE INIT: Database initialization complete ===")
  invisible(TRUE)
}

# -------------------------------------------------------------------------
# 4. Auto-run when sourced interactively
#    (So you can just: source('pulse-init-all.R') in an R console.)
# -------------------------------------------------------------------------
if (sys.nframe() == 0) {
  pulse_init_all()
}