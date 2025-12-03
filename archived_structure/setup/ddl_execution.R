# ------------------------------------------------------------
# PULSE Pipeline DDL Execution Script (Manual Controlled Order)
# Runs DDL files in EXACT order defined below.
# ------------------------------------------------------------

library(DBI)
library(readr)
library(purrr)

source("r/connect_to_pulse.R")
con <- connect_to_pulse()

# ------------------------------------------------------------
# Exact DDL order (schemas first, then your original sequence)
# ------------------------------------------------------------

ddl_execution_order <- c(
  "create_SCHEMAS.sql",             # NEW — must run first
  
  "create_PIPELINE_STEP.sql",
  "create_RULE_LIBRARY.sql",
  "create_RULE_EXECUTION_MAP.sql",
  "create_RULE_EXECUTION_LOG.sql",
  "create_STRUCTURE_QC_TABLE.sql",
  "create_DATA_PROFILE.sql",
  "create_TRANSFORM_LOG.sql",
  "create_METADATA.sql",
  "create_METADATA_HISTORY.sql",
  "create_AUDIT_LOG.sql",
  "create_BATCH_LOG.sql",
  
  "create_INGEST_FILE_LOG.sql",      # NEW — depends on batch_log
  
  "create_SOURCE_REGISTRY.sql",
  "create_RELEASE_LOG.sql"
)

# ------------------------------------------------------------
# Helper to run one SQL file
# ------------------------------------------------------------
run_sql <- function(file) {
  path <- file.path("sql/ddl", file)
  sql  <- read_file(path)
  
  # Split statements on semicolons
  stmts <- unlist(strsplit(sql, ";", fixed = TRUE))
  
  for (s in stmts) {
    clean <- trimws(s)
    if (clean != "") {
      dbExecute(con, paste0(clean, ";"))
    }
  }
  
  message("Executed: ", file)
}

# ------------------------------------------------------------
# Run files in the exact sequence above
# ------------------------------------------------------------

walk(ddl_execution_order, run_sql)

message("=== DDL Completed ===")
print(dbListTables(con))
