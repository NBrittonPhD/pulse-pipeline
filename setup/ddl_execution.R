ddl_execution_order <- c(
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
  "create_SOURCE_REGISTRY.sql",
  "create_RELEASE_LOG.sql"
)

walk(ddl_execution_order, function(f) {
  path <- file.path("sql/ddl", f)
  message("Running: ", f)
  try(
    dbExecute(con, readr::read_file(path)),
    silent = TRUE
  )
})

dbListTables(con)

