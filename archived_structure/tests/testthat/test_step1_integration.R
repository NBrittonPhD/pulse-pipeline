# ============================================================
# test_step1_integration.R
# Integration test: Step 1 via full runner
# ============================================================

project_root <- rprojroot::find_rstudio_root_file()
setwd(project_root)

library(testthat)
library(DBI)
library(yaml)

con <- connect_to_pulse()

test_that("Step 1 executes end-to-end through the runner", {
  
  # force only step 1 enabled
  DBI::dbExecute(con, "
      UPDATE governance.pipeline_step
      SET enabled = (step_id = 'STEP_001');
  ")
  
  # create temporary source params config
  params <- list(
    source_id               = "int001",
    source_name             = "Integration Source",
    system_type             = "CSV",
    update_frequency        = "monthly",
    data_owner              = "Tester",
    ingest_method           = "pull",
    expected_schema_version = "1.0.0",
    retention_policy        = NULL,
    pii_classification      = "PHI",
    active                  = TRUE
  )
  
  yaml::write_yaml(params, "config/source_params.yml")
  
  # run pipeline
  run_pipeline("integration_test_id")
  
  # Check registry populated
  out <- DBI::dbGetQuery(
    con,
    "SELECT source_id FROM governance.source_registry WHERE source_id = 'int001'"
  )
  
  expect_equal(out$source_id, "int001")
  
  # Check folder creation
  expect_true(dir.exists("raw/int001"))
  expect_true(dir.exists("staging/int001"))
  expect_true(dir.exists("validated/int001"))
  
  # Check audit log
  audit <- DBI::dbGetQuery(
    con,
    "
      SELECT action
      FROM governance.audit_log
      WHERE action LIKE '%source_registration%'
        AND details::text LIKE '%int001%'
      ORDER BY executed_at_utc DESC
      LIMIT 1;
    "
  )
  
  expect_true(
    isTRUE(grepl("source_registration", audit$action))
  )
})
