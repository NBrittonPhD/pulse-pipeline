test_that("Step 1 executes end-to-end through run_pipeline()", {
  
  # ---------------------------------------------------------------------------
  # 1. Establish project root (same logic as helper)
  # ---------------------------------------------------------------------------
  proj_root <- testthat::test_path("..", "..")
  
  # ---------------------------------------------------------------------------
  # 2. Write a temporary source_params.yml into config/
  # ---------------------------------------------------------------------------
  params <- list(
    source_id               = "integration_test",
    source_name             = "Integration Test Source",
    system_type             = "CSV",
    update_frequency        = "monthly",
    data_owner              = "Tester",
    ingest_method           = "pull",
    expected_schema_version = "1.0.0",
    retention_policy        = "Raw indefinite",
    pii_classification      = "PHI",
    active                  = TRUE
  )
  
  yaml::write_yaml(
    params,
    file.path(proj_root, "config", "source_params.yml")
  )
  
  # ---------------------------------------------------------------------------
  # 3. Connect to DB and prepare governance tables for the test
  # ---------------------------------------------------------------------------
  con <- connect_to_pulse()
  
  # Disable all steps except STEP_001 so the pipeline
  # only runs Source Registration for this test.
  DBI::dbExecute(
    con,
    "
    UPDATE governance.pipeline_step
    SET enabled = CASE WHEN step_id = 'STEP_001' THEN TRUE ELSE FALSE END;
    "
  )
  
  # Clean out prior test rows (idempotent setup)
  DBI::dbExecute(
    con,
    "DELETE FROM governance.source_registry WHERE source_id = 'integration_test';"
  )
  
  DBI::dbExecute(
    con,
    "DELETE FROM governance.audit_log WHERE ingest_id = 'integration_test_id';"
  )
  
  DBI::dbDisconnect(con)
  
  # ---------------------------------------------------------------------------
  # 4. Run the pipeline for this ingest_id (now only STEP_001 is enabled)
  # ---------------------------------------------------------------------------
  run_pipeline("integration_test_id")
  
  # ---------------------------------------------------------------------------
  # 5. Verify source was registered
  # ---------------------------------------------------------------------------
  con <- connect_to_pulse()
  
  out <- DBI::dbGetQuery(
    con,
    "SELECT * FROM governance.source_registry WHERE source_id = 'integration_test';"
  )
  
  expect_equal(nrow(out), 1L)
  
  # ---------------------------------------------------------------------------
  # 6. Verify at least one audit_log entry mentions this source_id
  # ---------------------------------------------------------------------------
  log <- DBI::dbGetQuery(
    con,
    "
      SELECT *
      FROM governance.audit_log
      WHERE ingest_id = 'integration_test_id'
         OR details::text LIKE '%integration_test%'
    "
  )
  
  expect_gte(nrow(log), 1L)
  
  DBI::dbDisconnect(con)
})
