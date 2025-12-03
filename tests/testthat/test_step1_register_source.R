test_that("validate_source_entry rejects invalid vocab", {
  proj_root <- getOption("pulse.proj_root", default = ".")
  settings <- yaml::read_yaml(file.path(proj_root, "config", "pipeline_settings.yml"))
  
  bad <- list(
    source_id = "bad",
    source_name = "Bad Source",
    system_type = "NOT_A_TYPE",
    update_frequency = "monthly",
    data_owner = "Owner",
    ingest_method = "pull",
    expected_schema_version = "1.0.0",
    retention_policy = NULL,
    pii_classification = "PHI",
    active = TRUE
  )
  
  expect_error(validate_source_entry(bad, settings))
})


test_that("validate_source_entry accepts correct vocab", {
  proj_root <- getOption("pulse.proj_root", default = ".")
  settings <- yaml::read_yaml(file.path(proj_root, "config", "pipeline_settings.yml"))
  
  good <- list(
    source_id = "test001",
    source_name = "Good Source",
    system_type = "CSV",
    update_frequency = "monthly",
    data_owner = "Owner",
    ingest_method = "pull",
    expected_schema_version = "1.0.0",
    retention_policy = NULL,
    pii_classification = "PHI",
    active = TRUE
  )
  
  expect_silent(validate_source_entry(good, settings))
})


test_that("register_source inserts new row into governance.source_registry", {
  con <- connect_to_pulse()
  DBI::dbExecute(con, "DELETE FROM governance.source_registry WHERE source_id='unit001'")
  
  register_source(
    con                = con,
    source_id          = "unit001",
    source_name        = "Unit Test Source",
    system_type        = "CSV",
    update_frequency   = "monthly",
    data_owner         = "Unit Tester",
    ingest_method      = "pull",
    expected_schema_version = "1.0.0",
    retention_policy   = NULL,
    pii_classification = "PHI",
    active             = TRUE,
    created_by         = "unit_tester"
  )
  
  out <- DBI::dbGetQuery(
    con,
    "SELECT source_id FROM governance.source_registry WHERE source_id='unit001'"
  )
  
  expect_equal(out$source_id, "unit001")
})

test_that("register_source writes audit_log entry on INSERT", {
  con <- connect_to_pulse()
  
  DBI::dbExecute(con, "DELETE FROM governance.source_registry WHERE source_id='unit002'")
  DBI::dbExecute(con, "DELETE FROM governance.audit_log WHERE details::text LIKE '%unit002%'")
  
  register_source(
    con                = con,
    source_id          = "unit002",
    source_name        = "Audit Test Source",
    system_type        = "CSV",
    update_frequency   = "monthly",
    data_owner         = "Tester",
    ingest_method      = "pull",
    expected_schema_version = "1.0.0",
    retention_policy   = NULL,
    pii_classification = "PHI",
    active             = TRUE,
    created_by         = "tester"
  )
  
  out <- DBI::dbGetQuery(
    con,
    "
    SELECT action, details
    FROM governance.audit_log
    WHERE details::text LIKE '%unit002%'
    ORDER BY executed_at_utc DESC
    LIMIT 1;
    "
  )
  
  expect_true(grepl("source_registration", out$action))
})

test_that("run_step1_register_source records pipeline_step completion", {
  con <- connect_to_pulse()
  
  # Prepare a unique source ID
  sid <- paste0("unit003_", as.integer(Sys.time()))
  
  source_params <- list(
    source_id               = sid,
    source_name             = "Wrapper Source",
    system_type             = "CSV",
    update_frequency        = "monthly",
    data_owner              = "Tester",
    ingest_method           = "pull",
    expected_schema_version = "1.0.0",
    retention_policy        = NULL,
    pii_classification      = "PHI",
    active                  = TRUE
  )
  
  # Run Step 1 wrapper
  run_step1_register_source(con = con, source_params = source_params)
  
  # Expect source created
  reg <- DBI::dbGetQuery(
    con,
    glue::glue("SELECT source_id FROM governance.source_registry WHERE source_id = '{sid}'")
  )
  expect_equal(reg$source_id, sid)
  
  # Expect audit entry
  aud <- DBI::dbGetQuery(
    con,
    glue::glue("
      SELECT action 
      FROM governance.audit_log
      WHERE details::text LIKE '%{sid}%'
      ORDER BY executed_at_utc DESC
      LIMIT 1
    ")
  )
  expect_true(nrow(aud) == 1)
  expect_true(grepl("source_registration", aud$action))
  
  # Expect pipeline_step entry
  step <- DBI::dbGetQuery(
    con,
    "
      SELECT step_id 
      FROM governance.pipeline_step
      WHERE step_id = 'STEP_001'
        AND enabled = TRUE
    "
  )
  expect_equal(step$step_id, "STEP_001")
})