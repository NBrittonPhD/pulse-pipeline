# ============================================================
# test_step1_register_source.R
# Unit tests for Step 1: register_source()
# ============================================================

# Ensure tests run from project root
project_root <- rprojroot::find_rstudio_root_file()
setwd(project_root)

library(testthat)
library(DBI)
library(yaml)

con <- connect_to_pulse()
settings <- yaml::read_yaml("config/pipeline_settings.yml")

# ------------------------------------------------------------
# Test 1: reject invalid vocab
# ------------------------------------------------------------
test_that("validate_source_entry rejects invalid vocab", {
  
  bad <- list(
    source_id = "bad001",
    source_name = "Bad Source",
    system_type = "TXT",                   # invalid
    update_frequency = "monthly",
    data_owner = "Owner",
    ingest_method = "pull",
    expected_schema_version = "1.0.0",
    pii_classification = "PHI",
    active = TRUE
  )
  
  expect_error(
    validate_source_entry(bad, settings),
    regexp = "Invalid system_type"
  )
})

# ------------------------------------------------------------
# Test 2: accept valid vocab
# ------------------------------------------------------------
test_that("validate_source_entry accepts correct vocab", {
  
  good <- list(
    source_id = "ok001",
    source_name = "Good Source",
    system_type = "CSV",
    update_frequency = "monthly",
    data_owner = "Owner",
    ingest_method = "pull",
    expected_schema_version = "1.0.0",
    pii_classification = "PHI",
    active = TRUE
  )
  
  expect_silent(
    validate_source_entry(good, settings)
  )
})

# ------------------------------------------------------------
# Test 3: inserting new source writes to governance.source_registry
# ------------------------------------------------------------
test_that("register_source inserts row into governance.source_registry", {
  
  DBI::dbExecute(con, "
      DELETE FROM governance.source_registry
      WHERE source_id = 'unit001';
  ")
  
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

# ------------------------------------------------------------
# Test 4: audit_log entry written for new source
# ------------------------------------------------------------
test_that("register_source writes audit_log entry", {
  
  register_source(
    con                = con,
    source_id          = "unit002",
    source_name        = "Audit Source",
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
    con, "
      SELECT action 
      FROM governance.audit_log
      WHERE action LIKE '%source_registry%'
        AND details::text LIKE '%unit002%'
      ORDER BY executed_at_utc DESC
      LIMIT 1;
    "
  )
  
  expect_true(
    isTRUE(grepl("source_registration", out$action))
  )
})

# ------------------------------------------------------------
# Test 5: wrapper run_step1_register_source logs to pipeline_step
# ------------------------------------------------------------
test_that("run_step1_register_source logs completion to pipeline_step", {
  
  DBI::dbExecute(con, "
      DELETE FROM governance.source_registry
      WHERE source_id = 'unit003';
  ")
  
  source_params <- list(
    source_id = "unit003",
    source_name = "Wrapper Source",
    system_type = "CSV",
    update_frequency = "monthly",
    data_owner = "Tester",
    ingest_method = "pull",
    expected_schema_version = "1.0.0",
    retention_policy = NULL,
    pii_classification = "PHI",
    active = TRUE
  )
  
  run_step1_register_source(con, source_params)
  
  step_log <- DBI::dbGetQuery(
    con,
    "
      SELECT step_id 
      FROM governance.pipeline_step
      WHERE step_id = 'STEP_001'
        AND enabled = TRUE;
    "
  )
  
  expect_equal(step_log$step_id, "STEP_001")
})
