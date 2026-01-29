# =============================================================================
# tests/testthat/test_step2_batch_logging.R
# =============================================================================
# Unit tests for Step 2: Batch Logging & File-Level Lineage
#
# Tests cover:
#   - log_batch_ingest() — batch + file row creation
#   - ingest_batch() — file-level ingestion with lineage updates
#
# Prerequisites:
#   - PULSE database running with governance schema
#   - source_registry must have at least one source
#   - reference.ingest_dictionary must be populated
#
# =============================================================================

source("r/steps/log_batch_ingest.R")
source("r/steps/ingest.R")

# -------------------------------------------------------------------
# log_batch_ingest() tests
# -------------------------------------------------------------------

test_that("log_batch_ingest() creates batch_log and file_log entries", {
  con <- connect_to_pulse()

  dir <- tempdir()
  f1 <- file.path(dir, "file1.csv")
  f2 <- file.path(dir, "file2.csv")

  writeLines(c("x,y", "1,2"), f1)
  writeLines(c("x,y", "3,4"), f2)

  ingest_id   <- paste0("ING_TEST_", format(Sys.time(), "%Y%m%d_%H%M%S"))
  source_id   <- "test_source"
  source_type <- "TEST"

  # Ensure source_id exists in source_registry (required by FK)
  tryCatch(
    DBI::dbExecute(con, glue::glue(
      "INSERT INTO governance.source_registry (source_id, source_name, system_type,
       update_frequency, data_owner, ingest_method, expected_schema_version,
       pii_classification)
       VALUES ('{source_id}', 'Test Source', 'CSV', 'ad_hoc', 'tester',
       'manual', '1.0.0', 'NonPHI')
       ON CONFLICT (source_id) DO NOTHING"
    )),
    error = function(e) message("Source already exists: ", e$message)
  )

  log_batch_ingest(
    con         = con,
    ingest_id   = ingest_id,
    source_id   = source_id,
    source_type = source_type,
    file_paths  = c(f1, f2)
  )

  # Check batch_log entry
  bl <- DBI::dbGetQuery(
    con,
    glue::glue("SELECT * FROM governance.batch_log WHERE ingest_id = '{ingest_id}'")
  )
  expect_equal(nrow(bl), 1)
  expect_equal(bl$status, "started")
  expect_equal(bl$file_count, 2L)

  # Check file_log entries
  fl <- DBI::dbGetQuery(
    con,
    glue::glue("SELECT * FROM governance.ingest_file_log WHERE ingest_id = '{ingest_id}'")
  )
  expect_equal(nrow(fl), 2)
  expect_true(all(fl$load_status == "pending"))

  DBI::dbDisconnect(con)
})

# -------------------------------------------------------------------

test_that("log_batch_ingest() rejects duplicate ingest_id", {
  con <- connect_to_pulse()

  dir <- tempdir()
  f1 <- file.path(dir, "dup_test.csv")
  writeLines(c("a,b", "1,2"), f1)

  ingest_id   <- paste0("ING_DUP_", format(Sys.time(), "%Y%m%d_%H%M%S"))
  source_id   <- "test_source"
  source_type <- "TEST"

  # First call should succeed
  log_batch_ingest(con, ingest_id, source_id, source_type, f1)

  # Second call with same ingest_id should error
  expect_error(
    log_batch_ingest(con, ingest_id, source_id, source_type, f1),
    "already exists"
  )

  DBI::dbDisconnect(con)
})

# -------------------------------------------------------------------
# ingest_batch() tests
# -------------------------------------------------------------------

test_that("ingest_batch() updates file_log status on missing file", {
  con <- connect_to_pulse()

  ingest_id   <- paste0("ING_MISS_", format(Sys.time(), "%Y%m%d_%H%M%S"))
  source_id   <- "test_source"
  source_type <- "TEST"

  # Create one real file and one that won't exist
  dir <- tempdir()
  good_file <- file.path(dir, "good_batch.csv")
  writeLines(c("a,b", "1,2"), good_file)
  missing_file <- file.path(dir, "nonexistent_batch.csv")

  # Log the batch first (creates pending rows)
  log_batch_ingest(con, ingest_id, source_id, source_type, c(good_file, missing_file))

  # Remove the "missing" file if it somehow exists
  if (file.exists(missing_file)) file.remove(missing_file)

  # Run ingestion — missing file should be marked as error
  result <- ingest_batch(con, ingest_id, dir, source_id, source_type)

  expect_equal(result$n_files, 2)
  expect_equal(result$n_error, 1)  # missing file
  expect_true(result$status %in% c("partial", "error"))

  # Verify file_log reflects the error
  fl <- DBI::dbGetQuery(
    con,
    glue::glue(
      "SELECT file_name, load_status FROM governance.ingest_file_log
       WHERE ingest_id = '{ingest_id}' ORDER BY file_name"
    )
  )
  error_row <- fl[fl$file_name == "nonexistent_batch.csv", ]
  expect_equal(error_row$load_status, "error")

  DBI::dbDisconnect(con)
})

# -------------------------------------------------------------------

test_that("ingest_batch() finalizes batch_log status", {
  con <- connect_to_pulse()

  ingest_id   <- paste0("ING_FINAL_", format(Sys.time(), "%Y%m%d_%H%M%S"))
  source_id   <- "test_source"
  source_type <- "TEST"

  dir <- tempdir()
  f1 <- file.path(dir, "final_test.csv")
  writeLines(c("a,b", "1,2"), f1)

  log_batch_ingest(con, ingest_id, source_id, source_type, f1)
  result <- ingest_batch(con, ingest_id, dir, source_id, source_type)

  # Check batch_log was updated
  bl <- DBI::dbGetQuery(
    con,
    glue::glue("SELECT status, batch_completed_at_utc FROM governance.batch_log
                WHERE ingest_id = '{ingest_id}'")
  )
  expect_true(bl$status %in% c("success", "error", "partial"))
  expect_false(is.na(bl$batch_completed_at_utc))

  DBI::dbDisconnect(con)
})
