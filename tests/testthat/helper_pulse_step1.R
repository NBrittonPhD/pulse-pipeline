# tests/testthat/helper_pulse_step1.R
# ------------------------------------------------------------------------------
# Test helper to load the PULSE Step-1 environment before running tests.
# This runs BEFORE any test_* files in this directory.
# ------------------------------------------------------------------------------

# tests/testthat/helper_pulse_step1.R
message(">> [helper_pulse_step1] Initializing PULSE environment for tests...")

# 1. Find project root (two levels up from tests/testthat/)
proj_root <- normalizePath(
  file.path(testthat::test_path(), "..", ".."),
  mustWork = TRUE
)
message(">> [helper_pulse_step1] proj_root = ", proj_root)

# IMPORTANT: don't rely on setwd; store this in an option instead
options(pulse.proj_root = proj_root)

# 2. Sanity checks for critical files (use proj_root)
stopifnot(file.exists(file.path(proj_root, "config/pipeline_settings.yml")))
stopifnot(file.exists(file.path(proj_root, "directory_structure.yml")))
stopifnot(file.exists(file.path(proj_root, "r/connect_to_pulse.R")))
stopifnot(file.exists(file.path(proj_root, "r/runner.R")))
stopifnot(file.exists(file.path(proj_root, "r/utilities/validate_source_entry.R")))
stopifnot(file.exists(file.path(proj_root, "r/utilities/create_source_folders.R")))
stopifnot(file.exists(file.path(proj_root, "r/utilities/write_pipeline_step.R")))
stopifnot(file.exists(file.path(proj_root, "r/steps/write_audit_event.R")))
stopifnot(file.exists(file.path(proj_root, "r/steps/register_source.R")))
stopifnot(file.exists(file.path(proj_root, "r/steps/run_step1_register_source.R")))

# 3. Load libraries used in Step 1
suppressPackageStartupMessages({
  library(DBI)
  library(glue)
  library(yaml)
  library(fs)
  library(jsonlite)
  library(uuid)
  library(dplyr)
  library(purrr)
})

# 4. Source files using proj_root
source(file.path(proj_root, "r/connect_to_pulse.R"))
source(file.path(proj_root, "r/utilities/validate_source_entry.R"))
source(file.path(proj_root, "r/utilities/create_source_folders.R"))
source(file.path(proj_root, "r/utilities/write_pipeline_step.R"))
source(file.path(proj_root, "r/steps/write_audit_event.R"))
source(file.path(proj_root, "r/steps/register_source.R"))
source(file.path(proj_root, "r/steps/run_step1_register_source.R"))
source(file.path(proj_root, "r/runner.R"))

message(">> [helper_pulse_step1] Step 1 functions loaded for tests.")
