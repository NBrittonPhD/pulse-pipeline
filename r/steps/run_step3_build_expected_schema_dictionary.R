# =============================================================================
# STEP 3 × CLUSTER 3 — Build Expected Schema Dictionary
# File: r/steps/run_step3_build_expected_schema_dictionary.R
# =============================================================================
# Purpose:
#   Wrapper script for Step 3A — Create the governed, versioned expected
#   schema dictionary based on reference.ingest_dictionary and the current
#   structural metadata in Postgres.
#
#   This wrapper:
#     • Loads user inputs (schema_version, effective dates)
#     • Calls build_expected_schema_dictionary()
#     • Saves the governed output to reference/expected_schema_dictionary.xlsx
#     • Prints clear progress messages for the user
#
# This script does NOT:
#   • Perform schema validation (that is Step 3B)
#   • Write QC artifacts (that is Step 3C)
#   • Orchestrate full Step 3 execution (handled by run_step3_schema_validation.R)
#
# Dependencies:
#   r/reference/build_expected_schema_dictionary.R
#   r/connect_to_pulse.R
#   DBI, writexl, readxl
#
# =============================================================================

library(DBI)
library(dplyr)
library(writexl)
library(readxl)

# Resolve project root for portable paths
proj_root <- getOption("pulse.proj_root", default = ".")

source(file.path(proj_root, "r/utilities/scalar_helpers.R"))
source(file.path(proj_root, "r/connect_to_pulse.R"))
source(file.path(proj_root, "r/reference/build_expected_schema_dictionary.R"))

# ------------------------------
# USER INPUT SECTION — EDIT BELOW
# ------------------------------
schema_version <- "2025.0"
effective_from <- Sys.Date()
effective_to   <- NA

output_path    <- "reference/expected_schema_dictionary.xlsx"
# ------------------------------
# END USER INPUT SECTION
# ------------------------------

message(">> STEP 3A: Building Expected Schema Dictionary...")
message("   - schema_version = ", schema_version)

con <- connect_to_pulse()

expected_schema <- build_expected_schema_dictionary(
  con            = con,
  schema_version = schema_version,
  effective_from = effective_from,
  effective_to   = effective_to
)

message(">> Assembled expected schema dictionary with:")
message("   • ", dplyr::n_distinct(expected_schema$lake_table_name), " lake tables")
message("   • ", nrow(expected_schema), " variables total")
message("   •   (includes variables not yet present in raw.*)")

# Ensure output directory exists
dir.create(dirname(output_path), showWarnings = FALSE, recursive = TRUE)

message(">> Writing governed schema dictionary to:")
message("   ", output_path)
writexl::write_xlsx(expected_schema, path = output_path)

message(">> DONE. Load it anytime with:")
message("   expected_schema <- readxl::read_excel(\"", output_path, "\")")

invisible(expected_schema)
