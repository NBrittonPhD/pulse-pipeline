# =============================================================================
# r/explore/run_extract_categorical_values.R
# =============================================================================
# Runner script to extract categorical values from raw.* tables
#
# USAGE:
#   1. Set your environment variables (or they're already set in your session)
#   2. Run this script: source("r/run_extract_categorical_values.R")
#
# OUTPUT:
#   - Console summary
#   - CSV file: output/profiling/categorical_values.csv
#   - CSV file: output/profiling/categorical_values_consolidated.csv
# =============================================================================

# Set project root for relative paths
proj_root <- getwd()
options(pulse.proj_root = proj_root)

# Load required packages
suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(dplyr)
  library(tibble)
  library(purrr)
  library(glue)
  library(stringr)
  library(readr)
})

# Source functions
source(file.path(proj_root, "r/connect_to_pulse.R"))
source(file.path(proj_root, "r/explore/extract_categorical_values.R"))

# =============================================================================
# RUN EXTRACTION
# =============================================================================

cat("\n")
cat("=================================================================\n")
cat("  PULSE Pipeline - Categorical Value Extractor\n")
cat("=================================================================\n")
cat("\n")

# Connect to database
cat(">> Connecting to database...\n")
con <- tryCatch(
  connect_to_pulse(),
  error = function(e) {
    cat("ERROR: Could not connect to database.\n")
    cat("       ", e$message, "\n")
    cat("\n")
    cat("Make sure these environment variables are set:\n")
    cat("  PULSE_DB, PULSE_HOST, PULSE_USER, PULSE_PW\n")
    cat("\n")
    cat("Example:\n")
    cat('  Sys.setenv(PULSE_DB   = "primeai_lake")\n')
    cat('  Sys.setenv(PULSE_HOST = "localhost")\n')
    cat('  Sys.setenv(PULSE_USER = "your_user")\n')
    cat('  Sys.setenv(PULSE_PW   = "your_password")\n')
    return(NULL)
  }
)

if (is.null(con)) {
  stop("Failed to connect to database. Exiting.")
}

# Ensure cleanup on exit
on.exit({
  if (DBI::dbIsValid(con)) {
    DBI::dbDisconnect(con)
    cat(">> Database connection closed.\n")
  }
}, add = TRUE)

cat(">> Connected successfully!\n\n")

# Run extraction
results <- extract_categorical_values(con, verbose = TRUE)

# Print summary to console
print_categorical_summary(results)

# =============================================================================
# EXPORT RESULTS
# =============================================================================

cat("\n")
cat(">> Exporting results...\n")

# Detailed results (one row per variable-table combination)
output_detailed <- file.path(proj_root, "output/profiling/categorical_values.csv")
export_categorical_values(results, output_detailed)

# Consolidated results (one row per variable, values combined)
consolidated <- consolidate_values(results)
output_consolidated <- file.path(proj_root, "output/profiling/categorical_values_consolidated.csv")
readr::write_csv(consolidated, output_consolidated)
cat(">> Exported consolidated view to: ", output_consolidated, "\n")

# =============================================================================
# SUMMARY STATISTICS
# =============================================================================

cat("\n")
cat("=================================================================\n")
cat("  EXTRACTION COMPLETE\n")
cat("=================================================================\n")
cat("\n")
cat("Total variable-table combinations queried: ", nrow(results), "\n")
cat("  - OK (constrained):     ", sum(results$status == "ok"), "\n")
cat("  - Unconstrained (>50):  ", sum(results$status == "unconstrained"), "\n")
cat("  - Column not found:     ", sum(results$status == "column_not_found"), "\n")
cat("  - All NULL values:      ", sum(results$status == "all_null"), "\n")
cat("  - Errors:               ", sum(results$status == "error"), "\n")
cat("\n")
cat("Output files:\n")
cat("  - Detailed:     ", output_detailed, "\n")
cat("  - Consolidated: ", output_consolidated, "\n")
cat("\n")

# Return results invisibly for further use
invisible(list(
  detailed = results,
  consolidated = consolidated
))
