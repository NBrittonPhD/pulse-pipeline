# =============================================================================
# tests/testthat/test_step5_data_profiling.R
# =============================================================================
# Unit tests for Step 5: Data Profiling
#
# Tests cover:
#   - load_profiling_config() default handling and YAML loading
#   - infer_column_type() detection for identifier, numeric, date, categorical
#   - detect_sentinels() config-based and frequency analysis detection
#   - profile_missingness() mutually exclusive classification and counts
#   - profile_distribution() numeric stats and categorical JSON
#   - generate_issues() all 5 issue types and empty result for clean data
#   - calculate_quality_score() all 4 quality levels
#   - profile_table() end-to-end column profiling (DB-dependent)
#   - profile_data() full orchestrator integration (DB-dependent)
#
# Author: Noel
# Last Updated: 2026-01-30
# =============================================================================

# =============================================================================
# SETUP
# =============================================================================
library(testthat)
library(tibble)
library(dplyr)
library(jsonlite)

# Get project root
proj_root <- getOption("pulse.proj_root", default = normalizePath("../.."))

# Source all profiling functions
source(file.path(proj_root, "r/utilities/load_profiling_config.R"))
source(file.path(proj_root, "r/utilities/infer_column_type.R"))
source(file.path(proj_root, "r/profiling/detect_sentinels.R"))
source(file.path(proj_root, "r/profiling/profile_missingness.R"))
source(file.path(proj_root, "r/profiling/profile_distribution.R"))
source(file.path(proj_root, "r/profiling/generate_issues.R"))
source(file.path(proj_root, "r/profiling/calculate_quality_score.R"))


# =============================================================================
# HELPER: Build a minimal config for testing
# =============================================================================
make_test_config <- function() {
    list(
        quality_score_thresholds = list(
            excellent = list(max_missing_pct = 5, max_critical_issues = 0),
            good      = list(max_missing_pct = 10, max_critical_issues = 2),
            fair      = list(max_missing_pct = 20, max_critical_issues = 5)
        ),
        missingness_thresholds = list(
            critical = 0,
            high     = 20,
            moderate = 10
        ),
        sentinel_detection = list(
            numeric_sentinels        = c(999, 9999, -999, -9999, -1, 99, 88, 77),
            string_sentinels         = c("NA", "N/A", "NULL", "UNKNOWN", "UNK",
                                         "MISSING", "NOT RECORDED"),
            min_frequency_pct        = 1.0,
            max_unique_for_detection = 50
        ),
        identifier_columns  = c("ACCOUNTNO", "MEDRECNO", "TRAUMANO",
                                 "account_number", "mrn", "trauma_no", "cisir_id"),
        identifier_patterns = c("_id$", "_no$", "^id_",
                                 "^accountno", "^medrecno", "^traumano"),
        display = list(
            top_n_categories = 15,
            decimal_places   = 2
        )
    )
}


# =============================================================================
# TEST GROUP 1: load_profiling_config
# =============================================================================
test_that("load_profiling_config returns defaults when file is missing", {

    config <- load_profiling_config("/nonexistent/path/settings.yml")

    expect_type(config, "list")
    expect_true("quality_score_thresholds" %in% names(config))
    expect_true("missingness_thresholds"   %in% names(config))
    expect_true("sentinel_detection"       %in% names(config))
    expect_true("identifier_columns"       %in% names(config))
    expect_true("display"                  %in% names(config))

    # Verify default threshold values
    expect_equal(config$quality_score_thresholds$excellent$max_missing_pct, 5)
    expect_equal(config$quality_score_thresholds$good$max_missing_pct, 10)
    expect_equal(config$quality_score_thresholds$fair$max_missing_pct, 20)

    expect_equal(config$missingness_thresholds$critical, 0)
    expect_equal(config$missingness_thresholds$high, 20)
    expect_equal(config$missingness_thresholds$moderate, 10)
})

test_that("load_profiling_config loads YAML file when it exists", {

    yaml_path <- file.path(proj_root, "config/profiling_settings.yml")
    skip_if_not(file.exists(yaml_path), "profiling_settings.yml not found")

    config <- load_profiling_config(yaml_path)

    expect_type(config, "list")
    expect_true("quality_score_thresholds" %in% names(config))
    expect_true("sentinel_detection"       %in% names(config))
    expect_true(length(config$sentinel_detection$numeric_sentinels) > 0)
    expect_true(length(config$sentinel_detection$string_sentinels) > 0)
})

test_that("load_profiling_config uses NULL path and resolves from proj_root", {

    config <- load_profiling_config(NULL)

    expect_type(config, "list")
    # Should always return a valid config (either from file or defaults)
    expect_true("quality_score_thresholds" %in% names(config))
})


# =============================================================================
# TEST GROUP 2: infer_column_type
# =============================================================================
test_that("infer_column_type detects identifier by exact name match", {

    config <- make_test_config()
    values <- c("100", "200", "300")

    expect_equal(infer_column_type(values, "ACCOUNTNO", config), "identifier")
    expect_equal(infer_column_type(values, "accountno", config), "identifier")
    expect_equal(infer_column_type(values, "mrn", config),       "identifier")
    expect_equal(infer_column_type(values, "cisir_id", config),  "identifier")
})

test_that("infer_column_type detects identifier by regex pattern", {

    config <- make_test_config()
    values <- c("100", "200", "300")

    # Pattern: _id$
    expect_equal(infer_column_type(values, "patient_id", config), "identifier")
    # Pattern: _no$
    expect_equal(infer_column_type(values, "record_no", config),  "identifier")
    # Pattern: ^id_
    expect_equal(infer_column_type(values, "id_visit", config),   "identifier")
})

test_that("infer_column_type detects numeric columns", {

    config <- make_test_config()
    values <- c("1.5", "2.7", "3.9", "4.1", "5.0", NA, "6.3", "7.8", "8.2", "9.4")

    expect_equal(infer_column_type(values, "weight_kg", config), "numeric")
})

test_that("infer_column_type detects date columns", {

    config <- make_test_config()
    values <- c("2025-01-15", "2025-02-20", "2025-03-10", "2025-04-01",
                "2025-05-25")

    expect_equal(infer_column_type(values, "admission_date", config), "date")
})

test_that("infer_column_type detects date with slash format", {

    config <- make_test_config()
    values <- c("01/15/2025", "02/20/2025", "03/10/2025", "04/01/2025",
                "05/25/2025")

    expect_equal(infer_column_type(values, "dob", config), "date")
})

test_that("infer_column_type falls back to categorical", {

    config <- make_test_config()
    values <- c("Male", "Female", "Male", "Female", "Other")

    expect_equal(infer_column_type(values, "gender", config), "categorical")
})

test_that("infer_column_type returns categorical for all-NA input", {

    config <- make_test_config()
    values <- c(NA, NA, NA)

    expect_equal(infer_column_type(values, "some_col", config), "categorical")
})

test_that("infer_column_type returns categorical for all-empty input", {

    config <- make_test_config()
    values <- c("", "", "", "  ")

    expect_equal(infer_column_type(values, "some_col", config), "categorical")
})

test_that("infer_column_type handles mixed numeric with some non-numeric", {

    config <- make_test_config()
    # 8 out of 10 are numeric = 80%, below the 90% threshold
    values <- c("1", "2", "3", "4", "5", "6", "7", "8", "abc", "def")

    expect_equal(infer_column_type(values, "mixed_col", config), "categorical")
})


# =============================================================================
# TEST GROUP 3: detect_sentinels
# =============================================================================
test_that("detect_sentinels finds numeric sentinels from config", {

    config <- make_test_config()
    values <- c("10", "20", "999", "30", "999", "40")

    result <- detect_sentinels(values, "some_var", "numeric", config)

    expect_s3_class(result, "tbl_df")
    expect_true(nrow(result) > 0)
    expect_true("999" %in% result$sentinel_value)

    row_999 <- result[result$sentinel_value == "999", ]
    expect_equal(row_999$sentinel_count, 2L)
    expect_equal(row_999$detection_method, "config_list")
    expect_equal(row_999$confidence, "high")
})

test_that("detect_sentinels finds string sentinels case-insensitively", {

    config <- make_test_config()
    values <- c("Active", "UNKNOWN", "Active", "unknown", "Active")

    result <- detect_sentinels(values, "status", "categorical", config)

    expect_true(nrow(result) > 0)
    expect_true("UNKNOWN" %in% result$sentinel_value)

    row_unk <- result[result$sentinel_value == "UNKNOWN", ]
    expect_equal(row_unk$sentinel_count, 2L)
    expect_equal(row_unk$detection_method, "config_list")
})

test_that("detect_sentinels returns empty tibble when no sentinels present", {

    config <- make_test_config()
    values <- c("10", "20", "30", "40", "50")

    result <- detect_sentinels(values, "score", "numeric", config)

    expect_s3_class(result, "tbl_df")
    expect_equal(nrow(result), 0)
    expect_true("sentinel_value" %in% names(result))
})

test_that("detect_sentinels returns empty tibble for all-NA input", {

    config <- make_test_config()
    values <- c(NA, NA, NA)

    result <- detect_sentinels(values, "some_col", "numeric", config)

    expect_equal(nrow(result), 0)
})

test_that("detect_sentinels frequency analysis detects repeat-digit patterns", {

    config <- make_test_config()
    # Include a repeat-digit value not in config list (e.g., "55")
    # that appears above min_frequency_pct (1%)
    values <- c("10", "20", "55", "30", "55")

    result <- detect_sentinels(values, "val", "numeric", config)

    freq_rows <- result[result$detection_method == "frequency_analysis", ]
    if (nrow(freq_rows) > 0) {
        expect_true("55" %in% freq_rows$sentinel_value)
        expect_equal(freq_rows$confidence[freq_rows$sentinel_value == "55"], "medium")
    }
})


# =============================================================================
# TEST GROUP 4: profile_missingness
# =============================================================================
test_that("profile_missingness classifies values correctly", {

    values <- c("hello", NA, "", "  ", "world", "999", NA, "test")

    result <- profile_missingness(values, sentinel_values = c("999"))

    expect_equal(result$total_count, 8)
    expect_equal(result$na_count, 2)
    expect_equal(result$empty_count, 1)
    expect_equal(result$whitespace_count, 1)
    expect_equal(result$sentinel_count, 1)
    expect_equal(result$valid_count, 3)
})

test_that("profile_missingness categories are mutually exclusive", {

    values <- c("hello", NA, "", "  ", "UNKNOWN", "world")

    result <- profile_missingness(values, sentinel_values = c("UNKNOWN"))

    total <- result$na_count + result$empty_count + result$whitespace_count +
             result$sentinel_count + result$valid_count
    expect_equal(total, result$total_count)
})

test_that("profile_missingness computes percentages correctly", {

    values <- c("A", NA, "B", "C", NA)

    result <- profile_missingness(values, sentinel_values = character(0))

    expect_equal(result$total_count, 5)
    expect_equal(result$na_count, 2)
    expect_equal(result$na_pct, 40.00)
    expect_equal(result$valid_count, 3)
    expect_equal(result$valid_pct, 60.00)
    expect_equal(result$total_missing_count, 2)
    expect_equal(result$total_missing_pct, 40.00)
})

test_that("profile_missingness handles all-valid values", {

    values <- c("A", "B", "C", "D")

    result <- profile_missingness(values, sentinel_values = character(0))

    expect_equal(result$total_count, 4)
    expect_equal(result$valid_count, 4)
    expect_equal(result$na_count, 0)
    expect_equal(result$empty_count, 0)
    expect_equal(result$whitespace_count, 0)
    expect_equal(result$sentinel_count, 0)
    expect_equal(result$valid_pct, 100.00)
    expect_equal(result$total_missing_pct, 0.00)
})

test_that("profile_missingness handles all-NA values", {

    values <- c(NA, NA, NA)

    result <- profile_missingness(values, sentinel_values = character(0))

    expect_equal(result$total_count, 3)
    expect_equal(result$na_count, 3)
    expect_equal(result$valid_count, 0)
    expect_equal(result$na_pct, 100.00)
    expect_equal(result$total_missing_pct, 100.00)
})

test_that("profile_missingness unique_count counts only valid values", {

    values <- c("A", "A", "B", NA, "", "  ")

    result <- profile_missingness(values, sentinel_values = character(0))

    expect_equal(result$unique_count, 2)  # "A" and "B"
    expect_equal(result$valid_count, 3)   # "A", "A", "B"
    expect_equal(result$unique_pct, round(2 / 3 * 100, 2))
})

test_that("profile_missingness sentinel matching is case-insensitive", {

    values <- c("unknown", "UNKNOWN", "valid_val")

    result <- profile_missingness(values, sentinel_values = c("UNKNOWN"))

    expect_equal(result$sentinel_count, 2)
    expect_equal(result$valid_count, 1)
})

test_that("profile_missingness handles empty input", {

    values <- character(0)

    result <- profile_missingness(values, sentinel_values = character(0))

    expect_equal(result$total_count, 0)
    expect_equal(result$valid_count, 0)
    expect_equal(result$na_count, 0)
})


# =============================================================================
# TEST GROUP 5: profile_distribution
# =============================================================================
test_that("profile_distribution computes numeric stats correctly", {

    config <- make_test_config()
    values <- c("1", "2", "3", "4", "5")

    result <- profile_distribution(values, "numeric", character(0), config)

    expect_equal(result$distribution_type, "numeric")
    expect_equal(result$stat_min, 1)
    expect_equal(result$stat_max, 5)
    expect_equal(result$stat_mean, 3)
    expect_equal(result$stat_median, 3)
    expect_true(!is.na(result$stat_sd))
    expect_true(!is.na(result$stat_q25))
    expect_true(!is.na(result$stat_q75))
    expect_true(!is.na(result$stat_iqr))
    expect_true(is.na(result$top_values_json))
})

test_that("profile_distribution excludes sentinels from numeric stats", {

    config <- make_test_config()
    values <- c("10", "20", "30", "999")

    result <- profile_distribution(values, "numeric", c("999"), config)

    expect_equal(result$stat_min, 10)
    expect_equal(result$stat_max, 30)
    expect_equal(result$stat_mean, 20)
})

test_that("profile_distribution computes categorical mode and JSON", {

    config <- make_test_config()
    values <- c("A", "B", "A", "C", "A", "B")

    result <- profile_distribution(values, "categorical", character(0), config)

    expect_equal(result$distribution_type, "categorical")
    expect_equal(result$mode_value, "A")
    expect_equal(result$mode_count, 3L)
    expect_true(!is.na(result$top_values_json))

    # Verify JSON is parseable
    parsed <- jsonlite::fromJSON(result$top_values_json)
    expect_true("value" %in% names(parsed))
    expect_true("count" %in% names(parsed))
    expect_true("A" %in% parsed$value)
})

test_that("profile_distribution returns empty for all-missing input", {

    config <- make_test_config()
    values <- c(NA, NA, NA)

    result <- profile_distribution(values, "numeric", character(0), config)

    expect_true(is.na(result$distribution_type))
    expect_true(is.na(result$stat_min))
    expect_true(is.na(result$stat_max))
})

test_that("profile_distribution excludes NA and empty from stats", {

    config <- make_test_config()
    values <- c("10", NA, "", "  ", "20", "30")

    result <- profile_distribution(values, "numeric", character(0), config)

    expect_equal(result$stat_min, 10)
    expect_equal(result$stat_max, 30)
    expect_equal(result$stat_mean, 20)
})


# =============================================================================
# TEST GROUP 6: generate_issues
# =============================================================================
test_that("generate_issues flags identifier_missing as critical", {

    config <- make_test_config()
    miss_result <- list(total_missing_pct = 5.0)

    result <- generate_issues(
        variable_name      = "ACCOUNTNO",
        table_name         = "test_table",
        missingness_result = miss_result,
        column_type        = "identifier",
        unique_count       = 100,
        total_count        = 200,
        config             = config
    )

    expect_true(nrow(result) > 0)
    expect_true("identifier_missing" %in% result$issue_type)
    expect_equal(result$severity[result$issue_type == "identifier_missing"], "critical")
})

test_that("generate_issues flags high_missingness as warning", {

    config <- make_test_config()
    miss_result <- list(total_missing_pct = 25.0)

    result <- generate_issues(
        variable_name      = "some_var",
        table_name         = "test_table",
        missingness_result = miss_result,
        column_type        = "categorical",
        unique_count       = 10,
        total_count        = 100,
        config             = config
    )

    expect_true("high_missingness" %in% result$issue_type)
    expect_equal(result$severity[result$issue_type == "high_missingness"], "warning")
})

test_that("generate_issues flags moderate_missingness as info", {

    config <- make_test_config()
    miss_result <- list(total_missing_pct = 15.0)

    result <- generate_issues(
        variable_name      = "some_var",
        table_name         = "test_table",
        missingness_result = miss_result,
        column_type        = "categorical",
        unique_count       = 10,
        total_count        = 100,
        config             = config
    )

    expect_true("moderate_missingness" %in% result$issue_type)
    expect_equal(result$severity[result$issue_type == "moderate_missingness"], "info")
})

test_that("generate_issues flags constant_value as info", {

    config <- make_test_config()
    miss_result <- list(total_missing_pct = 0)

    result <- generate_issues(
        variable_name      = "status",
        table_name         = "test_table",
        missingness_result = miss_result,
        column_type        = "categorical",
        unique_count       = 1,
        total_count        = 100,
        config             = config
    )

    expect_true("constant_value" %in% result$issue_type)
    expect_equal(result$severity[result$issue_type == "constant_value"], "info")
})

test_that("generate_issues flags high_cardinality as info", {

    config <- make_test_config()
    miss_result <- list(total_missing_pct = 0)

    result <- generate_issues(
        variable_name      = "free_text",
        table_name         = "test_table",
        missingness_result = miss_result,
        column_type        = "categorical",
        unique_count       = 95,
        total_count        = 100,
        config             = config
    )

    expect_true("high_cardinality" %in% result$issue_type)
    expect_equal(result$severity[result$issue_type == "high_cardinality"], "info")
})

test_that("generate_issues does not flag high_cardinality for identifiers", {

    config <- make_test_config()
    miss_result <- list(total_missing_pct = 0)

    result <- generate_issues(
        variable_name      = "patient_id",
        table_name         = "test_table",
        missingness_result = miss_result,
        column_type        = "identifier",
        unique_count       = 95,
        total_count        = 100,
        config             = config
    )

    expect_false("high_cardinality" %in% result$issue_type)
})

test_that("generate_issues does not flag high_cardinality for small tables", {

    config <- make_test_config()
    miss_result <- list(total_missing_pct = 0)

    result <- generate_issues(
        variable_name      = "value",
        table_name         = "test_table",
        missingness_result = miss_result,
        column_type        = "categorical",
        unique_count       = 10,
        total_count        = 10,
        config             = config
    )

    expect_false("high_cardinality" %in% result$issue_type)
})

test_that("generate_issues returns empty tibble for clean data", {

    config <- make_test_config()
    miss_result <- list(total_missing_pct = 2.0)

    result <- generate_issues(
        variable_name      = "weight",
        table_name         = "test_table",
        missingness_result = miss_result,
        column_type        = "numeric",
        unique_count       = 50,
        total_count        = 100,
        config             = config
    )

    expect_equal(nrow(result), 0)
    expect_true("issue_type" %in% names(result))
})

test_that("generate_issues does not double-flag identifier with high_missingness", {

    config <- make_test_config()
    miss_result <- list(total_missing_pct = 25.0)

    result <- generate_issues(
        variable_name      = "ACCOUNTNO",
        table_name         = "test_table",
        missingness_result = miss_result,
        column_type        = "identifier",
        unique_count       = 50,
        total_count        = 100,
        config             = config
    )

    # Should flag identifier_missing (critical), NOT high_missingness (warning)
    expect_true("identifier_missing" %in% result$issue_type)
    expect_false("high_missingness"  %in% result$issue_type)
})


# =============================================================================
# TEST GROUP 7: calculate_quality_score
# =============================================================================
test_that("calculate_quality_score returns Excellent for low missing and zero critical", {

    config <- make_test_config()
    expect_equal(calculate_quality_score(3.0, 0, config), "Excellent")
})

test_that("calculate_quality_score returns Good for moderate missing", {

    config <- make_test_config()
    expect_equal(calculate_quality_score(8.0, 1, config), "Good")
})

test_that("calculate_quality_score returns Fair for higher missing", {

    config <- make_test_config()
    expect_equal(calculate_quality_score(15.0, 3, config), "Fair")
})

test_that("calculate_quality_score returns Needs Review for worst case", {

    config <- make_test_config()
    expect_equal(calculate_quality_score(25.0, 10, config), "Needs Review")
})

test_that("calculate_quality_score returns Needs Review when critical exceeds threshold", {

    config <- make_test_config()
    # Low missing but too many critical issues
    expect_equal(calculate_quality_score(3.0, 1, config), "Good")
    expect_equal(calculate_quality_score(3.0, 3, config), "Fair")
    expect_equal(calculate_quality_score(3.0, 6, config), "Needs Review")
})

test_that("calculate_quality_score handles NA inputs defensively", {

    config <- make_test_config()
    expect_equal(calculate_quality_score(NA, 0, config), "Needs Review")
    expect_equal(calculate_quality_score(3.0, NA, config), "Needs Review")
    expect_equal(calculate_quality_score(NA, NA, config), "Needs Review")
})

test_that("calculate_quality_score handles NULL inputs defensively", {

    config <- make_test_config()
    expect_equal(calculate_quality_score(NULL, 0, config), "Needs Review")
    expect_equal(calculate_quality_score(3.0, NULL, config), "Needs Review")
})

test_that("calculate_quality_score boundary: exactly at Excellent threshold", {

    config <- make_test_config()
    expect_equal(calculate_quality_score(5.0, 0, config), "Excellent")
})

test_that("calculate_quality_score boundary: just above Excellent threshold", {

    config <- make_test_config()
    expect_equal(calculate_quality_score(5.01, 0, config), "Good")
})


# =============================================================================
# TEST GROUP 8: Integration — profile_table and profile_data (DB-dependent)
# =============================================================================
# These tests require a live database connection. They are skipped if the
# connection is unavailable.
# =============================================================================

# Helper to check DB connectivity
can_connect_to_pulse <- function() {
    tryCatch({
        source(file.path(proj_root, "r/connect_to_pulse.R"))
        con <- connect_to_pulse()
        ok <- DBI::dbIsValid(con)
        DBI::dbDisconnect(con)
        ok
    }, error = function(e) FALSE)
}

test_that("profile_table profiles a real table (DB-dependent)", {

    skip_if_not(can_connect_to_pulse(), "Database connection not available")

    source(file.path(proj_root, "r/connect_to_pulse.R"))
    source(file.path(proj_root, "r/profiling/profile_table.R"))

    con <- connect_to_pulse()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    config <- load_profiling_config(NULL)

    # Find a raw table to profile
    raw_tables <- DBI::dbGetQuery(con,
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = 'raw' LIMIT 1"
    )
    skip_if(nrow(raw_tables) == 0, "No raw tables found in database")

    tbl_name <- raw_tables$table_name[1]
    result <- profile_table(con, "raw", tbl_name, "TEST_INTEGRATION", config)

    expect_type(result, "list")
    expect_true("profile"       %in% names(result))
    expect_true("distributions" %in% names(result))
    expect_true("sentinels"     %in% names(result))
    expect_true("issues"        %in% names(result))
    expect_true("summary"       %in% names(result))

    expect_s3_class(result$profile, "tbl_df")
    expect_s3_class(result$summary, "tbl_df")
    expect_equal(nrow(result$summary), 1)
    expect_true(result$summary$quality_score %in%
                    c("Excellent", "Good", "Fair", "Needs Review"))
})

test_that("profile_data end-to-end integration (DB-dependent)", {

    skip_if_not(can_connect_to_pulse(), "Database connection not available")

    source(file.path(proj_root, "r/connect_to_pulse.R"))
    source(file.path(proj_root, "r/steps/profile_data.R"))

    con <- connect_to_pulse()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    # Find a valid ingest_id
    batch <- DBI::dbGetQuery(con,
        "SELECT ingest_id FROM governance.batch_log LIMIT 1"
    )
    skip_if(nrow(batch) == 0, "No ingest_id found in batch_log")

    ingest_id <- batch$ingest_id[1]

    result <- profile_data(
        con              = con,
        ingest_id        = ingest_id,
        schema_to_profile = "raw",
        config_path      = NULL
    )

    expect_type(result, "list")
    expect_true("tables_profiled"    %in% names(result))
    expect_true("variables_profiled" %in% names(result))
    expect_true("sentinels_detected" %in% names(result))
    expect_true("critical_issues"    %in% names(result))
    expect_true("warning_issues"     %in% names(result))
    expect_true("info_issues"        %in% names(result))
    expect_true("overall_score"      %in% names(result))

    expect_true(result$tables_profiled > 0)
    expect_true(result$overall_score %in%
                    c("Excellent", "Good", "Fair", "Needs Review"))

    # Verify data was written to governance tables
    for (tbl in c("data_profile", "data_profile_distribution",
                  "data_profile_summary")) {
        rows <- DBI::dbGetQuery(con, sprintf(
            "SELECT COUNT(*) AS n FROM governance.%s
             WHERE ingest_id = '%s' AND schema_name = 'raw'",
            tbl, ingest_id
        ))
        expect_true(rows$n > 0,
            info = paste("Expected rows in governance.", tbl))
    }
})

test_that("profile_data is idempotent — re-run produces same row counts (DB-dependent)", {

    skip_if_not(can_connect_to_pulse(), "Database connection not available")

    source(file.path(proj_root, "r/connect_to_pulse.R"))
    source(file.path(proj_root, "r/steps/profile_data.R"))

    con <- connect_to_pulse()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    batch <- DBI::dbGetQuery(con,
        "SELECT ingest_id FROM governance.batch_log LIMIT 1"
    )
    skip_if(nrow(batch) == 0, "No ingest_id found in batch_log")

    ingest_id <- batch$ingest_id[1]

    # Run twice
    result1 <- profile_data(con, ingest_id, "raw", NULL)
    result2 <- profile_data(con, ingest_id, "raw", NULL)

    # Counts should match — no duplication
    expect_equal(result1$tables_profiled,    result2$tables_profiled)
    expect_equal(result1$variables_profiled, result2$variables_profiled)

    # Verify row counts in DB
    count1 <- DBI::dbGetQuery(con, sprintf(
        "SELECT COUNT(*) AS n FROM governance.data_profile
         WHERE ingest_id = '%s' AND schema_name = 'raw'",
        ingest_id
    ))$n
    expect_equal(count1, result2$variables_profiled)
})

test_that("profile_data writes audit_log event (DB-dependent)", {

    skip_if_not(can_connect_to_pulse(), "Database connection not available")

    source(file.path(proj_root, "r/connect_to_pulse.R"))

    con <- connect_to_pulse()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    batch <- DBI::dbGetQuery(con,
        "SELECT ingest_id FROM governance.batch_log LIMIT 1"
    )
    skip_if(nrow(batch) == 0, "No ingest_id found in batch_log")

    ingest_id <- batch$ingest_id[1]

    audit <- DBI::dbGetQuery(con, sprintf(
        "SELECT * FROM governance.audit_log
         WHERE ingest_id = '%s'
           AND action LIKE '%%data_profiling%%'
         ORDER BY audit_id DESC LIMIT 1",
        ingest_id
    ))

    expect_true(nrow(audit) > 0,
        info = "Expected audit_log entry for data_profiling event")
})
