# =============================================================================
# tests/testthat/test_step4_metadata_sync.R
# =============================================================================
# Unit tests for Step 4: Metadata Synchronization
#
# Tests cover:
#   - load_metadata_dictionary() loading and standardization
#   - compare_metadata() change detection (INITIAL, ADD, UPDATE, REMOVE)
#   - sync_metadata() full integration with database
#   - get_current_metadata_version() helper
#
# Author: Noel
# Last Updated: 2026-01-30
# =============================================================================

# =============================================================================
# SETUP
# =============================================================================
library(testthat)
library(DBI)
library(dplyr)
library(tidyr)
library(tibble)
library(glue)
library(readxl)

# Get project root
proj_root <- getOption("pulse.proj_root", default = normalizePath("../.."))

# Source dependencies
source(file.path(proj_root, "r/connect_to_pulse.R"))
source(file.path(proj_root, "r/reference/load_metadata_dictionary.R"))
source(file.path(proj_root, "r/utilities/compare_metadata.R"))
source(file.path(proj_root, "r/reference/get_current_metadata_version.R"))


# =============================================================================
# HELPER: Build a minimal test dictionary tibble
# =============================================================================
make_test_dict <- function(n = 2, source_type = "TEST") {
    tibble(
        source_type             = source_type,
        source_table_name       = "test_source",
        source_variable_name    = paste0("src_var_", seq_len(n)),
        data_type               = rep("text", n),
        lake_table_name         = "test_table",
        lake_variable_name      = paste0("var_", seq_len(n)),
        variable_label          = paste0("Variable ", seq_len(n)),
        variable_definition     = NA_character_,
        value_labels            = NA_character_,
        variable_unit           = NA_character_,
        valid_min               = NA_real_,
        valid_max               = NA_real_,
        allowed_values          = NA_character_,
        is_identifier           = FALSE,
        is_phi                  = FALSE,
        is_required             = FALSE,
        validated_table_target  = NA_character_,
        validated_variable_name = NA_character_,
        notes                   = NA_character_,
        needs_further_review    = NA_character_
    )
}


# =============================================================================
# TEST 1: load_metadata_dictionary loads Excel correctly
# =============================================================================
test_that("load_metadata_dictionary loads Excel correctly", {

    dict_path <- file.path(proj_root, "reference/CURRENT_core_metadata_dictionary.xlsx")
    skip_if_not(file.exists(dict_path), "Core dictionary not found")

    dict <- load_metadata_dictionary(dict_path)

    expect_true(nrow(dict) > 0)
    expect_true("lake_table_name" %in% names(dict))
    expect_true("lake_variable_name" %in% names(dict))
    expect_true("source_type" %in% names(dict))
    expect_true("data_type" %in% names(dict))
})


# =============================================================================
# TEST 2: load_metadata_dictionary standardizes boolean fields
# =============================================================================
test_that("load_metadata_dictionary standardizes Y/N to boolean", {

    dict_path <- file.path(proj_root, "reference/CURRENT_core_metadata_dictionary.xlsx")
    skip_if_not(file.exists(dict_path), "Core dictionary not found")

    dict <- load_metadata_dictionary(dict_path)

    expect_type(dict$is_identifier, "logical")
    expect_type(dict$is_phi, "logical")
    expect_type(dict$is_required, "logical")
    expect_true(all(!is.na(dict$is_identifier)))
    expect_true(all(!is.na(dict$is_phi)))
    expect_true(all(!is.na(dict$is_required)))
})


# =============================================================================
# TEST 3: load_metadata_dictionary filters by source_type
# =============================================================================
test_that("load_metadata_dictionary filters by source_type", {

    dict_path <- file.path(proj_root, "reference/CURRENT_core_metadata_dictionary.xlsx")
    skip_if_not(file.exists(dict_path), "Core dictionary not found")

    full_dict <- load_metadata_dictionary(dict_path)
    filtered <- load_metadata_dictionary(dict_path, source_type_filter = "CISIR")

    expect_true(nrow(filtered) > 0)
    expect_true(nrow(filtered) < nrow(full_dict))
    expect_true(all(filtered$source_type == "CISIR"))
})


# =============================================================================
# TEST 4: compare_metadata detects INITIAL on empty current
# =============================================================================
test_that("compare_metadata detects INITIAL on empty current", {

    new_dict <- make_test_dict(3)
    current_dict <- tibble()

    changes <- compare_metadata(new_dict, current_dict)

    expect_true(nrow(changes) > 0)
    expect_true(all(changes$change_type == "INITIAL"))
    expect_true(all(is.na(changes$old_value)))
    # 3 variables × 17 tracked fields = 51 INITIAL records
    expect_equal(nrow(changes), 51)
})


# =============================================================================
# TEST 5: compare_metadata detects ADD
# =============================================================================
test_that("compare_metadata detects ADD for new variables", {

    # Current has var_1 only
    current_dict <- make_test_dict(1)
    # New has var_1 and var_2
    new_dict <- make_test_dict(2)

    changes <- compare_metadata(new_dict, current_dict)

    add_changes <- changes %>% filter(change_type == "ADD")

    expect_true(nrow(add_changes) > 0)
    expect_true(all(add_changes$lake_variable_name == "var_2"))
    expect_true(all(is.na(add_changes$old_value)))
})


# =============================================================================
# TEST 6: compare_metadata detects REMOVE
# =============================================================================
test_that("compare_metadata detects REMOVE for deleted variables", {

    # Current has var_1 and var_2
    current_dict <- make_test_dict(2)
    # New has var_1 only
    new_dict <- make_test_dict(1)

    changes <- compare_metadata(new_dict, current_dict)

    remove_changes <- changes %>% filter(change_type == "REMOVE")

    expect_true(nrow(remove_changes) > 0)
    expect_true(all(remove_changes$lake_variable_name == "var_2"))
    expect_true(all(is.na(remove_changes$new_value)))
})


# =============================================================================
# TEST 7: compare_metadata detects UPDATE
# =============================================================================
test_that("compare_metadata detects UPDATE for changed fields", {

    current_dict <- make_test_dict(1)
    new_dict <- make_test_dict(1)
    new_dict$variable_label <- "Updated Label"

    changes <- compare_metadata(new_dict, current_dict)

    update_changes <- changes %>% filter(change_type == "UPDATE")

    expect_equal(nrow(update_changes), 1)
    expect_equal(update_changes$field_changed[1], "variable_label")
    expect_equal(update_changes$old_value[1], "Variable 1")
    expect_equal(update_changes$new_value[1], "Updated Label")
})


# =============================================================================
# TEST 8: compare_metadata returns empty for identical dictionaries
# =============================================================================
test_that("compare_metadata returns no changes for identical dictionaries", {

    dict <- make_test_dict(3)

    changes <- compare_metadata(dict, dict)

    expect_equal(nrow(changes), 0)
})


# =============================================================================
# TEST 9: compare_metadata handles NA-to-value transitions
# =============================================================================
test_that("compare_metadata detects NA to value change as UPDATE", {

    current_dict <- make_test_dict(1)
    current_dict$variable_unit <- NA_character_

    new_dict <- make_test_dict(1)
    new_dict$variable_unit <- "kg"

    changes <- compare_metadata(new_dict, current_dict)

    update_changes <- changes %>% filter(change_type == "UPDATE")

    expect_true(nrow(update_changes) >= 1)
    unit_change <- update_changes %>% filter(field_changed == "variable_unit")
    expect_equal(nrow(unit_change), 1)
    expect_true(is.na(unit_change$old_value[1]))
    expect_equal(unit_change$new_value[1], "kg")
})


# =============================================================================
# TEST 10: get_current_metadata_version returns integer
# =============================================================================
test_that("get_current_metadata_version returns version from database", {

    skip_if_not(
        tryCatch({ con <- connect_to_pulse(); DBI::dbIsValid(con) },
                 error = function(e) FALSE),
        "Database not available"
    )

    con <- connect_to_pulse()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    version <- get_current_metadata_version(con)

    expect_true(is.numeric(version))
    expect_true(version >= 0)
})


# =============================================================================
# TEST 11: sync_metadata performs full sync (integration)
# =============================================================================
test_that("sync_metadata performs first sync correctly", {

    skip_if_not(
        tryCatch({ con <- connect_to_pulse(); DBI::dbIsValid(con) },
                 error = function(e) FALSE),
        "Database not available"
    )

    con <- connect_to_pulse()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    dict_path <- file.path(proj_root, "reference/CURRENT_core_metadata_dictionary.xlsx")
    skip_if_not(file.exists(dict_path), "Core dictionary not found")

    # Source full sync function
    source(file.path(proj_root, "r/reference/sync_metadata.R"))

    # Get state before
    version_before <- get_current_metadata_version(con)

    # Run sync
    result <- sync_metadata(con = con, dict_path = dict_path)

    # Check return structure
    expect_true(is.list(result))
    expect_true("version_number" %in% names(result))
    expect_true("total_variables" %in% names(result))
    expect_true("adds" %in% names(result))
    expect_true("updates" %in% names(result))
    expect_true("removes" %in% names(result))
    expect_true("total_changes" %in% names(result))
    expect_true("rows_synced" %in% names(result))

    # Version should have incremented
    expect_equal(result$version_number, version_before + 1)

    # Should have synced rows
    expect_true(result$rows_synced > 0)
})


# =============================================================================
# TEST 12: sync_metadata writes to metadata_history
# =============================================================================
test_that("sync_metadata populates metadata_history table", {

    skip_if_not(
        tryCatch({ con <- connect_to_pulse(); DBI::dbIsValid(con) },
                 error = function(e) FALSE),
        "Database not available"
    )

    con <- connect_to_pulse()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    # Check that history table has records
    history_count <- DBI::dbGetQuery(con, "
        SELECT COUNT(*) as n FROM reference.metadata_history
    ")

    expect_true(history_count$n > 0)

    # Check that history has expected columns
    sample <- DBI::dbGetQuery(con, "
        SELECT * FROM reference.metadata_history LIMIT 1
    ")

    expect_true("version_number" %in% names(sample))
    expect_true("lake_table_name" %in% names(sample))
    expect_true("field_changed" %in% names(sample))
    expect_true("change_type" %in% names(sample))
})


# =============================================================================
# TEST 13: sync_metadata is idempotent (second run = no changes)
# =============================================================================
test_that("sync_metadata second run detects no changes", {

    skip_if_not(
        tryCatch({ con <- connect_to_pulse(); DBI::dbIsValid(con) },
                 error = function(e) FALSE),
        "Database not available"
    )

    con <- connect_to_pulse()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    dict_path <- file.path(proj_root, "reference/CURRENT_core_metadata_dictionary.xlsx")
    skip_if_not(file.exists(dict_path), "Core dictionary not found")

    source(file.path(proj_root, "r/reference/sync_metadata.R"))

    # Run sync (should detect 0 changes since data hasn't changed)
    result <- sync_metadata(con = con, dict_path = dict_path)

    # If the previous test already synced, this should have 0 changes
    # (updates = 0 for the fields that came from Excel)
    expect_true(result$updates == 0)
    expect_true(result$removes == 0)
})
