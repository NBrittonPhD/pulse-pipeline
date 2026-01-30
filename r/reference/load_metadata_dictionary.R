# =============================================================================
# load_metadata_dictionary
# =============================================================================
# Purpose:      Load the core metadata dictionary from Excel and standardize
#               it for database synchronization. This function reads
#               CURRENT_core_metadata_dictionary.xlsx, validates required
#               columns, standardizes Y/N fields to boolean, and checks for
#               duplicate composite keys.
#
# Inputs:
#   - dict_path:          character path to the Excel dictionary file
#   - source_type_filter: character (optional) filter to a specific source_type
#
# Outputs:      Tibble with standardized dictionary ready for DB operations
#
# Side Effects: None (pure function)
#
# Dependencies: readxl, dplyr, glue, tibble
#
# Author:       Noel
# Last Updated: 2026-01-30
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(readxl)
library(dplyr)
library(glue)
library(tibble)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
load_metadata_dictionary <- function(dict_path, source_type_filter = NULL) {

    # =========================================================================
    # VALIDATE FILE EXISTS
    # =========================================================================
    if (!file.exists(dict_path)) {
        stop(glue("[load_metadata_dictionary] ERROR: Dictionary file not found: {dict_path}"))
    }

    message(glue("[load_metadata_dictionary] Loading dictionary from: {dict_path}"))

    # =========================================================================
    # READ EXCEL FILE
    # =========================================================================
    dict <- readxl::read_excel(dict_path)

    message(glue("[load_metadata_dictionary]   Loaded {nrow(dict)} rows, {ncol(dict)} columns"))

    # =========================================================================
    # VALIDATE REQUIRED COLUMNS
    # =========================================================================
    # These columns must be present in the Excel file for the sync to work.
    # Without them, we cannot uniquely identify variables or map them to tables.
    # =========================================================================
    required_cols <- c(
        "source_type", "lake_table_name", "lake_variable_name", "data_type"
    )

    missing_cols <- setdiff(required_cols, names(dict))

    if (length(missing_cols) > 0) {
        stop(glue(
            "[load_metadata_dictionary] ERROR: Missing required columns: ",
            "{paste(missing_cols, collapse = ', ')}"
        ))
    }

    # =========================================================================
    # STANDARDIZE COLUMN SET
    # =========================================================================
    # Define the complete list of columns that reference.metadata expects.
    # Any columns present in the Excel that are NOT in this list are ignored.
    # Any columns in this list that are NOT in the Excel are added as NA.
    # =========================================================================
    db_columns <- c(
        "source_type", "source_table_name", "source_variable_name", "data_type",
        "lake_table_name", "lake_variable_name",
        "variable_label", "variable_definition", "value_labels",
        "variable_unit", "valid_min", "valid_max", "allowed_values",
        "is_identifier", "is_phi", "is_required",
        "validated_table_target", "validated_variable_name",
        "notes", "needs_further_review"
    )

    # Add missing columns as NA
    for (col in db_columns) {
        if (!col %in% names(dict)) {
            dict[[col]] <- NA_character_
        }
    }

    # Select only database columns in correct order
    dict <- dict[, db_columns]

    # =========================================================================
    # STANDARDIZE Y/N FIELDS TO BOOLEAN
    # =========================================================================
    # The Excel file may have "Y", "N", "Yes", "No", "TRUE", "FALSE", etc.
    # We convert all boolean flag fields to TRUE/FALSE for database storage.
    # =========================================================================
    yn_fields <- c("is_identifier", "is_phi", "is_required")

    for (field in yn_fields) {
        dict[[field]] <- dplyr::case_when(
            toupper(as.character(dict[[field]])) %in% c("Y", "YES", "TRUE", "1") ~ TRUE,
            toupper(as.character(dict[[field]])) %in% c("N", "NO", "FALSE", "0") ~ FALSE,
            is.na(dict[[field]]) ~ FALSE,
            TRUE ~ FALSE
        )
    }

    # =========================================================================
    # ENSURE NUMERIC COLUMNS ARE NUMERIC
    # =========================================================================
    dict$valid_min <- as.numeric(dict$valid_min)
    dict$valid_max <- as.numeric(dict$valid_max)

    # =========================================================================
    # FILTER BY SOURCE TYPE (OPTIONAL)
    # =========================================================================
    if (!is.null(source_type_filter)) {
        original_count <- nrow(dict)
        dict <- dplyr::filter(dict, source_type == source_type_filter)
        message(glue(
            "[load_metadata_dictionary]   Filtered to source_type = '{source_type_filter}': ",
            "{nrow(dict)} of {original_count} rows"
        ))
    }

    # =========================================================================
    # CHECK FOR DUPLICATE COMPOSITE KEYS
    # =========================================================================
    # The database PK is (lake_table_name, lake_variable_name, source_type).
    # Duplicates here would cause the upsert to fail.
    # =========================================================================
    dupes <- dict %>%
        dplyr::count(lake_table_name, lake_variable_name, source_type) %>%
        dplyr::filter(n > 1)

    if (nrow(dupes) > 0) {
        warning(glue(
            "[load_metadata_dictionary] WARNING: Found {nrow(dupes)} duplicate keys. ",
            "First: {dupes$lake_table_name[1]}|{dupes$lake_variable_name[1]}|{dupes$source_type[1]}"
        ))
    }

    # =========================================================================
    # RETURN STANDARDIZED DICTIONARY
    # =========================================================================
    message(glue("[load_metadata_dictionary]   Dictionary ready: {nrow(dict)} variables"))

    return(tibble::as_tibble(dict))
}
