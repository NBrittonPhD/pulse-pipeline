# =============================================================================
# compare_metadata
# =============================================================================
# Purpose:      Compare a new metadata dictionary (from Excel) against the
#               current metadata in the database, producing a field-level diff.
#               Returns one row per changed field, classified as INITIAL, ADD,
#               UPDATE, or REMOVE.
#
# Inputs:
#   - new_dict:     tibble from load_metadata_dictionary() (new Excel data)
#   - current_dict: tibble from database query (current reference.metadata rows)
#                   May be empty (0 rows) if this is the first sync.
#
# Outputs:      Tibble with columns:
#                 lake_table_name, lake_variable_name, source_type,
#                 field_changed, old_value, new_value, change_type
#
# Side Effects: None (pure function)
#
# Dependencies: dplyr, tidyr, tibble, glue
#
# Author:       Noel
# Last Updated: 2026-01-30
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(dplyr)
library(tidyr)
library(tibble)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
compare_metadata <- function(new_dict, current_dict) {

    # =========================================================================
    # DEFINE TRACKED FIELDS
    # =========================================================================
    # These are the fields we monitor for changes. Identity columns
    # (lake_table_name, lake_variable_name, source_type) are keys, not tracked.
    # =========================================================================
    TRACKED_FIELDS <- c(
        "source_table_name", "source_variable_name", "data_type",
        "variable_label", "variable_definition", "value_labels",
        "variable_unit", "valid_min", "valid_max", "allowed_values",
        "is_identifier", "is_phi", "is_required",
        "validated_table_target", "validated_variable_name",
        "notes", "needs_further_review"
    )

    # =========================================================================
    # HANDLE EMPTY CURRENT DICTIONARY (FIRST SYNC)
    # =========================================================================
    # If the database has no active metadata rows, every variable in the new
    # dictionary is an INITIAL load. We pivot each tracked field into a row.
    # =========================================================================
    if (nrow(current_dict) == 0) {
        message("[compare_metadata]   First sync detected - all variables will be INITIAL")

        changes <- new_dict %>%
            dplyr::mutate(dplyr::across(dplyr::all_of(TRACKED_FIELDS), as.character)) %>%
            tidyr::pivot_longer(
                cols = dplyr::all_of(TRACKED_FIELDS),
                names_to = "field_changed",
                values_to = "new_value"
            ) %>%
            dplyr::mutate(
                old_value = NA_character_,
                change_type = "INITIAL"
            ) %>%
            dplyr::select(
                lake_table_name, lake_variable_name, source_type,
                field_changed, old_value, new_value, change_type
            )

        message(glue("[compare_metadata]   Total changes: {nrow(changes)}"))
        return(changes)
    }

    # =========================================================================
    # CREATE COMPOSITE KEYS FOR MATCHING
    # =========================================================================
    # The PK is (lake_table_name, lake_variable_name, source_type).
    # We create a pipe-delimited key for efficient set operations.
    # =========================================================================
    new_dict <- new_dict %>%
        dplyr::mutate(
            .key = paste(lake_table_name, lake_variable_name, source_type, sep = "|")
        )

    current_dict <- current_dict %>%
        dplyr::mutate(
            .key = paste(lake_table_name, lake_variable_name, source_type, sep = "|")
        )

    new_keys <- unique(new_dict$.key)
    current_keys <- unique(current_dict$.key)

    # =========================================================================
    # CLASSIFY KEYS: ADD, REMOVE, COMMON
    # =========================================================================
    added_keys <- setdiff(new_keys, current_keys)
    removed_keys <- setdiff(current_keys, new_keys)
    common_keys <- intersect(new_keys, current_keys)

    message(glue(
        "[compare_metadata]   Comparing: {length(added_keys)} adds, ",
        "{length(removed_keys)} removes, {length(common_keys)} to check for updates"
    ))

    changes_list <- list()

    # =========================================================================
    # PROCESS ADDS
    # =========================================================================
    # Variables in the new dictionary that don't exist in the current database.
    # Every tracked field for each new variable becomes an ADD change row.
    # =========================================================================
    if (length(added_keys) > 0) {
        added_changes <- new_dict %>%
            dplyr::filter(.key %in% added_keys) %>%
            dplyr::mutate(dplyr::across(dplyr::all_of(TRACKED_FIELDS), as.character)) %>%
            tidyr::pivot_longer(
                cols = dplyr::all_of(TRACKED_FIELDS),
                names_to = "field_changed",
                values_to = "new_value"
            ) %>%
            dplyr::mutate(
                old_value = NA_character_,
                change_type = "ADD"
            ) %>%
            dplyr::select(
                lake_table_name, lake_variable_name, source_type,
                field_changed, old_value, new_value, change_type
            )

        changes_list[["adds"]] <- added_changes
    }

    # =========================================================================
    # PROCESS REMOVES
    # =========================================================================
    # Variables in the current database that don't exist in the new dictionary.
    # Every tracked field for each removed variable becomes a REMOVE change row.
    # =========================================================================
    if (length(removed_keys) > 0) {
        removed_changes <- current_dict %>%
            dplyr::filter(.key %in% removed_keys) %>%
            dplyr::mutate(dplyr::across(dplyr::any_of(TRACKED_FIELDS), as.character)) %>%
            tidyr::pivot_longer(
                cols = dplyr::any_of(TRACKED_FIELDS),
                names_to = "field_changed",
                values_to = "old_value"
            ) %>%
            dplyr::mutate(
                new_value = NA_character_,
                change_type = "REMOVE"
            ) %>%
            dplyr::select(
                lake_table_name, lake_variable_name, source_type,
                field_changed, old_value, new_value, change_type
            )

        changes_list[["removes"]] <- removed_changes
    }

    # =========================================================================
    # PROCESS UPDATES (FIELD-LEVEL COMPARISON)
    # =========================================================================
    # For variables that exist in both, compare each tracked field value.
    # Only fields where old != new generate a change row.
    # We normalize NAs to empty strings for comparison to avoid NA == NA issues.
    # =========================================================================
    if (length(common_keys) > 0) {
        update_changes <- list()

        for (k in common_keys) {
            new_row <- new_dict %>% dplyr::filter(.key == k)
            current_row <- current_dict %>% dplyr::filter(.key == k)

            for (field in TRACKED_FIELDS) {
                new_val <- as.character(new_row[[field]][1])
                old_val <- as.character(current_row[[field]][1])

                # Normalize NA to empty string for comparison
                new_val_cmp <- ifelse(is.na(new_val), "", new_val)
                old_val_cmp <- ifelse(is.na(old_val), "", old_val)

                if (new_val_cmp != old_val_cmp) {
                    update_changes[[length(update_changes) + 1]] <- tibble::tibble(
                        lake_table_name = new_row$lake_table_name[1],
                        lake_variable_name = new_row$lake_variable_name[1],
                        source_type = new_row$source_type[1],
                        field_changed = field,
                        old_value = ifelse(old_val_cmp == "", NA_character_, old_val),
                        new_value = ifelse(new_val_cmp == "", NA_character_, new_val),
                        change_type = "UPDATE"
                    )
                }
            }
        }

        if (length(update_changes) > 0) {
            changes_list[["updates"]] <- dplyr::bind_rows(update_changes)
        }
    }

    # =========================================================================
    # COMBINE ALL CHANGES
    # =========================================================================
    if (length(changes_list) > 0) {
        all_changes <- dplyr::bind_rows(changes_list)
    } else {
        all_changes <- tibble::tibble(
            lake_table_name = character(),
            lake_variable_name = character(),
            source_type = character(),
            field_changed = character(),
            old_value = character(),
            new_value = character(),
            change_type = character()
        )
    }

    message(glue("[compare_metadata]   Total changes detected: {nrow(all_changes)}"))

    return(all_changes)
}
