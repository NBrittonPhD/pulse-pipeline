# =============================================================================
# update_type_decision_table
# =============================================================================
# Purpose:      Update the type_decision_table.xlsx by merging the current
#               core metadata dictionary with existing human-reviewed type
#               decisions. This function:
#
#               1. Refreshes `suggested_type` from the core dict's `data_type`
#               2. PRESERVES existing `final_type` and `decision_note` values
#                  (human governance decisions are never overwritten)
#               3. Flags new variables as "PENDING REVIEW"
#               4. Drops orphaned variables (no longer in core dict) with a
#                  logged warning
#
#               This makes the core metadata dictionary the upstream driver
#               of the type decision workflow while respecting the human
#               override layer.
#
# Inputs:
#   - core_dict_path:     character path to CURRENT_core_metadata_dictionary.xlsx
#   - type_decision_path: character path to existing type_decision_table.xlsx
#   - output_path:        character path for the updated file (defaults to
#                          type_decision_path, overwriting in place)
#   - archive_existing:   logical, if TRUE archives old file before overwriting
#
# Outputs:      list with (status, total_rows, new_rows, preserved_rows,
#               removed_rows, output_path)
#
# Side Effects: Writes updated type_decision_table.xlsx, optionally archives
#               the previous version
#
# Dependencies: readxl, writexl, dplyr, tibble
#
# Author:       Noel
# Last Updated: 2026-01-29
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(readxl)
library(writexl)
library(dplyr)
library(tibble)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
update_type_decision_table <- function(core_dict_path     = NULL,
                                        type_decision_path = NULL,
                                        output_path        = NULL,
                                        archive_existing   = TRUE) {

  # =========================================================================
  # RESOLVE PATHS
  # =========================================================================
  proj_root <- getOption("pulse.proj_root", default = ".")

  if (is.null(core_dict_path)) {
    core_dict_path <- file.path(proj_root, "reference",
                                "CURRENT_core_metadata_dictionary.xlsx")
  }

  if (is.null(type_decision_path)) {
    type_decision_path <- file.path(proj_root, "reference", "type_decisions",
                                    "type_decision_table.xlsx")
  }

  if (is.null(output_path)) {
    output_path <- type_decision_path
  }

  message("=================================================================")
  message("[update_type_decision_table] STEP C: UPDATE TYPE DECISIONS")
  message("=================================================================")
  message("[update_type_decision_table] Core dict path:      ", core_dict_path)
  message("[update_type_decision_table] Type decision path:  ",
          type_decision_path)
  message("[update_type_decision_table] Output path:         ", output_path)

  # =========================================================================
  # INPUT VALIDATION
  # =========================================================================
  if (!file.exists(core_dict_path)) {
    stop("[update_type_decision_table] ERROR: Core metadata dictionary not ",
         "found at: ", core_dict_path)
  }

  # =========================================================================
  # LOAD CORE METADATA DICTIONARY
  # =========================================================================
  message("[update_type_decision_table] Loading core metadata dictionary...")

  core_dict <- readxl::read_excel(core_dict_path) %>%
    tibble::as_tibble()

  message("[update_type_decision_table] Loaded ", nrow(core_dict),
          " rows from core dict.")

  # Validate required columns
  required_core_cols <- c("lake_table_name", "lake_variable_name", "data_type")
  missing_core <- setdiff(required_core_cols, names(core_dict))
  if (length(missing_core) > 0) {
    stop("[update_type_decision_table] ERROR: Core dict missing required ",
         "columns: ", paste(missing_core, collapse = ", "))
  }

  # =========================================================================
  # DERIVE NEW UNIVERSE FROM CORE DICT
  # =========================================================================
  # The "new universe" is the complete set of (table_name, variable) pairs
  # from the core dict, with suggested_type derived from data_type. This
  # represents every variable that should have a type decision.
  # =========================================================================
  message("[update_type_decision_table] Deriving new variable universe ",
          "from core dict...")

  new_universe <- core_dict %>%
    dplyr::transmute(
      table_name     = tolower(trimws(lake_table_name)),
      variable       = tolower(trimws(lake_variable_name)),
      suggested_type = tolower(trimws(data_type))
    ) %>%
    dplyr::filter(!is.na(table_name), !is.na(variable)) %>%
    dplyr::distinct(table_name, variable, .keep_all = TRUE)

  message("[update_type_decision_table] New universe: ", nrow(new_universe),
          " variables across ",
          dplyr::n_distinct(new_universe$table_name), " tables.")

  # =========================================================================
  # LOAD EXISTING TYPE DECISION TABLE (if it exists)
  # =========================================================================
  has_existing <- file.exists(type_decision_path)

  if (has_existing) {
    message("[update_type_decision_table] Loading existing type decisions...")

    old_decisions <- readxl::read_excel(type_decision_path) %>%
      tibble::as_tibble() %>%
      dplyr::mutate(
        table_name = tolower(trimws(table_name)),
        variable   = tolower(trimws(variable))
      )

    message("[update_type_decision_table] Loaded ", nrow(old_decisions),
            " existing decisions.")
  } else {
    message("[update_type_decision_table] No existing type decision file ",
            "found. Starting fresh.")
    old_decisions <- tibble::tibble(
      table_name    = character(),
      variable      = character(),
      suggested_type = character(),
      final_type    = character(),
      decision_note = character()
    )
  }

  # =========================================================================
  # MERGE: LEFT JOIN NEW UNIVERSE TO OLD DECISIONS
  # =========================================================================
  # The merge preserves human governance decisions (final_type, decision_note)
  # from the old table while refreshing suggested_type from the core dict.
  #
  # Three scenarios:
  #   1. Variable exists in BOTH: preserve final_type + decision_note
  #   2. Variable is NEW (core dict only): final_type = NA, note = PENDING
  #   3. Variable is ORPHANED (old only): dropped, warning logged
  # =========================================================================
  message("[update_type_decision_table] Merging type decisions...")

  merged <- new_universe %>%
    dplyr::left_join(
      old_decisions %>%
        dplyr::select(table_name, variable, final_type, decision_note),
      by = c("table_name", "variable")
    ) %>%
    dplyr::mutate(
      # Preserve existing final_type if it was set by a human
      final_type = dplyr::case_when(
        !is.na(final_type) & trimws(final_type) != "" ~ final_type,
        TRUE ~ NA_character_
      ),
      # Preserve existing decision_note if it exists, otherwise mark PENDING
      decision_note = dplyr::case_when(
        !is.na(decision_note) & trimws(decision_note) != "" ~ decision_note,
        TRUE ~ "PENDING REVIEW"
      )
    )

  # =========================================================================
  # IDENTIFY ORPHANED ROWS
  # =========================================================================
  # Variables in the old decisions that no longer exist in the core dict.
  # These are dropped from the output but logged as a warning.
  # =========================================================================
  if (nrow(old_decisions) > 0) {
    orphans <- old_decisions %>%
      dplyr::anti_join(new_universe, by = c("table_name", "variable"))
  } else {
    orphans <- tibble::tibble()
  }

  n_orphans <- nrow(orphans)

  if (n_orphans > 0) {
    warning("[update_type_decision_table] WARNING: ", n_orphans,
            " orphaned rows dropped (no longer in core dict).")
    message("[update_type_decision_table] Orphaned variables:")
    for (i in seq_len(min(n_orphans, 20))) {
      row <- orphans[i, ]
      message("  - ", row$table_name, ".", row$variable)
    }
    if (n_orphans > 20) {
      message("  ... and ", n_orphans - 20, " more.")
    }
  }

  # =========================================================================
  # IDENTIFY NEW ROWS
  # =========================================================================
  new_rows <- merged %>%
    dplyr::filter(decision_note == "PENDING REVIEW")

  n_new <- nrow(new_rows)
  n_preserved <- nrow(merged) - n_new

  if (n_new > 0) {
    message("[update_type_decision_table] New variables (PENDING REVIEW): ",
            n_new)
    for (i in seq_len(min(n_new, 20))) {
      row <- new_rows[i, ]
      message("  + ", row$table_name, ".", row$variable,
              " (suggested: ", row$suggested_type, ")")
    }
    if (n_new > 20) {
      message("  ... and ", n_new - 20, " more.")
    }
  } else {
    message("[update_type_decision_table] No new variables found.")
  }

  message("[update_type_decision_table] Preserved decisions: ", n_preserved)

  # =========================================================================
  # ARCHIVE EXISTING FILE (if requested)
  # =========================================================================
  if (archive_existing && has_existing) {
    archive_dir <- file.path(dirname(type_decision_path), "archive")
    dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)

    timestamp <- format(Sys.time(), "%Y_%m_%d_%H%M%S")
    archive_name <- paste0("type_decision_table_", timestamp, ".xlsx")
    archive_path <- file.path(archive_dir, archive_name)

    file.copy(type_decision_path, archive_path)
    message("[update_type_decision_table] Archived old file to: ",
            archive_path)
  }

  # =========================================================================
  # WRITE OUTPUT
  # =========================================================================
  message("[update_type_decision_table] Writing updated type decision table...")

  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  writexl::write_xlsx(merged, output_path)

  message("[update_type_decision_table] Written to: ", output_path)

  # =========================================================================
  # RETURN RESULTS
  # =========================================================================
  message("=================================================================")
  message("[update_type_decision_table] UPDATE COMPLETE")
  message("=================================================================")
  message("  Total rows:     ", nrow(merged))
  message("  New (pending):  ", n_new)
  message("  Preserved:      ", n_preserved)
  message("  Orphans dropped:", n_orphans)
  message("  Output:         ", output_path)
  message("=================================================================")

  return(list(
    status         = "success",
    total_rows     = nrow(merged),
    new_rows       = n_new,
    preserved_rows = n_preserved,
    removed_rows   = n_orphans,
    output_path    = output_path
  ))
}
