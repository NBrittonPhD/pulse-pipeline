# =============================================================================
# migrate_source_table_name
# =============================================================================
# Purpose:      One-time migration function that adds the `source_table_name`
#               column to CURRENT_core_metadata_dictionary.xlsx by joining
#               against the existing ingest_dictionary.xlsx. This is a
#               prerequisite for the automatic metadata propagation workflow.
#
#               After this migration, the core metadata dictionary becomes a
#               20-column file (19 original + source_table_name) and can
#               serve as the single source of truth for deriving the
#               ingest_dictionary automatically.
#
# Inputs:
#   - core_dict_path:   character path to CURRENT_core_metadata_dictionary.xlsx
#   - ingest_dict_path: character path to ingest_dictionary.xlsx (source of
#                        source_table_name values)
#   - output_path:      character path for the updated core dict output
#   - archive_before:   logical, if TRUE archives the old core dict before
#                        overwriting
#
# Outputs:      list with (status, rows_migrated, rows_unmatched,
#               unmatched_variables, output_path)
#
# Side Effects: Writes updated CURRENT_core_metadata_dictionary.xlsx,
#               optionally archives the previous version
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
migrate_source_table_name <- function(core_dict_path   = NULL,
                                       ingest_dict_path = NULL,
                                       output_path      = NULL,
                                       archive_before   = TRUE) {

  # =========================================================================
  # RESOLVE PATHS
  # =========================================================================
  # Use project root for relative path resolution, matching the convention
  # established in build_expected_schema_dictionary.R and sync_metadata.R.
  # =========================================================================
  proj_root <- getOption("pulse.proj_root", default = ".")

  if (is.null(core_dict_path)) {
    core_dict_path <- file.path(proj_root, "reference",
                                "CURRENT_core_metadata_dictionary.xlsx")
  }

  if (is.null(ingest_dict_path)) {
    ingest_dict_path <- file.path(proj_root, "reference",
                                  "ingest_dictionary.xlsx")
  }

  if (is.null(output_path)) {
    output_path <- core_dict_path
  }

  message("=================================================================")
  message("[migrate_source_table_name] ONE-TIME MIGRATION")
  message("=================================================================")
  message("[migrate_source_table_name] Core dict path:   ", core_dict_path)
  message("[migrate_source_table_name] Ingest dict path: ", ingest_dict_path)
  message("[migrate_source_table_name] Output path:      ", output_path)

  # =========================================================================
  # INPUT VALIDATION
  # =========================================================================
  if (!file.exists(core_dict_path)) {
    stop("[migrate_source_table_name] ERROR: Core metadata dictionary not found at: ",
         core_dict_path)
  }

  if (!file.exists(ingest_dict_path)) {
    stop("[migrate_source_table_name] ERROR: Ingest dictionary not found at: ",
         ingest_dict_path)
  }

  # =========================================================================
  # LOAD CORE METADATA DICTIONARY
  # =========================================================================
  message("[migrate_source_table_name] Loading core metadata dictionary...")

  core_dict <- readxl::read_excel(core_dict_path) %>%
    tibble::as_tibble()

  message("[migrate_source_table_name] Loaded ", nrow(core_dict), " rows x ",
          ncol(core_dict), " cols from core dict.")

  # Check if source_table_name already exists — if so, migration is not needed
  if ("source_table_name" %in% names(core_dict)) {
    message("[migrate_source_table_name] Column 'source_table_name' already ",
            "exists in core dict. Migration not needed.")
    return(list(
      status              = "already_migrated",
      rows_migrated       = nrow(core_dict),
      rows_unmatched      = 0L,
      unmatched_variables = tibble::tibble(),
      output_path         = output_path
    ))
  }

  # Validate expected columns exist in core dict for the join
  required_core_cols <- c("source_type", "source_variable_name",
                          "lake_table_name", "lake_variable_name")
  missing_core <- setdiff(required_core_cols, names(core_dict))
  if (length(missing_core) > 0) {
    stop("[migrate_source_table_name] ERROR: Core dict missing required columns: ",
         paste(missing_core, collapse = ", "))
  }

  # =========================================================================
  # LOAD INGEST DICTIONARY
  # =========================================================================
  message("[migrate_source_table_name] Loading ingest dictionary...")

  ingest_dict <- readxl::read_excel(ingest_dict_path) %>%
    tibble::as_tibble()

  message("[migrate_source_table_name] Loaded ", nrow(ingest_dict), " rows x ",
          ncol(ingest_dict), " cols from ingest dict.")

  # Validate expected columns exist in ingest dict
  required_ingest_cols <- c("source_type", "source_table_name",
                            "source_variable_name", "lake_table_name",
                            "lake_variable_name")
  missing_ingest <- setdiff(required_ingest_cols, names(ingest_dict))
  if (length(missing_ingest) > 0) {
    stop("[migrate_source_table_name] ERROR: Ingest dict missing required columns: ",
         paste(missing_ingest, collapse = ", "))
  }

  # =========================================================================
  # BUILD LOOKUP TABLE FROM INGEST DICTIONARY
  # =========================================================================
  # The lookup maps the composite key (source_type, source_variable_name,
  # lake_table_name, lake_variable_name) to source_table_name. We normalize
  # all text to lowercase + trimmed for consistent matching.
  # =========================================================================
  message("[migrate_source_table_name] Building source_table_name lookup...")

  lookup <- ingest_dict %>%
    dplyr::transmute(
      source_type_norm = tolower(trimws(source_type)),
      source_var_norm  = tolower(trimws(source_variable_name)),
      lake_table_norm  = tolower(trimws(lake_table_name)),
      lake_var_norm    = tolower(trimws(lake_variable_name)),
      source_table_name = trimws(source_table_name)
    ) %>%
    dplyr::distinct()

  message("[migrate_source_table_name] Lookup has ", nrow(lookup),
          " distinct entries.")

  # =========================================================================
  # JOIN CORE DICT TO LOOKUP
  # =========================================================================
  # Left join so every core dict row is preserved. Rows without a match
  # will get source_table_name = NA. These are reported as unmatched.
  # =========================================================================
  message("[migrate_source_table_name] Joining source_table_name to core dict...")

  core_with_keys <- core_dict %>%
    dplyr::mutate(
      source_type_norm = tolower(trimws(source_type)),
      source_var_norm  = tolower(trimws(source_variable_name)),
      lake_table_norm  = tolower(trimws(lake_table_name)),
      lake_var_norm    = tolower(trimws(lake_variable_name))
    )

  migrated <- core_with_keys %>%
    dplyr::left_join(
      lookup,
      by = c("source_type_norm", "source_var_norm",
             "lake_table_norm", "lake_var_norm")
    )

  # Remove temporary join key columns
  migrated <- migrated %>%
    dplyr::select(-source_type_norm, -source_var_norm,
                  -lake_table_norm, -lake_var_norm)

  # =========================================================================
  # REORDER COLUMNS
  # =========================================================================
  # Place source_table_name as the 2nd column (after source_type, before
  # source_variable_name) for logical grouping of source-related fields.
  # =========================================================================
  col_order <- names(migrated)
  st_pos <- which(col_order == "source_type")

  # Insert source_table_name right after source_type
  new_order <- c(
    col_order[1:st_pos],
    "source_table_name",
    col_order[(st_pos + 1):(length(col_order) - 1)]
    # The last element was source_table_name from the join, already named
  )
  # Remove any duplicates in ordering
  new_order <- unique(new_order)
  migrated <- migrated %>% dplyr::select(dplyr::all_of(new_order))

  # =========================================================================
  # REPORT UNMATCHED ROWS
  # =========================================================================
  unmatched <- migrated %>%
    dplyr::filter(is.na(source_table_name))

  rows_unmatched <- nrow(unmatched)
  rows_matched <- nrow(migrated) - rows_unmatched

  message("[migrate_source_table_name] Matched rows:   ", rows_matched)
  message("[migrate_source_table_name] Unmatched rows: ", rows_unmatched)

  if (rows_unmatched > 0) {
    warning("[migrate_source_table_name] WARNING: ", rows_unmatched,
            " rows could not be matched to a source_table_name. ",
            "These will have source_table_name = NA. ",
            "Review unmatched_variables in the returned result.")

    unmatched_summary <- unmatched %>%
      dplyr::select(source_type, source_variable_name,
                    lake_table_name, lake_variable_name)

    message("[migrate_source_table_name] Unmatched variables:")
    for (i in seq_len(min(nrow(unmatched_summary), 20))) {
      row <- unmatched_summary[i, ]
      message("  - ", row$source_type, " | ", row$source_variable_name,
              " | ", row$lake_table_name, " | ", row$lake_variable_name)
    }
    if (rows_unmatched > 20) {
      message("  ... and ", rows_unmatched - 20, " more.")
    }
  }

  # =========================================================================
  # ARCHIVE EXISTING CORE DICT (if requested)
  # =========================================================================
  if (archive_before && file.exists(output_path)) {
    archive_dir <- file.path(dirname(output_path), "archive")
    dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)

    timestamp <- format(Sys.time(), "%Y_%m_%d_%H%M%S")
    archive_name <- paste0("core_metadata_dictionary_pre_migration_",
                           timestamp, ".xlsx")
    archive_path <- file.path(archive_dir, archive_name)

    file.copy(output_path, archive_path)
    message("[migrate_source_table_name] Archived old core dict to: ",
            archive_path)
  }

  # =========================================================================
  # WRITE UPDATED CORE DICT
  # =========================================================================
  message("[migrate_source_table_name] Writing updated core dict...")

  writexl::write_xlsx(migrated, output_path)

  message("[migrate_source_table_name] Written ", nrow(migrated), " rows x ",
          ncol(migrated), " cols to: ", output_path)

  # =========================================================================
  # RETURN RESULTS
  # =========================================================================
  message("=================================================================")
  message("[migrate_source_table_name] MIGRATION COMPLETE")
  message("=================================================================")
  message("  Total rows:     ", nrow(migrated))
  message("  Columns:        ", ncol(migrated), " (was ",
          ncol(core_dict), ")")
  message("  Matched:        ", rows_matched)
  message("  Unmatched (NA): ", rows_unmatched)
  message("=================================================================")

  return(list(
    status              = "success",
    rows_migrated       = nrow(migrated),
    rows_unmatched      = rows_unmatched,
    unmatched_variables = if (rows_unmatched > 0) unmatched_summary else tibble::tibble(),
    output_path         = output_path
  ))
}
