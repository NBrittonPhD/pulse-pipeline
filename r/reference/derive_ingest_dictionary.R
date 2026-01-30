# =============================================================================
# derive_ingest_dictionary
# =============================================================================
# Purpose:      Derive the ingest_dictionary.xlsx from the core metadata
#               dictionary. This extracts the 5 mapping columns needed for
#               the ingestion workflow, making the core dict the single
#               source of truth for source-to-lake variable mappings.
#
#               This replaces the previous manual maintenance of
#               ingest_dictionary.xlsx as an independent file.
#
# Inputs:
#   - core_dict_path: character path to CURRENT_core_metadata_dictionary.xlsx
#   - output_path:    character path for the derived ingest_dictionary.xlsx
#   - overwrite:      logical, if TRUE overwrites existing output (archives
#                      the old version first)
#
# Outputs:      list with (status, rows_derived, tables_count,
#               source_types_count, output_path)
#
# Side Effects: Writes ingest_dictionary.xlsx, optionally archives the
#               previous version
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
derive_ingest_dictionary <- function(core_dict_path = NULL,
                                      output_path    = NULL,
                                      overwrite      = TRUE) {

  # =========================================================================
  # RESOLVE PATHS
  # =========================================================================
  proj_root <- getOption("pulse.proj_root", default = ".")

  if (is.null(core_dict_path)) {
    core_dict_path <- file.path(proj_root, "reference",
                                "CURRENT_core_metadata_dictionary.xlsx")
  }

  if (is.null(output_path)) {
    output_path <- file.path(proj_root, "reference",
                             "ingest_dictionary.xlsx")
  }

  message("=================================================================")
  message("[derive_ingest_dictionary] STEP A: DERIVE INGEST DICTIONARY")
  message("=================================================================")
  message("[derive_ingest_dictionary] Core dict path: ", core_dict_path)
  message("[derive_ingest_dictionary] Output path:    ", output_path)

  # =========================================================================
  # INPUT VALIDATION
  # =========================================================================
  if (!file.exists(core_dict_path)) {
    stop("[derive_ingest_dictionary] ERROR: Core metadata dictionary not ",
         "found at: ", core_dict_path)
  }

  # =========================================================================
  # LOAD CORE METADATA DICTIONARY
  # =========================================================================
  message("[derive_ingest_dictionary] Loading core metadata dictionary...")

  core_dict <- readxl::read_excel(core_dict_path) %>%
    tibble::as_tibble()

  message("[derive_ingest_dictionary] Loaded ", nrow(core_dict), " rows.")

  # =========================================================================
  # VALIDATE source_table_name COLUMN EXISTS
  # =========================================================================
  # The core dict must have source_table_name (added by the one-time
  # migration). If it's missing, the user needs to run the migration first.
  # =========================================================================
  required_cols <- c("source_type", "source_table_name",
                     "source_variable_name", "lake_table_name",
                     "lake_variable_name")
  missing_cols <- setdiff(required_cols, names(core_dict))

  if (length(missing_cols) > 0) {
    if ("source_table_name" %in% missing_cols) {
      stop("[derive_ingest_dictionary] ERROR: Core dict is missing ",
           "'source_table_name' column. Ensure the dictionary Excel file ",
           "includes a 'source_table_name' column.")
    }
    stop("[derive_ingest_dictionary] ERROR: Core dict missing required ",
         "columns: ", paste(missing_cols, collapse = ", "))
  }

  # =========================================================================
  # EXTRACT INGEST DICTIONARY COLUMNS
  # =========================================================================
  # Select exactly the 5 columns that make up the ingest dictionary.
  # Filter out rows where lake_table_name or lake_variable_name is NA,
  # matching the filter in build_expected_schema_dictionary.R (lines 64-67).
  # Remove exact duplicates.
  # =========================================================================
  message("[derive_ingest_dictionary] Extracting 5 ingest dictionary columns...")

  ingest_dict <- core_dict %>%
    dplyr::select(
      source_type,
      source_table_name,
      source_variable_name,
      lake_table_name,
      lake_variable_name
    ) %>%
    dplyr::filter(
      !is.na(lake_table_name),
      !is.na(lake_variable_name)
    ) %>%
    dplyr::distinct()

  n_tables <- dplyr::n_distinct(ingest_dict$lake_table_name)
  n_source_types <- dplyr::n_distinct(ingest_dict$source_type)

  message("[derive_ingest_dictionary] Derived ", nrow(ingest_dict),
          " rows across ", n_tables, " lake tables and ",
          n_source_types, " source types.")

  # =========================================================================
  # ARCHIVE EXISTING FILE (if overwriting)
  # =========================================================================
  if (overwrite && file.exists(output_path)) {
    archive_dir <- file.path(dirname(output_path), "archive")
    dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)

    timestamp <- format(Sys.time(), "%Y_%m_%d_%H%M%S")
    archive_name <- paste0("ingest_dictionary_", timestamp, ".xlsx")
    archive_path <- file.path(archive_dir, archive_name)

    file.copy(output_path, archive_path)
    message("[derive_ingest_dictionary] Archived old ingest dict to: ",
            archive_path)
  } else if (!overwrite && file.exists(output_path)) {
    stop("[derive_ingest_dictionary] ERROR: Output file already exists and ",
         "overwrite = FALSE. Set overwrite = TRUE or specify a different ",
         "output_path.")
  }

  # =========================================================================
  # WRITE OUTPUT
  # =========================================================================
  message("[derive_ingest_dictionary] Writing ingest dictionary...")

  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  writexl::write_xlsx(ingest_dict, output_path)

  message("[derive_ingest_dictionary] Written to: ", output_path)

  # =========================================================================
  # RETURN RESULTS
  # =========================================================================
  message("=================================================================")
  message("[derive_ingest_dictionary] DERIVATION COMPLETE")
  message("=================================================================")
  message("  Rows derived:    ", nrow(ingest_dict))
  message("  Lake tables:     ", n_tables)
  message("  Source types:    ", n_source_types)
  message("  Output:          ", output_path)
  message("=================================================================")

  return(list(
    status             = "success",
    rows_derived       = nrow(ingest_dict),
    tables_count       = n_tables,
    source_types_count = n_source_types,
    output_path        = output_path
  ))
}
