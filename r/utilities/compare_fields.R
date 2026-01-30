# =============================================================================
# STEP 3 × CLUSTER 3 — SCHEMA VALIDATION ENGINE
# FILE: r/utilities/compare_fields.R
# FUNCTION: compare_fields()
# =============================================================================
# Purpose:
#   Compare the expected schema for a given lake table against the observed
#   schema in Postgres (or other catalog-derived metadata) and identify
#   structural mismatches at the table/column level.
#
#   This helper returns a structured tibble of "issues" that can be written
#   directly into governance.structure_qc_table by the higher-level
#   validate_schema() step function.
#
# Inputs:
#   expected_schema (tibble/data.frame)
#     • One row per expected column for a single lake_table_name.
#     • Required columns:
#         - lake_table_name    (chr)
#         - lake_variable_name (chr)
#         - data_type          (chr)  # logical Postgres type, e.g. "text"
#         - is_required        (lgl)  # TRUE if column must exist in table
#         - target_type        (chr)  # desired SQL type for staging (from type_decision_table)
#     • Optional columns:
#         - version_number     (int)  # version tag for expected schema
#
#   observed_schema (tibble/data.frame)
#     • One row per observed column in raw.<lake_table_name>.
#     • Required columns:
#         - lake_table_name    (chr)
#         - lake_variable_name (chr)
#         - data_type          (chr)
#         - udt_name           (chr)
#
#   lake_table_name (character scalar)
#     • Name of the lake table being validated, e.g. "cisir_labs_wth_grp".
#
#   schema_version (character scalar, optional)
#     • Version of the expected schema used for this comparison.
#     • If NULL, will be derived from expected_schema$version_number (unique).
#
# Outputs:
#   A named list with:
#     • status         : "success" (always, unless inputs invalid)
#     • lake_table_name: character scalar
#     • schema_version : character scalar
#     • n_issues       : integer count of detected issues
#     • issues         : tibble with 0+ rows, columns:
#           - lake_table_name
#           - lake_variable_name
#           - issue_code
#           - issue_type
#           - issue_group
#           - severity
#           - is_blocking
#           - expected_value
#           - observed_value
#           - check_context
#           - schema_version
#
# Side effects:
#   • None. This function is pure and does not write to the database.
#     Writing into governance.structure_qc_table is handled by
#     validate_schema().
#
# Author:
#   Noel + PULSE Pipeline (with assist from ChatGPT)
#
# Revision date:
#   2026-01-30
# =============================================================================

compare_fields <- function(expected_schema,
                           observed_schema,
                           lake_table_name,
                           schema_version = NULL) {
  # ----------------------------------------------------------------------------
  # Input validation and normalization
  # ----------------------------------------------------------------------------
  # We enforce that expected_schema and observed_schema are data.frames/tibbles
  # and contain the minimally required columns. If they do not, we fail loudly
  # with a clear error message so upstream callers can handle it deterministically.
  # ----------------------------------------------------------------------------
  if (!is.data.frame(expected_schema)) {
    stop("compare_fields(): 'expected_schema' must be a data.frame or tibble.")
  }
  
  if (!is.data.frame(observed_schema)) {
    stop("compare_fields(): 'observed_schema' must be a data.frame or tibble.")
  }
  
  required_expected_cols <- c(
    "lake_table_name",
    "lake_variable_name",
    "data_type",
    "is_required",
    "target_type"
  )
  
  missing_expected_cols <- setdiff(required_expected_cols, names(expected_schema))
  if (length(missing_expected_cols) > 0) {
    stop(
      "compare_fields(): 'expected_schema' is missing required columns: ",
      paste(missing_expected_cols, collapse = ", ")
    )
  }
  
  # For observed schema we require the identity columns plus type info.
  required_observed_cols <- c(
    "lake_table_name",
    "lake_variable_name",
    "data_type",
    "udt_name"
  )
  
  missing_observed_cols <- setdiff(required_observed_cols, names(observed_schema))
  if (length(missing_observed_cols) > 0) {
    stop(
      "compare_fields(): 'observed_schema' is missing required columns: ",
      paste(missing_observed_cols, collapse = ", ")
    )
  }
  
  # We rely on dplyr-style operations but do not assume the caller has attached
  # the tidyverse. Use explicit namespace calls to avoid accidental masking.
  # ----------------------------------------------------------------------------
  # Ensure both data frames are filtered to the target lake_table_name. This
  # allows callers to pass multi-table schemas without pre-filtering.
  # ----------------------------------------------------------------------------
  expected_tbl <- expected_schema |>
    dplyr::filter(.data$lake_table_name == !!lake_table_name)
  
  observed_tbl <- observed_schema |>
    dplyr::filter(.data$lake_table_name == !!lake_table_name)
  
  # Derive schema_version if not supplied explicitly. We look for
  # version_number in the expected schema and cast to character.
  if (is.null(schema_version)) {
    if ("version_number" %in% names(expected_tbl)) {
      version_values <- expected_tbl |>
        dplyr::distinct(.data$version_number) |>
        dplyr::pull(.data$version_number)

      if (length(version_values) == 1L) {
        schema_version <- as.character(version_values[[1L]])
      } else {
        schema_version <- NA_character_
      }
    } else {
      schema_version <- NA_character_
    }
  }
  
  # ----------------------------------------------------------------------------
  # Helper to build a single issue row as a tibble
  # ----------------------------------------------------------------------------
  make_issue <- function(lake_variable_name,
                         issue_code,
                         issue_type,
                         issue_group,
                         severity,
                         is_blocking,
                         expected_value,
                         observed_value,
                         check_context) {
    # Return a one-row tibble with the canonical fields needed by
    # governance.structure_qc_table (minus ingest_id and IDs that will be
    # attached later by validate_schema()).
    tibble::tibble(
      lake_table_name   = lake_table_name,
      lake_variable_name = lake_variable_name,
      issue_code        = issue_code,
      issue_type        = issue_type,
      issue_group       = issue_group,
      severity          = severity,
      is_blocking       = is_blocking,
      expected_value    = expected_value,
      observed_value    = observed_value,
      check_context     = check_context,
      schema_version    = schema_version
    )
  }
  
  issues <- list()
  
  # ----------------------------------------------------------------------------
  # 1. Missing required columns
  # ----------------------------------------------------------------------------
  # Any variable flagged as is_required == TRUE in the expected schema that
  # does not appear in the observed schema is treated as a critical structural
  # issue. We do not attempt to infer or auto-create these columns here.
  # ----------------------------------------------------------------------------
  expected_required <- expected_tbl |>
    dplyr::filter(.data$is_required)
  
  observed_names <- observed_tbl$lake_variable_name
  
  missing_required <- expected_required |>
    dplyr::filter(!.data$lake_variable_name %in% observed_names)
  
  if (nrow(missing_required) > 0) {
    for (i in seq_len(nrow(missing_required))) {
      row <- missing_required[i, ]
      issues[[length(issues) + 1L]] <- make_issue(
        lake_variable_name = row$lake_variable_name,
        issue_code   = "SCHEMA_MISSING_COLUMN",
        issue_type   = "Missing required column",
        issue_group  = "structural",
        severity     = "critical",
        is_blocking  = TRUE,
        expected_value = paste0(
          "Required column '", row$lake_variable_name,
          "' must exist with type ", row$data_type
        ),
        observed_value = "Column not present in observed schema",
        check_context  = "variable_level"
      )
    }
  }
  
  # ----------------------------------------------------------------------------
  # 2. Unexpected columns (present in observed, absent from expected)
  # ----------------------------------------------------------------------------
  # Columns that appear in the observed schema but are not defined in the
  # expected schema are treated as critical structural issues. A downstream
  # metadata-update process can later reclassify them as allowed columns in
  # a new schema_version.
  # ----------------------------------------------------------------------------
  expected_names <- expected_tbl$lake_variable_name
  
  unexpected <- observed_tbl |>
    dplyr::filter(!.data$lake_variable_name %in% expected_names)
  
  if (nrow(unexpected) > 0) {
    for (i in seq_len(nrow(unexpected))) {
      row <- unexpected[i, ]
      issues[[length(issues) + 1L]] <- make_issue(
        lake_variable_name = row$lake_variable_name,
        issue_code   = "SCHEMA_UNEXPECTED_COLUMN",
        issue_type   = "Unexpected column not in expected schema",
        issue_group  = "structural",
        severity     = "critical",
        is_blocking  = TRUE,
        expected_value = "No column with this name is defined in expected schema",
        observed_value = paste0(
          "Observed unexpected column '", row$lake_variable_name,
          "' with type ", row$data_type,
          " (", row$udt_name, ")"
        ),
        check_context  = "variable_level"
      )
    }
  }
  
  # ----------------------------------------------------------------------------
  # 3. Type mismatches for columns present in both schemas
  # ----------------------------------------------------------------------------
  # For variables present in both expected and observed schemas, we compare
  # data_type. The expected schema (from the dictionary) has data_type only;
  # the observed schema has both data_type and udt_name from Postgres.
  # Mismatches are treated as warnings, since they may be recoverable via
  # harmonization casting.
  # ----------------------------------------------------------------------------
  common_names <- intersect(expected_names, observed_names)

  if (length(common_names) > 0L) {
    expected_common <- expected_tbl |>
      dplyr::filter(.data$lake_variable_name %in% common_names) |>
      dplyr::select(
        .data$lake_variable_name,
        expected_data_type = .data$data_type
      )

    observed_common <- observed_tbl |>
      dplyr::filter(.data$lake_variable_name %in% common_names) |>
      dplyr::select(
        .data$lake_variable_name,
        observed_data_type = .data$data_type,
        observed_udt_name  = .data$udt_name
      )

    type_compare <- expected_common |>
      dplyr::left_join(observed_common, by = "lake_variable_name") |>
      dplyr::mutate(
        type_mismatch = (.data$expected_data_type != .data$observed_data_type)
      ) |>
      dplyr::filter(.data$type_mismatch)

    if (nrow(type_compare) > 0) {
      for (i in seq_len(nrow(type_compare))) {
        row <- type_compare[i, ]
        issues[[length(issues) + 1L]] <- make_issue(
          lake_variable_name = row$lake_variable_name,
          issue_code   = "SCHEMA_TYPE_MISMATCH",
          issue_type   = "Data type mismatch between expected and observed",
          issue_group  = "dtype",
          severity     = "warning",
          is_blocking  = FALSE,
          expected_value = row$expected_data_type,
          observed_value = paste0(
            row$observed_data_type, " (", row$observed_udt_name, ")"
          ),
          check_context  = "variable_level"
        )
      }
    }
  }
  
  # ----------------------------------------------------------------------------
  # 6. Target type validation (for staging schema coercion)
  # ----------------------------------------------------------------------------
  # Compare the observed data_type against the target_type from the
  # type_decision_table. This validates whether the current Postgres type
  # matches the desired type for the staging schema.
  #
  # We normalize both types for comparison since Postgres reports types in

  # different formats (e.g., "character varying" vs "text", "integer" vs "int4").
  # ----------------------------------------------------------------------------

  # Helper function to normalize type names for comparison
  normalize_type <- function(dtype, udt) {
    dtype_lower <- tolower(ifelse(is.na(dtype), "", dtype))
    udt_lower   <- tolower(ifelse(is.na(udt), "", udt))

    dplyr::case_when(
      # Integer types
      dtype_lower %in% c("integer", "int", "int4", "smallint", "int2", "bigint", "int8") ~ "integer",
      udt_lower %in% c("int4", "int2", "int8") ~ "integer",
      # Numeric types
      dtype_lower %in% c("numeric", "decimal", "real", "double precision") ~ "numeric",
      udt_lower %in% c("numeric", "float4", "float8") ~ "numeric",
      # Boolean types
      dtype_lower == "boolean" | udt_lower == "bool" ~ "boolean",
      # Date types
      dtype_lower == "date" ~ "date",
      # Timestamp types
      grepl("timestamp", dtype_lower) ~ "timestamp",
      # Time types
      dtype_lower == "time" | grepl("^time", dtype_lower) ~ "time",
      # Text types (default)
      dtype_lower %in% c("text", "character varying", "varchar", "char", "character") ~ "text",
      udt_lower %in% c("text", "varchar", "bpchar") ~ "text",
      # Fallback
      TRUE ~ "text"
    )
  }

  # Check for target type mismatches and missing target types
  if (length(common_names) > 0L) {
    target_check <- expected_tbl |>
      dplyr::filter(.data$lake_variable_name %in% common_names) |>
      dplyr::select(
        .data$lake_variable_name,
        .data$target_type
      ) |>
      dplyr::left_join(
        observed_tbl |>
          dplyr::select(
            .data$lake_variable_name,
            observed_data_type = .data$data_type,
            observed_udt_name  = .data$udt_name
          ),
        by = "lake_variable_name"
      ) |>
      dplyr::mutate(
        observed_type_normalized = normalize_type(.data$observed_data_type, .data$observed_udt_name),
        target_type_normalized   = tolower(ifelse(is.na(.data$target_type), "", .data$target_type))
      )

    # 6a. Missing target type (variable not in type_decision_table)
    missing_target <- target_check |>
      dplyr::filter(is.na(.data$target_type) | .data$target_type == "")

    if (nrow(missing_target) > 0) {
      for (i in seq_len(nrow(missing_target))) {
        row <- missing_target[i, ]
        issues[[length(issues) + 1L]] <- make_issue(
          lake_variable_name = row$lake_variable_name,
          issue_code   = "TYPE_TARGET_MISSING",
          issue_type   = "No target type defined in type_decision_table",
          issue_group  = "dtype",
          severity     = "warning",
          is_blocking  = FALSE,
          expected_value = "target_type should be defined in type_decision_table.xlsx",
          observed_value = paste0("observed type: ", row$observed_data_type, " (", row$observed_udt_name, ")"),
          check_context  = "variable_level"
        )
      }
    }

    # 6b. Target type mismatch (observed type != target type)
    target_mismatch <- target_check |>
      dplyr::filter(
        !is.na(.data$target_type),
        .data$target_type != "",
        .data$observed_type_normalized != .data$target_type_normalized
      )

    if (nrow(target_mismatch) > 0) {
      for (i in seq_len(nrow(target_mismatch))) {
        row <- target_mismatch[i, ]
        issues[[length(issues) + 1L]] <- make_issue(
          lake_variable_name = row$lake_variable_name,
          issue_code   = "TYPE_TARGET_MISMATCH",
          issue_type   = "Observed type does not match target type for staging",
          issue_group  = "dtype",
          severity     = "warning",
          is_blocking  = FALSE,
          expected_value = paste0("target_type = ", row$target_type),
          observed_value = paste0(
            "observed: ", row$observed_data_type, " (", row$observed_udt_name, ") ",
            "-> normalized: ", row$observed_type_normalized
          ),
          check_context  = "variable_level"
        )
      }
    }
  }

  # ----------------------------------------------------------------------------
  # Bind all issues into a single tibble
  # ----------------------------------------------------------------------------
  issues_tbl <- if (length(issues) == 0L) {
    tibble::tibble(
      lake_table_name    = character(),
      lake_variable_name = character(),
      issue_code         = character(),
      issue_type         = character(),
      issue_group        = character(),
      severity           = character(),
      is_blocking        = logical(),
      expected_value     = character(),
      observed_value     = character(),
      check_context      = character(),
      schema_version     = character()
    )
  } else {
    dplyr::bind_rows(issues)
  }
  
  # ----------------------------------------------------------------------------
  # Return a structured list for validate_schema() to consume. The higher-level
  # function will enrich these with ingest_id, source_id, source_type,
  # qc_issue_id, hashes, timestamps, and write them into structure_qc_table.
  # ----------------------------------------------------------------------------
  list(
    status         = "success",
    lake_table_name = lake_table_name,
    schema_version = schema_version,
    n_issues       = nrow(issues_tbl),
    issues         = issues_tbl
  )
}
