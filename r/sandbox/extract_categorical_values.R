# =============================================================================
# r/sandbox/extract_categorical_values.R
# =============================================================================
# PURPOSE
#   Extract DISTINCT values for categorical variables from raw.* tables
#   to populate the "allowed_values" column in a metadata dictionary.
#
# USAGE
#   source("r/connect_to_pulse.R")
#   source("r/sandbox/extract_categorical_values.R")
#
#   con <- connect_to_pulse()
#   results <- extract_categorical_values(con)
#   DBI::dbDisconnect(con)
#
# OUTPUT
#   Returns a tibble with columns:
#     - variable_name
#     - table_name
#     - unique_count
#     - unique_values (comma-separated, alphabetically sorted)
#     - status (ok, unconstrained, not_found, error)
#
# NOTES
#   - Only extracts DISTINCT values (no PHI)
#   - Variables with >50 unique values are flagged as "unconstrained"
#   - Safe to share output since it contains only value domains
# =============================================================================

library(DBI)
library(dplyr)
library(tibble)
library(purrr)
library(glue)
library(stringr)

# -----------------------------------------------------------------------------
# Variable-to-Table Mapping
# -----------------------------------------------------------------------------
# This mapping is based on the raw_ingest_profile.csv analysis
# Update this if your schema changes

VARIABLE_TABLE_MAP <- list(
  # GROUP 1 - DEMOGRAPHICS
  gender = c("cisir_encounter", "clarity_patient_ustc"),
  race = c("cisir_encounter", "clarity_patient_ustc"),
  ethnic_group = c("clarity_patient_ustc"),
  ethnicity_id = c("cisir_encounter"),
  Ethinicity = c("cisir_encounter"),
  language = c("cisir_encounter"),
  marital_status = c("cisir_encounter"),

  # GROUP 2 - GEOGRAPHY
  state = c("cisir_encounter"),
  country = c("cisir_encounter"),
  poi_state = c("trauma_registry_demo_scores"),

  # GROUP 3 - DISPOSITION
  discharge_disposition = c("cisir_encounter"),
  disposition = c("trauma_registry_operations"),
  admitstatus = c("trauma_registry_complications", "trauma_registry_demo_scores"),

  # GROUP 4 - CATEGORIES
  category = c("cisir_complications", "cisir_preexisting_conditions", "trauma_registry_pmh"),
  dx_category = c("cisir_dx"),

  # GROUP 5 - MEDICATION
  admin_action = c("cisir_meds_wth_grp"),
  pharmacy_class = c("cisir_meds_wth_grp"),
  pharmacy_subclass = c("cisir_meds_wth_grp"),
  therapeutic_class = c("cisir_meds_wth_grp"),

  # GROUP 6 - BP LOCATION
  max_diastolic_b_p_location = c("cisir_vitals_minmax"),
  max_systolic_b_p_location = c("cisir_vitals_minmax"),
  min_diastolic_b_p_location = c("cisir_vitals_minmax"),
  min_systolic_b_p_location = c("cisir_vitals_minmax"),

  # GROUP 7 - RECORD METHOD
  max_diastolic_b_p_record_method = c("cisir_vitals_minmax"),
  max_systolic_b_p_record_method = c("cisir_vitals_minmax"),
  min_diastolic_b_p_record_method = c("cisir_vitals_minmax"),
  min_systolic_b_p_record_method = c("cisir_vitals_minmax"),
  max_heartrate_source = c("cisir_vitals_minmax"),
  min_heartrate_source = c("cisir_vitals_minmax"),

  # GROUP 8 - MOTOR/SENSATION
  max_motor_respone_lle = c("cisir_vitals_minmax"),  # Note: typo in source data (respone)
  max_motor_response_lue = c("cisir_vitals_minmax"),
  max_motor_response_rle = c("cisir_vitals_minmax"),
  max_motor_response_rue = c("cisir_vitals_minmax"),
  min_motor_respone_lle = c("cisir_vitals_minmax"),  # Note: typo in source data (respone)
  min_motor_respone_rle = c("cisir_vitals_minmax"),  # Note: typo in source data (respone)
  min_motor_response_lue = c("cisir_vitals_minmax"),
  min_motor_response_rue = c("cisir_vitals_minmax"),
  max_sensation_lle = c("cisir_vitals_minmax"),
  max_sensation_lue = c("cisir_vitals_minmax"),
  max_sensation_rle = c("cisir_vitals_minmax"),
  max_sensation_rue = c("cisir_vitals_minmax"),
  min_sensation_lle = c("cisir_vitals_minmax"),
  min_sensation_lue = c("cisir_vitals_minmax"),
  min_sensation_rle = c("cisir_vitals_minmax"),
  min_sensation_rue = c("cisir_vitals_minmax"),

  # GROUP 9 - VITALS OTHER (not found in current schema - will check dynamically)
  # gait_transfer_max, gait_transfer_min, max_patient_position, min_patient_position

  # GROUP 10 - LAB/SPECIMEN
  unit_of_measure = c("clarity_lab_results_ustc"),
  units = c(
    "trauma_registry_labs_2009", "trauma_registry_labs_2010", "trauma_registry_labs_2011",
    "trauma_registry_labs_2012", "trauma_registry_labs_2013", "trauma_registry_labs_2014",
    "trauma_registry_labs_2015", "trauma_registry_labs_2016", "trauma_registry_labs_2017",
    "trauma_registry_labs_2018", "trauma_registry_labs_2019", "trauma_registry_labs_2020",
    "trauma_registry_labs_2021", "trauma_registry_labs_2022", "trauma_registry_labs_2023"
  ),
  order_type = c("clarity_order_micro_result_ustc"),
  specimen_source = c("clarity_order_micro_result_ustc"),
  specimen_type = c("clarity_order_micro_result_ustc"),

  # GROUP 11 - ENCOUNTER/SERVICE
  admit_type_name = c("cisir_encounter"),
  admit_unit = c("cisir_encounter"),
  discharge_unit = c("cisir_encounter"),
  event_type = c("clarity_pat_enc_flw_ustc"),
  event_subtype = c("clarity_pat_enc_flw_ustc"),
  hosp_service = c("clarity_pat_enc_flw_ustc"),
  level_of_care = c("clarity_pat_enc_flw_ustc"),
  pat_class = c("clarity_pat_enc_flw_ustc"),
  specialty = c("clarity_pat_enc_flw_ustc"),
  means_of_arrival = c("cisir_encounter"),
  ord_phase_of_care = c("clarity_order_med_ustc"),
  response = c("trauma_registry_ems_procs_all"),
  revenue_location = c("cisir_encounter"),
  tru_disp_unit = c("trauma_registry_demo_scores"),
  unit_flag = c("cisir_encounter")
)

# -----------------------------------------------------------------------------
# Core Function: Extract distinct values for a single variable from one table
# -----------------------------------------------------------------------------
get_distinct_values <- function(con, table_name, variable_name, max_values = 50) {

  # Check if column exists in table
  col_check_sql <- glue::glue_sql(
    "SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = 'raw'
       AND table_name = {table_name}
       AND column_name = {variable_name}",
    .con = con
  )

  col_exists <- DBI::dbGetQuery(con, col_check_sql)

  if (nrow(col_exists) == 0) {
    return(tibble::tibble(
      variable_name = variable_name,
      table_name = paste0("raw.", table_name),
      unique_count = NA_integer_,
      unique_values = NA_character_,
      status = "column_not_found"
    ))
  }

  # Get count of distinct non-null values
  count_sql <- glue::glue_sql(
    "SELECT COUNT(DISTINCT {`variable_name`}) AS n_distinct
     FROM raw.{`table_name`}
     WHERE {`variable_name`} IS NOT NULL",
    .con = con
  )

  count_result <- tryCatch(
    DBI::dbGetQuery(con, count_sql),
    error = function(e) {
      return(data.frame(n_distinct = NA_integer_))
    }
  )

  n_distinct <- count_result$n_distinct[1]

  if (is.na(n_distinct) || n_distinct == 0) {
    return(tibble::tibble(
      variable_name = variable_name,
      table_name = paste0("raw.", table_name),
      unique_count = 0L,
      unique_values = "(all null)",
      status = "all_null"
    ))
  }

  # Determine if unconstrained
  if (n_distinct > max_values) {
    # Get just a sample of values
    sample_sql <- glue::glue_sql(
      "SELECT DISTINCT {`variable_name`} AS val
       FROM raw.{`table_name`}
       WHERE {`variable_name`} IS NOT NULL
       ORDER BY {`variable_name`}
       LIMIT 20",
      .con = con
    )

    sample_result <- tryCatch(
      DBI::dbGetQuery(con, sample_sql),
      error = function(e) {
        return(data.frame(val = character(0)))
      }
    )

    sample_values <- paste0(
      paste(sort(sample_result$val), collapse = ", "),
      " ... (", n_distinct, " total - UNCONSTRAINED)"
    )

    return(tibble::tibble(
      variable_name = variable_name,
      table_name = paste0("raw.", table_name),
      unique_count = as.integer(n_distinct),
      unique_values = sample_values,
      status = "unconstrained"
    ))
  }

  # Get all distinct values
  values_sql <- glue::glue_sql(
    "SELECT DISTINCT {`variable_name`} AS val
     FROM raw.{`table_name`}
     WHERE {`variable_name`} IS NOT NULL
     ORDER BY {`variable_name`}",
    .con = con
  )

  values_result <- tryCatch(
    DBI::dbGetQuery(con, values_sql),
    error = function(e) {
      return(data.frame(val = character(0)))
    }
  )

  if (nrow(values_result) == 0) {
    return(tibble::tibble(
      variable_name = variable_name,
      table_name = paste0("raw.", table_name),
      unique_count = 0L,
      unique_values = "(no values)",
      status = "no_values"
    ))
  }

  values_str <- paste(sort(values_result$val), collapse = ", ")

  tibble::tibble(
    variable_name = variable_name,
    table_name = paste0("raw.", table_name),
    unique_count = as.integer(nrow(values_result)),
    unique_values = values_str,
    status = "ok"
  )
}

# -----------------------------------------------------------------------------
# Main Function: Extract all categorical values
# -----------------------------------------------------------------------------
extract_categorical_values <- function(con,
                                       variable_map = VARIABLE_TABLE_MAP,
                                       max_values = 50,
                                       verbose = TRUE) {

  if (verbose) message(">> Starting categorical value extraction...")

  # Expand the map into a flat list of (variable, table) pairs
  extraction_list <- purrr::imap_dfr(variable_map, function(tables, var) {
    tibble::tibble(
      variable_name = var,
      table_name = tables
    )
  })

  if (verbose) {
    message(">> Found ", nrow(extraction_list), " variable-table combinations to query")
  }

  # Process each combination
  results <- purrr::pmap_dfr(extraction_list, function(variable_name, table_name) {
    if (verbose) {
      message("   Querying: raw.", table_name, ".", variable_name)
    }

    tryCatch(
      get_distinct_values(con, table_name, variable_name, max_values),
      error = function(e) {
        tibble::tibble(
          variable_name = variable_name,
          table_name = paste0("raw.", table_name),
          unique_count = NA_integer_,
          unique_values = paste("ERROR:", e$message),
          status = "error"
        )
      }
    )
  })

  if (verbose) {
    message(">> Extraction complete!")
    message("   Total queries: ", nrow(results))
    message("   OK: ", sum(results$status == "ok"))
    message("   Unconstrained: ", sum(results$status == "unconstrained"))
    message("   Not found: ", sum(results$status == "column_not_found"))
    message("   All null: ", sum(results$status == "all_null"))
    message("   Errors: ", sum(results$status == "error"))
  }

  results
}

# -----------------------------------------------------------------------------
# Helper: Consolidate values across multiple tables
# -----------------------------------------------------------------------------
consolidate_values <- function(results) {
  results %>%
    dplyr::filter(status %in% c("ok", "unconstrained")) %>%
    dplyr::group_by(variable_name) %>%
    dplyr::summarise(
      tables = paste(table_name, collapse = "; "),
      total_unique_count = sum(unique_count, na.rm = TRUE),
      all_values = paste(unique_values, collapse = " | "),
      .groups = "drop"
    ) %>%
    dplyr::arrange(variable_name)
}

# -----------------------------------------------------------------------------
# Helper: Export to CSV
# -----------------------------------------------------------------------------
export_categorical_values <- function(results,
                                      output_path = "output/profiling/categorical_values.csv") {

  # Ensure directory exists
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)

  # Write CSV
  readr::write_csv(results, output_path)

  message(">> Exported to: ", output_path)
  invisible(output_path)
}

# -----------------------------------------------------------------------------
# Helper: Pretty print for console review
# -----------------------------------------------------------------------------
print_categorical_summary <- function(results, max_width = 80) {

  cat("\n")
  cat(strrep("=", max_width), "\n")
  cat("CATEGORICAL VALUES EXTRACTION SUMMARY\n")
  cat(strrep("=", max_width), "\n\n")

  # Group by variable for cleaner display
  by_var <- split(results, results$variable_name)

  for (var_name in sort(names(by_var))) {
    var_data <- by_var[[var_name]]

    cat("VARIABLE: ", var_name, "\n")
    cat(strrep("-", max_width), "\n")

    for (i in seq_len(nrow(var_data))) {
      row <- var_data[i, ]
      cat("  Table: ", row$table_name, "\n")
      cat("  Status: ", row$status, "\n")
      cat("  Count: ", row$unique_count, "\n")

      # Truncate long value lists
      values_str <- row$unique_values
      if (nchar(values_str) > max_width - 10) {
        values_str <- paste0(substr(values_str, 1, max_width - 13), "...")
      }
      cat("  Values: ", values_str, "\n")
      cat("\n")
    }
  }

  cat(strrep("=", max_width), "\n")
}

# =============================================================================
# EXAMPLE USAGE (uncomment to run)
# =============================================================================
#
# # Load connection function
# source("r/connect_to_pulse.R")
#
# # Connect to database
# con <- connect_to_pulse()
#
# # Extract categorical values
# results <- extract_categorical_values(con, verbose = TRUE)
#
# # View results
# print_categorical_summary(results)
#
# # Export to CSV
# export_categorical_values(results, "output/profiling/categorical_values.csv")
#
# # Get consolidated view (combine values from multiple tables per variable)
# consolidated <- consolidate_values(results)
# View(consolidated)
#
# # Clean up
# DBI::dbDisconnect(con)
