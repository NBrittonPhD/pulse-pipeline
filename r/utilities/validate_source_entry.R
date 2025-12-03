# r/utilities/validate_source_entry.R
# =============================================================================
# validate_source_entry()
#
# Ensures all required fields are present and that values match the allowed
# vocabulary defined in config/pipeline_settings.yml.
#
# Stops with a clear, human-readable error message if any check fails.
#
# Inputs:
#   candidate (list): fields provided to register_source()
#   settings  (list): loaded pipeline settings YAML
#
# Returns:
#   TRUE (invisible) if all validations succeed.
# =============================================================================

validate_source_entry <- function(candidate, settings) {
  
  # ---------------------------------------------------------------------------
  # 1. Required fields that must exist and not be NULL
  #    (pulled from settings$required_source_fields)
  # ---------------------------------------------------------------------------
  required_fields <- settings$required_source_fields
  
  missing <- setdiff(required_fields, names(candidate))
  if (length(missing) > 0) {
    stop(glue::glue(
      "Missing required fields: {paste(missing, collapse = ', ')}"
    ))
  }
  
  nulls <- required_fields[sapply(candidate[required_fields], is.null)]
  if (length(nulls) > 0) {
    stop(glue::glue(
      "Required fields cannot be NULL: {paste(nulls, collapse = ', ')}"
    ))
  }
  
  # ---------------------------------------------------------------------------
  # 2. Validate controlled vocabularies
  #    (these live at top level in pipeline_settings.yml)
  # ---------------------------------------------------------------------------
  
  allowed_system_types       <- settings$allowed_system_type
  allowed_update_frequencies <- settings$allowed_update_frequency
  allowed_ingest_methods     <- settings$allowed_ingest_method
  allowed_pii                <- settings$allowed_pii_classification
  
  # system_type
  if (!(candidate$system_type %in% allowed_system_types)) {
    stop(glue::glue(
      "Invalid system_type '{candidate$system_type}'. ",
      "Allowed: {paste(allowed_system_types, collapse = ', ')}"
    ))
  }
  
  # update_frequency
  if (!(candidate$update_frequency %in% allowed_update_frequencies)) {
    stop(glue::glue(
      "Invalid update_frequency '{candidate$update_frequency}'. ",
      "Allowed: {paste(allowed_update_frequencies, collapse = ', ')}"
    ))
  }
  
  # ingest_method
  if (!(candidate$ingest_method %in% allowed_ingest_methods)) {
    stop(glue::glue(
      "Invalid ingest_method '{candidate$ingest_method}'. ",
      "Allowed: {paste(allowed_ingest_methods, collapse = ', ')}"
    ))
  }
  
  # pii_classification
  if (!(candidate$pii_classification %in% allowed_pii)) {
    stop(glue::glue(
      "Invalid pii_classification '{candidate$pii_classification}'. ",
      "Allowed: {paste(allowed_pii, collapse = ', ')}"
    ))
  }
  
  # ---------------------------------------------------------------------------
  # 3. Validate boolean fields
  # ---------------------------------------------------------------------------
  if (!is.logical(candidate$active) || length(candidate$active) != 1) {
    stop("Field 'active' must be TRUE or FALSE.")
  }
  
  invisible(TRUE)
}
