write_source_params <- function(
    source_id,
    source_name,
    system_type       = "CSV",
    update_frequency  = "monthly",
    data_owner        = NULL,
    ingest_method     = "pull",
    expected_schema_version = "1.0.0",
    retention_policy  = NULL,
    pii_classification = "PHI",
    active            = TRUE,
    file              = "config/source_params.yml"
) {
  params <- list(
    source_id               = source_id,
    source_name             = source_name,
    system_type             = system_type,
    update_frequency        = update_frequency,
    data_owner              = data_owner,
    ingest_method           = ingest_method,
    expected_schema_version = expected_schema_version,
    retention_policy        = retention_policy,
    pii_classification      = pii_classification,
    active                  = active
  )
  
  yaml::write_yaml(params, file)
  message("✓ source_params.yml written to ", file)
  return(invisible(params))
}
