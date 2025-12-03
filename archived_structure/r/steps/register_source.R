# =============================================================================
# STEP 1 — REGISTER SOURCE
# Inserts/updates SOURCE_REGISTRY, validates vocab + metadata, creates dirs,
# and writes audit events. Designed for Option B (full metadata support).
# =============================================================================

suppressPackageStartupMessages({
  library(DBI)
  library(glue)
  library(jsonlite)
  library(uuid)
})

# -----------------------------------------------------------------------------
# write_audit_event()
# Unified audit-logging function for governance.audit_log
# -----------------------------------------------------------------------------
write_audit_event <- function(
    con,
    ingest_id   = NA_character_,
    event_type,
    object_type,
    object_name,
    details = NULL,
    status  = NULL
) {
  
  audit_id <- paste0("AUD_", uuid::UUIDgenerate())
  
  action_parts <- c(event_type, status, object_type, object_name)
  action <- paste(stats::na.omit(action_parts), collapse = "|")
  
  details_json <- jsonlite::toJSON(
    list(
      event_type  = event_type,
      object_type = object_type,
      object_name = object_name,
      status      = status,
      payload     = details
    ),
    auto_unbox = TRUE,
    null       = "null"
  )
  
  executed_by <- tryCatch(
    DBI::dbGetQuery(con, "SELECT current_user")[[1]],
    error = function(e) Sys.info()[["user"]] %||% "unknown_user"
  )
  
  sql <- "
    INSERT INTO governance.audit_log (
      audit_id,
      ingest_id,
      action,
      details,
      executed_by
    )
    VALUES (?,?,?,?,?);
  "
  
  DBI::dbExecute(
    con,
    sql,
    params = list(audit_id, ingest_id, action, details_json, executed_by)
  )
  
  invisible(audit_id)
}

# -----------------------------------------------------------------------------
# validate_source_entry()
# Validates core + extended metadata (Option B).
# -----------------------------------------------------------------------------
validate_source_entry <- function(source_params, settings) {
  
  required <- c(
    "source_id",
    "source_name",
    "system_type",
    "update_frequency",
    "ingest_method",
    "pii_classification"
  )
  
  missing <- setdiff(required, names(source_params))
  if (length(missing) > 0) {
    stop(glue("Missing required source parameters: {paste(missing, collapse=', ')}"))
  }
  
  # Controlled vocab
  if (!source_params$system_type %in% settings$allowed_system_type) {
    stop(glue("Invalid system_type: {source_params$system_type}"))
  }
  
  if (!source_params$update_frequency %in% settings$allowed_update_frequency) {
    stop(glue("Invalid update_frequency: {source_params$update_frequency}"))
  }
  
  if (!source_params$ingest_method %in% settings$allowed_ingest_method) {
    stop(glue("Invalid ingest_method: {source_params$ingest_method}"))
  }
  
  if (!source_params$pii_classification %in% settings$allowed_pii_classification) {
    stop(glue("Invalid pii_classification: {source_params$pii_classification}"))
  }
  
  # Optional extended metadata
  if (!is.null(source_params$expected_schema_version)) {
    if (!grepl("^[0-9]+\\.[0-9]+\\.[0-9]+$", source_params$expected_schema_version)) {
      stop("expected_schema_version must follow semantic versioning, e.g. '1.0.0'")
    }
  }
  
  if (!is.null(source_params$active)) {
    if (!is.logical(source_params$active)) {
      stop("'active' must be TRUE or FALSE")
    }
  }
  
  TRUE
}

# -----------------------------------------------------------------------------
# create_source_folders()
# Builds raw/, staging/, validated/, archive/, etc.
# -----------------------------------------------------------------------------
create_source_folders <- function(source_id, settings) {
  
  dirs <- settings$directory_structure
  
  for (d in dirs) {
    full <- file.path(d, source_id)
    if (!dir.exists(full)) dir.create(full, recursive = TRUE)
  }
  
  invisible(TRUE)
}

# -----------------------------------------------------------------------------
# register_source()
# INSERTs or UPDATEs full metadata into governance.source_registry (Option B)
# -----------------------------------------------------------------------------
register_source <- function(
    con,
    source_id,
    source_name,
    system_type,
    update_frequency,
    data_owner = NULL,
    ingest_method,
    expected_schema_version = NULL,
    retention_policy = NULL,
    pii_classification,
    active = TRUE,
    settings,
    ...
) {
  
  # Validate metadata
  validate_source_entry(
    source_params = list(
      source_id                = source_id,
      source_name              = source_name,
      system_type              = system_type,
      update_frequency         = update_frequency,
      data_owner               = data_owner,
      ingest_method            = ingest_method,
      expected_schema_version  = expected_schema_version,
      retention_policy         = retention_policy,
      pii_classification       = pii_classification,
      active                   = active
    ),
    settings = settings
  )
  
  existing <- DBI::dbGetQuery(
    con,
    "SELECT source_id FROM governance.source_registry WHERE source_id = $1;",
    params = list(source_id)
  )
  
  # INSERT new
  if (nrow(existing) == 0) {
    
    DBI::dbExecute(
      con,
      "
        INSERT INTO governance.source_registry (
          source_id,
          source_name,
          system_type,
          update_frequency,
          data_owner,
          ingest_method,
          expected_schema_version,
          retention_policy,
          pii_classification,
          active,
          created_at_utc,
          last_modified_utc
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
      ",
      params = list(
        source_id,
        source_name,
        system_type,
        update_frequency,
        data_owner,
        ingest_method,
        expected_schema_version,
        retention_policy,
        pii_classification,
        active
      )
    )
    
    write_audit_event(
      con,
      ingest_id   = NA,
      event_type  = "REGISTER_SOURCE",
      object_type = "source",
      object_name = source_id,
      details     = list(params = list(
        source_id, source_name, system_type, update_frequency,
        data_owner, ingest_method, expected_schema_version,
        retention_policy, pii_classification, active
      )),
      status = "success"
    )
    
    message(glue("Source '{source_id}' registered."))
    
  } else {
    
    # UPDATE existing
    DBI::dbExecute(
      con,
      "
        UPDATE governance.source_registry
        SET
          source_name        = $1,
          system_type        = $1,
          update_frequency   = $1,
          data_owner         = $1,
          ingest_method      = $1,
          expected_schema_version = $1,
          retention_policy   = $1,
          pii_classification = $1,
          active             = ?,
          last_modified_utc  = CURRENT_TIMESTAMP
        WHERE source_id = $1;
      ",
      params = list(
        source_id,
        source_name,
        system_type,
        update_frequency,
        data_owner,
        ingest_method,
        expected_schema_version,
        retention_policy,
        pii_classification,
        active
      )
    )
    
    write_audit_event(
      con,
      ingest_id   = NA,
      event_type  = "UPDATE_SOURCE",
      object_type = "source",
      object_name = source_id,
      details     = list(params = list(
        source_id, source_name, system_type, update_frequency,
        data_owner, ingest_method, expected_schema_version,
        retention_policy, pii_classification, active
      )),
      status = "success"
    )
    
    message(glue("Source '{source_id}' updated."))
  }
  
  # Build folders
  create_source_folders(source_id, settings)
  
  message("Step 1 complete.")
  invisible(TRUE)
}

# -----------------------------------------------------------------------------
# Wrapper used by runner.R
# -----------------------------------------------------------------------------
run_step1_register_source <- function(con, source_params, settings = NULL) {
  
  do.call(
    register_source,
    c(
      list(con = con),
      source_params,
      list(settings = settings)
    )
  )
  
  invisible(TRUE)
}
