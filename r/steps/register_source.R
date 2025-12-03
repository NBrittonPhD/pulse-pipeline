# =============================================================================
# register_source.R
# -----------------------------------------------------------------------------
# Main function for registering or updating a data source in the PULSE pipeline.
#
# Responsibilities:
#   - Validate inputs against controlled vocabularies
#   - Insert or update governance.source_registry
#   - Create on-disk folder structure for new sources
#   - Write governance audit_log events for inserts/updates
#
# This is Step 1 core logic. It is wrapped by run_step1_register_source().
# =============================================================================

register_source <- function(
    con,
    source_id,
    source_name,
    system_type,
    update_frequency,
    data_owner,
    ingest_method,
    expected_schema_version,
    retention_policy = NULL,
    pii_classification,
    active = TRUE,
    created_by = NULL   # DB default SESSION_USER applies if NULL
) {
  
  # -------------------------------------------------------------------------
  # 1. Load pipeline settings (vocab lists)
  # -------------------------------------------------------------------------
  proj_root <- getOption("pulse.proj_root", default = ".")
  settings <- yaml::read_yaml(file.path(proj_root, "config", "pipeline_settings.yml"))
  
  
  # -------------------------------------------------------------------------
  # 2. Pack fields for validation
  # -------------------------------------------------------------------------
  candidate <- list(
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
  
  # -------------------------------------------------------------------------
  # 3. Validate fields (stops if invalid)
  # -------------------------------------------------------------------------
  validate_source_entry(candidate, settings)
  
  # -------------------------------------------------------------------------
  # 4. Check if source_id exists
  # -------------------------------------------------------------------------
  existing <- DBI::dbGetQuery(
    con,
    glue::glue("
      SELECT COUNT(*) AS n
      FROM governance.source_registry
      WHERE source_id = '{source_id}'
    ")
  )$n > 0
  
  # -------------------------------------------------------------------------
  # 5. Determine created_by value
  #    If user supplied one → quote it
  #    If NULL → use DB DEFAULT (SESSION_USER)
  # -------------------------------------------------------------------------
  created_by_value <- if (!is.null(created_by)) {
    paste0("'", created_by, "'")
  } else {
    "DEFAULT"
  }
  
  # =========================================================================
  # INSERT LOGIC: new source_id
  # =========================================================================
  if (!existing) {
    
    sql <- glue::glue("
      INSERT INTO governance.source_registry (
        source_id, source_name, system_type, update_frequency,
        data_owner, ingest_method, expected_schema_version,
        retention_policy, pii_classification, active, created_by
      )
      VALUES (
        '{source_id}',
        '{source_name}',
        '{system_type}',
        '{update_frequency}',
        '{data_owner}',
        '{ingest_method}',
        '{expected_schema_version}',
        {if (is.null(retention_policy)) 'NULL' else paste0(\"'\", retention_policy, \"'\")},
        '{pii_classification}',
        {tolower(as.character(active))},
        {created_by_value}
      );
    ")
    
    DBI::dbExecute(con, sql)
    
    # -----------------------------------------------------------------------
    # 6. Create folder structure
    # -----------------------------------------------------------------------
    proj_root <- getOption("pulse.proj_root", default = ".")
    created_dirs <- create_source_folders(
      source_id = source_id,
      base_path = proj_root
    )
    
    # -----------------------------------------------------------------------
    # 7. Write audit log entry (insert event)
    # -----------------------------------------------------------------------
    write_audit_event(
      con,
      ingest_id    = NULL,
      event_type   = "source_registration",
      object_type  = "table",
      object_name  = "governance.source_registry",
      details      = list(
        action       = "insert",
        source_id    = source_id,
        created_dirs = created_dirs
      ),
      status       = "success"
    )
    
  } else {
    
    # =========================================================================
    # UPDATE LOGIC: existing source_id
    # (last_modified_utc is updated automatically by trigger)
    # =========================================================================
    
    sql <- glue::glue("
      UPDATE governance.source_registry
      SET
        source_name = '{source_name}',
        system_type = '{system_type}',
        update_frequency = '{update_frequency}',
        data_owner = '{data_owner}',
        ingest_method = '{ingest_method}',
        expected_schema_version = '{expected_schema_version}',
        retention_policy = {if (is.null(retention_policy)) 'NULL' else paste0(\"'\", retention_policy, \"'\")},
        pii_classification = '{pii_classification}',
        active = {tolower(as.character(active))}
      WHERE source_id = '{source_id}';
    ")
    
    DBI::dbExecute(con, sql)
    
    # -----------------------------------------------------------------------
    # 8. Write audit log entry (update event)
    # -----------------------------------------------------------------------
    write_audit_event(
      con,
      ingest_id    = NULL,
      event_type   = "source_update",
      object_type  = "table",
      object_name  = "governance.source_registry",
      details      = list(
        action    = "update",
        source_id = source_id
      ),
      status       = "success"
    )
  }
  
  invisible(TRUE)
}