# =============================================================================
# write_audit_event.R
# -----------------------------------------------------------------------------
# Inserts an audit record into governance.audit_log.
#
# This function is used by:
#   - register_source()   (for source_registration and source_update)
#   - future steps (batch ingest, schema validation, harmonization, etc.)
#
# PARAMETERS:
#   con          : DBI connection
#   ingest_id    : Ingest batch identifier (nullable)
#   event_type   : High-level event type (e.g., "source_registration")
#   object_type  : "table", "file", "function", etc.
#   object_name  : Name of the affected object (e.g., "governance.source_registry")
#   details      : A named list with arbitrary event metadata (will be JSON encoded)
#   status       : "success", "error", "warning", etc.
#
# RETURNS:
#   audit_id (invisible)
#
# =============================================================================

write_audit_event <- function(
    con,
    ingest_id   = NULL,
    event_type,
    object_type,
    object_name,
    details     = NULL,
    status      = NULL
) {
  
  # ----------------------------------------------------------
  # 1. Collision-proof audit_id (UUID-based)
  # ----------------------------------------------------------
  audit_id <- paste0("AUD_", uuid::UUIDgenerate())
  
  # ----------------------------------------------------------
  # 2. Convert NULL → NA for DBI parameter binding
  # ----------------------------------------------------------
  if (is.null(ingest_id)) ingest_id <- NA_character_
  
  # ----------------------------------------------------------
  # 3. Construct human-readable action string
  #    Examples:
  #       "source_registration|success|table|governance.source_registry"
  #       "source_update|success|table|governance.source_registry"
  # ----------------------------------------------------------
  action_parts <- c(event_type, status, object_type, object_name)
  action <- paste(stats::na.omit(action_parts), collapse = "|")
  
  # ----------------------------------------------------------
  # 4. JSON-encode details payload
  # ----------------------------------------------------------
  details_list <- list(
    event_type  = event_type,
    object_type = object_type,
    object_name = object_name,
    status      = status,
    payload     = details
  )
  
  details_json <- jsonlite::toJSON(
    details_list,
    auto_unbox = TRUE,
    null       = "null"
  )
  
  # ----------------------------------------------------------
  # 5. Identify executing user
  # ----------------------------------------------------------
  executed_by <- tryCatch(
    DBI::dbGetQuery(con, "SELECT current_user")[[1]],
    error = function(e) Sys.info()[["user"]]
  )
  
  # ----------------------------------------------------------
  # 6. Insert into governance.audit_log (parameterized SQL)
  # ----------------------------------------------------------
  sql <- "
    INSERT INTO governance.AUDIT_LOG (
      audit_id,
      ingest_id,
      action,
      details,
      executed_by
    )
    VALUES ($1, $2, $3, $4, $5);
  "
  
  DBI::dbExecute(
    con,
    sql,
    params = list(
      audit_id,
      ingest_id,
      action,
      details_json,
      executed_by
    )
  )
  
  invisible(audit_id)
}