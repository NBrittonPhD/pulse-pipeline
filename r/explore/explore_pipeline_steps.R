# =============================================================================
# explore_pipeline_steps
# =============================================================================
# Purpose:
#   Inspect the governance.pipeline_step table to view which pipeline steps
#   are registered, their execution order, and whether they are enabled.
#   Useful for confirming step registration after running Steps 1-3 and
#   understanding the overall pipeline configuration.
#
# Usage:
#   source("r/explore/explore_pipeline_steps.R")
#   con <- connect_to_pulse()
#   explore_pipeline_steps(con)                       # All steps, ordered
#   explore_pipeline_steps(con, enabled_only = TRUE)  # Only enabled steps
#   explore_pipeline_steps(con, step_type = "R")      # Filter by type
#
# Author: Noel
# Last Updated: 2026-01-29
# =============================================================================

library(DBI)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
explore_pipeline_steps <- function(con,
                                   step_type = NULL,
                                   enabled_only = FALSE) {

    # -------------------------------------------------------------------------
    # INPUT VALIDATION
    # -------------------------------------------------------------------------
    if (!inherits(con, "DBIConnection")) {
        stop("[explore_pipeline_steps] ERROR: 'con' must be a valid DBI connection.")
    }

    # -------------------------------------------------------------------------
    # BUILD QUERY WITH OPTIONAL FILTERS
    # -------------------------------------------------------------------------
    where_clauses <- c()

    if (!is.null(step_type)) {
        where_clauses <- c(where_clauses, glue("step_type = '{step_type}'"))
    }

    if (enabled_only) {
        where_clauses <- c(where_clauses, "enabled = TRUE")
    }

    where_sql <- ""
    if (length(where_clauses) > 0) {
        where_sql <- paste("WHERE", paste(where_clauses, collapse = " AND "))
    }

    query <- glue("
        SELECT
            step_id,
            step_order,
            step_name,
            step_description,
            step_type,
            code_snippet,
            enabled,
            created_at_utc,
            last_modified_utc
        FROM governance.pipeline_step
        {where_sql}
        ORDER BY step_order ASC
    ")

    # -------------------------------------------------------------------------
    # EXECUTE AND RETURN
    # -------------------------------------------------------------------------
    result <- DBI::dbGetQuery(con, query)

    if (nrow(result) == 0) {
        message("[explore_pipeline_steps] No pipeline steps found.")
    } else {
        message(glue("[explore_pipeline_steps] Returned {nrow(result)} step(s)."))
    }

    result
}
