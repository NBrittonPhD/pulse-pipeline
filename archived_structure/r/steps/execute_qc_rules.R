execute_qc_rules <- function(ingest_id, con) {
  message("Executing QC rules...")
  
  # Load all enabled rules
  rules <- dbReadTable(con, "rule_library") %>%
    dplyr::filter(enabled == TRUE)
  
  # Load all enabled rule-variable mappings
  mappings <- dbReadTable(con, "rule_execution_map") %>%
    dplyr::filter(enabled == TRUE)
  
  # Loop over each mapping
  for (i in seq_len(nrow(mappings))) {
    
    map  <- mappings[i, ]
    rule <- rules[rules$rule_id == map$rule_id, ]
    
    # Start with rule template
    sql <- rule$rule_sql
    
    # Required substitutions
    sql <- gsub("\\{table\\}",    map$lake_table,    sql)
    sql <- gsub("\\{variable\\}", map$lake_variable, sql)
    
    # Parse JSON rule parameters (if any)
    if (!is.na(map$rule_params) && nzchar(map$rule_params)) {
      params <- jsonlite::fromJSON(map$rule_params)
    } else {
      params <- list()
    }
    
    # Allowed values
    if ("allowed" %in% names(params)) {
      allowed_vals <- paste0("'", paste(params$allowed, collapse = "','"), "'")
      sql <- gsub("\\{allowed\\}", allowed_vals, sql)
    }
    
    # Numeric min/max
    if ("min" %in% names(params)) sql <- gsub("\\{min\\}", params$min, sql)
    if ("max" %in% names(params)) sql <- gsub("\\{max\\}", params$max, sql)
    
    # Execute the QC SQL
    result <- tryCatch(
      dbGetQuery(con, sql),
      error = function(e) {
        warning(paste("QC rule failed for", map$rule_id, ":", conditionMessage(e)))
        data.frame(failing_rows = NA)
      }
    )
    
    # Extract failing row count safely
    if (nrow(result) == 0 || !"failing_rows" %in% names(result)) {
      failing <- NA
    } else {
      failing <- as.integer(result$failing_rows[1])
    }
    
    # Log into rule_execution_log
    dbExecute(
      con,
      glue::glue("
        INSERT INTO rule_execution_log (
            ingest_id,
            rule_id,
            lake_table,
            lake_variable,
            failing_rows,
            executed_at_utc
        )
        VALUES (
            '{ingest_id}',
            '{map$rule_id}',
            '{map$lake_table}',
            '{map$lake_variable}',
            {ifelse(is.na(failing), 'NULL', failing)},
            CURRENT_TIMESTAMP
        );
      ")
    )
  }
}
