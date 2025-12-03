sync_metadata <- function(ingest_id = NULL, con) {
  message("Syncing metadata with CORE_METADATA_DICTIONARY.xlsx...")
  
  # Load Excel
  meta <- readxl::read_excel("CORE_METADATA_DICTIONARY.xlsx") %>%
    dplyr::mutate(across(everything(), as.character))
  
  # Required: lake_schema must exist
  if (!"lake_schema" %in% names(meta)) {
    stop("CORE metadata file is missing lake_schema column.")
  }
  
  # Upsert each row
  for (i in seq_len(nrow(meta))) {
    row <- meta[i,]
    
    dbExecute(con, glue::glue("
      INSERT INTO metadata (
        table_name, variable_name, variable_label, variable_definition,
        variable_unit, data_type, allowed_values, missing_value_code,
        status, notes, lake_schema
      )
      VALUES (
        '{row$table_name}', '{row$variable_name}', '{row$variable_label}',
        '{row$variable_definition}', '{row$variable_unit}', '{row$data_type}',
        '{row$allowed_values}', '{row$missing_value_code}', '{row$status}',
        '{row$notes}', '{row$lake_schema}'
      )
      ON CONFLICT (table_name, variable_name)
      DO UPDATE SET
        variable_label = EXCLUDED.variable_label,
        variable_definition = EXCLUDED.variable_definition,
        variable_unit = EXCLUDED.variable_unit,
        data_type = EXCLUDED.data_type,
        allowed_values = EXCLUDED.allowed_values,
        missing_value_code = EXCLUDED.missing_value_code,
        status = EXCLUDED.status,
        notes = EXCLUDED.notes,
        lake_schema = EXCLUDED.lake_schema,
        last_modified_utc = CURRENT_TIMESTAMP;
    "))
  }
  
  message("Metadata sync complete.")
}
