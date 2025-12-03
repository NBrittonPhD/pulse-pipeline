# =============================================================================
# next_ingest_sequence(): Get next continuous sequence number for a source
# =============================================================================

next_ingest_sequence <- function(con, source_id) {
  
  # Pull the highest sequence currently used for this source
  res <- DBI::dbGetQuery(
    con,
    glue::glue("
      SELECT ingest_id
      FROM batch_log
      WHERE source_id = '{source_id}'
      ORDER BY ingested_at_utc DESC
      LIMIT 1;
    ")
  )
  
  # If no records exist for this source yet, start at 1
  if (nrow(res) == 0) {
    return(1L)
  }
  
  # Extract the sequence component from ingest_id
  last_ingest_id <- res$ingest_id[[1]]
  seq_str <- sub(".*_", "", last_ingest_id)
  
  suppressWarnings({
    seq_num <- as.integer(seq_str)
  })
  
  if (is.na(seq_num)) {
    stop(glue::glue("Could not parse sequence number from {last_ingest_id}."))
  }
  
  return(seq_num + 1L)
}
