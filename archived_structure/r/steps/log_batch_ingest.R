# =============================================================================
# Step 2: Batch Logging & Ingestion Recording
# Option A: ingest_id = "<source_id>_<YYMMDD>"
# =============================================================================

suppressPackageStartupMessages({
  library(DBI)
  library(glue)
  library(digest)
  library(readr)
})

# -----------------------------------------------------------------------------
# Helper: next_ingest_sequence()
# -----------------------------------------------------------------------------
next_ingest_sequence <- function(con, source_id) {
  res <- DBI::dbGetQuery(
    con,
    "
      SELECT ingest_id
      FROM governance.batch_log
      WHERE source_id = ?
      ORDER BY ingested_at_utc DESC
      LIMIT 1;
    ",
    params = list(source_id)
  )
  
  if (nrow(res) == 0) return(1L)
  
  last_ingest_id <- res$ingest_id[[1]]
  seq_str <- sub(".*_", "", last_ingest_id)
  seq_num <- suppressWarnings(as.integer(seq_str))
  
  if (is.na(seq_num)) {
    stop(glue("Could not parse sequence number from ingest_id: {last_ingest_id}"))
  }
  
  seq_num + 1L
}

# -----------------------------------------------------------------------------
# Helper: insert_batch_log_record()
# -----------------------------------------------------------------------------
insert_batch_log_record <- function(
    con,
    ingest_id,
    source_id,
    file_name,
    file_size_bytes,
    sha256_checksum,
    row_count_raw,
    ingested_by,
    ingest_method   = "manual_upload",
    load_status     = "success",
    error_message   = NA_character_,
    validated_flag  = FALSE,
    archived_flag   = FALSE
) {
  
  sql <- "
    INSERT INTO governance.batch_log (
  ingest_id,
  source_id,
  file_name,
  file_size_bytes,
  sha256_checksum,
  row_count_raw,
  ingested_at_utc,
  ingested_by,
  ingest_method,
  load_status,
  error_message,
  validated_flag,
  archived_flag
)
VALUES (
  ?, ?, ?, ?, ?, ?,
  CURRENT_TIMESTAMP,
  ?, ?, ?, ?, ?, ?
);

  "
  
  DBI::dbExecute(
    con,
    sql,
    params = list(
      ingest_id,
      source_id,
      file_name,
      file_size_bytes,
      sha256_checksum,
      row_count_raw,
      ingested_by,
      ingest_method,
      load_status,
      error_message,
      validated_flag,
      archived_flag
    )
  )
}

# -----------------------------------------------------------------------------
# Step 2 Orchestrator: log_batch_ingest()
# -----------------------------------------------------------------------------
log_batch_ingest <- function(
    ingest_id,     # "<source_id>_<YYMMDD>"
    con,
    settings,
    source_id    = NULL,
    incoming_dir = NULL,
    archive_dir  = NULL
) {
  
  # ---------------------------------------------------------------------------
  # CORRECT SOURCE_ID PARSER
  # ---------------------------------------------------------------------------
  if (is.null(source_id)) {
    
    clean_id <- tolower(ingest_id)
    
    # Validate the pattern
    if (!grepl("_[0-9]{6}$", clean_id)) {
      stop(glue(
        "ingest_id must be in format '<source_id>_<YYMMDD>'. Received: {ingest_id}"
      ))
    }
    
    # KEEP EVERYTHING BEFORE "_YYMMDD"
    source_id <- sub("_[0-9]{6}$", "", clean_id)
  }
  
  # Build dirs
  if (is.null(incoming_dir)) incoming_dir <- file.path("raw", source_id, "incoming")
  if (is.null(archive_dir))  archive_dir  <- file.path("raw", source_id, "archive")
  
  message("========================================================")
  message("Step 2: Batch Logging & Ingestion Recording")
  message("Source: ", source_id)
  message("Incoming dir: ", incoming_dir)
  message("Archive dir:  ", archive_dir)
  message("--------------------------------------------------------")
  
  # ---------------------------------------------------------------------------
  # Validate source registration
  # ---------------------------------------------------------------------------
  src <- DBI::dbGetQuery(
    con,
    "
      SELECT source_id, active
      FROM source_registry
      WHERE source_id = ?;
    ",
    params = list(source_id)
  )
  
  if (nrow(src) == 0) stop(glue("Source '{source_id}' not found in SOURCE_REGISTRY."))
  if (!isTRUE(src$active[[1]])) stop(glue("Source '{source_id}' is not active."))
  
  # ---------------------------------------------------------------------------
  # Find incoming raw files
  # ---------------------------------------------------------------------------
  incoming_files <- list.files(incoming_dir, full.names = TRUE)
  message("Found ", length(incoming_files), " incoming file(s).")
  
  if (length(incoming_files) == 0) {
    stop(glue("No incoming files found for source '{source_id}'. Pipeline halted at Step 2."))
  }
  
  # ---------------------------------------------------------------------------
  # Internal governed timestamp
  # ---------------------------------------------------------------------------
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  
  seq_start <- next_ingest_sequence(con, source_id)
  message("Starting sequence at: ", seq_start)
  
  ingested_by <- Sys.info()[["user"]] %||% Sys.getenv("USER", unset = "unknown_user")
  
  results <- list()
  
  # ---------------------------------------------------------------------------
  # PHASE 2.5 — Batch Log Inserts
  # ---------------------------------------------------------------------------
  for (i in seq_along(incoming_files)) {
    
    file_path <- incoming_files[i]
    file_name <- basename(file_path)
    
    seq_num <- seq_start + (i - 1)
    seq_str <- sprintf("%06d", seq_num)
    
    governed_ingest_id <- paste0(
      "BATCH_", toupper(source_id), "_",
      timestamp, "_", seq_str
    )
    
    message("\n--- Processing ", file_name, " ---")
    message("Governed ingest_id: ", governed_ingest_id)
    
    file_size <- file.info(file_path)$size
    checksum  <- digest::digest(file = file_path, algo = "sha256")
    
    row_vec   <- readr::count_fields(file_path, tokenizer = readr::tokenizer_csv())
    row_count <- max(0L, length(row_vec) - 1L)
    
    insert_batch_log_record(
      con             = con,
      ingest_id       = governed_ingest_id,
      source_id       = source_id,
      file_name       = file_name,
      file_size_bytes = file_size,
      sha256_checksum = checksum,
      row_count_raw   = row_count,
      ingested_by     = ingested_by,
      ingest_method   = "manual_upload",
      load_status     = "success",
      error_message   = NA_character_,
      validated_flag  = FALSE,
      archived_flag   = FALSE
    )
    
    results[[i]] <- list(
      ingest_id       = governed_ingest_id,
      file_name       = file_name,
      file_path       = file_path,
      file_size_bytes = file_size,
      sha256_checksum = checksum,
      row_count_raw   = row_count
    )
    
    message("  → BATCH_LOG record inserted")
  }
  
  # ---------------------------------------------------------------------------
  # PHASE 3.2 — File Archival
  # ---------------------------------------------------------------------------
  for (i in seq_along(results)) {
    
    file_name  <- results[[i]]$file_name
    old_path   <- results[[i]]$file_path
    ingest_id2 <- results[[i]]$ingest_id
    
    new_dir <- file.path(archive_dir, ingest_id2)
    if (!dir.exists(new_dir)) dir.create(new_dir, recursive = TRUE)
    
    new_path <- file.path(new_dir, file_name)
    file.rename(old_path, new_path)
    
    post_checksum <- digest::digest(file = new_path, algo = "sha256")
    if (!identical(post_checksum, results[[i]]$sha256_checksum)) {
      stop(glue("Checksum mismatch after archiving file: {file_name}"))
    }
    
    DBI::dbExecute(
      con,
      "
        UPDATE governance.batch_log
        SET archived_flag = TRUE
        WHERE ingest_id = ?;
      ",
      params = list(ingest_id2)
    )
  }
  
  message("--------------------------------------------------------")
  message("Step 2 complete: metadata written + files archived.")
  message("========================================================")
  
  return(results)
}
