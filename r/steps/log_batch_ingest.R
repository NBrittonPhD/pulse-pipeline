# =============================================================================
# r/steps/log_batch_ingest.R
# Step 2 Logging + Delegating Type-Safe Ingestion
# =============================================================================

library(DBI)
library(glue)
library(fs)
library(digest)
library(readr)
library(dplyr)

# ----------------------------------------
# Create batch_log row + pending file rows
# ----------------------------------------
log_batch_ingest <- function(con, ingest_id, source_id, source_type, file_paths) {
  
  existing <- dbGetQuery(
    con,
    glue("SELECT 1 FROM governance.batch_log WHERE ingest_id = '{ingest_id}'")
  )
  
  if (nrow(existing) > 0) {
    stop(glue("log_batch_ingest(): ingest_id '{ingest_id}' already exists."))
  }
  
  # insert into batch_log
  dbExecute(
    con,
    glue("
      INSERT INTO governance.batch_log (
        ingest_id, status, file_count, source_id,
        ingest_timestamp, batch_started_at_utc
      )
      VALUES (
        '{ingest_id}', 'started', {length(file_paths)}, '{source_id}',
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
      )
    ")
  )
  
  # insert pending file log rows
  for (fp in file_paths) {
    fname   <- basename(fp)
    fp_norm <- normalizePath(fp)
    
    dbExecute(
      con,
      glue("
        INSERT INTO governance.ingest_file_log (
          ingest_id, file_name, file_path, load_status
        )
        VALUES (
          '{ingest_id}', '{fname}', '{fp_norm}', 'pending'
        )
      ")
    )
  }
  
  invisible(TRUE)
}

# =============================================================================
# ingest_batch() — SAFE, DEFENSIVE, STRICT
# =============================================================================

ingest_batch <- function(con, ingest_id, raw_path, source_id, source_type,
                         type_decisions = NULL) {
  
  files <- dbGetQuery(
    con,
    glue("
      SELECT ingest_file_id, file_name, file_path
        FROM governance.ingest_file_log
       WHERE ingest_id = '{ingest_id}'
       ORDER BY ingest_file_id
    ")
  )
  
  success_count <- 0L
  error_count   <- 0L
  
  for (i in seq_len(nrow(files))) {
    
    id    <- files$ingest_file_id[i]
    fname <- files$file_name[i]
    fpath <- files$file_path[i]
    
    message(">> [Step 2] Ingesting file ", fname, " with source_type = ", source_type)
    
    # ---- Does the file exist? ----
    if (!file.exists(fpath)) {
      message("   - ERROR: file does not exist on disk: ", fpath)
      dbExecute(
        con,
        glue("
          UPDATE governance.ingest_file_log
             SET load_status      = 'error',
                 completed_at_utc = CURRENT_TIMESTAMP
           WHERE ingest_file_id   = {id}
        ")
      )
      error_count <- error_count + 1L
      next
    }
    
    # ---- Ingest the file ----
    result <- try(
      ingest_one_file(con, fpath, source_type = source_type),
      silent = TRUE
    )
    
    # If try() caught an error, result is a "try-error" char vector
    if (inherits(result, "try-error")) {
      message("   - ERROR during ingest_one_file(): ", as.character(result))
      
      dbExecute(
        con,
        glue("
          UPDATE governance.ingest_file_log
             SET load_status      = 'error',
                 completed_at_utc = CURRENT_TIMESTAMP
           WHERE ingest_file_id   = {id}
        ")
      )
      
      error_count <- error_count + 1L
      next
    }
    
    # ---- Normalize to an error if not a valid success list ----
    bad <- (
      !is.list(result) ||
        is.null(result$status) ||
        result$status != "success"
    )
    
    if (bad) {
      
      lk <- if (is.list(result) && !is.null(result$lake_table)) result$lake_table else NA_character_
      
      message("   - Ingest returned non-success status. lake_table = ", lk)
      
      dbExecute(
        con,
        glue("
          UPDATE governance.ingest_file_log
             SET load_status      = 'error',
                 lake_table_name  = COALESCE('{lk}', lake_table_name),
                 completed_at_utc = CURRENT_TIMESTAMP
           WHERE ingest_file_id   = {id}
        ")
      )
      
      error_count <- error_count + 1L
      next
    }
    
    # ---- SUCCESS ----
    message("   - SUCCESS → lake_table = ", result$lake_table,
            ", rows = ", result$row_count)
    
    dbExecute(
      con,
      glue("
        UPDATE governance.ingest_file_log
           SET load_status      = 'success',
               lake_table_name  = '{result$lake_table}',
               file_size_bytes  = {result$file_size_bytes},
               row_count        = {result$row_count},
               checksum         = '{result$checksum}',
               completed_at_utc = CURRENT_TIMESTAMP
         WHERE ingest_file_id   = {id}
      ")
    )
    
    success_count <- success_count + 1L
  }

  # ---- Promote unique lake tables to staging (if type_decisions provided) ----
  if (!is.null(type_decisions) && success_count > 0L) {

    # Collect unique lake_table names from successfully ingested files
    successful_tables <- DBI::dbGetQuery(
      con,
      glue("
        SELECT DISTINCT lake_table_name
          FROM governance.ingest_file_log
         WHERE ingest_id  = '{ingest_id}'
           AND load_status = 'success'
           AND lake_table_name IS NOT NULL
      ")
    )$lake_table_name

    if (length(successful_tables) > 0) {
      message("\n>> [Step 2] Promoting ", length(successful_tables),
              " table(s) to staging...")

      for (stbl in successful_tables) {
        promo <- tryCatch(
          promote_to_staging(con, stbl, type_decisions),
          error = function(e) {
            list(status = "error", lake_table = stbl,
                 error_message = conditionMessage(e))
          }
        )

        if (identical(promo$status, "promoted")) {
          message("   - staging.", stbl, " OK (",
                  promo$n_rows, " rows, ",
                  promo$n_typed, "/", promo$n_columns, " typed)")
        } else {
          message("   - staging.", stbl, " WARN: ",
                  promo$error_message %||% "promotion failed")
        }
      }
    }
  }

  # ---- Final batch status ----
  final_status <- dplyr::case_when(
    success_count == 0L ~ "error",
    error_count == 0L   ~ "success",
    TRUE                ~ "partial"
  )
  
  dbExecute(
    con,
    glue("
      UPDATE governance.batch_log
         SET status                 = '{final_status}',
             files_success          = {success_count},
             files_error            = {error_count},
             batch_completed_at_utc = CURRENT_TIMESTAMP
       WHERE ingest_id = '{ingest_id}'
    ")
  )
  
  list(
    ingest_id = ingest_id,
    status    = final_status,
    n_files   = nrow(files),
    n_success = success_count,
    n_error   = error_count
  )
}
