# =============================================================================
# review_step2_ingestion.R — Review Batch Ingestion Results
# =============================================================================
# Purpose: Inspect batch logs and file-level ingestion details from Step 2.
#          Shows batch summary, per-file status, and row counts.
#
# HOW TO USE:
#   1. Optionally set source_filter below to narrow results
#   2. Run: source("r/review/review_step2_ingestion.R")
#
# Author: Noel
# =============================================================================


# =============================================================================
# USER INPUT SECTION — EDIT BELOW
# =============================================================================

# Set to a source_id to filter, or NULL to see all sources
source_filter <- NULL
# source_filter <- "trauma_registry2026_toy"

# How many recent batches to show (NULL = all)
max_batches <- 20

# =============================================================================
# END USER INPUT SECTION
# =============================================================================


# =============================================================================
# INITIALIZE
# =============================================================================
source("pulse-init-all.R")
con <- connect_to_pulse()


# =============================================================================
# QUERY 1: BATCH OVERVIEW
# =============================================================================

cat("\n")
cat("===================================================================\n")
cat("           STEP 2 REVIEW: BATCH INGESTION                         \n")
cat("===================================================================\n\n")

batch_where <- if (!is.null(source_filter)) {
    glue::glue("WHERE source_id = '{source_filter}'")
} else {
    ""
}
batch_limit <- if (!is.null(max_batches)) {
    glue::glue("LIMIT {max_batches}")
} else {
    ""
}

batches <- DBI::dbGetQuery(con, glue::glue("
    SELECT ingest_id,
           source_id,
           status,
           file_count,
           files_success,
           files_error,
           started_at_utc
    FROM governance.batch_log
    {batch_where}
    ORDER BY started_at_utc DESC
    {batch_limit}
"))

cat("--- Batch Summary ---\n\n")
if (nrow(batches) == 0) {
    cat("  No batches found.\n")
} else {
    cat(glue::glue("  Total batches: {nrow(batches)}"), "\n")
    cat(glue::glue("  Successful:    {sum(batches$status == 'success')}"), "\n")
    cat(glue::glue("  Failed:        {sum(batches$status != 'success')}"), "\n\n")
    print(batches, row.names = FALSE)
}


# =============================================================================
# QUERY 2: FILE-LEVEL DETAIL FOR MOST RECENT BATCH
# =============================================================================

if (nrow(batches) > 0) {
    latest <- batches$ingest_id[1]
    cat(glue::glue("\n\n--- File Detail: {latest} ---\n\n"))

    files <- DBI::dbGetQuery(con, glue::glue("
        SELECT original_filename,
               target_table,
               row_count,
               column_count,
               status,
               loaded_at_utc
        FROM governance.ingest_file_log
        WHERE ingest_id = '{latest}'
        ORDER BY original_filename
    "))

    if (nrow(files) == 0) {
        cat("  No file records found.\n")
    } else {
        cat(glue::glue("  Files: {nrow(files)}"), "\n")
        cat(glue::glue("  Total rows: {format(sum(files$row_count, na.rm = TRUE), big.mark = ',')}"), "\n\n")
        print(files, row.names = FALSE)
    }
}


# =============================================================================
# CLEANUP
# =============================================================================
cat("\n===================================================================\n")
if (DBI::dbIsValid(con)) DBI::dbDisconnect(con)
