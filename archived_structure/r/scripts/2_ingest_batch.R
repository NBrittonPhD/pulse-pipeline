# =============================================================================
# 2_ingest_batch.R
# Run Step 2: Batch Logging & Ingestion Recording for a single source
#
# HOW TO USE:
#   1. Edit the USER INPUT SECTION below.
#   2. Save this file.
#   3. Run the entire script.
#
# This will:
#   - Connect to the PULSE database
#   - Run Step 2 for the specified source_id
#   - Log BATCH_LOG entries for each incoming file
#   - Move files into archive/<ingest_id>/ subfolders
#   - Flip archived_flag in BATCH_LOG
#   - Emit best-effort audit_log entries
#
# IMPORTANT:
#   - This script will FAIL if there are no incoming files
#     (Option C: no files = error).
# =============================================================================

suppressPackageStartupMessages({
  library(DBI)
})

# -----------------------------
# USER INPUT SECTION
# -----------------------------

source_id <- "tr2026_test"
# Optionally override directories (usually you leave these as NULL):
incoming_dir_override <- NULL
archive_dir_override  <- NULL

# -----------------------------
# Setup and run
# -----------------------------

# Load connection + step code
source("r/connect_to_pulse.R")
source("r/steps/log_batch_ingest.R")

con <- connect_to_pulse()

incoming_dir <- if (is.null(incoming_dir_override)) {
  file.path("raw", source_id, "incoming")
} else {
  incoming_dir_override
}

archive_dir <- if (is.null(archive_dir_override)) {
  file.path("raw", source_id, "archive")
} else {
  archive_dir_override
}

cat("Running Step 2 for source_id:", source_id, "\n")
cat("Incoming dir:", incoming_dir, "\n")
cat("Archive dir: ", archive_dir, "\n\n")

res <- try(
  log_batch_ingest(
    source_id    = source_id,
    con          = con,
    incoming_dir = incoming_dir,
    archive_dir  = archive_dir
  ),
  silent = FALSE
)

DBI::dbDisconnect(con)

cat("\nStep 2 completed.\n")
