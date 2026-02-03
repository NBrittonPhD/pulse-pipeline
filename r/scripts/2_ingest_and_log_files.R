# =============================================================================
# 2_ingest_and_log_files.R
# Step 2 — Batch Logging & File-Level Lineage (Strict Source Type Mode)
# =============================================================================
# This script provides a human-friendly interface for running Step 2 of the
# PULSE pipeline: batch logging + ingestion + lineage creation.
#
# HOW TO USE:
#   1. Open this file: r/scripts/2_ingest_and_log_files.R
#   2. Edit the fields in the USER INPUT SECTION.
#   3. Save the file.
#   4. Run:
#        source("r/scripts/2_ingest_and_log_files.R")
#
# This will:
#   - Connect to Postgres
#   - Detect incoming files for the source
#   - Create a batch_log entry
#   - Create pending ingest_file_log rows
#   - Ingest each file using strict source_type enforcement
#   - Update lineage per file
#   - Finalize batch_log summary
#
# =============================================================================


# ------------------------------
# USER INPUT SECTION — EDIT BELOW
# ------------------------------

# 1. Your source identifier (e.g., "cisir2026_test", "trauma_registry2026_test")
source_id <- "cisir2026_toy"   # EDIT ME

# 2. Source type (MUST match allowed_source_types in pipeline_settings.yml)
source_type <- "CISIR"          # EDIT ME 

# ------------------------------
# END USER INPUT SECTION
# ------------------------------

# 3. Path to incoming files for this source (usually do not edit)
raw_path <- glue::glue("raw/{source_id}/incoming")

# 4. Auto-generate ingest_id (recommended)
ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
ingest_id <- glue::glue("ING_{source_id}_{ts}")
message(">> Using ingest_id = ", ingest_id)


# ------------------------------
# Initialize PULSE system
# ------------------------------
source("pulse-init-all.R")

# Load Step 2 logic
source("r/steps/log_batch_ingest.R")   # contains log_batch_ingest(), ingest_batch()

# Load ingestion action logic
source("r/steps/ingest.R")             # contains ingest_one_file()

# Load staging promotion utility
source("r/build_tools/promote_to_staging.R")  # contains promote_to_staging()

# Check raw directory
if (!fs::dir_exists(raw_path)) {
  stop(glue::glue("Raw directory does not exist: {raw_path}"))
}

# Connect to DB
con <- connect_to_pulse()

# Load type_decisions for automatic staging promotion
type_decisions <- tryCatch({
  td <- readxl::read_excel("reference/type_decisions/type_decision_table.xlsx")
  names(td) <- tolower(names(td))
  # Handle both naming conventions for counting tables
  tbl_col <- if ("lake_table_name" %in% names(td)) "lake_table_name" else "table_name"
  message(">> Type decisions loaded: ", nrow(td), " rows (",
          length(unique(td[[tbl_col]])), " tables)")
  td
},
error = function(e) {
  message(">> WARNING: Could not load type_decision_table.xlsx — ",
          "staging promotion will be skipped.")
  message("   Reason: ", conditionMessage(e))
  NULL
})

# List incoming files
files <- fs::dir_ls(raw_path, regexp = "\\.csv$", recurse = FALSE)

if (length(files) == 0) {
  stop(glue::glue("No CSV files found in {raw_path}"))
}

message(">> Found ", length(files), " incoming file(s):")
print(basename(files))


# ------------------------------
# STEP 2A: Log the batch + file rows
# ------------------------------
message(">> Running log_batch_ingest()...")
log_batch_ingest(
  con         = con,
  ingest_id   = ingest_id,
  source_id   = source_id,
  source_type = source_type,    # <---- STRICT MODE ENABLED HERE
  file_paths  = files
)
message(">> Batch logging complete.")


# ------------------------------
# STEP 2B: Ingest + update lineage
# ------------------------------
message(">> Running ingest_batch()...")
result <- ingest_batch(
  con            = con,
  ingest_id      = ingest_id,
  raw_path       = raw_path,
  source_id      = source_id,
  source_type    = source_type,
  type_decisions = type_decisions
)



# ------------------------------
# Summary
# ------------------------------
message("\n==============================")
message("  STEP 2 SUMMARY")
message("==============================")
message("Ingest ID:       ", result$ingest_id)
message("Final Status:    ", result$status)
message("Files Processed: ", result$n_files)
message(" - Success: ", result$n_success)
message(" - Error:   ", result$n_error)
if (!is.null(type_decisions)) {
  message("Staging:         auto-promoted (see messages above)")
} else {
  message("Staging:         skipped (no type_decisions)")
}
message("==============================\n")

message(">> Step 2 complete.")
message(">> You may now proceed to schema validation (Step 3).")
