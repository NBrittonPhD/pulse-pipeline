
---
  
  ## 3. JSON Snapshot Generator (B1-style: bytes + base64)
  
#Rather than hand-waving base64, the safe/accurate way is to let **your own repo contents** drive the snapshot.

# =============================================================================
# generate_step1_step2_snapshot.R
# -----------------------------------------------------------------------------
# Generate a JSON snapshot of all Step 1 + Step 2 core files.
#
# Includes:
#   - Relative path from project root
#   - File role / description
#   - Language / type
#   - Byte size
#   - MD5 hash
#   - Base64-encoded contents
#
# Output:
#   - Writes: snapshots/step1_step2_snapshot.json
# =============================================================================

library(jsonlite)
library(digest)
library(base64enc)
library(fs)
library(tools)

proj_root <- getOption("pulse.proj_root", default = getwd())

# Ensure output folder exists
dir_create(file.path(proj_root, "snapshots"))

# ---------------------------------------------------------------------------
# Helper: build file descriptor
# ---------------------------------------------------------------------------
describe_file <- function(path, role = NULL, language = NULL) {
  full <- file.path(proj_root, path)
  
  if (!file_exists(full)) {
    warning("File does not exist, skipping: ", full)
    return(NULL)
  }
  
  raw_bytes <- readBin(full, what = "raw", n = file_size(full))
  bytes_len <- length(raw_bytes)
  md5       <- digest(raw_bytes, algo = "md5")
  b64       <- base64encode(raw_bytes)
  
  list(
    path           = path,
    role           = role,
    language       = language,
    bytes          = bytes_len,
    hash_md5       = md5,
    base64_contents = b64
  )
}

# ---------------------------------------------------------------------------
# Inventory of Step 1 + Step 2 core files
# (extend this list as needed)
# ---------------------------------------------------------------------------

files_to_snapshot <- list(
  list(path = "r/connect_to_pulse.R",               role = "db_connection",          language = "R"),
  list(path = "r/runner.R",                         role = "pipeline_runner",        language = "R"),
  list(path = "r/utilities/load_source_params.R",   role = "step1_util",             language = "R"),
  list(path = "r/utilities/validate_source_entry.R",role = "step1_validation",       language = "R"),
  list(path = "r/utilities/create_source_folders.R",role = "step1_folders",          language = "R"),
  list(path = "r/utilities/write_pipeline_step.R",  role = "governance_pipeline_step", language = "R"),
  list(path = "r/steps/register_source.R",          role = "step1_core",             language = "R"),
  list(path = "r/steps/run_step1_register_source.R",role = "step1_wrapper",          language = "R"),
  list(path = "r/steps/write_audit_event.R",        role = "governance_audit",       language = "R"),
  list(path = "r/action/ingest.R",                  role = "step2_ingest_action",    language = "R"),
  list(path = "r/steps/log_batch_ingest.R",         role = "step2_batch_logging",    language = "R"),
  list(path = "r/scripts/1_onboard_new_source.R",   role = "step1_user_script",      language = "R"),
  list(path = "r/scripts/2_ingest_and_log_files.R", role = "step2_user_script",      language = "R"),
  list(path = "pulse-launch.R",                     role = "pipeline_entrypoint",    language = "R"),
  list(path = "pulse-init-all.R",                   role = "init_script",            language = "R"),
  list(path = "config/pipeline_settings.yml",       role = "global_config",          language = "YAML"),
  list(path = "config/source_params.yml",           role = "source_config",          language = "YAML"),
  list(path = "directory_structure.yml",            role = "folder_template",        language = "YAML"),
  list(path = "sql/ddl/create_SOURCE_REGISTRY.sql", role = "ddl_source_registry",    language = "SQL"),
  list(path = "sql/ddl/create_AUDIT_LOG.sql",       role = "ddl_audit_log",          language = "SQL"),
  list(path = "sql/ddl/create_PIPELINE_STEP.sql",   role = "ddl_pipeline_step",      language = "SQL"),
  list(path = "sql/ddl/create_BATCH_LOG.sql",       role = "ddl_batch_log",          language = "SQL"),
  list(path = "sql/ddl/create_INGEST_FILE_LOG.sql", role = "ddl_ingest_file_log",    language = "SQL"),
  list(path = "sql/inserts/STEP_001_register_source.sql", role = "seed_step1",     language = "SQL"),
  list(path = "sql/inserts/STEP_002_batch_logging_and_ingestion.sql", role = "seed_step2",     language = "SQL")
)

# Build descriptors
file_entries <- purrr::compact(
  purrr::map(files_to_snapshot, ~ describe_file(.x$path, .x$role, .x$language))
)

snapshot <- list(
  version     = "step1_step2_snapshot_v1",
  description = "Full Step 1 × Cluster 1 and Step 2 × Cluster 2 code/config snapshot for PULSE.",
  generated_at_utc = format(Sys.time(), tz = "UTC"),
  project_root = basename(normalizePath(proj_root)),
  files       = file_entries
)

out_path <- file.path(proj_root, "snapshots", "step1_step2_snapshot.json")
write_json(snapshot, out_path, pretty = TRUE, auto_unbox = TRUE)

message("Snapshot written to: ", out_path)
