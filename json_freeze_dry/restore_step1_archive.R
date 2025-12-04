# =============================================================================
# restore_step1_archive.R
# -----------------------------------------------------------------------------
# Reconstructs the full Step 1 PULSE code archive from a JSON snapshot.
#
# Usage:
#   1. Place step1_code_archive_v1.json at your project root.
#   2. Run:
#         source("restore_step1_archive.R")
#
# This will recreate the entire folder structure and all files exactly
# as they were stored (base64-decoded) in the archive.
# =============================================================================

library(jsonlite)
library(base64enc)
library(fs)

# ---------------------------------------------------------------------------
# 1. Load archive JSON
# ---------------------------------------------------------------------------
archive_path <- "step1_code_archive_v1.json"

if (!file.exists(archive_path)) {
  stop("Archive file not found: ", archive_path)
}

message(">> Loading archive: ", archive_path)
archive <- jsonlite::fromJSON(archive_path)

# ---------------------------------------------------------------------------
# 2. Recreate each file
# ---------------------------------------------------------------------------
message(">> Restoring ", length(archive$files), " files...")

for (i in seq_len(nrow(archive$files))) {
  
  file_info <- archive$files[i, ]
  out_path  <- file_info$path
  
  # Ensure directory exists
  dir_create(dirname(out_path), recurse = TRUE)
  
  # Decode Base64
  contents <- base64enc::base64decode(file_info$contents_base64)
  contents_char <- rawToChar(contents)
  
  # Write file
  writeLines(contents_char, con = out_path)
  
  message("   ✓ Restored: ", out_path)
}

# ---------------------------------------------------------------------------
# 3. Final message
# ---------------------------------------------------------------------------
message("\n=== Restoration Complete ===")
message("All files from Step 1 archive have been reconstructed.")
