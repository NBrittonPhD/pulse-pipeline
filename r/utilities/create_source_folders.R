# =============================================================================
# create_source_folders.R
# -----------------------------------------------------------------------------
# Utility to create the folder tree for a new source_id based on the
# directory_structure.yml template.
#
# Used by:
#   - register_source()  (after inserting a NEW source into governance.source_registry)
#
# INPUTS
#   source_id : character
#       Stable ID for the source (e.g., "tr2026", "ictR_clarity").
#
#   base_path : character
#       Root of the PULSE repo on disk. Defaults to "." (current working dir),
#       so that paths like "./raw/<source_id>/incoming" are created.
#
# CONFIG
#   Expects a YAML file at:
#       <base_path>/directory_structure.yml
#
#   Example structure:
#       raw:
#         - "{source_id}/incoming/"
#         - "{source_id}/archive/"
#       staging:
#         - "{source_id}/incoming/"
#         - "{source_id}/archive/"
#       validated:
#         - "{source_id}/"
#       governance:
#         - "logs/"
#         - "qc/"
#         - "reports/"
#
# RETURNS
#   A character vector of full paths that were created.
#   Throws an error if any directory cannot be created.
# =============================================================================

create_source_folders <- function(source_id, base_path = ".") {
  
  # 1. Load the directory template
  template_path <- file.path(base_path, "directory_structure.yml")
  
  if (!file.exists(template_path)) {
    stop(
      paste0(
        "directory_structure.yml not found at: ", template_path,
        ". Cannot create folders for source_id = '", source_id, "'."
      ),
      call. = FALSE
    )
  }
  
  template <- yaml::read_yaml(template_path)
  
  created <- character(0)   # track actual paths created
  
  # 2. Helper to expand "{source_id}" placeholders
  expand_path <- function(p) {
    gsub("\\{source_id\\}", source_id, p)
  }
  
  # 3. Iterate through each top-level zone (raw, staging, validated, governance)
  for (zone in names(template)) {
    
    # each zone contains a list of subpaths
    for (sub in template[[zone]]) {
      
      rel_path <- file.path(zone, expand_path(sub))
      full_path <- file.path(base_path, rel_path)
      
      # normalize path (in case of trailing slashes, etc.)
      full_path <- normalizePath(full_path, mustWork = FALSE)
      
      # 4. Create directory (fs::dir_create is robust & cross-platform)
      tryCatch(
        {
          fs::dir_create(full_path, recurse = TRUE)
          created <- c(created, full_path)
        },
        error = function(e) {
          stop(
            glue::glue(
              "Failed to create folder '{full_path}'. Error: {e$message}"
            ),
            call. = FALSE
          )
        }
      )
    }
  }
  
  return(created)
}