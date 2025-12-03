# =============================================================================
# load_source_params.R
# -----------------------------------------------------------------------------
# Loads the YAML file config/source_params.yml and returns it as a list.
#
# This is used by:
#   - execute_step() for STEP_001 (register_source)
#   - pulse_launch()
#   - onboarding scripts (1_onboard_new_source.R)
#
# Behavior:
#   - Reads config/source_params.yml
#   - Returns a named R list of fields required by register_source()
#
# Uses the pulse.proj_root option if set (e.g., by tests), otherwise getwd().
# =============================================================================

load_source_params <- function(path = NULL) {
  
  proj_root <- getOption("pulse.proj_root", default = getwd())
  
  # If no path supplied, default to config/source_params.yml under proj_root
  if (is.null(path)) {
    path <- file.path(proj_root, "config", "source_params.yml")
  } else {
    # If a relative path is supplied, anchor it at proj_root
    if (!fs::is_absolute_path(path)) {
      path <- file.path(proj_root, path)
    }
  }
  
  if (!file.exists(path)) {
    stop(
      sprintf(
        "source_params.yml not found at %s. \n          Please run write_source_params_yaml() or your onboarding script first.",
        path
      )
    )
  }
  
  yaml::read_yaml(path)
}