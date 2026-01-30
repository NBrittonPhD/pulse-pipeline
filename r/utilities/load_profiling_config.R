# =============================================================================
# load_profiling_config
# =============================================================================
# Purpose:      Load profiling configuration from YAML with built-in defaults.
#               If the YAML file is missing, defaults are used silently so that
#               profiling can run without a config file present.
#
# Inputs:
#   - config_path: character path to profiling_settings.yml (optional)
#                  If NULL, uses config/profiling_settings.yml relative to
#                  pulse.proj_root.
#
# Outputs:      Named list of configuration settings
#
# Side Effects: None (pure function)
#
# Dependencies: yaml
#
# Author:       Noel
# Last Updated: 2026-01-30
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(yaml)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
load_profiling_config <- function(config_path = NULL) {

    # =========================================================================
    # RESOLVE PATH
    # =========================================================================
    proj_root <- getOption("pulse.proj_root", default = getwd())

    if (is.null(config_path)) {
        config_path <- file.path(proj_root, "config", "profiling_settings.yml")
    } else if (!file.exists(config_path)) {
        # Try relative to project root
        candidate <- file.path(proj_root, config_path)
        if (file.exists(candidate)) {
            config_path <- candidate
        }
    }

    # =========================================================================
    # HARDCODED DEFAULTS
    # =========================================================================
    # These defaults are used if the YAML file is missing or if a key is absent
    # from the file. This ensures profiling always has sensible thresholds.
    # =========================================================================
    defaults <- list(
        quality_score_thresholds = list(
            excellent = list(max_missing_pct = 5, max_critical_issues = 0),
            good      = list(max_missing_pct = 10, max_critical_issues = 2),
            fair      = list(max_missing_pct = 20, max_critical_issues = 5)
        ),
        missingness_thresholds = list(
            critical = 0,
            high     = 20,
            moderate = 10
        ),
        sentinel_detection = list(
            numeric_sentinels       = c(999, 9999, -999, -9999, -1, 99, 88, 77),
            string_sentinels        = c("NA", "N/A", "NULL", "UNKNOWN", "UNK",
                                        "MISSING", "NOT RECORDED"),
            min_frequency_pct       = 1.0,
            max_unique_for_detection = 50
        ),
        identifier_columns = c("ACCOUNTNO", "MEDRECNO", "TRAUMANO",
                                "account_number", "mrn", "trauma_no", "cisir_id"),
        identifier_patterns = c("_id$", "_no$", "^id_",
                                 "^accountno", "^medrecno", "^traumano"),
        display = list(
            top_n_categories = 15,
            decimal_places   = 2
        )
    )

    # =========================================================================
    # LOAD FROM FILE (IF EXISTS)
    # =========================================================================
    if (file.exists(config_path)) {
        message(sprintf("[load_profiling_config] Loading config from: %s", config_path))
        file_config <- yaml::read_yaml(config_path)
        # Merge: file values override defaults
        config <- modifyList(defaults, file_config)
    } else {
        message("[load_profiling_config] Config file not found, using defaults.")
        config <- defaults
    }

    return(config)
}
