# =============================================================================
# 1_onboard_new_source.R
# Onboard a New Source into the PULSE Pipeline
# =============================================================================
# This script provides a simple, human-friendly workflow for onboarding a new 
# source into the PULSE pipeline.
#
# HOW TO USE:
#   1. Open this file: r/scripts/1_onboard_new_source.R
#   2. Edit ONLY the fields in the "USER INPUT SECTION" below.
#   3. Save the file.
#   4. From the project root, run:
#        source("r/scripts/1_onboard_new_source.R")
#
# This will:
#   - Initialize the PULSE database + schemas (via pulse-init-all.R)
#   - Write config/source_params.yml based on your inputs
#   - Run Step 1 of the pipeline (register_source)
#   - Validate vocab, create folders, write audit logs
#   - Confirm that STEP_001 is active in governance.pipeline_step
#
# =============================================================================

# =============================================================================
# USER INPUT SECTION — EDIT THESE FIELDS
# =============================================================================

# A unique identifier for this onboarding run.
# Convention: <source_id>_<time or test string>, e.g. "tr2026_test_01"
ingest_id <- "trauma_registry2026_test_002"

# Core metadata for the source you are onboarding.
# Only edit the values on the right-hand side.
source_params <- list(
  # Unique identifier for the source (stable, machine-readable). 
  # Should be the file name where the raw data resides.
  # Keep short; lowercase; use letters, numbers, and underscores.
  source_id = "trauma_registry2026_test",
  
  # Human-readable name for the source.
  source_name = "Example Source Attempt",
  
  # System type of the upstream data source.
  # Must match allowed values in config/pipeline_settings.yml:
  #   CSV, XLSX, SQL, API, FHIR, Other
  system_type = "CSV",
  
  # Expected update cadence for this source.
  # Must match allowed values in pipeline_settings.yml, e.g.:
  #   daily, weekly, biweekly, monthly, quarterly, annually, ad_hoc
  update_frequency = "monthly",
  
  # Who owns or manages the upstream dataset (person or group).
  data_owner = "Data Owner Name",
  
  # Ingestion pathway for this source.
  # Must match allowed values in pipeline_settings.yml:
  #   push, pull, api, sftp, manual
  ingest_method = "pull",
  
  # Schema version you expect this source to conform to.
  # Example: "1.0.0"
  expected_schema_version = "1.0.0",
  
  # Retention policy for raw + staging data.
  # NULL is allowed. Otherwise, a natural-language description is fine.
  retention_policy = "Raw indefinite; staging 30 days",
  
  # PHI/PII classification level.
  # Must match allowed values in pipeline_settings.yml:
  #   PHI, Limited, NonPHI
  pii_classification = "PHI",
  
  # Should this source be treated as active for ingestion?
  active = TRUE
)

# =============================================================================
# END USER INPUT SECTION
# =============================================================================

# ------------------------------
# Initialize PULSE system
# ------------------------------
# This sets up:
#   - DB connection infrastructure
#   - Core schemas (raw, staging, validated, governance, reference)
#   - Core governance tables (SOURCE_REGISTRY, AUDIT_LOG, PIPELINE_STEP, etc.)
#   - Any required reference structures
source("pulse-init-all.R")

# ------------------------------
# Load pulse_launch()
# ------------------------------
# pulse_launch() is a helper that:
#   - Optionally writes config/source_params.yml
#   - Calls run_pipeline(ingest_id)
#   - Ensures STEP_001 (register_source) is run with your source_params
source("pulse-launch.R")


# ------------------------------
# Execute onboarding
# ------------------------------
message(">> Beginning onboarding process for source_id = ", source_params$source_id)

pulse_launch(
  ingest_id          = ingest_id,
  source_params      = source_params,
  auto_write_params  = TRUE   # writes config/source_params.yml for you
)

message(">> Onboarding completed successfully!")
message("   - Source registered in governance.source_registry")
message("   - Folders created under raw/, staging/, validated/")
message("   - Audit log entry written to governance.audit_log")
message("   - STEP_001 present/enabled in governance.pipeline_step")
message("   - You may now proceed to ingestion (Step 2: batch logging).")
