# =============================================================================
# Onboard a New Source into the PULSE Pipeline
# =============================================================================
# This script provides a simple, human-friendly workflow for onboarding a new 
# source into the PULSE pipeline. 
#
# HOW TO USE:
#   1. Fill in the editable section marked "USER INPUT SECTION".
#   2. Save the file.
#   3. Run the entire script.
#
# This will:
#   - Write config/source_params.yml
#   - Run step 1 of the pipeline (register_source)
#   - Validate vocab, create folders, write audit logs
#   - Record pipeline_step metadata for Step 1
#
# =============================================================================

# ------------------------------#
# Initialize PULSE system
# ------------------------------#
source("pulse-init-all.R")

# -----------------------------
# Load pulse_launch()
# -----------------------------
source("pulse-launch.R")

# =============================================================================
# USER INPUT SECTION — EDIT THESE FIELDS
# =============================================================================

ingest_id <- "tr2026_test_251202" # format for ingest_id = <source_id>_<YYMMDD>

source_params <- list(
  source_id = "tr2026_test",                            # Unique identifier for the source (stable, machine-readable). Keep short; lowercase; alphanumeric + underscores.
  source_name = "Example Source Attempt",               # Human-readable name for display.One of the allowed vocabulary terms: CSV, XLSX, SQL, API, FHIR, Other
  system_type = "CSV",                                  # One of the allowed vocabulary terms: CSV, XLSX, SQL, API, FHIR, Other
  update_frequency = "monthly",                         # Expected update cadence for this source. Allowed vocabulary (example; check pipeline_settings.yml for canonical list): daily, weekly, biweekly, monthly, quarterly, annually, ad_hoc
  data_owner = "Data Owner Name",                       # Who owns or manages the upstream dataset.
  ingest_method = "pull",                               # Ingestion pathway — matches allowed vocabulary: push, pull, api, sftp, manual
  expected_schema_version = "1.0.0",                    # What schema version you expect this source to conform to. Example: "1.0.0"
  retention_policy = "Raw indefinite. Staging 30 days", # Retention policy for raw + staging zones.NULL is allowed. Otherwise a natural language string is fine.
  pii_classification = "PHI",                           # PHI / PII classification level. Allowed vocabulary from pipeline_settings.yml: PHI, Limited, NonPHI
  active = TRUE                                         # Should this source be treated as active for ingestion?
  )

# =============================================================================
# END USER INPUT SECTION
# =============================================================================


# -----------------------------
# Execute onboarding
# -----------------------------
message(">> Beginning onboarding process ...")

pulse_launch(
  ingest_id = ingest_id,
  source_params = source_params,
  auto_write_params = TRUE
)

message(">> Onboarding completed successfully!")
message("   - Source registered")
message("   - Folders created")
message("   - Audit log entry written")
message("   - pipeline_step updated")
message("   - You may now proceed to ingestion or step 2.")
