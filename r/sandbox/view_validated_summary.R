# =============================================================================
# view_validated_summary.R
# =============================================================================
# Open this file in RStudio and run it to see validated table summaries
# in the RStudio Viewer pane.
# =============================================================================

source("pulse-init-all.R")
con <- connect_to_pulse()

# ---------------------------------------------------------------------------
# 1. Row counts for all validated tables
# ---------------------------------------------------------------------------
table_counts <- DBI::dbGetQuery(con, "
  SELECT
    t.table_name,
    (SELECT COUNT(*) FROM validated.admission) AS rows
  FROM information_schema.tables t
  WHERE t.table_schema = 'validated' AND t.table_name = 'admission'
  UNION ALL SELECT 'admission_vitals', COUNT(*) FROM validated.admission_vitals
  UNION ALL SELECT 'blood_products', COUNT(*) FROM validated.blood_products
  UNION ALL SELECT 'complications', COUNT(*) FROM validated.complications
  UNION ALL SELECT 'demographics', COUNT(*) FROM validated.demographics
  UNION ALL SELECT 'diagnoses', COUNT(*) FROM validated.diagnoses
  UNION ALL SELECT 'discharge', COUNT(*) FROM validated.discharge
  UNION ALL SELECT 'injuries', COUNT(*) FROM validated.injuries
  UNION ALL SELECT 'injury_event', COUNT(*) FROM validated.injury_event
  UNION ALL SELECT 'insurance', COUNT(*) FROM validated.insurance
  UNION ALL SELECT 'labs', COUNT(*) FROM validated.labs
  UNION ALL SELECT 'medications', COUNT(*) FROM validated.medications
  UNION ALL SELECT 'micro_cultures', COUNT(*) FROM validated.micro_cultures
  UNION ALL SELECT 'micro_sensitivities', COUNT(*) FROM validated.micro_sensitivities
  UNION ALL SELECT 'patient_tracking', COUNT(*) FROM validated.patient_tracking
  UNION ALL SELECT 'pmh', COUNT(*) FROM validated.pmh
  UNION ALL SELECT 'prehospital_procedures', COUNT(*) FROM validated.prehospital_procedures
  UNION ALL SELECT 'prehospital_transport', COUNT(*) FROM validated.prehospital_transport
  UNION ALL SELECT 'prehospital_vitals', COUNT(*) FROM validated.prehospital_vitals
  UNION ALL SELECT 'procedures', COUNT(*) FROM validated.procedures
  UNION ALL SELECT 'toxicology', COUNT(*) FROM validated.toxicology
  UNION ALL SELECT 'trauma_scores', COUNT(*) FROM validated.trauma_scores
  UNION ALL SELECT 'vitals', COUNT(*) FROM validated.vitals
  ORDER BY rows DESC
")

# ---------------------------------------------------------------------------
# 2. Breakdown by source type (from demographics as example)
# ---------------------------------------------------------------------------
source_breakdown <- DBI::dbGetQuery(con, "
  SELECT
    source_type,
    COUNT(DISTINCT ingest_id) AS ingests,
    COUNT(*) AS total_rows
  FROM validated.demographics
  GROUP BY source_type
  ORDER BY source_type
")

# ---------------------------------------------------------------------------
# 3. Sample data from key tables
# ---------------------------------------------------------------------------
sample_demographics <- DBI::dbGetQuery(con, "
  SELECT source_type, account_number, mrn, sex, race, date_of_birth
  FROM validated.demographics
  LIMIT 20
")

sample_labs <- DBI::dbGetQuery(con, "
  SELECT source_type, account_number, lab_test, result_value, result_units
  FROM validated.labs
  WHERE result_value IS NOT NULL
  LIMIT 20
")

DBI::dbDisconnect(con)

# ---------------------------------------------------------------------------
# View in RStudio
# ---------------------------------------------------------------------------
View(table_counts, "Validated Tables - Row Counts")
View(source_breakdown, "Data by Source Type")
View(sample_demographics, "Sample Demographics")
View(sample_labs, "Sample Labs")

cat("
=============================================
  STEP 6 HARMONIZATION SUMMARY
=============================================
  Total validated tables: 23
  Total rows:            ", sum(table_counts$rows), "

  Run View() calls above to see data in RStudio.
=============================================
")
