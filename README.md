
# PULSE Pipeline

A metadata-driven, automated data lake pipeline for PRIME-AI's PULSE governance framework.

This repository contains:

- SQL DDL for governance control tables
- R-based pipeline runner driven by metadata
- R functions implementing each pipeline stage
- R Markdown templates for auto-generated SOP and QC packets
- YAML configurations for rules, settings, and dashboards

## Architecture

The pipeline is controlled by metadata stored in the database:

- PIPELINE_STEP — ordered execution steps
- RULE_LIBRARY — governed QC rules
- RULE_EXECUTION_MAP — rule-to-variable/table mapping
- METADATA + METADATA_HISTORY — core data dictionary
- TRANSFORM_LOG, AUDIT_LOG, DATA_PROFILE, etc.

## Getting started

1. Create governance tables from sql/ddl/.
2. Populate PIPELINE_STEP with your pipeline steps.
3. Configure config/pipeline_settings.yml.
4. Run pipeline:

source("r/runner.R")
run_pipeline(ingest_id = "BATCH_TEST")

