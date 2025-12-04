
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

## Flow Chart
    A1[User Runs<br>1_onboard_new_source.R] --> A2[pulse-init-all.R<br>(Bootstrap Environment)]
    A2 --> A3[pulse_launch()]

    %% Pipeline Launch
    A3 --> B1[run_pipeline()]

    %% Runner Initialization
    B1 --> B2[connect_to_pulse()]
    B1 --> B3[load_pipeline_settings()]
    B1 --> B4[get_pipeline_steps()]
    B1 --> B5[execute_step()]

    %% Step Recognition
    B5 --> C1{Step ID == STEP_001?}
    C1 -- Yes --> C2[run_step1_register_source()]
    C1 -- No --> C9[Other Step Handler<br>(SQL / R / RMD)]

    %% Step 1 Wrapper
    C2 --> D1[register_source()]

    %% Core Step Logic
    D1 --> D2[validate_source_entry()]
    D1 --> D3[create_source_folders()]
    D1 --> D4[write_audit_event()]
    D1 --> D5[Update governance.source_registry]

    %% Step Completion Metadata
    C2 --> D6[write_pipeline_step()]

    %% Completion Return
    D6 --> B1
    B1 --> Z[Pipeline Continues / Completes]


