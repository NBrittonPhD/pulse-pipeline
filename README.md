# PULSE Pipeline

A metadata-driven, automated data lake pipeline for PRIME-AI’s PULSE governance framework.

This repository contains:

- SQL DDL for governance control tables  
- R-based pipeline runner driven by metadata  
- R functions implementing each pipeline stage  
- R Markdown templates for automated SOP and QC packet generation  
- YAML configurations for rules, settings, dashboards, and source metadata  

---

# Architecture

The pipeline is fully metadata-driven and controlled by schemas in Postgres:

- PIPELINE_STEP — ordered execution steps  
- SOURCE_REGISTRY — registered data sources  
- AUDIT_LOG — governed change log  
- INGEST_DICTIONARY — harmonization rules  
- BATCH_LOG + INGEST_FILE_LOG — ingest lineage  
- METADATA + METADATA_HISTORY — global data dictionary  
- RULE_LIBRARY, RULE_EXECUTION_LOG — QC governance  

## Flow Chart

```mermaid
flowchart TD

    %% ============================================================
    %% STEP 1 × CLUSTER 1 — SOURCE REGISTRATION
    %% ============================================================

    subgraph STEP1[Step 1 × Cluster 1 — Source Registration]
        direction TB

        S1A[User runs\nr/scripts/1_onboard_new_source.R]

        %% Initialization
        S1A --> S1B[pulse-init-all.R\n(Bootstrap env, schemas, tables)]
        S1A --> S1C[source(\"pulse-launch.R\")]

        %% High-level launcher
        S1C --> S1D[pulse_launch(ingest_id,\nsource_params,\nauto_write_params = TRUE)]
        S1D --> S1E[Write config/source_params.yml]
        S1D --> S1F[run_pipeline(ingest_id)]

        %% Runner
        S1F --> S1G[connect_to_pulse()]
        S1F --> S1H[load_pipeline_settings()]
        S1F --> S1I[get_pipeline_steps()\n(governance.pipeline_step)]
        S1F --> S1J[execute_step(STEP_001)]

        %% Step 1 dispatch
        S1J --> S1K[load_source_params()\n(from config/source_params.yml)]
        S1K --> S1L[run_step1_register_source(con,\nsource_params, settings)]

        %% Core Step 1 logic
        S1L --> S1M[validate_source_entry()\n(vocab + required fields)]
        S1L --> S1N[register_source()]

        %% register_source() side effects
        S1N --> S1O[INSERT/UPDATE\ngovernance.source_registry]
        S1N --> S1P[create_source_folders()\n(raw/, staging/, validated/, governance/)]
        S1N --> S1Q[write_audit_event()\n(event_type = \"source_registration\")]

        %% Step definition / metadata
        S1L --> S1R[write_pipeline_step()\n(record STEP_001 metadata)]
    end


    %% ============================================================
    %% STEP 2 × CLUSTER 2 — BATCH LOGGING & FILE LINEAGE
    %% ============================================================

    subgraph STEP2[Step 2 × Cluster 2 — Batch Logging & File Lineage]
        direction TB

        T1[User runs\nr/scripts/2_ingest_and_log_files.R]

        %% Initialization + inputs
        T1 --> T2[pulse-init-all.R]
        T1 --> T3[Set USER INPUTS:\nsource_id,\nsource_type,\nraw_path,\ningest_id]
        T1 --> T4[source(\"r/steps/log_batch_ingest.R\")\nsource(\"r/action/ingest.R\")]

        %% DB connection + file discovery
        T2 --> T5[connect_to_pulse()]
        T3 --> T6[fs::dir_ls(raw/{source_id}/incoming,\n\"*.csv\")\n→ files[]]

        %% Step 2A: batch + file logging
        T5 --> T7[log_batch_ingest(\ncon, ingest_id,\nsource_id, source_type,\nfile_paths = files)]
        T7 --> BATCH[governance.batch_log\n• one row per ingest_id]
        T7 --> FILELOG[governance.ingest_file_log\n• one row per file\n(load_status = 'pending')]

        %% Step 2B: ingest + lineage updates
        T5 --> T8[ingest_batch(\ncon, ingest_id,\nraw_path,\nsource_id, source_type)]
        T8 --> T9[Loop over\ningest_file_log rows\nfor this ingest_id]

        %% Per-file ingestion
        T9 --> T10[ingest_one_file(\ncon, file_path,\nsource_type)]
        T10 --> T10A[Load reference.ingest_dictionary\n(filter by source_type)]
        T10A --> T10B[Infer lake_table_name\n(from source_table_name,\noptionally lab_year)]
        T10B --> RAW[Append harmonized data\ninto raw.<lake_table>]
        T10B --> T10C[Return result list:\nstatus, lake_table,\nrow_count, file_size_bytes,\nchecksum]

        %% Update file-level lineage
        T10C --> T11[UPDATE governance.ingest_file_log\nSET lake_table_name,\nfile_size_bytes,\nrow_count,\nchecksum,\nload_status\n('success' or 'error'),\ncompleted_at_utc]

        %% Finalize batch
        T11 --> T12[Summarize\nfiles_success / files_error]
        T12 --> T13[UPDATE governance.batch_log\nSET status\n('success'/'partial'/'error'),\nfiles_success,\nfiles_error,\nbatch_completed_at_utc]
    end
```

---

# PULSE Pipeline — Step 1 × Cluster 1: Source Registration

## Purpose

Step 1 establishes a new data source in the governed metadata ecosystem.  
It ensures:

1. **Validation** of all source metadata against vocabularies in `pipeline_settings.yml`  
2. **Registration** of the source in `governance.source_registry`  
3. **Creation of standardized folder structures** (raw / staging / validated / governance)  
4. **Audit logging** of source creation or updates  
5. **Metadata-driven documentation** through `pipeline_step` records  

---

## Key Database Objects

### `governance.source_registry`
Stores all metadata defining a source.

### `governance.audit_log`
Captures governed events.

### `directory_structure.yml`
Defines canonical folder templates.

---

## Key R Components (Step 1)

- `validate_source_entry()`  
- `create_source_folders()`  
- `register_source()`  
- `run_step1_register_source()`  
- `1_onboard_new_source.R`  

---

## How to Run Step 1

```r
source("r/scripts/1_onboard_new_source.R")
```

# Step 2 × Cluster 2: Batch Logging & File Lineage

## Purpose

Step 2 ingests raw files for a given source **with full lineage and strict type safety**.

It does three main things:

1. Registers the ingest event  
2. Tracks each file  
3. Delegates ingestion to `ingest_one_file()`  

---

## Key Database Objects

- `governance.batch_log`  
- `governance.ingest_file_log`  
- `reference.ingest_dictionary`  
- `raw.<lake_table>`  

---

## Key R Components (Step 2)

- `ingest_one_file()`  
- `log_batch_ingest()`  
- `ingest_batch()`  
- `2_ingest_and_log_files.R`  

---

## How to Run Step 2

```r
source("r/scripts/2_ingest_and_log_files.R")
```

---
