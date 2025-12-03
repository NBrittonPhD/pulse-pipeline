# ================================================================
#   PULSE PIPELINE LAUNCH SCRIPT
#   Creates directory structure + starter files with content
#   No emojis. Pure R. Safe for production.
# ================================================================

message("Starting PULSE pipeline scaffold creation...")

# --------------------------
# 1. DIRECTORY STRUCTURE
# --------------------------

dirs <- c(
  "sql/ddl",
  "r/utilities",
  "r",
  "rmd",
  "config",
  "diagrams",
  "docs/SOP_rendered"
)

for (d in dirs) {
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
}

message("Directory structure created.")

# --------------------------
# 2. FILE CONTENT TEMPLATES
# --------------------------

readme_text <- '
# PULSE Pipeline

A metadata-driven, automated data lake pipeline for PRIME-AI\'s PULSE governance framework.

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
'

gitignore_text <- '
# R
.Rhistory
.RData
.Ruserdata
.Rproj.user/

# RStudio
*.Rproj.user
.Rprofile

# Python
__pycache__/
*.pyc

# OS
.DS_Store
Thumbs.db

# Outputs
docs/SOP_rendered/
qc_output/
release_packages/
'

runner_text <- '
library(DBI)
library(glue)
library(dplyr)
library(purrr)

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  dbname   = Sys.getenv("PULSE_DB"),
  host     = Sys.getenv("PULSE_HOST"),
  user     = Sys.getenv("PULSE_USER"),
  password = Sys.getenv("PULSE_PW")
)

get_pipeline_steps <- function(con) {
  dbReadTable(con, "PIPELINE_STEP") %>%
    filter(enabled == TRUE) %>%
    arrange(step_order)
}

execute_step <- function(step, con, ingest_id = NULL) {
  message(paste0("Running step ", step$step_order, ": ", step$step_name))

  if (step$step_type == "SQL") {
    sql <- glue(step$code_snippet)
    DBI::dbExecute(con, sql)

  } else if (step$step_type == "R") {
    fn <- sub("\\(.*", "", step$code_snippet)
    do.call(fn, list(ingest_id = ingest_id, con = con))

  } else if (step$step_type == "RMD") {
    rmarkdown::render(
      input = step$code_snippet,
      params = list(ingest_id = ingest_id),
      output_dir = "docs/SOP_rendered/"
    )
  }
}

run_pipeline <- function(ingest_id) {
  steps <- get_pipeline_steps(con)
  for (i in seq_len(nrow(steps))) {
    execute_step(steps[i, ], con = con, ingest_id = ingest_id)
  }
}
'

stub_fn <- function(msg) {
  paste0(
    "function(ingest_id = NULL, con) {\n",
    "  message(\"", msg, "\")\n",
    "}"
  )
}

register_source_text    <- paste0("register_source <- ",    stub_fn("Registering source (placeholder)"))
log_batch_ingest_text   <- paste0("log_batch_ingest <- ",   stub_fn("Logging batch ingestion (placeholder)"))
validate_schema_text    <- paste0("validate_schema <- ",    stub_fn("Validating schema (placeholder)"))
profile_data_text       <- paste0("profile_data <- ",       stub_fn("Profiling data (placeholder)"))
sync_metadata_text      <- paste0("sync_metadata <- ",      stub_fn("Syncing metadata (placeholder)"))
harmonize_data_text     <- paste0("harmonize_data <- ",     stub_fn("Harmonizing data (placeholder)"))
execute_qc_rules_text   <- paste0("execute_qc_rules <- ",   stub_fn("Executing QC rules (placeholder)"))
generate_qc_packet_text <- paste0("generate_qc_packet <- ", stub_fn("Generating QC packet (placeholder)"))
finalize_release_text   <- paste0("finalize_release <- ",   stub_fn("Finalizing release (placeholder)"))

sop_text <- '
---
title: "PULSE Standard Operating Procedure"
output: word_document
params:
  ingest_id: "placeholder"
---

```{r setup, include=FALSE}
library(DBI)
con <- DBI::dbConnect(RPostgres::Postgres())
steps <- dbReadTable(con, "PIPELINE_STEP")
rules <- dbReadTable(con, "RULE_LIBRARY")
meta  <- dbReadTable(con, "METADATA")
```

# Pipeline Steps
```{r}
knitr::kable(steps)
```

# Governing Rule Library
```{r}
knitr::kable(rules)
```

# Core Metadata Dictionary
```{r}
knitr::kable(meta)
```
'

qc_text <- '
---
title: "PULSE QC Packet"
output: html_document
params:
  ingest_id: NULL
---

```{r}
rules <- dbReadTable(con, "RULE_EXECUTION_LOG") %>%
  filter(ingest_id == params$ingest_id)
knitr::kable(rules)
```
'

yaml_text <- '
database:
  driver: Postgres
  host: ${PULSE_HOST}
  user: ${PULSE_USER}
  password: ${PULSE_PW}
  dbname: ${PULSE_DB}

pipeline:
  generate_sop: true
  generate_qc_packet: true
'

ddl_text <- '
CREATE TABLE PIPELINE_STEP (
    step_id            VARCHAR(50) PRIMARY KEY,
    step_order         INTEGER NOT NULL,
    step_name          VARCHAR(100) NOT NULL,
    step_description   TEXT,
    step_type          VARCHAR(20),
    code_snippet       TEXT,
    enabled            BOOLEAN DEFAULT TRUE,
    created_at_utc     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified_utc  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
'

# --------------------------
# 3. WRITE FILES
# --------------------------

writeLines(readme_text,                "README.md")
writeLines(gitignore_text,             ".gitignore")

writeLines(runner_text,                "r/runner.R")
writeLines(register_source_text,       "r/register_source.R")
writeLines(log_batch_ingest_text,      "r/log_batch_ingest.R")
writeLines(validate_schema_text,       "r/validate_schema.R")
writeLines(profile_data_text,          "r/profile_data.R")
writeLines(sync_metadata_text,         "r/sync_metadata.R")
writeLines(harmonize_data_text,        "r/harmonize_data.R")
writeLines(execute_qc_rules_text,      "r/execute_qc_rules.R")
writeLines(generate_qc_packet_text,    "r/generate_qc_packet.R")
writeLines(finalize_release_text,      "r/finalize_release.R")

writeLines(sop_text,                   "rmd/sop_template.Rmd")
writeLines(qc_text,                    "rmd/qc_packet_template.Rmd")

writeLines(yaml_text,                  "config/pipeline_settings.yml")
writeLines("",                        "config/rule_categories.yml")
writeLines("",                        "config/dashboard.yml")

writeLines(ddl_text,                   "sql/ddl/create_PIPELINE_STEP.sql")

message("PULSE pipeline scaffold created successfully.")
