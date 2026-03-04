# PULSE Pipeline

A metadata-driven, automated data lake pipeline for PRIME-AI's PULSE governance framework. Built in R and PostgreSQL, the pipeline ingests raw clinical CSV data from multiple sources, validates schemas, profiles data quality, harmonizes data across sources, and tracks every action through a comprehensive audit trail.

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Database | PostgreSQL |
| Language | R |
| Configuration | YAML |
| Testing | testthat |

---

## Prerequisites

**Environment variables** (set before running any step):

```r
Sys.setenv(PULSE_DB   = "primeai_lake")
Sys.setenv(PULSE_HOST = "localhost")
Sys.setenv(PULSE_USER = "your_username")
Sys.setenv(PULSE_PW   = "your_password")
```

**R packages:**

```r
install.packages(c(
  "DBI", "RPostgres", "dplyr", "tibble", "glue",
  "vroom", "readr", "readxl", "writexl", "digest",
  "fs", "yaml", "jsonlite", "uuid", "stringr", "purrr"
))
```

**Database bootstrap** (run once per environment):

```r
source("pulse-init-all.R")
```

---

## Quick Start

```r
source("r/scripts/1_onboard_new_source.R")   # Register a data source
source("r/scripts/2_ingest_and_log_files.R") # Ingest files with full lineage
source("r/scripts/3_validate_schema.R")       # Validate raw schemas
source("r/scripts/4_sync_metadata.R")         # Sync metadata dictionary
source("r/scripts/5_profile_data.R")          # Profile data quality
source("r/scripts/6_harmonize_data.R")        # Harmonize to validated tables
```

Each script has a **USER INPUT SECTION** at the top for setting parameters like `source_id`, `ingest_id`, and `source_type`.

---

## Pipeline Overview

| Step | Name | Status | Docs |
|------|------|--------|------|
| 1 | Source Registration | Complete | `docs/step1/` |
| 2 | Batch Logging & Ingestion | Complete | `docs/step2/` |
| 3 | Schema Validation | Complete | `docs/step3/` |
| 4 | Metadata Synchronization | Complete | `docs/step4/` |
| 5 | Data Profiling | Complete | `docs/step5/` |
| 6 | Harmonization (staging → validated) | In Testing | `docs/step6/` |
| 7 | QC Rule Execution | Next | — |
| 8 | QC Dashboard | Not Started | — |

---

## Architecture

Five PostgreSQL schemas:

| Schema | Purpose |
|--------|---------|
| `governance` | Audit trail, batch lineage, QC results, profiling |
| `reference` | Metadata definitions, ingest dictionaries, harmonization maps |
| `raw` | Landing zone — all TEXT columns |
| `staging` | Typed tables auto-promoted from raw |
| `validated` | Cross-source harmonized tables (23 clinical domains) |

Key reference files in `reference/`:

| File | Purpose |
|------|---------|
| `CURRENT_core_metadata_dictionary.xlsx` | Master variable dictionary (1,268 variables, 47 tables) |
| `ingest_dictionary.xlsx` | Source column → lake variable mappings |
| `type_decisions/type_decision_table.xlsx` | Target SQL types per variable |

---

## Documentation

Full project documentation is in `docs/`:

- **`docs/project_summary.md`** — Detailed technical reference (architecture, step-by-step flows, all key files, database tables, configuration, development tools)
- **`docs/STYLE_GUIDE.md`** — Code conventions and patterns
- **`docs/step*/`** — Per-step SOPs, function atlases, developer onboarding guides, and governance notes

---

## Testing

```r
testthat::test_dir("tests/testthat/")
```

Requires a running PostgreSQL instance with the PULSE database bootstrapped.

---

## Data Protection

Raw data, staging, and validated directories are excluded from git. Reference Excel files in `reference/` are tracked so they are versioned alongside the code that reads them.
