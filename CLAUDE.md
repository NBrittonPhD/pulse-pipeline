# PULSE Pipeline — Claude Code Instructions

**Project:** PRIME-AI PULSE Data Lake Pipeline  
**Author:** Noel  
**Last Updated:** 2026-01-30

---

## Overview

This pipeline ingests clinical data from multiple sources (CISIR, CLARITY, TRAUMA_REGISTRY), validates schemas, profiles data quality, harmonizes across sources, and produces QC dashboards for governance review.

**Tech Stack:** R + PostgreSQL + Flexdashboard

---

## Pipeline Steps

| Step | Name | Status | Guide |
|------|------|--------|-------|
| 1 | Source Registration | ✅ Complete | `docs/step1/` |
| 2 | Batch Logging & Ingestion | ✅ Complete | `docs/step2/` |
| 3 | Schema Validation | ✅ Complete | `docs/step3/` |
| 4 | Metadata Synchronization | ✅ Complete | `docs/step4/` |
| 5 | Data Profiling (raw.*) | ✅ Complete | `docs/step5/` |
| 6 | Harmonization (staging.* → validated.*) | 🔲 Not Started | `claude/CLAUDE_STEP6_HARMONIZATION.md` |
| 7 | QC Rule Execution | 🔲 Not Started | `claude/CLAUDE_STEP7_QC_RULES.md` |
| 8 | QC Dashboard | 🔲 Not Started | `claude/CLAUDE_STEP8_QC_DASHBOARD.md` |
| 9 | Release Finalization | 📋 Not Specified | See `Steps_List.docx` |
| 10 | Governance Documentation | 📋 Not Specified | See `Steps_List.docx` |

---

## Database Schemas

| Schema | Purpose | Status |
|--------|---------|--------|
| `governance` | Pipeline control, audit trail, batch lineage, QC results | ✅ Exists |
| `reference` | Metadata definitions, ingest dictionaries | ✅ Exists |
| `raw` | Landing zone (all TEXT columns) | ✅ Populated |
| `staging` | Typed tables (after type casting) | ✅ Populated (37 tables) |
| `validated` | Final curated tables (cross-source harmonized) | 🔲 Empty |

---

## Key Reference Files

| File | Purpose |
|------|---------|
| `reference/CURRENT_core_metadata_dictionary.xlsx` | Master dictionary (1,268 variables, 47 tables) |
| `reference/ingest_dictionary.xlsx` | Source-to-lake column mapping |
| `reference/type_decisions/type_decision_table.xlsx` | Target SQL types per variable |
| `config/pipeline_settings.yml` | Controlled vocabularies, thresholds |

---

## To Implement a Step

1. **Read the step guide:** Open the corresponding `claude/CLAUDE_STEP*.md` file
2. **Follow implementation order:** Each guide lists tasks in sequence
3. **Create DDLs first:** Database tables before R functions
4. **Write tests:** Each step has test specifications
5. **Update status:** Mark complete in this file when done

---

## Code Conventions

- **One function per R script file**
- **Verbose annotation:** Narrative-style comments explaining logic
- **USER INPUT SECTION:** At top of user-facing scripts in `r/scripts/`
- **Metadata-driven:** No hardcoded table/column names
- **Snake_case:** For variables, functions, file names
- **Environment variables:** `PULSE_DB`, `PULSE_HOST`, `PULSE_USER`, `PULSE_PW`

---

## Quick Reference: Existing Governance Tables

```
governance.source_registry     -- Registered data sources
governance.audit_log           -- Event trail
governance.pipeline_step       -- Step definitions
governance.batch_log           -- Ingest batches
governance.ingest_file_log     -- File-level lineage
governance.structure_qc_table  -- Schema validation issues
reference.metadata             -- Dictionary definitions (synced from Excel)
reference.metadata_history     -- Field-level change audit trail
```

---

## Quick Reference: Database Connection

```r
source("pulse-init-all.R")
con <- connect_to_pulse()
# ... do work ...
DBI::dbDisconnect(con)
```
