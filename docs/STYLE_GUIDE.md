# PULSE PIPELINE — STYLE GUIDE
**Author:** Noel
**Purpose:** Define the strict coding, documentation, and structural standards used across the PRIME-AI PULSE Data Lake Pipeline.

This guide governs how **every script, function, SQL file, documentation artifact, and metadata object** must be written moving forward. It ensures consistency, reproducibility, traceability, and long-term maintainability across all pipeline steps.

---

# ============================================================
# OVERARCHING PHILOSOPHY
# ============================================================

1. **One function per script. Always.**
   - Each `.R` file in `steps/` and `utilities/` contains exactly one exported function.
   - The filename matches the function name (e.g., `compare_fields.R` contains `compare_fields()`).
   - Exception: wrapper scripts in `r/scripts/` are procedural, not function-based.

2. **Extremely verbose, narrative-style annotation throughout every file.**
   - Comment every logical section with loud section headers.
   - Explain the "why," not just the "what."

3. **Clear USER INPUT SECTION at the top of user-facing scripts.**
   - All parameters the user must set are grouped at the top.
   - Each parameter has inline comments explaining valid values.

4. **All behavior must be metadata-driven.**
   - No hardcoded field names, table names, or column lists.
   - Read expected schemas from `reference.metadata` and `reference.ingest_dictionary`.
   - Read controlled vocabularies from `config/pipeline_settings.yml`.

5. **Strict separation of concerns:**
   - `r/scripts/` = user interaction (wrapper scripts)
   - `r/steps/` = step orchestration functions
   - `r/utilities/` = reusable helper functions
   - `r/reference/` = metadata management (sync, build)

6. **All functions fail loudly, deterministically, and safely.**
   - Use `stop()` with informative messages for critical errors.
   - Use `warning()` for non-critical issues.
   - Never silently swallow errors.

---

# ============================================================
# DIRECTORY STRUCTURE
# ============================================================

```
r/
  scripts/           # User-facing wrapper scripts (1_onboard_new_source.R, etc.)
  steps/             # Step orchestration functions (register_source.R, etc.)
  utilities/         # Reusable helper functions (compare_fields.R, etc.)
  reference/         # Metadata sync and schema building functions
  build_tools/       # Destructive dev/maintenance utilities (clear logs, drop tables)
  explore/           # Read-only inspection tools (explore_batch_log.R, etc.)
  prep/              # Test data generation (make_toy_raw_extracts.R, etc.)

sql/
  ddl/               # Table creation scripts (CREATE TABLE IF NOT EXISTS)
  inserts/           # Seed data and pipeline step definitions

config/              # YAML configuration files
reference/           # Pipeline input dictionaries (tracked in git)
  type_decisions/    # Type governance files (type_decision_table, decision notes)
  archive/           # Timestamped prior versions of metadata dictionaries
output/
  profiling/         # Data profiling outputs from r/sandbox/ (tracked in git)
raw/                 # Raw data zone (not tracked in git)
staging/             # Staging zone (not tracked in git)
validated/           # Validated zone (not tracked in git)
tests/testthat/      # Unit and integration tests
docs/                # Step documentation (5 files per step folder)
```

---

# ============================================================
# FUNCTION DESIGN STANDARDS
# ============================================================

Every function file must begin with this header block:

```r
# =============================================================================
# <Function Name>
# =============================================================================
# Purpose:      <What this function does>
# Inputs:       <List all parameters with types and descriptions>
# Outputs:      <What the function returns>
# Side Effects: <Database writes, file creation, etc.>
# Dependencies: <Other functions, packages, tables required>
# Author:       Noel
# Last Updated: <Date>
# =============================================================================
```

**Example (from compare_fields.R):**

```r
# =============================================================================
# compare_fields
# =============================================================================
# Purpose:      Compare expected field definitions against observed table schema
#               to identify missing, extra, or mismatched fields.
# Inputs:
#   - expected_schema: tibble with expected field definitions
#   - observed_schema: tibble with observed field definitions
#   - lake_table_name: character string identifying the table being compared
#   - schema_version:  character schema version identifier (optional)
# Outputs:      list with (status, n_issues, issues tibble)
# Side Effects: None (pure function)
# Dependencies: dplyr, tibble
# Author:       Noel
# Last Updated: 2026-01-07
# =============================================================================
```

---

# ============================================================
# WRAPPER SCRIPT STANDARDS
# ============================================================

Wrapper scripts in `r/scripts/` are the user-facing entry points. They are procedural (not function-based) and follow this template:

```r
# =============================================================================
# <Step Name> — WRAPPER SCRIPT
# =============================================================================
# Purpose:      <What this step accomplishes>
# Usage:        Source this script to execute Step X
# Author:       Noel
# Last Updated: <Date>
# =============================================================================

# =============================================================================
# USER INPUT SECTION — MODIFY THESE VALUES
# =============================================================================
# <Parameter descriptions with valid values>

source_id   <- "cisir2026_toy"
ingest_id   <- "ING_cisir2026_toy_20260128_170000"
source_type <- "CISIR"

# =============================================================================
# LOAD DEPENDENCIES
# =============================================================================
source("pulse-init-all.R")
# ...
```

Each wrapper script must:
- Have a clear USER INPUT SECTION at the top
- Source dependencies explicitly
- Establish and close database connections
- Print a summary to console on completion
- Handle errors with `tryCatch()` and log failures

---

# ============================================================
# CODE ANNOTATION RULES
# ============================================================

**Loud Section Headers** — Separate logical blocks with full-width comment bars:

```r
# =========================================================================
# DETECT MISSING FIELDS
# =========================================================================
```

**Inline Explanations** — Explain non-obvious logic:

```r
# Severity = ERROR if a required field is missing, because downstream
# harmonization cannot proceed without it. Optional fields get WARNING.
severity <- ifelse(field_info$is_required, "ERROR", "WARNING")
```

**Verbose Messages** — Every significant action should emit a `message()`:

```r
message(paste0("[validate_schema] Loaded ", nrow(expected_schema), " expected field definitions."))
message(paste0("[validate_schema] Validating table ", i, "/", nrow(raw_tables), ": ", table_name))
```

**Function Tag Prefix** — Messages should be prefixed with the function name in square brackets:

```r
message("[compare_fields] Comparison complete for table 'cisir_encounter'. Issues found: 3")
```

---

# ============================================================
# NAMING CONVENTIONS
# ============================================================

| Object | Convention | Example |
|--------|------------|---------|
| source_id | lowercase | `cisir2026_toy` |
| source_type | ALL CAPS | `CISIR` |
| ingest_id | ING\_\<source_id\>\_\<timestamp\> | `ING_cisir2026_toy_20260128_170000` |
| lake_table | lowercase | `cisir_vitals_minmax` |
| variable names | snake_case | `admit_date` |
| function names | snake_case | `compare_fields` |
| R files | snake_case.R | `compare_fields.R` |
| wrapper scripts | N\_description.R | `3_validate_schema.R` |
| SQL DDL files | create\_TABLENAME.sql | `create_STRUCTURE_QC_TABLE.sql` |
| SQL insert files | insert\_TABLENAME.sql | `insert_PIPELINE_STEP_step1.sql` |
| test files | test\_stepN\_description.R | `test_step3_schema_validation.R` |
| doc files | stepN\_description.md | `step3_function_atlas.md` |

---

# ============================================================
# TESTING STANDARDS
# ============================================================

All tests use the `testthat` framework and live in `tests/testthat/`.

**What tests must verify:**

- **Correct DB writes** — Confirm rows are inserted into the expected tables with the expected values.
- **Error control** — Confirm that invalid inputs produce informative `stop()` errors, not silent failures.
- **Deterministic behavior** — Same inputs always produce same outputs.
- **Full coverage of allowed/blocked paths** — Test both the happy path and edge cases (empty tables, duplicate IDs, missing fields).

**Test file naming:**

- One test file per step: `test_step1_register_source.R`, `test_step2_batch_logging.R`, `test_step3_schema_validation.R`
- Integration tests: `test_step1_integration.R`

**Running tests:**

```r
testthat::test_dir("tests/testthat/")
testthat::test_file("tests/testthat/test_step3_schema_validation.R")
```

Tests require a running PostgreSQL instance with the PULSE database bootstrapped via `pulse-init-all.R`.

---

# ============================================================
# DOCUMENTATION REQUIRED PER PIPELINE STEP
# ============================================================

Every step folder in `docs/` contains exactly **5 files** with uniform naming:

| File | Purpose |
|------|---------|
| `stepN_clusterN_snapshot.json` | Machine-readable inventory of all step artifacts (tables, functions, scripts, tests, status) |
| `stepN_developer_onboarding.md` | Onboarding guide for new developers working on this step |
| `stepN_function_atlas.md` | Full function reference with signatures, returns, and side effects |
| `stepN_governance.md` | Governance tables, principles, and downstream dependencies |
| `stepN_sop_summary.md` | Standard operating procedure with step-by-step summary and mermaid flowchart |

Additionally, `docs/` contains two cross-step files:

| File | Purpose |
|------|---------|
| `function_dependency_table.md` | Complete function dependency table across all steps |
| `STYLE_GUIDE.md` | This file |

---

# ============================================================
# SQL STANDARDS
# ============================================================

**DDL files** use `CREATE TABLE IF NOT EXISTS` and include:
- Column comments
- CHECK constraints for enum-like columns
- Foreign key constraints where applicable
- Indexes for frequently queried columns

**Naming:** SQL files use uppercase table names in the filename (`create_STRUCTURE_QC_TABLE.sql`) while the actual table names in SQL are lowercase (`governance.structure_qc_table`).

**Schema qualification:** Always use schema-qualified table names (`governance.batch_log`, not just `batch_log`).

---

# ============================================================
# HARD RULES
# ============================================================

1. **One function per script** (except wrapper scripts)
2. **Metadata-driven logic only** — no hardcoded field names or table names
3. **Strict source_type enforcement** — never mix data across source types
4. **Extremely verbose annotation** — every logical section commented
5. **User input section always present** in wrapper scripts
6. **No mixing responsibilities between layers** — scripts, steps, and utilities stay separate
7. **No guessing logic** — only mappings defined in metadata are allowed
8. **Always produce the full documentation pack** at the end of each step
9. **All functions fail loudly** with informative `stop()` messages
10. **Follow naming conventions exactly** as defined in this guide

---

# END OF STYLE GUIDE
