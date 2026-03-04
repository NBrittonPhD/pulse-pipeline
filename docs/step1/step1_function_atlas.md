# Function Atlas — Step 1
## Source Registration

---

This reference lists all functions used in Step 1, grouped by purpose, with details about inputs, outputs, and responsibilities.

---

## Core Functions

### `register_source()`

**File:** `r/steps/register_source.R`

**Purpose:** Core Step 1 engine. Validates the source definition, inserts or updates `governance.source_registry`, creates on-disk folder structure for new sources, and writes audit log events.

**Signature:**
```r
register_source(
    con,                        # DBIConnection (required)
    source_id,                  # character: unique source identifier (required)
    source_name,                # character: human-readable name (required)
    system_type,                # character: one of allowed vocab (required)
    update_frequency,           # character: expected cadence (required)
    data_owner,                 # character: upstream data owner (required)
    ingest_method,              # character: one of allowed vocab (required)
    expected_schema_version,    # character: semantic version string (required)
    retention_policy = NULL,    # character: text description (optional)
    pii_classification,         # character: PHI / Limited / NonPHI (required)
    active = TRUE,              # logical: active flag (default TRUE)
    created_by = NULL           # character: user executing the action (optional)
)
```

**Returns:** TRUE (invisible)

**Side Effects:**
- Inserts or updates `governance.source_registry`
- Creates folder structure via `create_source_folders()` for new sources
- Writes `source_registration` or `source_update` event to `governance.audit_log`

---

### `run_step1_register_source()`

**File:** `r/steps/run_step1_register_source.R`

**Purpose:** Step 1 wrapper executed by the pipeline runner. Calls `register_source()` with unpacked source parameters and records step completion.

**Signature:**
```r
run_step1_register_source(
    con,                    # DBIConnection (required)
    source_params,          # list: source registration parameters (required)
    settings = NULL         # list: pipeline settings (optional, loaded if NULL)
)
```

**Returns:** TRUE (invisible)

**Side Effects:**
- Calls `register_source()` with unpacked `source_params`
- Writes `STEP_001` completion to `governance.pipeline_step` via `write_pipeline_step()`

---

### `validate_source_entry()`

**File:** `r/utilities/validate_source_entry.R`

**Purpose:** Validates a candidate source definition against required fields and controlled vocabularies defined in `pipeline_settings.yml`.

**Signature:**
```r
validate_source_entry(
    candidate,              # list: fields provided to register_source() (required)
    settings                # list: loaded pipeline settings YAML (required)
)
```

**Returns:** TRUE (invisible) if all validations succeed

**Side Effects:**
- Stops with informative error message if validation fails (missing field, invalid vocab, incorrect type)

---

### `create_source_folders()`

**File:** `r/utilities/create_source_folders.R`

**Purpose:** Builds the on-disk folder tree for a source using the template defined in `directory_structure.yml`.

**Signature:**
```r
create_source_folders(
    source_id,              # character: stable ID for the source (required)
    base_path = "."         # character: root of PULSE repo on disk (default ".")
)
```

**Returns:** Character vector of full paths that were created

**Side Effects:**
- Creates directories under the project root across `raw/`, `staging/`, `validated/`, and governance zones

---

### `write_audit_event()`

**File:** `r/steps/write_audit_event.R`

**Purpose:** Inserts a single, governed event record into `governance.audit_log` with a UUID-based `audit_id` and JSON-encoded details.

**Signature:**
```r
write_audit_event(
    con,                    # DBIConnection (required)
    ingest_id = NULL,       # character: ingest batch identifier (optional)
    event_type,             # character: e.g., "source_registration" (required)
    object_type,            # character: "table", "file", "function", etc. (required)
    object_name,            # character: name of affected object (required)
    details = NULL,         # list: arbitrary event metadata, JSON-encoded (optional)
    status = NULL           # character: "success", "error", "warning" (optional)
)
```

**Returns:** `audit_id` (invisible)

**Side Effects:**
- Inserts one row into `governance.audit_log`

---

### `write_pipeline_step()`

**File:** `r/utilities/write_pipeline_step.R`

**Purpose:** Upserts a pipeline step definition in `governance.pipeline_step`. This is configuration, not per-run history — it ensures the step table always reflects the current definition of each step (order, name, type, code snippet, enabled status).

**Signature:**
```r
write_pipeline_step(
    con,                    # DBIConnection (required)
    step_id,                # character: step identifier, e.g., "STEP_001" (required)
    step_order,             # integer: execution order (required)
    step_name,              # character: human-readable step name (required)
    step_description,       # character: what the step does (required)
    step_type,              # character: "R", "SQL", or "RMD" (required)
    code_snippet,           # character: function or file to execute (required)
    enabled = TRUE          # logical: whether step is active (default TRUE)
)
```

**Returns:** TRUE (invisible)

**Side Effects:**
- Inserts a new row if `step_id` does not exist, or updates metadata and bumps `last_modified_utc` if it does

---

## Configuration Loaders

### `load_source_params()`

**File:** `r/utilities/load_source_params.R`

**Purpose:** Loads source registration parameters from `config/source_params.yml`.

**Signature:**
```r
load_source_params(
    path = NULL             # character: path to YAML file (default: config/source_params.yml)
)
```

**Returns:** Named list of fields required by `register_source()`

**Side Effects:**
- Reads YAML file from disk; stops with error if file not found

---

### `load_pipeline_settings()`

**File:** `r/runner.R`

**Purpose:** Loads global pipeline settings (schemas, controlled vocabularies, folder template references) from `config/pipeline_settings.yml`.

**Signature:**
```r
load_pipeline_settings()
```

**Returns:** Named list parsed from YAML

**Side Effects:**
- Reads YAML file from disk

---

## Pipeline Runner Functions

### `run_pipeline()`

**File:** `r/runner.R`

**Purpose:** Main pipeline orchestration function. Connects to the database, loads settings, fetches enabled steps, and executes them in order.

**Signature:**
```r
run_pipeline(
    ingest_id               # character: unique identifier for pipeline run (required)
)
```

**Returns:** TRUE (invisible)

**Side Effects:**
- Establishes and disconnects DB connection
- Executes all enabled steps in sequence via `execute_step()`

---

### `execute_step()`

**File:** `r/runner.R`

**Purpose:** Dispatches a single pipeline step based on `step_type` (SQL, R, or RMD). Special-case handling for `STEP_001` and `STEP_002`.

**Signature:**
```r
execute_step(
    step,                   # data.frame row: pipeline step definition (required)
    con,                    # DBIConnection (required)
    ingest_id = NULL,       # character: current ingest identifier (optional)
    settings                # list: pipeline settings (required)
)
```

**Returns:** TRUE (invisible)

**Side Effects:**
- Executes SQL, R functions, or RMarkdown renders depending on `step_type`

---

### `get_pipeline_steps()`

**File:** `r/runner.R`

**Purpose:** Reads `governance.pipeline_step`, filters to enabled steps, and orders by `step_order`.

**Signature:**
```r
get_pipeline_steps(
    con,                    # DBIConnection (required)
    settings                # list: pipeline settings (required)
)
```

**Returns:** data.frame of enabled pipeline steps, sorted by `step_order`

**Side Effects:**
- Reads from `governance.pipeline_step`

---

### `pulse_launch()`

**File:** `pulse-launch.R`

**Purpose:** High-level entry point for pipeline runs. Optionally writes `source_params.yml` from provided parameters and then calls `run_pipeline()`.

**Signature:**
```r
pulse_launch(
    ingest_id,                  # character: unique identifier (required)
    source_params,              # list: source registration parameters (required)
    auto_write_params = TRUE,   # logical: write source_params to YAML (default TRUE)
    params_path = "config/source_params.yml"  # character: YAML output path
)
```

**Returns:** TRUE (invisible)

**Side Effects:**
- Creates `config/` directory if missing
- Optionally writes `source_params.yml`
- Sources `r/runner.R` and calls `run_pipeline()`

---

## Dependency Graph

```
1_onboard_new_source.R (user script)
    └── pulse_launch()
            └── run_pipeline()
                    └── execute_step()
                            └── run_step1_register_source()
                                    ├── register_source()
                                    │       ├── validate_source_entry()
                                    │       ├── create_source_folders()
                                    │       └── write_audit_event()
                                    └── write_pipeline_step()
```

---

## Database Tables

### `governance.source_registry`

Authoritative record of every onboarded data source. One row per source.

**Key Columns:**
- `source_id` (PK) — unique, machine-readable source identifier
- `source_name` — human-readable name
- `system_type` — upstream data format (controlled vocabulary)
- `update_frequency` — expected cadence (controlled vocabulary)
- `data_owner` — responsible party
- `ingest_method` — ingestion pathway (controlled vocabulary)
- `expected_schema_version` — semantic version string
- `retention_policy` — free-text retention description (nullable)
- `pii_classification` — data sensitivity level (controlled vocabulary)
- `active` — boolean flag to enable/disable the source
- `created_at_utc`, `last_modified_utc` — timestamps
- `created_by` — database user (DEFAULT SESSION_USER)

### `governance.audit_log`

Structured governance event record. One row per event.

**Key Columns:**
- `audit_id` (PK) — UUID-based unique identifier (`AUD_` prefix)
- `ingest_id` — batch identifier (nullable; NULL for source registration events)
- `action` — pipe-delimited string (e.g., `source_registration|success|table|governance.source_registry`)
- `details` — JSONB payload with event_type, object_type, object_name, status, and payload
- `executed_by` — database user who triggered the event
- `executed_at_utc` — when the event occurred (DEFAULT CURRENT_TIMESTAMP)

### `governance.pipeline_step`

Configuration registry for all pipeline steps. One row per step definition.

**Key Columns:**
- `step_id` (PK) — step identifier (e.g., `STEP_001`)
- `step_order` — execution sequence number
- `step_name` — machine-readable step name
- `step_description` — human-readable description
- `step_type` — execution type (`R`, `SQL`, or `RMD`)
- `code_snippet` — function or file to invoke
- `enabled` — whether the step is active
- `created_at_utc`, `last_modified_utc` — timestamps
