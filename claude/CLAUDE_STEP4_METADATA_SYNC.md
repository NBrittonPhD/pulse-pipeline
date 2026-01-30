# CLAUDE_STEP4_METADATA_SYNC.md

# Step 4: Metadata Synchronization

**Purpose:** Synchronize the core metadata dictionary (Excel) with the database, tracking all changes with full version history.

**Prerequisite:** Steps 1-3 complete (source registered, data ingested, schema validated)

**Output:** Updated `reference.metadata` table with version tracking and `reference.metadata_history` for audit trail

---

## Table of Contents

1. [Overview](#overview)
2. [Database Tables](#database-tables)
3. [Functions to Create](#functions-to-create)
4. [User Script](#user-script)
5. [Implementation Order](#implementation-order)
6. [Testing](#testing)
7. [Acceptance Criteria](#acceptance-criteria)

---

## Overview

### Why This Step Exists

The `CURRENT_core_metadata_dictionary.xlsx` file is the source of truth for all variable definitions, data types, validation rules, and labels. This step:

1. Loads the Excel dictionary
2. Compares it to the current database state
3. Detects field-level changes (adds, updates, removes)
4. Writes changes to a history table for audit trail
5. Upserts the main metadata table with a new version number

### Execution Flow

```
User runs r/scripts/4_sync_metadata.R
    ↓
Load CURRENT_core_metadata_dictionary.xlsx
    ↓
Query current reference.metadata (if exists)
    ↓
compare_metadata(): Detect field-level changes
    ↓
Get next version_number (MAX + 1)
    ↓
Write changes to reference.metadata_history
    ↓
Upsert reference.metadata (INSERT/UPDATE/soft-delete)
    ↓
Write governance.audit_log event
    ↓
Return summary: {version, adds, updates, removes}
```

---

## Database Tables

### Table 1: reference.metadata (MODIFY EXISTING)

The existing `reference.metadata` table needs additional columns to store the full dictionary.

**DDL File:** `sql/ddl/alter_METADATA_add_columns.sql`

```sql
-- =============================================================================
-- alter_METADATA_add_columns.sql
-- Purpose: Add columns to reference.metadata to store full dictionary fields
-- Run once to upgrade existing table
-- =============================================================================

-- Add new columns if they don't exist
ALTER TABLE reference.metadata 
ADD COLUMN IF NOT EXISTS source_type TEXT,
ADD COLUMN IF NOT EXISTS source_variable_name TEXT,
ADD COLUMN IF NOT EXISTS variable_label TEXT,
ADD COLUMN IF NOT EXISTS variable_definition TEXT,
ADD COLUMN IF NOT EXISTS value_labels TEXT,
ADD COLUMN IF NOT EXISTS variable_unit TEXT,
ADD COLUMN IF NOT EXISTS valid_min NUMERIC,
ADD COLUMN IF NOT EXISTS valid_max NUMERIC,
ADD COLUMN IF NOT EXISTS allowed_values TEXT,
ADD COLUMN IF NOT EXISTS is_identifier CHAR(1) DEFAULT 'N',
ADD COLUMN IF NOT EXISTS is_phi CHAR(1) DEFAULT 'N',
ADD COLUMN IF NOT EXISTS is_required CHAR(1) DEFAULT 'N',
ADD COLUMN IF NOT EXISTS validated_table_target TEXT,
ADD COLUMN IF NOT EXISTS validated_variable_name TEXT,
ADD COLUMN IF NOT EXISTS notes TEXT,
ADD COLUMN IF NOT EXISTS needs_further_review TEXT,
ADD COLUMN IF NOT EXISTS version_number INTEGER DEFAULT 1,
ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE,
ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add index on version_number for efficient queries
CREATE INDEX IF NOT EXISTS idx_metadata_version 
ON reference.metadata(version_number);

-- Add index on active status
CREATE INDEX IF NOT EXISTS idx_metadata_active 
ON reference.metadata(is_active);

-- Add composite index for lookups
CREATE INDEX IF NOT EXISTS idx_metadata_table_var 
ON reference.metadata(lake_table_name, lake_variable_name);

COMMENT ON TABLE reference.metadata IS 
'Expected schema definitions synced from core_metadata_dictionary.xlsx. 
Version-controlled with soft deletes (is_active = FALSE for removed variables).';
```

**Final Column List for reference.metadata:**

| Column | Type | Description |
|--------|------|-------------|
| `lake_table_name` | TEXT | Target table name in raw/staging schema |
| `lake_variable_name` | TEXT | Target column name |
| `source_type` | TEXT | CISIR, CLARITY, or TRAUMA_REGISTRY |
| `source_variable_name` | TEXT | Original column name in source |
| `data_type` | TEXT | Expected data type (text, integer, numeric, date, etc.) |
| `variable_label` | TEXT | Short human-readable label |
| `variable_definition` | TEXT | Full description of variable |
| `value_labels` | TEXT | Code definitions (e.g., "1=Male, 2=Female") |
| `variable_unit` | TEXT | Unit of measurement (e.g., "kg", "mmHg") |
| `valid_min` | NUMERIC | Minimum allowed value |
| `valid_max` | NUMERIC | Maximum allowed value |
| `allowed_values` | TEXT | Constraint expression (e.g., "0-100", "Y, N") |
| `is_identifier` | CHAR(1) | Y/N - Is this an identifier column? |
| `is_phi` | CHAR(1) | Y/N - Contains PHI? |
| `is_required` | CHAR(1) | Y/N - Required field? |
| `validated_table_target` | TEXT | Target table in validated schema |
| `validated_variable_name` | TEXT | Target column in validated schema |
| `notes` | TEXT | Additional notes |
| `needs_further_review` | TEXT | Flag for variables needing review |
| `version_number` | INTEGER | Metadata version (increments on sync) |
| `is_active` | BOOLEAN | TRUE = active, FALSE = soft-deleted |
| `created_at` | TIMESTAMP | When record was created |
| `updated_at` | TIMESTAMP | When record was last modified |

**Primary Key:** Composite of (`lake_table_name`, `lake_variable_name`, `source_type`)

---

### Table 2: reference.metadata_history (NEW)

Tracks every field-level change across versions for full audit trail.

**DDL File:** `sql/ddl/create_METADATA_HISTORY.sql`

```sql
-- =============================================================================
-- create_METADATA_HISTORY.sql
-- Purpose: Track field-level changes to reference.metadata across versions
-- Author: Noel
-- =============================================================================

CREATE TABLE IF NOT EXISTS reference.metadata_history (
    history_id SERIAL PRIMARY KEY,
    
    -- Version info
    version_number INTEGER NOT NULL,
    
    -- What changed
    lake_table_name TEXT NOT NULL,
    lake_variable_name TEXT NOT NULL,
    source_type TEXT,
    field_changed TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    
    -- Change classification
    change_type TEXT NOT NULL CHECK (change_type IN ('INITIAL', 'ADD', 'UPDATE', 'REMOVE')),
    
    -- Audit fields
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT DEFAULT CURRENT_USER
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_metadata_history_version 
ON reference.metadata_history(version_number);

CREATE INDEX IF NOT EXISTS idx_metadata_history_table 
ON reference.metadata_history(lake_table_name);

CREATE INDEX IF NOT EXISTS idx_metadata_history_change_type 
ON reference.metadata_history(change_type);

CREATE INDEX IF NOT EXISTS idx_metadata_history_changed_at 
ON reference.metadata_history(changed_at);

COMMENT ON TABLE reference.metadata_history IS 
'Audit trail of all changes to reference.metadata. One row per field changed per variable.';

COMMENT ON COLUMN reference.metadata_history.change_type IS 
'INITIAL = first load, ADD = new variable, UPDATE = field value changed, REMOVE = variable deleted';
```

---

## Functions to Create

### Function 1: load_metadata_dictionary()

**File:** `r/reference/load_metadata_dictionary.R`

**Purpose:** Load the Excel dictionary and standardize it for comparison/insertion.

```r
# =============================================================================
# load_metadata_dictionary.R
# Purpose: Load core metadata dictionary from Excel and standardize for DB sync
# Author: Noel
# =============================================================================

#' Load Metadata Dictionary from Excel
#'
#' Reads CURRENT_core_metadata_dictionary.xlsx and prepares it for database
#' synchronization. Standardizes column names, handles NA values, and validates
#' required fields.
#'
#' @param dict_path Path to the Excel dictionary file
#' @param source_type_filter Optional: filter to specific source_type (e.g., "CISIR")
#' @return Tibble with standardized dictionary ready for DB operations
#'
#' @details
#' Expected Excel columns:
#'   - source_type, source_variable_name, data_type
#'   - lake_table_name, lake_variable_name
#'   - variable_label, variable_definition, value_labels
#'   - variable_unit, valid_min, valid_max, allowed_values
#'   - is_identifier, is_phi, is_required
#'   - validated_table_target, validated_variable_name
#'   - notes, needs_further_review
#'
#' @export
load_metadata_dictionary <- function(dict_path, source_type_filter = NULL) {


    # -------------------------------------------------------------------------
    # Validate file exists
    # -------------------------------------------------------------------------
    
    if (!file.exists(dict_path)) {
        stop(glue::glue("Dictionary file not found: {dict_path}"))
    }
    
    log_message(glue::glue("Loading metadata dictionary from: {dict_path}"))
    
    # -------------------------------------------------------------------------
    # Read Excel file
    # -------------------------------------------------------------------------
    
    dict <- readxl::read_excel(dict_path)
    
    log_message(glue::glue("  Loaded {nrow(dict)} rows, {ncol(dict)} columns"))
    
    # -------------------------------------------------------------------------
    # Validate required columns exist
    # -------------------------------------------------------------------------
    
    required_cols <- c(
        "source_type", "lake_table_name", "lake_variable_name", "data_type"
    )
    
    missing_cols <- setdiff(required_cols, names(dict))
    
    if (length(missing_cols) > 0) {
        stop(glue::glue(
            "Missing required columns in dictionary: {paste(missing_cols, collapse = ', ')}"
        ))
    }
    
    # -------------------------------------------------------------------------
    # Standardize column names to match database
    # -------------------------------------------------------------------------
    
    # Expected columns in database order
    db_columns <- c(
        "source_type", "source_variable_name", "data_type",
        "lake_table_name", "lake_variable_name",
        "variable_label", "variable_definition", "value_labels",
        "variable_unit", "valid_min", "valid_max", "allowed_values",
        "is_identifier", "is_phi", "is_required",
        "validated_table_target", "validated_variable_name",
        "notes", "needs_further_review"
    )
    
    # Add missing columns as NA
    for (col in db_columns) {
        if (!col %in% names(dict)) {
            dict[[col]] <- NA_character_
        }
    }
    
    # Select only database columns in correct order
    dict <- dict[, db_columns]
    
    # -------------------------------------------------------------------------
    # Standardize Y/N fields
    # -------------------------------------------------------------------------
    
    yn_fields <- c("is_identifier", "is_phi", "is_required")
    
    for (field in yn_fields) {
        dict[[field]] <- dplyr::case_when(
            toupper(dict[[field]]) %in% c("Y", "YES", "TRUE", "1") ~ "Y",
            toupper(dict[[field]]) %in% c("N", "NO", "FALSE", "0") ~ "N",
            is.na(dict[[field]]) ~ "N",
            TRUE ~ "N"
        )
    }
    
    # -------------------------------------------------------------------------
    # Filter by source_type if requested
    # -------------------------------------------------------------------------
    
    if (!is.null(source_type_filter)) {
        original_count <- nrow(dict)
        dict <- dplyr::filter(dict, source_type == source_type_filter)
        log_message(glue::glue(
            "  Filtered to source_type = '{source_type_filter}': {nrow(dict)} of {original_count} rows"
        ))
    }
    
    # -------------------------------------------------------------------------
    # Final validation
    # -------------------------------------------------------------------------
    
    # Check for duplicate keys
    dict <- dict %>%
        dplyr::mutate(
            composite_key = paste(lake_table_name, lake_variable_name, source_type, sep = "|")
        )
    
    dupes <- dict %>%
        dplyr::count(composite_key) %>%
        dplyr::filter(n > 1)
    
    if (nrow(dupes) > 0) {
        warning(glue::glue(
            "Found {nrow(dupes)} duplicate keys in dictionary. First: {dupes$composite_key[1]}"
        ))
    }
    
    dict <- dplyr::select(dict, -composite_key)
    
    log_message(glue::glue("  Dictionary ready: {nrow(dict)} variables"))
    
    return(tibble::as_tibble(dict))
}
```

---

### Function 2: compare_metadata()

**File:** `r/utilities/compare_metadata.R`

**Purpose:** Compare new dictionary to current database state, returning field-level diff.

```r
# =============================================================================
# compare_metadata.R
# Purpose: Compare metadata dictionaries to detect field-level changes
# Author: Noel
# =============================================================================

#' Compare Metadata Dictionaries
#'
#' Performs field-level comparison between new dictionary (from Excel) and
#' current dictionary (from database). Returns one row per change detected.
#'
#' @param new_dict Tibble from load_metadata_dictionary()
#' @param current_dict Tibble from database query (or empty tibble if first sync)
#' @return Tibble with columns: lake_table_name, lake_variable_name, source_type,
#'         field_changed, old_value, new_value, change_type
#'
#' @details
#' Change types:
#'   - INITIAL: First time this variable is being loaded (current_dict is empty)
#'   - ADD: Variable exists in new but not in current
#'   - UPDATE: Variable exists in both but field value differs
#'   - REMOVE: Variable exists in current but not in new
#'
#' @export
compare_metadata <- function(new_dict, current_dict) {

    # -------------------------------------------------------------------------
    # Define which fields to track for changes
    # -------------------------------------------------------------------------
    
    TRACKED_FIELDS <- c(
        "source_type", "source_variable_name", "data_type",
        "variable_label", "variable_definition", "value_labels",
        "variable_unit", "valid_min", "valid_max", "allowed_values",
        "is_identifier", "is_phi", "is_required",
        "validated_table_target", "validated_variable_name",
        "notes", "needs_further_review"
    )
    
    # -------------------------------------------------------------------------
    # Handle empty current dictionary (first sync)
    # -------------------------------------------------------------------------
    
    if (nrow(current_dict) == 0) {
        log_message("  First sync detected - all variables will be INITIAL")
        
        changes <- new_dict %>%
            tidyr::pivot_longer(
                cols = dplyr::all_of(TRACKED_FIELDS),
                names_to = "field_changed",
                values_to = "new_value"
            ) %>%
            dplyr::mutate(
                old_value = NA_character_,
                new_value = as.character(new_value),
                change_type = "INITIAL"
            ) %>%
            dplyr::select(
                lake_table_name, lake_variable_name, source_type,
                field_changed, old_value, new_value, change_type
            )
        
        return(changes)
    }
    
    # -------------------------------------------------------------------------
    # Create composite keys for matching
    # -------------------------------------------------------------------------
    
    new_dict <- new_dict %>%
        dplyr::mutate(
            key = paste(lake_table_name, lake_variable_name, source_type, sep = "|")
        )
    
    current_dict <- current_dict %>%
        dplyr::mutate(
            key = paste(lake_table_name, lake_variable_name, source_type, sep = "|")
        )
    
    new_keys <- unique(new_dict$key)
    current_keys <- unique(current_dict$key)
    
    # -------------------------------------------------------------------------
    # Identify ADDs, UPDATEs, and REMOVEs
    # -------------------------------------------------------------------------
    
    added_keys <- setdiff(new_keys, current_keys)
    removed_keys <- setdiff(current_keys, new_keys)
    common_keys <- intersect(new_keys, current_keys)
    
    log_message(glue::glue(
        "  Comparing: {length(added_keys)} adds, {length(removed_keys)} removes, {length(common_keys)} to check for updates"
    ))
    
    changes_list <- list()
    
    # -------------------------------------------------------------------------
    # Process ADDs
    # -------------------------------------------------------------------------
    
    if (length(added_keys) > 0) {
        added_changes <- new_dict %>%
            dplyr::filter(key %in% added_keys) %>%
            tidyr::pivot_longer(
                cols = dplyr::all_of(TRACKED_FIELDS),
                names_to = "field_changed",
                values_to = "new_value"
            ) %>%
            dplyr::mutate(
                old_value = NA_character_,
                new_value = as.character(new_value),
                change_type = "ADD"
            ) %>%
            dplyr::select(
                lake_table_name, lake_variable_name, source_type,
                field_changed, old_value, new_value, change_type
            )
        
        changes_list[["adds"]] <- added_changes
    }
    
    # -------------------------------------------------------------------------
    # Process REMOVEs
    # -------------------------------------------------------------------------
    
    if (length(removed_keys) > 0) {
        removed_changes <- current_dict %>%
            dplyr::filter(key %in% removed_keys) %>%
            tidyr::pivot_longer(
                cols = dplyr::any_of(TRACKED_FIELDS),
                names_to = "field_changed",
                values_to = "old_value"
            ) %>%
            dplyr::mutate(
                old_value = as.character(old_value),
                new_value = NA_character_,
                change_type = "REMOVE"
            ) %>%
            dplyr::select(
                lake_table_name, lake_variable_name, source_type,
                field_changed, old_value, new_value, change_type
            )
        
        changes_list[["removes"]] <- removed_changes
    }
    
    # -------------------------------------------------------------------------
    # Process UPDATEs (field-level comparison)
    # -------------------------------------------------------------------------
    
    if (length(common_keys) > 0) {
        update_changes <- list()
        
        for (k in common_keys) {
            new_row <- new_dict %>% dplyr::filter(key == k)
            current_row <- current_dict %>% dplyr::filter(key == k)
            
            for (field in TRACKED_FIELDS) {
                new_val <- as.character(new_row[[field]][1])
                old_val <- as.character(current_row[[field]][1])
                
                # Normalize NA comparisons
                new_val <- ifelse(is.na(new_val), "", new_val)
                old_val <- ifelse(is.na(old_val), "", old_val)
                
                if (new_val != old_val) {
                    update_changes[[length(update_changes) + 1]] <- tibble::tibble(
                        lake_table_name = new_row$lake_table_name[1],
                        lake_variable_name = new_row$lake_variable_name[1],
                        source_type = new_row$source_type[1],
                        field_changed = field,
                        old_value = ifelse(old_val == "", NA_character_, old_val),
                        new_value = ifelse(new_val == "", NA_character_, new_val),
                        change_type = "UPDATE"
                    )
                }
            }
        }
        
        if (length(update_changes) > 0) {
            changes_list[["updates"]] <- dplyr::bind_rows(update_changes)
        }
    }
    
    # -------------------------------------------------------------------------
    # Combine all changes
    # -------------------------------------------------------------------------
    
    if (length(changes_list) > 0) {
        all_changes <- dplyr::bind_rows(changes_list)
    } else {
        all_changes <- tibble::tibble(
            lake_table_name = character(),
            lake_variable_name = character(),
            source_type = character(),
            field_changed = character(),
            old_value = character(),
            new_value = character(),
            change_type = character()
        )
    }
    
    log_message(glue::glue("  Total changes detected: {nrow(all_changes)}"))
    
    return(all_changes)
}
```

---

### Function 3: sync_metadata()

**File:** `r/reference/sync_metadata.R`

**Purpose:** Main orchestrator function that performs the full sync operation.

```r
# =============================================================================
# sync_metadata.R
# Purpose: Synchronize metadata dictionary from Excel to database
# Author: Noel
# =============================================================================

#' Synchronize Metadata Dictionary to Database
#'
#' Loads the core metadata dictionary from Excel, compares to current database
#' state, logs all changes to metadata_history, and upserts the metadata table.
#'
#' @param con DBI connection to PULSE database
#' @param dict_path Path to CURRENT_core_metadata_dictionary.xlsx
#' @param source_type_filter Optional: filter to specific source_type
#' @return List with: version_number, adds, updates, removes, total_changes
#'
#' @details
#' This function:
#' 1. Loads dictionary from Excel via load_metadata_dictionary()
#' 2. Queries current reference.metadata
#' 3. Compares via compare_metadata()
#' 4. Writes changes to reference.metadata_history
#' 5. Upserts reference.metadata (INSERT new, UPDATE existing, soft-delete removed)
#' 6. Writes audit event to governance.audit_log
#'
#' @export
sync_metadata <- function(con, dict_path, source_type_filter = NULL) {

    log_message("=" %>% strrep(70))
    log_message("STEP 4: METADATA SYNCHRONIZATION")
    log_message("=" %>% strrep(70))
    
    # -------------------------------------------------------------------------
    # Source dependencies
    # -------------------------------------------------------------------------
    
    source("r/reference/load_metadata_dictionary.R")
    source("r/utilities/compare_metadata.R")
    source("r/steps/write_audit_event.R")
    
    # -------------------------------------------------------------------------
    # Load new dictionary from Excel
    # -------------------------------------------------------------------------
    
    log_message("Loading dictionary from Excel...")
    new_dict <- load_metadata_dictionary(dict_path, source_type_filter)
    
    # -------------------------------------------------------------------------
    # Query current metadata from database
    # -------------------------------------------------------------------------
    
    log_message("Querying current metadata from database...")
    
    current_dict <- DBI::dbGetQuery(con, "
        SELECT *
        FROM reference.metadata
        WHERE is_active = TRUE
    ") %>% tibble::as_tibble()
    
    log_message(glue::glue("  Current database has {nrow(current_dict)} active variables"))
    
    # -------------------------------------------------------------------------
    # Compare dictionaries
    # -------------------------------------------------------------------------
    
    log_message("Comparing dictionaries...")
    changes <- compare_metadata(new_dict, current_dict)
    
    # Count change types
    change_summary <- changes %>%
        dplyr::count(change_type) %>%
        tidyr::pivot_wider(names_from = change_type, values_from = n, values_fill = 0)
    
    n_initial <- change_summary$INITIAL %||% 0
    n_adds <- change_summary$ADD %||% 0
    n_updates <- change_summary$UPDATE %||% 0
    n_removes <- change_summary$REMOVE %||% 0
    
    log_message(glue::glue(
        "  Changes: {n_initial} initial, {n_adds} adds, {n_updates} updates, {n_removes} removes"
    ))
    
    # -------------------------------------------------------------------------
    # Determine version number
    # -------------------------------------------------------------------------
    
    current_max_version <- DBI::dbGetQuery(con, "
        SELECT COALESCE(MAX(version_number), 0) as max_version
        FROM reference.metadata
    ")$max_version[1]
    
    new_version <- current_max_version + 1
    
    log_message(glue::glue("  New version number: {new_version}"))
    
    # -------------------------------------------------------------------------
    # Write changes to metadata_history
    # -------------------------------------------------------------------------
    
    if (nrow(changes) > 0) {
        log_message("Writing changes to reference.metadata_history...")
        
        history_records <- changes %>%
            dplyr::mutate(
                version_number = new_version,
                changed_at = Sys.time()
            )
        
        DBI::dbWriteTable(
            con,
            DBI::Id(schema = "reference", table = "metadata_history"),
            history_records,
            append = TRUE,
            row.names = FALSE
        )
        
        log_message(glue::glue("  Wrote {nrow(history_records)} history records"))
    }
    
    # -------------------------------------------------------------------------
    # Upsert metadata table
    # -------------------------------------------------------------------------
    
    log_message("Upserting reference.metadata...")
    
    # Prepare new_dict for insertion
    new_dict <- new_dict %>%
        dplyr::mutate(
            version_number = new_version,
            is_active = TRUE,
            updated_at = Sys.time()
        )
    
    # Get keys for different operations
    new_keys <- paste(new_dict$lake_table_name, new_dict$lake_variable_name, new_dict$source_type, sep = "|")
    
    if (nrow(current_dict) > 0) {
        current_keys <- paste(current_dict$lake_table_name, current_dict$lake_variable_name, current_dict$source_type, sep = "|")
        
        # Soft-delete removed variables
        removed_keys <- setdiff(current_keys, new_keys)
        if (length(removed_keys) > 0) {
            log_message(glue::glue("  Soft-deleting {length(removed_keys)} removed variables..."))
            
            for (key in removed_keys) {
                parts <- strsplit(key, "\\|")[[1]]
                DBI::dbExecute(con, glue::glue_sql("
                    UPDATE reference.metadata
                    SET is_active = FALSE, updated_at = NOW(), version_number = {new_version}
                    WHERE lake_table_name = {parts[1]}
                      AND lake_variable_name = {parts[2]}
                      AND source_type = {parts[3]}
                ", .con = con))
            }
        }
    }
    
    # Upsert all variables from new dictionary
    # Using a temp table approach for efficiency
    
    temp_table <- paste0("temp_metadata_", format(Sys.time(), "%Y%m%d%H%M%S"))
    
    DBI::dbWriteTable(
        con,
        DBI::Id(schema = "reference", table = temp_table),
        new_dict,
        temporary = TRUE,
        row.names = FALSE
    )
    
    # Perform upsert
    upsert_sql <- glue::glue_sql("
        INSERT INTO reference.metadata (
            lake_table_name, lake_variable_name, source_type, source_variable_name,
            data_type, variable_label, variable_definition, value_labels,
            variable_unit, valid_min, valid_max, allowed_values,
            is_identifier, is_phi, is_required,
            validated_table_target, validated_variable_name,
            notes, needs_further_review,
            version_number, is_active, updated_at
        )
        SELECT 
            lake_table_name, lake_variable_name, source_type, source_variable_name,
            data_type, variable_label, variable_definition, value_labels,
            variable_unit, valid_min, valid_max, allowed_values,
            is_identifier, is_phi, is_required,
            validated_table_target, validated_variable_name,
            notes, needs_further_review,
            version_number, is_active, updated_at
        FROM reference.{`temp_table`}
        ON CONFLICT (lake_table_name, lake_variable_name, source_type)
        DO UPDATE SET
            source_variable_name = EXCLUDED.source_variable_name,
            data_type = EXCLUDED.data_type,
            variable_label = EXCLUDED.variable_label,
            variable_definition = EXCLUDED.variable_definition,
            value_labels = EXCLUDED.value_labels,
            variable_unit = EXCLUDED.variable_unit,
            valid_min = EXCLUDED.valid_min,
            valid_max = EXCLUDED.valid_max,
            allowed_values = EXCLUDED.allowed_values,
            is_identifier = EXCLUDED.is_identifier,
            is_phi = EXCLUDED.is_phi,
            is_required = EXCLUDED.is_required,
            validated_table_target = EXCLUDED.validated_table_target,
            validated_variable_name = EXCLUDED.validated_variable_name,
            notes = EXCLUDED.notes,
            needs_further_review = EXCLUDED.needs_further_review,
            version_number = EXCLUDED.version_number,
            is_active = EXCLUDED.is_active,
            updated_at = EXCLUDED.updated_at
    ", .con = con)
    
    DBI::dbExecute(con, upsert_sql)
    
    log_message(glue::glue("  Upserted {nrow(new_dict)} variables"))
    
    # -------------------------------------------------------------------------
    # Write audit log event
    # -------------------------------------------------------------------------
    
    log_message("Writing audit log event...")
    
    write_audit_event(
        con = con,
        event_type = "metadata_sync",
        event_description = glue::glue(
            "Metadata sync v{new_version}: {n_adds + n_initial} adds, {n_updates} updates, {n_removes} removes"
        ),
        source_id = source_type_filter %||% "ALL",
        details = list(
            version_number = new_version,
            dict_path = dict_path,
            total_variables = nrow(new_dict),
            adds = n_adds + n_initial,
            updates = n_updates,
            removes = n_removes
        )
    )
    
    # -------------------------------------------------------------------------
    # Return summary
    # -------------------------------------------------------------------------
    
    log_message("=" %>% strrep(70))
    log_message(glue::glue("METADATA SYNC COMPLETE - Version {new_version}"))
    log_message("=" %>% strrep(70))
    
    return(list(
        version_number = new_version,
        total_variables = nrow(new_dict),
        adds = n_adds + n_initial,
        updates = n_updates,
        removes = n_removes,
        total_changes = nrow(changes)
    ))
}
```

---

### Function 4: get_current_metadata_version()

**File:** `r/reference/get_current_metadata_version.R`

**Purpose:** Simple helper to get the current metadata version number.

```r
# =============================================================================
# get_current_metadata_version.R
# Purpose: Get current metadata version number from database
# Author: Noel
# =============================================================================

#' Get Current Metadata Version
#'
#' Returns the current (maximum) version number from reference.metadata.
#' Returns 0 if table is empty.
#'
#' @param con DBI connection to PULSE database
#' @return Integer version number
#' @export
get_current_metadata_version <- function(con) {
    result <- DBI::dbGetQuery(con, "
        SELECT COALESCE(MAX(version_number), 0) as version
        FROM reference.metadata
    ")
    return(result$version[1])
}
```

---

## User Script

**File:** `r/scripts/4_sync_metadata.R`

```r
# =============================================================================
# 4_sync_metadata.R — Synchronize Metadata Dictionary
# =============================================================================
# Purpose: Sync the core metadata dictionary from Excel to the database.
#          Run this before data profiling (Step 5) to ensure metadata is current.
#
# What this script does:
#   1. Loads CURRENT_core_metadata_dictionary.xlsx
#   2. Compares to current reference.metadata table
#   3. Detects field-level changes (adds, updates, removes)
#   4. Writes changes to reference.metadata_history
#   5. Upserts reference.metadata with new version number
#   6. Logs audit event
#
# Author: Noel
# =============================================================================

# ┌───────────────────────────────────────────────────────────────────────────┐
# │ USER INPUT SECTION                                                        │
# └───────────────────────────────────────────────────────────────────────────┘

# Path to the metadata dictionary Excel file
dict_path <- "reference/CURRENT_core_metadata_dictionary.xlsx"

# Optional: Filter to a specific source type (set to NULL for all sources)
# Options: "CISIR", "CLARITY", "TRAUMA_REGISTRY", or NULL
source_type_filter <- NULL

# ┌───────────────────────────────────────────────────────────────────────────┐
# │ EXECUTION (do not modify below this line)                                 │
# └───────────────────────────────────────────────────────────────────────────┘

# Initialize pipeline
source("pulse-init-all.R")

# Connect to database
con <- connect_to_pulse()

# Source the sync function
source("r/reference/sync_metadata.R")

# Execute sync
result <- sync_metadata(
    con = con,
    dict_path = dict_path,
    source_type_filter = source_type_filter
)

# Print summary
cat("\n")
cat("═══════════════════════════════════════════════════════════════════════\n")
cat("                    METADATA SYNC SUMMARY                              \n")
cat("═══════════════════════════════════════════════════════════════════════\n")
cat(glue::glue("
  Version:          {result$version_number}
  Total Variables:  {result$total_variables}
  
  Changes:
    Adds:           {result$adds}
    Updates:        {result$updates}
    Removes:        {result$removes}
    ─────────────────
    Total:          {result$total_changes}
"))
cat("\n═══════════════════════════════════════════════════════════════════════\n")

# Disconnect
DBI::dbDisconnect(con)

cat("\nMetadata sync complete. Run Step 5 to profile data.\n")
```

---

## Implementation Order

Complete these tasks in sequence:

### Task 4.1: Create DDL files

1. Create `sql/ddl/alter_METADATA_add_columns.sql`
2. Create `sql/ddl/create_METADATA_HISTORY.sql`
3. Run both DDLs against database

**Verification:**
```sql
-- Check metadata columns
SELECT column_name FROM information_schema.columns 
WHERE table_schema = 'reference' AND table_name = 'metadata'
ORDER BY ordinal_position;

-- Check history table exists
SELECT * FROM reference.metadata_history LIMIT 0;
```

### Task 4.2: Create R functions

1. Create `r/reference/load_metadata_dictionary.R`
2. Create `r/utilities/compare_metadata.R`
3. Create `r/reference/sync_metadata.R`
4. Create `r/reference/get_current_metadata_version.R`

### Task 4.3: Create user script

1. Create `r/scripts/4_sync_metadata.R`

### Task 4.4: Add constraint to metadata table

The upsert requires a unique constraint:

```sql
-- Add unique constraint for upsert to work
ALTER TABLE reference.metadata 
ADD CONSTRAINT metadata_unique_key 
UNIQUE (lake_table_name, lake_variable_name, source_type);
```

### Task 4.5: Test

1. Run `4_sync_metadata.R` for first sync (all INITIAL)
2. Verify `reference.metadata` has all variables
3. Verify `reference.metadata_history` has INITIAL records
4. Modify one variable in Excel, re-run sync
5. Verify UPDATE record appears in history

---

## Testing

### Test File: `tests/testthat/test_step4_metadata_sync.R`

```r
# =============================================================================
# test_step4_metadata_sync.R
# Tests for Step 4: Metadata Synchronization
# =============================================================================

library(testthat)

# Source functions
source("r/reference/load_metadata_dictionary.R")
source("r/utilities/compare_metadata.R")

test_that("load_metadata_dictionary loads Excel correctly", {
    dict <- load_metadata_dictionary("reference/CURRENT_core_metadata_dictionary.xlsx")
    
    expect_true(nrow(dict) > 0)
    expect_true("lake_table_name" %in% names(dict))
    expect_true("lake_variable_name" %in% names(dict))
    expect_true("data_type" %in% names(dict))
})

test_that("load_metadata_dictionary standardizes Y/N fields", {
    dict <- load_metadata_dictionary("reference/CURRENT_core_metadata_dictionary.xlsx")
    
    expect_true(all(dict$is_identifier %in% c("Y", "N")))
    expect_true(all(dict$is_phi %in% c("Y", "N")))
    expect_true(all(dict$is_required %in% c("Y", "N")))
})

test_that("compare_metadata detects INITIAL on empty current", {
    new_dict <- tibble::tibble(
        lake_table_name = "test_table",
        lake_variable_name = "test_var",
        source_type = "TEST",
        data_type = "text",
        source_variable_name = "test",
        variable_label = "Test",
        variable_definition = NA,
        value_labels = NA,
        variable_unit = NA,
        valid_min = NA,
        valid_max = NA,
        allowed_values = NA,
        is_identifier = "N",
        is_phi = "N",
        is_required = "N",
        validated_table_target = NA,
        validated_variable_name = NA,
        notes = NA,
        needs_further_review = NA
    )
    
    current_dict <- tibble::tibble()
    
    changes <- compare_metadata(new_dict, current_dict)
    
    expect_true(all(changes$change_type == "INITIAL"))
})

test_that("compare_metadata detects ADD", {
    new_dict <- tibble::tibble(
        lake_table_name = c("test_table", "test_table"),
        lake_variable_name = c("var1", "var2"),
        source_type = c("TEST", "TEST"),
        data_type = c("text", "integer"),
        source_variable_name = c("v1", "v2"),
        variable_label = c("Var 1", "Var 2"),
        variable_definition = c(NA, NA),
        value_labels = c(NA, NA),
        variable_unit = c(NA, NA),
        valid_min = c(NA, NA),
        valid_max = c(NA, NA),
        allowed_values = c(NA, NA),
        is_identifier = c("N", "N"),
        is_phi = c("N", "N"),
        is_required = c("N", "N"),
        validated_table_target = c(NA, NA),
        validated_variable_name = c(NA, NA),
        notes = c(NA, NA),
        needs_further_review = c(NA, NA)
    )
    
    current_dict <- new_dict[1, ]  # Only first row exists
    
    changes <- compare_metadata(new_dict, current_dict)
    
    add_changes <- changes %>% dplyr::filter(change_type == "ADD")
    expect_true(nrow(add_changes) > 0)
    expect_true(all(add_changes$lake_variable_name == "var2"))
})

test_that("compare_metadata detects UPDATE", {
    base_dict <- tibble::tibble(
        lake_table_name = "test_table",
        lake_variable_name = "var1",
        source_type = "TEST",
        data_type = "text",
        source_variable_name = "v1",
        variable_label = "Original Label",
        variable_definition = NA,
        value_labels = NA,
        variable_unit = NA,
        valid_min = NA,
        valid_max = NA,
        allowed_values = NA,
        is_identifier = "N",
        is_phi = "N",
        is_required = "N",
        validated_table_target = NA,
        validated_variable_name = NA,
        notes = NA,
        needs_further_review = NA
    )
    
    new_dict <- base_dict
    new_dict$variable_label <- "Updated Label"
    
    changes <- compare_metadata(new_dict, base_dict)
    
    update_changes <- changes %>% dplyr::filter(change_type == "UPDATE")
    expect_true(nrow(update_changes) == 1)
    expect_equal(update_changes$field_changed[1], "variable_label")
    expect_equal(update_changes$old_value[1], "Original Label")
    expect_equal(update_changes$new_value[1], "Updated Label")
})

test_that("compare_metadata detects REMOVE", {
    current_dict <- tibble::tibble(
        lake_table_name = c("test_table", "test_table"),
        lake_variable_name = c("var1", "var2"),
        source_type = c("TEST", "TEST"),
        data_type = c("text", "integer"),
        source_variable_name = c("v1", "v2"),
        variable_label = c("Var 1", "Var 2"),
        variable_definition = c(NA, NA),
        value_labels = c(NA, NA),
        variable_unit = c(NA, NA),
        valid_min = c(NA, NA),
        valid_max = c(NA, NA),
        allowed_values = c(NA, NA),
        is_identifier = c("N", "N"),
        is_phi = c("N", "N"),
        is_required = c("N", "N"),
        validated_table_target = c(NA, NA),
        validated_variable_name = c(NA, NA),
        notes = c(NA, NA),
        needs_further_review = c(NA, NA)
    )
    
    new_dict <- current_dict[1, ]  # Remove second row
    
    changes <- compare_metadata(new_dict, current_dict)
    
    remove_changes <- changes %>% dplyr::filter(change_type == "REMOVE")
    expect_true(nrow(remove_changes) > 0)
    expect_true(all(remove_changes$lake_variable_name == "var2"))
})
```

---

## Acceptance Criteria

Step 4 is complete when:

- [x] `sql/ddl/recreate_METADATA_v2.sql` exists and runs without error (replaced `alter_METADATA_add_columns.sql`)
- [x] `sql/ddl/create_METADATA_HISTORY.sql` exists and runs without error
- [x] `reference.metadata` table has all dictionary columns with composite PK
- [x] `reference.metadata_history` table exists
- [x] `r/reference/load_metadata_dictionary.R` loads Excel correctly
- [x] `r/utilities/compare_metadata.R` detects all change types
- [x] `r/reference/sync_metadata.R` performs full sync with version tracking
- [x] `r/scripts/4_sync_metadata.R` runs successfully
- [x] First sync creates INITIAL records in history
- [x] Subsequent syncs detect ADD/UPDATE/REMOVE correctly
- [x] Audit log event written for each sync
- [x] All tests in `test_step4_metadata_sync.R` pass (52 PASS, 0 FAIL)

---

## Next Step

After Step 4 is complete, proceed to `CLAUDE_STEP5_DATA_PROFILING.md`.
