# =============================================================================
# sync_metadata
# =============================================================================
# Purpose:      Synchronize the core metadata dictionary from Excel to the
#               database with full version tracking and field-level audit trail.
#
#               This function:
#                 1. Loads the dictionary from Excel via load_metadata_dictionary()
#                 2. Queries current reference.metadata for active variables
#                 3. Compares via compare_metadata() to detect field-level changes
#                 4. Determines the next version number
#                 5. Writes all changes to reference.metadata_history
#                 6. Upserts reference.metadata (INSERT new, UPDATE existing,
#                    soft-delete removed)
#                 7. Writes audit event to governance.audit_log
#
# Inputs:
#   - con:                DBI connection object
#   - dict_path:          character path to core metadata dictionary Excel
#   - source_type_filter: character (optional) filter to specific source_type
#
# Outputs:      List with:
#                 version_number, total_variables, adds, updates, removes,
#                 total_changes
#
# Side Effects:
#   - Writes to reference.metadata (upsert)
#   - Writes to reference.metadata_history (append)
#   - Writes to governance.audit_log (append)
#
# Dependencies:
#   - DBI, dplyr, glue, tibble
#   - load_metadata_dictionary() from r/reference/load_metadata_dictionary.R
#   - compare_metadata() from r/utilities/compare_metadata.R
#   - write_audit_event() from r/steps/write_audit_event.R
#
# Author:       Noel
# Last Updated: 2026-01-30
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(DBI)
library(dplyr)
library(glue)
library(tibble)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
sync_metadata <- function(con, dict_path, source_type_filter = NULL) {

    message("=================================================================")
    message("[sync_metadata] STEP 4: METADATA SYNCHRONIZATION")
    message("=================================================================")

    # =========================================================================
    # INPUT VALIDATION
    # =========================================================================
    if (!inherits(con, "DBIConnection")) {
        stop("[sync_metadata] ERROR: 'con' must be a valid DBI connection object.")
    }

    if (!DBI::dbIsValid(con)) {
        stop("[sync_metadata] ERROR: Database connection is not valid.")
    }

    if (is.null(dict_path) || !file.exists(dict_path)) {
        stop(glue("[sync_metadata] ERROR: Dictionary file not found: {dict_path}"))
    }

    # =========================================================================
    # SOURCE DEPENDENCIES
    # =========================================================================
    proj_root <- getOption("pulse.proj_root", default = ".")

    source(file.path(proj_root, "r", "reference", "load_metadata_dictionary.R"))
    source(file.path(proj_root, "r", "utilities", "compare_metadata.R"))
    source(file.path(proj_root, "r", "steps", "write_audit_event.R"))

    # =========================================================================
    # LOAD NEW DICTIONARY FROM EXCEL
    # =========================================================================
    message("[sync_metadata] Loading dictionary from Excel...")

    new_dict <- load_metadata_dictionary(dict_path, source_type_filter)

    # =========================================================================
    # QUERY CURRENT METADATA FROM DATABASE
    # =========================================================================
    message("[sync_metadata] Querying current metadata from database...")

    current_dict <- DBI::dbGetQuery(con, "
        SELECT *
        FROM reference.metadata
        WHERE is_active = TRUE
    ") %>% tibble::as_tibble()

    message(glue("[sync_metadata]   Current database has {nrow(current_dict)} active variables"))

    # =========================================================================
    # COMPARE DICTIONARIES
    # =========================================================================
    message("[sync_metadata] Comparing dictionaries...")

    changes <- compare_metadata(new_dict, current_dict)

    # =========================================================================
    # COUNT CHANGE TYPES
    # =========================================================================
    n_initial <- sum(changes$change_type == "INITIAL")
    n_adds    <- sum(changes$change_type == "ADD")
    n_updates <- sum(changes$change_type == "UPDATE")
    n_removes <- sum(changes$change_type == "REMOVE")

    message(glue(
        "[sync_metadata]   Changes: {n_initial} initial, {n_adds} adds, ",
        "{n_updates} updates, {n_removes} removes"
    ))

    # =========================================================================
    # DETERMINE VERSION NUMBER
    # =========================================================================
    current_max_version <- DBI::dbGetQuery(con, "
        SELECT COALESCE(MAX(version_number), 0) as max_version
        FROM reference.metadata
    ")$max_version[1]

    new_version <- current_max_version + 1L

    message(glue("[sync_metadata]   New version number: {new_version}"))

    # =========================================================================
    # WRITE CHANGES TO METADATA HISTORY
    # =========================================================================
    if (nrow(changes) > 0) {
        message("[sync_metadata] Writing changes to reference.metadata_history...")

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

        message(glue("[sync_metadata]   Wrote {nrow(history_records)} history records"))
    } else {
        message("[sync_metadata]   No changes detected — skipping history write.")
    }

    # =========================================================================
    # UPSERT METADATA TABLE
    # =========================================================================
    message("[sync_metadata] Upserting reference.metadata...")

    # -------------------------------------------------------------------------
    # Prepare new_dict for insertion with governance columns
    # -------------------------------------------------------------------------
    new_dict <- new_dict %>%
        dplyr::mutate(
            version_number = new_version,
            is_active = TRUE,
            updated_at = Sys.time(),
            created_at = Sys.time()
        )

    # -------------------------------------------------------------------------
    # Soft-delete removed variables
    # -------------------------------------------------------------------------
    # Variables in the current DB but not in the new dictionary get
    # is_active = FALSE rather than being physically deleted.
    # -------------------------------------------------------------------------
    if (nrow(current_dict) > 0) {
        new_keys <- paste(new_dict$lake_table_name, new_dict$lake_variable_name,
                          new_dict$source_type, sep = "|")
        current_keys <- paste(current_dict$lake_table_name, current_dict$lake_variable_name,
                              current_dict$source_type, sep = "|")

        removed_keys <- setdiff(current_keys, new_keys)

        if (length(removed_keys) > 0) {
            message(glue("[sync_metadata]   Soft-deleting {length(removed_keys)} removed variables..."))

            for (key in removed_keys) {
                parts <- strsplit(key, "\\|")[[1]]
                DBI::dbExecute(con, glue::glue_sql("
                    UPDATE reference.metadata
                    SET is_active = FALSE,
                        updated_at = NOW(),
                        version_number = {new_version}
                    WHERE lake_table_name = {parts[1]}
                      AND lake_variable_name = {parts[2]}
                      AND source_type = {parts[3]}
                ", .con = con))
            }
        }
    }

    # -------------------------------------------------------------------------
    # Upsert via temp table + INSERT ON CONFLICT
    # -------------------------------------------------------------------------
    # Write new dictionary to a temp table, then upsert into reference.metadata.
    # This is more efficient than row-by-row operations for 1000+ variables.
    # -------------------------------------------------------------------------
    temp_table <- paste0("temp_metadata_", format(Sys.time(), "%Y%m%d%H%M%S"))

    DBI::dbWriteTable(
        con,
        temp_table,
        new_dict,
        temporary = TRUE,
        row.names = FALSE
    )

    upsert_sql <- glue::glue_sql("
        INSERT INTO reference.metadata (
            lake_table_name, lake_variable_name, source_type,
            source_table_name, source_variable_name, data_type,
            variable_label, variable_definition, value_labels,
            variable_unit, valid_min, valid_max, allowed_values,
            is_identifier, is_phi, is_required,
            validated_table_target, validated_variable_name,
            notes, needs_further_review,
            version_number, is_active, created_at, updated_at
        )
        SELECT
            lake_table_name, lake_variable_name, source_type,
            source_table_name, source_variable_name, data_type,
            variable_label, variable_definition, value_labels,
            variable_unit, valid_min, valid_max, allowed_values,
            is_identifier, is_phi, is_required,
            validated_table_target, validated_variable_name,
            notes, needs_further_review,
            version_number, is_active, created_at, updated_at
        FROM {`temp_table`}
        ON CONFLICT (lake_table_name, lake_variable_name, source_type)
        DO UPDATE SET
            source_table_name = EXCLUDED.source_table_name,
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

    message(glue("[sync_metadata]   Upserted {nrow(new_dict)} variables"))

    # =========================================================================
    # WRITE AUDIT LOG EVENT
    # =========================================================================
    message("[sync_metadata] Writing audit log event...")

    write_audit_event(
        con         = con,
        event_type  = "metadata_sync",
        object_type = "table",
        object_name = "reference.metadata",
        status      = "success",
        details     = list(
            version_number  = new_version,
            dict_path       = dict_path,
            source_filter   = source_type_filter %||% "ALL",
            total_variables = nrow(new_dict),
            initial         = n_initial,
            adds            = n_adds,
            updates         = n_updates,
            removes         = n_removes
        )
    )

    # =========================================================================
    # RETURN SUMMARY
    # =========================================================================
    message("=================================================================")
    message(glue("[sync_metadata] METADATA SYNC COMPLETE — Version {new_version}"))
    message("=================================================================")
    message(glue("  Total variables: {nrow(new_dict)}"))
    message(glue("  Initial:         {n_initial}"))
    message(glue("  Adds:            {n_adds}"))
    message(glue("  Updates:         {n_updates}"))
    message(glue("  Removes:         {n_removes}"))
    message(glue("  Total changes:   {nrow(changes)}"))
    message("=================================================================")

    return(list(
        version_number  = new_version,
        total_variables = nrow(new_dict),
        adds            = n_adds + n_initial,
        updates         = n_updates,
        removes         = n_removes,
        total_changes   = nrow(changes),
        rows_synced     = nrow(new_dict)
    ))
}
