# =============================================================================
# sync_harmonization_map
# =============================================================================
# Purpose:      Synchronize harmonization mappings from the metadata dictionary
#               (reference.metadata) to the reference.harmonization_map table.
#
#               This function is the critical bridge between the metadata
#               dictionary (CURRENT_core_metadata_dictionary.xlsx, synced to
#               reference.metadata) and the harmonization engine. It reads
#               the validated_table_target and validated_variable_name columns
#               from reference.metadata and materializes them as explicit
#               column-level mappings in reference.harmonization_map.
#
#               Specifically:
#                 1. Queries reference.metadata for rows with validated targets
#                 2. Expands comma-separated validated_table_target values
#                    (shared identifiers like account_number map to many tables)
#                 3. Maps: lake_table_name → source_table,
#                          lake_variable_name → source_column,
#                          validated_table_target → target_table,
#                          validated_variable_name → target_column
#                 4. Auto-detects transform_type:
#                    'direct' if source_column == target_column,
#                    'rename' if they differ
#                 5. Upserts to reference.harmonization_map, preserving any
#                    manually curated expression/coalesce/constant overrides
#
# Inputs:
#   - con:    DBI connection object (required)
#   - source: character: "metadata" (default) to read from reference.metadata,
#             or a file path to an Excel harmonization map override
#
# Outputs:    Named list with:
#               count          - total mappings synced
#               tables_mapped  - distinct validated target tables
#               sources_mapped - distinct source types
#
# Side Effects:
#   - Writes to reference.harmonization_map (upsert)
#
# Dependencies:
#   - DBI, dplyr, glue, stringr, tidyr
#
# Author:       Noel
# Last Updated: 2026-02-04
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(DBI)
library(dplyr)
library(glue)
library(stringr)
library(tidyr)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
sync_harmonization_map <- function(con, source = "metadata") {

    message("=================================================================")
    message("[sync_harmonization_map] SYNC HARMONIZATION MAPPINGS")
    message("=================================================================")

    # =========================================================================
    # INPUT VALIDATION
    # =========================================================================
    if (!inherits(con, "DBIConnection")) {
        stop("[sync_harmonization_map] ERROR: 'con' must be a valid DBI connection object.")
    }
    if (!DBI::dbIsValid(con)) {
        stop("[sync_harmonization_map] ERROR: Database connection is not valid.")
    }
    if (is.null(source) || !nzchar(source)) {
        stop("[sync_harmonization_map] ERROR: 'source' must be 'metadata' or a file path.")
    }

    # =========================================================================
    # LOAD MAPPINGS FROM SOURCE
    # =========================================================================

    if (source == "metadata") {

        # -----------------------------------------------------------------
        # Read from reference.metadata — the authoritative source of truth.
        # The validated_table_target and validated_variable_name columns
        # are populated from CURRENT_core_metadata_dictionary.xlsx via
        # sync_metadata() in Step 4.
        # -----------------------------------------------------------------
        message("[sync_harmonization_map] Reading from reference.metadata...")

        meta <- DBI::dbGetQuery(con, "
            SELECT source_type,
                   lake_table_name,
                   lake_variable_name,
                   validated_table_target,
                   validated_variable_name
            FROM reference.metadata
            WHERE is_active = TRUE
              AND validated_table_target IS NOT NULL
              AND TRIM(validated_table_target) <> ''
              AND validated_variable_name IS NOT NULL
              AND TRIM(validated_variable_name) <> ''
        ")

        if (nrow(meta) == 0) {
            stop("[sync_harmonization_map] ERROR: No rows in reference.metadata ",
                 "have validated_table_target set. Run sync_metadata() first.")
        }

        message(glue("[sync_harmonization_map] Metadata rows with validated mappings: {nrow(meta)}"))

        # -----------------------------------------------------------------
        # Expand comma-separated validated_table_target values.
        # Shared identifiers like account_number and mrn map to multiple
        # validated tables (e.g., "admission, demographics, labs, vitals").
        # We expand each comma-separated entry into its own row.
        #
        # This uses the same logic as generate_validated_ddls.R (lines 100-104).
        # -----------------------------------------------------------------
        meta_expanded <- meta %>%
            mutate(validated_table_target = str_trim(validated_table_target)) %>%
            separate_rows(validated_table_target, sep = "\\s*,\\s*") %>%
            mutate(validated_table_target = str_trim(validated_table_target)) %>%
            filter(validated_table_target != "")

        message(glue("[sync_harmonization_map] After expanding multi-target rows: {nrow(meta_expanded)}"))

        # -----------------------------------------------------------------
        # Build the harmonization map dataframe.
        # Map metadata columns to harmonization_map columns and auto-detect
        # the transform_type: 'direct' if source and target column names
        # match (case-insensitive), 'rename' if they differ.
        # -----------------------------------------------------------------
        map_df <- meta_expanded %>%
            transmute(
                source_type      = source_type,
                source_table     = lake_table_name,
                source_column    = lake_variable_name,
                target_table     = validated_table_target,
                target_column    = validated_variable_name,
                transform_type   = ifelse(
                    tolower(trimws(lake_variable_name)) == tolower(trimws(validated_variable_name)),
                    "direct",
                    "rename"
                ),
                transform_expression = NA_character_,
                is_active        = TRUE,
                priority         = 100L,
                notes            = NA_character_
            )

    } else {

        # -----------------------------------------------------------------
        # Read from an Excel file override (future use).
        # -----------------------------------------------------------------
        if (!file.exists(source)) {
            stop(glue("[sync_harmonization_map] ERROR: File not found: {source}"))
        }

        message(glue("[sync_harmonization_map] Reading from Excel: {source}"))
        map_df <- readxl::read_excel(source) %>%
            mutate(
                is_active = ifelse(is.na(is_active), TRUE, as.logical(is_active)),
                priority  = ifelse(is.na(priority), 100L, as.integer(priority))
            )
    }

    # =========================================================================
    # UPSERT TO reference.harmonization_map
    # =========================================================================
    # Uses the temp-table + INSERT ON CONFLICT pattern (same approach as
    # sync_metadata.R). On conflict, we update transform_type ONLY for
    # direct/rename mappings — manually curated expression/coalesce/constant
    # overrides are preserved.
    # =========================================================================

    message(glue("[sync_harmonization_map] Upserting {nrow(map_df)} mappings..."))

    # Write to a temporary staging table
    temp_table <- paste0("temp_harm_map_", format(Sys.time(), "%Y%m%d%H%M%S"))

    DBI::dbWriteTable(
        con,
        temp_table,
        map_df,
        temporary = TRUE,
        row.names = FALSE,
        overwrite = TRUE
    )

    # Upsert: insert new mappings, update existing direct/rename mappings,
    # but preserve manually curated expression/coalesce/constant overrides.
    upsert_sql <- glue("
        INSERT INTO reference.harmonization_map (
            source_type, source_table, source_column,
            target_table, target_column,
            transform_type, transform_expression,
            is_active, priority, notes,
            created_at, updated_at
        )
        SELECT
            source_type, source_table, source_column,
            target_table, target_column,
            transform_type, transform_expression,
            is_active, priority, notes,
            NOW(), NOW()
        FROM \"{temp_table}\"
        ON CONFLICT (source_type, source_table, source_column, target_table, target_column)
        DO UPDATE SET
            transform_type = CASE
                WHEN reference.harmonization_map.transform_type IN ('expression', 'constant', 'coalesce')
                THEN reference.harmonization_map.transform_type
                ELSE EXCLUDED.transform_type
            END,
            transform_expression = CASE
                WHEN reference.harmonization_map.transform_type IN ('expression', 'constant', 'coalesce')
                THEN reference.harmonization_map.transform_expression
                ELSE EXCLUDED.transform_expression
            END,
            is_active  = EXCLUDED.is_active,
            updated_at = NOW()
    ")

    rows_affected <- DBI::dbExecute(con, upsert_sql)

    # Clean up temp table
    DBI::dbExecute(con, glue("DROP TABLE IF EXISTS \"{temp_table}\""))

    message(glue("[sync_harmonization_map] Upsert complete. Rows affected: {rows_affected}"))

    # =========================================================================
    # SUMMARY
    # =========================================================================
    result <- list(
        count          = nrow(map_df),
        tables_mapped  = n_distinct(map_df$target_table),
        sources_mapped = n_distinct(map_df$source_type)
    )

    message("=================================================================")
    message(glue("[sync_harmonization_map] Mappings synced:  {result$count}"))
    message(glue("[sync_harmonization_map] Target tables:    {result$tables_mapped}"))
    message(glue("[sync_harmonization_map] Source types:     {result$sources_mapped}"))
    message("=================================================================")

    return(result)
}
