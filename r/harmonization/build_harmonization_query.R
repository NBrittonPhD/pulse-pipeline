# =============================================================================
# build_harmonization_query
# =============================================================================
# Purpose:      Build a SQL SELECT statement that transforms one staging table
#               into the format expected by a validated table.
#
#               Given a set of column mappings for a single (source_type,
#               source_table) pair, this function generates a SELECT that:
#                 - Adds governance constants (source_type, source_table, ingest_id)
#                 - Maps each source column to its target column name
#                 - Handles different transform types:
#                     direct:     source_col AS target_col (same name)
#                     rename:     source_col AS target_col (different name)
#                     expression: (SQL expression) AS target_col
#                     constant:   'literal value' AS target_col
#                     coalesce:   (COALESCE expression) AS target_col
#                 - Verifies source columns exist in the staging table
#                 - Uses NULL for missing source columns (with a warning)
#
#               The caller (harmonize_table) wraps this SELECT with an
#               INSERT INTO validated.{target_table}.
#
# Inputs:
#   - con:          DBI connection object (for identifier quoting)
#   - mappings:     tibble: column mappings for one (source_type, source_table)
#   - source_table: character: staging table name
#   - source_type:  character: CISIR, CLARITY, or TRAUMA_REGISTRY
#   - ingest_id:    character: batch identifier for tagging
#
# Outputs:      Character string: SQL SELECT statement
#
# Dependencies:
#   - DBI, glue
#
# Author:       Noel
# Last Updated: 2026-02-04
# =============================================================================

# =============================================================================
# LOAD REQUIRED PACKAGES
# =============================================================================
library(DBI)
library(glue)

# =============================================================================
# FUNCTION DEFINITION
# =============================================================================
build_harmonization_query <- function(con, mappings, source_table, source_type, ingest_id) {

    # =========================================================================
    # INPUT VALIDATION
    # =========================================================================
    if (!inherits(con, "DBIConnection")) {
        stop("[build_harmonization_query] ERROR: 'con' must be a valid DBI connection object.")
    }
    if (is.null(mappings) || nrow(mappings) == 0) {
        stop("[build_harmonization_query] ERROR: mappings must be a non-empty data frame.")
    }
    if (is.null(source_table) || !nzchar(source_table)) {
        stop("[build_harmonization_query] ERROR: source_table must be a non-empty string.")
    }
    if (is.null(ingest_id) || !nzchar(ingest_id)) {
        stop("[build_harmonization_query] ERROR: ingest_id must be a non-empty string.")
    }

    # =========================================================================
    # VERIFY SOURCE COLUMNS EXIST IN STAGING TABLE
    # =========================================================================
    # Query information_schema to get the actual column names in the staging
    # table. This prevents the entire harmonization from failing due to a
    # single missing or renamed column.
    # =========================================================================

    staging_col_info <- DBI::dbGetQuery(con, glue("
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'staging'
          AND table_name = '{source_table}'
    "))
    staging_cols <- staging_col_info$column_name

    if (length(staging_cols) == 0) {
        stop(glue("[build_harmonization_query] ERROR: staging.{source_table} has no columns ",
                  "or does not exist in information_schema."))
    }

    # Build a lookup of staging column name → data_type for cast detection
    staging_type_lookup <- setNames(staging_col_info$data_type, staging_col_info$column_name)

    # Look up target column types from the validated table so we can add
    # CASTs when the staging type doesn't match (e.g., TEXT → NUMERIC).
    target_tbl_name <- mappings$target_table[1]
    target_col_info <- DBI::dbGetQuery(con, glue("
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'validated'
          AND table_name = '{target_tbl_name}'
    "))
    target_type_lookup <- setNames(target_col_info$data_type, target_col_info$column_name)

    # =========================================================================
    # BUILD SELECT EXPRESSIONS
    # =========================================================================

    # -----------------------------------------------------------------
    # Governance constants: every validated row gets tagged with its
    # source provenance and batch lineage.
    # -----------------------------------------------------------------
    governance_exprs <- c(
        glue("'{source_type}' AS source_type"),
        glue("'{source_table}' AS source_table"),
        glue("'{ingest_id}' AS ingest_id")
    )

    # -----------------------------------------------------------------
    # Column mapping expressions: one per mapping row. Each row defines
    # how a staging column transforms into a validated column.
    # -----------------------------------------------------------------
    col_exprs <- character(nrow(mappings))
    missing_cols <- character(0)

    for (i in seq_len(nrow(mappings))) {
        m <- mappings[i, ]
        target_quoted <- DBI::dbQuoteIdentifier(con, m$target_column)

        if (m$transform_type %in% c("direct", "rename")) {
            # For direct and rename, SELECT the source column AS the target name.
            # If the source column doesn't exist in staging, use NULL instead
            # and log a warning. If the staging type differs from the validated
            # type, wrap in a CAST to prevent implicit conversion errors.
            if (m$source_column %in% staging_cols) {
                source_quoted <- DBI::dbQuoteIdentifier(con, m$source_column)

                # Check if we need a CAST: compare staging vs validated types
                src_type <- staging_type_lookup[[m$source_column]]
                tgt_type <- target_type_lookup[[m$target_column]]

                if (!is.null(src_type) && !is.null(tgt_type) && src_type != tgt_type) {
                    # Map Postgres data_type to a SQL CAST type
                    cast_type <- switch(tgt_type,
                        "integer"                      = "INTEGER",
                        "bigint"                       = "BIGINT",
                        "smallint"                     = "SMALLINT",
                        "numeric"                      = "NUMERIC",
                        "double precision"             = "DOUBLE PRECISION",
                        "real"                         = "REAL",
                        "boolean"                      = "BOOLEAN",
                        "date"                         = "DATE",
                        "time without time zone"       = "TIME",
                        "timestamp without time zone"  = "TIMESTAMP",
                        "timestamp with time zone"     = "TIMESTAMPTZ",
                        "text"                         = "TEXT",
                        "character varying"            = "TEXT",
                        NULL
                    )

                    if (!is.null(cast_type)) {
                        # For numeric casts from text, use a safe pattern that
                        # returns NULL for non-numeric values (e.g., "Negative",
                        # "<0.02", "0-2") instead of failing the entire INSERT.
                        numeric_types <- c("INTEGER", "BIGINT", "SMALLINT",
                                           "NUMERIC", "DOUBLE PRECISION", "REAL")
                        is_text_source <- src_type %in% c("text", "character varying")

                        if (is_text_source && cast_type %in% numeric_types) {
                            col_exprs[i] <- glue(
                                "CASE WHEN {source_quoted} ~ '^-?[0-9]*\\.?[0-9]+([eE][+-]?[0-9]+)?$' ",
                                "THEN CAST({source_quoted} AS {cast_type}) ELSE NULL END AS {target_quoted}"
                            )
                        } else {
                            col_exprs[i] <- glue("CAST({source_quoted} AS {cast_type}) AS {target_quoted}")
                        }
                    } else {
                        col_exprs[i] <- glue("{source_quoted} AS {target_quoted}")
                    }
                } else {
                    col_exprs[i] <- glue("{source_quoted} AS {target_quoted}")
                }
            } else {
                missing_cols <- c(missing_cols, m$source_column)
                col_exprs[i] <- glue("NULL AS {target_quoted}")
            }

        } else if (m$transform_type == "expression") {
            # For expression mappings, use the SQL expression directly.
            # The expression is assumed to be valid SQL referencing staging columns.
            expr <- m$transform_expression
            if (is.na(expr) || !nzchar(trimws(expr))) {
                message(glue("[build_harmonization_query] WARNING: expression mapping for ",
                             "{m$target_column} has no transform_expression. Using NULL."))
                col_exprs[i] <- glue("NULL AS {target_quoted}")
            } else {
                col_exprs[i] <- glue("({expr}) AS {target_quoted}")
            }

        } else if (m$transform_type == "constant") {
            # For constant mappings, inject a literal value.
            val <- m$transform_expression
            if (is.na(val)) {
                col_exprs[i] <- glue("NULL AS {target_quoted}")
            } else {
                # Escape single quotes in the constant value
                val_escaped <- gsub("'", "''", val)
                col_exprs[i] <- glue("'{val_escaped}' AS {target_quoted}")
            }

        } else if (m$transform_type == "coalesce") {
            # For coalesce mappings, use the expression (which should be a
            # COALESCE(...) call referencing multiple staging columns).
            expr <- m$transform_expression
            if (is.na(expr) || !nzchar(trimws(expr))) {
                message(glue("[build_harmonization_query] WARNING: coalesce mapping for ",
                             "{m$target_column} has no transform_expression. Using NULL."))
                col_exprs[i] <- glue("NULL AS {target_quoted}")
            } else {
                col_exprs[i] <- glue("({expr}) AS {target_quoted}")
            }

        } else {
            # Unknown transform type — use NULL and warn
            message(glue("[build_harmonization_query] WARNING: Unknown transform_type ",
                         "'{m$transform_type}' for {m$source_column} → {m$target_column}. Using NULL."))
            col_exprs[i] <- glue("NULL AS {target_quoted}")
        }
    }

    # Log warnings for missing columns
    if (length(missing_cols) > 0) {
        message(glue("[build_harmonization_query] WARNING: {length(missing_cols)} source column(s) ",
                     "not found in staging.{source_table}: ",
                     "{paste(missing_cols, collapse = ', ')}. Using NULL."))
    }

    # =========================================================================
    # ASSEMBLE THE SELECT STATEMENT
    # =========================================================================
    all_exprs <- c(governance_exprs, col_exprs)
    select_clause <- paste(all_exprs, collapse = ",\n    ")

    # Quote the staging table name for safety (handles special characters)
    table_quoted <- DBI::dbQuoteIdentifier(con, DBI::Id(schema = "staging", table = source_table))

    query <- glue("SELECT\n    {select_clause}\nFROM {table_quoted}")

    return(query)
}
