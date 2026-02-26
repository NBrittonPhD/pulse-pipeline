# =============================================================================
# generate_validated_ddls.R
# =============================================================================
# Purpose:
#   One-time build tool that generates CREATE TABLE DDL files for the
#   validated.* schema. Reads validated_table_target and validated_variable_name
#   from reference.metadata, resolves proper SQL types from the staging schema
#   (information_schema.columns), and writes one .sql file per validated table.
#
# How It Works:
#   1. Queries reference.metadata for all rows where validated_table_target
#      is not NULL/empty, building a mapping of validated tables → columns
#   2. Handles comma-separated validated_table_target values by expanding
#      shared identifiers (account_number, mrn, etc.) into each listed table
#   3. Resolves column types from staging.* via information_schema.columns
#      (staging already has correct types from the type_decision_table)
#   4. Deduplicates columns per validated table (picks most specific type
#      if multiple sources disagree: NUMERIC > INTEGER > TEXT, etc.)
#   5. Prepends 5 standard governance columns to every table
#   6. Generates DDL SQL matching the project's existing style
#   7. Writes each DDL to sql/ddl/create_VALIDATED_<TABLE_NAME>.sql
#
# HOW TO USE:
#   1. Set working directory to the pipeline root
#   2. Run:
#        source("r/build_tools/generate_validated_ddls.R")
#
# Outputs:
#   - sql/ddl/create_VALIDATED_<TABLE>.sql  (one per validated table)
#   - Console summary of tables and column counts
#
# Dependencies:
#   - pulse-init-all.R (for connect_to_pulse)
#   - reference.metadata must be populated (via sync_metadata)
#   - staging.* tables must exist (via promote_to_staging)
#
# Author:       Noel
# Last Updated: 2026-02-04
# =============================================================================

library(DBI)
library(dplyr)
library(glue)
library(tidyr)
library(stringr)

# --------------------------------------------------------------------------
# Initialize PULSE system
# --------------------------------------------------------------------------
source("pulse-init-all.R")
con <- connect_to_pulse()

message(">> Starting validated DDL generation...")

# --------------------------------------------------------------------------
# 1. Query reference.metadata for validated table/column mappings
# --------------------------------------------------------------------------
# Pull every active row that has a validated_table_target assigned.
# Columns we need:
#   - validated_table_target : target table name(s) — may be comma-separated
#   - validated_variable_name: target column name in validated table
#   - lake_table_name        : staging source table (for type lookup)
#   - lake_variable_name     : staging source column (for type lookup)
#   - source_type            : CISIR / CLARITY / TRAUMA_REGISTRY
#   - target_type            : target SQL type from type_decision_table
#   - is_identifier          : whether this is an identifier column

meta_raw <- DBI::dbGetQuery(con, "
  SELECT validated_table_target,
         validated_variable_name,
         lake_table_name,
         lake_variable_name,
         source_type,
         target_type,
         is_identifier
    FROM reference.metadata
   WHERE is_active = TRUE
     AND validated_table_target IS NOT NULL
     AND TRIM(validated_table_target) <> ''
     AND validated_variable_name IS NOT NULL
     AND TRIM(validated_variable_name) <> ''
   ORDER BY validated_table_target, validated_variable_name
")

message(">> Metadata rows with validated mappings: ", nrow(meta_raw))

if (nrow(meta_raw) == 0) {
  DBI::dbDisconnect(con)
  stop("No rows in reference.metadata have validated_table_target set. ",
       "Run sync_metadata() first.", call. = FALSE)
}

# --------------------------------------------------------------------------
# 2. Expand comma-separated validated_table_target values
# --------------------------------------------------------------------------
# Some identifiers (account_number, mrn, etc.) map to multiple validated
# tables via a comma-separated list in validated_table_target. We expand
# each comma-separated entry into its own row.

meta_expanded <- meta_raw %>%
  mutate(validated_table_target = str_trim(validated_table_target)) %>%
  separate_rows(validated_table_target, sep = "\\s*,\\s*") %>%
  mutate(validated_table_target = str_trim(validated_table_target)) %>%
  filter(validated_table_target != "")

message(">> After expanding multi-target rows: ", nrow(meta_expanded))

# --------------------------------------------------------------------------
# 3. Resolve SQL types from staging information_schema.columns
# --------------------------------------------------------------------------
# Staging tables already have correct types (from type_decision_table +
# promote_to_staging). We look up actual column types from Postgres to
# ensure our DDLs match what's in production.

staging_types <- DBI::dbGetQuery(con, "
  SELECT table_name,
         column_name,
         UPPER(
           CASE
             WHEN data_type = 'character varying' THEN 'VARCHAR(' || character_maximum_length || ')'
             WHEN data_type = 'numeric' AND numeric_precision IS NOT NULL
               THEN 'NUMERIC(' || numeric_precision || ',' || numeric_scale || ')'
             WHEN data_type = 'integer' THEN 'INTEGER'
             WHEN data_type = 'bigint' THEN 'BIGINT'
             WHEN data_type = 'smallint' THEN 'SMALLINT'
             WHEN data_type = 'boolean' THEN 'BOOLEAN'
             WHEN data_type = 'date' THEN 'DATE'
             WHEN data_type = 'time without time zone' THEN 'TIME'
             WHEN data_type = 'time with time zone' THEN 'TIMETZ'
             WHEN data_type = 'timestamp without time zone' THEN 'TIMESTAMP'
             WHEN data_type = 'timestamp with time zone' THEN 'TIMESTAMPTZ'
             WHEN data_type = 'double precision' THEN 'DOUBLE PRECISION'
             WHEN data_type = 'real' THEN 'REAL'
             WHEN data_type = 'text' THEN 'TEXT'
             ELSE UPPER(data_type)
           END
         ) AS sql_type
    FROM information_schema.columns
   WHERE table_schema = 'staging'
   ORDER BY table_name, ordinal_position
")

message(">> Staging columns catalogued: ", nrow(staging_types))

# Build a lookup: key = "table_name.column_name" → sql_type
staging_type_lookup <- staging_types %>%
  mutate(lookup_key = paste0(table_name, ".", column_name)) %>%
  select(lookup_key, sql_type)

# --------------------------------------------------------------------------
# 4. Resolve type for each validated column
# --------------------------------------------------------------------------
# For each validated column, look up the type from its staging source.
# If the metadata has a target_type, we use that as a fallback.
# If multiple sources contribute the same validated column, we pick
# the most specific type using a type-specificity ranking.

type_specificity <- c(
  "TIMESTAMPTZ" = 10, "TIMESTAMP" = 9, "DATE" = 8, "TIMETZ" = 7, "TIME" = 6,
  "DOUBLE PRECISION" = 5, "REAL" = 5, "NUMERIC" = 4, "BIGINT" = 3,
  "INTEGER" = 2, "SMALLINT" = 1, "BOOLEAN" = 0, "TEXT" = -1
)

resolve_type <- function(types) {
  # Given a vector of SQL types from different sources for the same
  # validated column, pick the most specific (highest ranking) non-TEXT type.
  # Falls back to TEXT if all are TEXT or unranked.
  types <- types[!is.na(types) & types != ""]
  if (length(types) == 0) return("TEXT")

  # Normalize for matching: strip precision/length suffixes for ranking
  base_types <- str_extract(types, "^[A-Z ]+") %>% str_trim()

  scores <- vapply(base_types, function(bt) {
    if (bt %in% names(type_specificity)) type_specificity[[bt]] else -1L
  }, numeric(1))

  # Return the original type string (with precision) for the best match
  types[which.max(scores)]
}

# Join metadata with staging types to get the resolved SQL type
meta_typed <- meta_expanded %>%
  mutate(lookup_key = paste0(lake_table_name, ".", lake_variable_name)) %>%
  left_join(staging_type_lookup, by = "lookup_key") %>%
  mutate(
    # Use staging type if available, fall back to metadata target_type, then TEXT
    resolved_type = case_when(
      !is.na(sql_type) & sql_type != "" ~ sql_type,
      !is.na(target_type) & target_type != "" ~ toupper(trimws(target_type)),
      TRUE ~ "TEXT"
    )
  )

# --------------------------------------------------------------------------
# 5. Build per-table column definitions
# --------------------------------------------------------------------------
# Group by validated table + column, deduplicate by picking the most
# specific type across sources, and collect source_type info.

validated_columns <- meta_typed %>%
  group_by(validated_table_target, validated_variable_name) %>%
  summarise(
    resolved_type = resolve_type(resolved_type),
    sources       = paste(sort(unique(source_type)), collapse = ", "),
    is_identifier = any(is_identifier, na.rm = TRUE),
    .groups       = "drop"
  ) %>%
  arrange(validated_table_target, validated_variable_name)

# Get the list of validated tables and their source types
validated_tables <- meta_typed %>%
  group_by(validated_table_target) %>%
  summarise(
    sources   = paste(sort(unique(source_type)), collapse = ", "),
    n_columns = n_distinct(validated_variable_name),
    .groups   = "drop"
  ) %>%
  arrange(validated_table_target)

message("\n>> Validated tables to generate: ", nrow(validated_tables))
for (i in seq_len(nrow(validated_tables))) {
  row <- validated_tables[i, ]
  message("   ", sprintf("%-3d", i), " ", row$validated_table_target,
          " (", row$n_columns, " domain cols, sources: ", row$sources, ")")
}

# --------------------------------------------------------------------------
# 6. Define governance columns (prepended to every table)
# --------------------------------------------------------------------------
governance_cols <- tibble::tribble(
  ~col_name,     ~col_type,                          ~col_extra,
  "validated_id", "SERIAL",                           "PRIMARY KEY",
  "source_type",  "TEXT",                             "NOT NULL",
  "source_table", "TEXT",                             "NOT NULL",
  "ingest_id",    "TEXT",                             "NOT NULL",
  "created_at",   "TIMESTAMPTZ",                      "NOT NULL DEFAULT NOW()"
)

# --------------------------------------------------------------------------
# 7. Generate and write DDL files
# --------------------------------------------------------------------------
output_dir <- "sql/ddl"
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

# Identifier columns that get indexes when they appear in a table
indexable_identifiers <- c("account_number", "mrn", "trauma_number",
                           "cisir_id", "encounter_csn", "pat_id")

ddl_files_written <- character(0)

for (i in seq_len(nrow(validated_tables))) {

  tbl_info <- validated_tables[i, ]
  tbl_name <- tbl_info$validated_table_target

  # Get this table's domain columns
  tbl_cols <- validated_columns %>%
    filter(validated_table_target == tbl_name)

  # Build file name: create_VALIDATED_<TABLE_NAME>.sql
  file_name <- paste0("create_VALIDATED_", toupper(tbl_name), ".sql")
  file_path <- file.path(output_dir, file_name)

  # Total columns = governance (5) + domain
  total_cols <- nrow(governance_cols) + nrow(tbl_cols)

  # -----------------------------------------------------------------------
  # Build DDL string
  # -----------------------------------------------------------------------
  separator <- paste0("-- ", strrep("=", 77))

  # Header
  header <- paste0(
    separator, "\n",
    "-- ", file_name, "\n",
    separator, "\n",
    "-- Purpose:      Create validated.", tbl_name, " table for harmonized\n",
    "--               cross-source data.\n",
    "--\n",
    "-- Schema:       validated\n",
    "-- Grain:        One row per source record mapped into this domain\n",
    "-- Columns:      ", total_cols, " (", nrow(governance_cols), " governance + ",
    nrow(tbl_cols), " domain)\n",
    "-- Sources:      ", tbl_info$sources, "\n",
    "--\n",
    "-- Dependencies: validated schema must exist\n",
    "--               governance.batch_log must exist (FK target for ingest_id)\n",
    "--\n",
    "-- Generated by: r/build_tools/generate_validated_ddls.R\n",
    "-- Author:       Noel\n",
    "-- Last Updated: ", format(Sys.Date(), "%Y-%m-%d"), "\n",
    separator, "\n"
  )

  # CREATE TABLE
  create_open <- paste0(
    "\nCREATE TABLE IF NOT EXISTS validated.", tbl_name, " (\n"
  )

  # Calculate padding width: longest column name + 1 space (min 24)
  all_col_names <- c(governance_cols$col_name, tbl_cols$validated_variable_name)
  pad_width <- max(24L, max(nchar(all_col_names)) + 1L)
  fmt_str <- paste0("%-", pad_width, "s")

  # Governance columns section
  gov_lines <- "    -- Governance columns\n"
  for (g in seq_len(nrow(governance_cols))) {
    gc <- governance_cols[g, ]
    col_def <- paste0("    ", sprintf(fmt_str, gc$col_name),
                      gc$col_type, " ", gc$col_extra)
    # Add comma (there will always be domain cols after governance cols)
    col_def <- paste0(col_def, ",")
    gov_lines <- paste0(gov_lines, col_def, "\n")
  }

  # Domain columns section
  domain_lines <- "\n    -- Domain columns\n"
  for (d in seq_len(nrow(tbl_cols))) {
    dc <- tbl_cols[d, ]
    col_def <- paste0("    ", sprintf(fmt_str, dc$validated_variable_name),
                      dc$resolved_type)
    # Add comma unless last domain column
    if (d < nrow(tbl_cols)) {
      col_def <- paste0(col_def, ",")
    }
    domain_lines <- paste0(domain_lines, col_def, "\n")
  }

  # FK constraint
  fk_block <- paste0(
    "\n",
    "    -- Foreign key to batch lineage\n",
    "    ,CONSTRAINT fk_", tbl_name, "_ingest\n",
    "        FOREIGN KEY (ingest_id)\n",
    "        REFERENCES governance.batch_log (ingest_id)\n",
    "        ON DELETE CASCADE\n"
  )

  create_close <- ");\n"

  # -----------------------------------------------------------------------
  # Indexes
  # -----------------------------------------------------------------------
  idx_separator <- paste0(
    "\n", separator, "\n",
    "-- INDEXES\n",
    separator, "\n"
  )

  idx_lines <- ""

  # Always index source_type and ingest_id
  idx_lines <- paste0(idx_lines, "\nCREATE INDEX IF NOT EXISTS idx_",
                      tbl_name, "_source_type\n",
                      "    ON validated.", tbl_name, " (source_type);\n")

  idx_lines <- paste0(idx_lines, "\nCREATE INDEX IF NOT EXISTS idx_",
                      tbl_name, "_ingest_id\n",
                      "    ON validated.", tbl_name, " (ingest_id);\n")

  # Index identifier columns that appear in this table
  tbl_col_names <- tbl_cols$validated_variable_name
  id_cols_present <- intersect(indexable_identifiers, tbl_col_names)

  for (id_col in id_cols_present) {
    idx_lines <- paste0(idx_lines, "\nCREATE INDEX IF NOT EXISTS idx_",
                        tbl_name, "_", id_col, "\n",
                        "    ON validated.", tbl_name, " (", id_col, ");\n")
  }

  # -----------------------------------------------------------------------
  # Comments
  # -----------------------------------------------------------------------
  cmt_separator <- paste0(
    "\n", separator, "\n",
    "-- COMMENTS\n",
    separator, "\n"
  )

  table_comment <- paste0(
    "\nCOMMENT ON TABLE validated.", tbl_name, " IS\n",
    "'Harmonized ", gsub("_", " ", tbl_name),
    " data combined from: ", tbl_info$sources,
    ". Populated by Step 6 harmonization.';\n"
  )

  col_comments <- ""

  # Comment governance columns
  gov_comments <- c(
    source_type  = "Origin data source: CISIR, CLARITY, or TRAUMA_REGISTRY.",
    source_table = "Staging table this row was sourced from.",
    ingest_id    = "Batch identifier linking to governance.batch_log for lineage.",
    created_at   = "Timestamp when this row was inserted into the validated table."
  )

  for (gc_name in names(gov_comments)) {
    col_comments <- paste0(col_comments,
      "\nCOMMENT ON COLUMN validated.", tbl_name, ".", gc_name, " IS\n",
      "'", gov_comments[[gc_name]], "';\n"
    )
  }

  # -----------------------------------------------------------------------
  # Assemble full DDL
  # -----------------------------------------------------------------------
  full_ddl <- paste0(
    header,
    create_open,
    gov_lines,
    domain_lines,
    fk_block,
    create_close,
    idx_separator,
    idx_lines,
    cmt_separator,
    table_comment,
    col_comments
  )

  # Write to file
  writeLines(full_ddl, file_path)
  ddl_files_written <- c(ddl_files_written, file_path)

  message(">> Written: ", file_name, " (",
          total_cols, " columns, ",
          length(id_cols_present), " identifier indexes)")
}

# --------------------------------------------------------------------------
# 8. Summary
# --------------------------------------------------------------------------
message("\n==============================")
message("  DDL GENERATION SUMMARY")
message("==============================")
message("Validated tables:    ", nrow(validated_tables))
message("DDL files written:   ", length(ddl_files_written))
message("Output directory:    ", normalizePath(output_dir))
message("==============================\n")

# Disconnect
DBI::dbDisconnect(con)
message(">> DDL generation complete.")
