# =============================================================================
# r/reference/build_expected_schema_dictionary.R
# FUNCTION: build_expected_schema_dictionary()
# =============================================================================
# PURPOSE
#   Build the authoritative, versioned expected schema dictionary for all
#   lake tables governed by reference.ingest_dictionary in Postgres.
#
#   This dictionary captures:
#     • All expected lake tables and lake variables
#     • Structural metadata from information_schema.columns
#     • Variables not yet present in Postgres → nullable + not required
#     • Primary key metadata
#     • type_descriptor (Postgres structural truth only)
#     • Versioning metadata
#     • Column-level and table-level SHA256 hashes for drift detection
#
# INPUTS
#   con             DBI connection object
#   schema_version  e.g. "2025.0"
#   effective_from  Date
#   effective_to    Date or NA
#
# OUTPUTS
#   Tibble with one row per expected variable
#
# =============================================================================

build_expected_schema_dictionary <- function(con,
                                             schema_version  = "2025.0",
                                             effective_from  = Sys.Date(),
                                             effective_to    = NA) {
  
  # Required packages ----------------------------------------------------------
  required_pkgs <- c("DBI", "dplyr", "tibble", "digest", "stringr")
  missing_pkgs <- required_pkgs[!vapply(required_pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing_pkgs) > 0) {
    stop("Missing required packages: ", paste(missing_pkgs, collapse = ", "))
  }
  
  `%>%` <- dplyr::`%>%`
  
  # Normalize dates ------------------------------------------------------------
  effective_from_val <- as.Date(effective_from)
  effective_to_val   <- if (length(effective_to) == 1 && is.na(effective_to)) NA else as.Date(effective_to)
  
  # =============================================================================
  # SECTION 1 — Load ingest_dictionary
  # =============================================================================
  ingest_raw <- DBI::dbReadTable(con, DBI::Id(schema = "reference", table = "ingest_dictionary"))
  
  ingest_dict <- ingest_raw %>%
    tibble::as_tibble() %>%
    dplyr::mutate(
      lake_table_name      = tolower(.data$lake_table_name),
      lake_variable_name   = tolower(.data$lake_variable_name),
      source_type          = tolower(.data$source_type),
      source_table_name    = tolower(.data$source_table_name),
      source_variable_name = tolower(.data$source_variable_name)
    ) %>%
    dplyr::filter(
      !is.na(lake_table_name),
      !is.na(lake_variable_name)
    ) %>%
    dplyr::distinct(lake_table_name, lake_variable_name, .keep_all = TRUE)
  
  all_lake_tables <- sort(unique(ingest_dict$lake_table_name))
  if (length(all_lake_tables) == 0) {
    stop("No lake_table_name values found in reference.ingest_dictionary.")
  }
  
  # =============================================================================
  # SECTION 2 — Load structural metadata from information_schema.columns
  # =============================================================================
  sql_cols <- paste0(
    "SELECT table_schema, table_name, column_name, data_type, udt_name, ",
    "       character_maximum_length, numeric_precision, numeric_scale, ",
    "       is_nullable, column_default, ordinal_position ",
    "FROM information_schema.columns ",
    "WHERE table_schema = 'raw' ",
    "  AND table_name IN (",
    paste(sprintf("'%s'", all_lake_tables), collapse = ","),
    ")"
  )
  
  struct_df <- DBI::dbGetQuery(con, sql_cols) %>%
    tibble::as_tibble() %>%
    dplyr::mutate(
      lake_table_name    = tolower(.data$table_name),
      lake_variable_name = tolower(.data$column_name),
      is_nullable        = .data$is_nullable == "YES",
      length             = .data$character_maximum_length,
      precision          = .data$numeric_precision,
      scale              = .data$numeric_scale,
      default_value      = .data$column_default
    ) %>%
    dplyr::select(
      table_schema,
      lake_table_name,
      lake_variable_name,
      data_type,
      udt_name,
      length,
      precision,
      scale,
      default_value,
      is_nullable,
      ordinal_position
    )
  
  # =============================================================================
  # SECTION 3 — Primary key metadata
  # =============================================================================
  sql_pk <- paste0(
    "SELECT tc.table_schema, tc.table_name, kcu.column_name ",
    "FROM information_schema.table_constraints tc ",
    "JOIN information_schema.key_column_usage kcu ",
    "  ON tc.constraint_name = kcu.constraint_name ",
    " AND tc.table_schema = kcu.table_schema ",
    "WHERE tc.constraint_type = 'PRIMARY KEY' ",
    "  AND tc.table_schema = 'raw' ",
    "  AND tc.table_name IN (",
    paste(sprintf("'%s'", all_lake_tables), collapse = ","),
    ")"
  )
  
  pk_df <- DBI::dbGetQuery(con, sql_pk)
  
  pk_tbl <- tibble::as_tibble(pk_df) %>%
    dplyr::mutate(
      lake_table_name    = tolower(.data$table_name),
      lake_variable_name = tolower(.data$column_name),
      is_primary_key     = TRUE
    ) %>%
    dplyr::select(table_schema, lake_table_name, lake_variable_name, is_primary_key)
  
  struct_plus_pk <- struct_df %>%
    dplyr::left_join(pk_tbl,
                     by = c("table_schema", "lake_table_name", "lake_variable_name")) %>%
    dplyr::mutate(
      is_primary_key = dplyr::coalesce(.data$is_primary_key, FALSE)
    )
  
  # =============================================================================
  # SECTION 4 — Merge ingest variables with structural metadata
  # =============================================================================
  merged <- ingest_dict %>%
    dplyr::left_join(
      struct_plus_pk,
      by = c("lake_table_name", "lake_variable_name")
    )
  
  # Missing variables → nullable + not required
  merged2 <- merged %>%
    dplyr::mutate(
      is_nullable = dplyr::case_when(
        !is.na(.data$is_nullable) ~ .data$is_nullable,
        TRUE ~ TRUE       # missing column defaults to nullable
      ),
      is_required = dplyr::case_when(
        !is.na(.data$is_nullable) ~ !.data$is_nullable,  # NOT NULL → required
        TRUE ~ FALSE                                       # missing → optional
      )
    )
  
  # =============================================================================
  # SECTION 5 — Versioning + type_descriptor (Option A = Postgres truth only)
  # =============================================================================
  versioned <- merged2 %>%
    dplyr::mutate(
      schema_version = schema_version,
      effective_from = effective_from_val,
      effective_to   = effective_to_val
    )
  
  typed <- versioned %>%
    dplyr::mutate(
      type_descriptor = dplyr::case_when(
        !is.na(length)                        ~ paste0("varchar(", length, ")"),
        !is.na(precision) & !is.na(scale)     ~ paste0("numeric(", precision, ",", scale, ")"),
        data_type == "boolean"                ~ "boolean",
        data_type == "integer"                ~ "integer",
        data_type == "date"                   ~ "date",
        stringr::str_detect(ifelse(is.na(data_type), "", data_type), "timestamp") ~ "timestamp",
        data_type == "text"                   ~ "text",
        TRUE                                  ~ "text"
      )
    )
  
  # =============================================================================
  # SECTION 6 — Hashes (type-stable, all fields cast to character)
  # =============================================================================
  hash_safe <- typed %>%
    dplyr::mutate(
      schema_version_chr       = as.character(schema_version),
      table_schema_chr         = ifelse(is.na(table_schema), "", table_schema),
      lake_table_name_chr      = ifelse(is.na(lake_table_name), "", lake_table_name),
      lake_variable_name_chr   = ifelse(is.na(lake_variable_name), "", lake_variable_name),
      data_type_chr            = ifelse(is.na(data_type), "", data_type),
      udt_name_chr             = ifelse(is.na(udt_name), "", udt_name),
      length_chr               = ifelse(is.na(length), "", as.character(length)),
      precision_chr            = ifelse(is.na(precision), "", as.character(precision)),
      scale_chr                = ifelse(is.na(scale), "", as.character(scale)),
      default_value_chr        = ifelse(is.na(default_value), "", as.character(default_value)),
      is_nullable_chr          = ifelse(is.na(is_nullable), "", as.character(is_nullable)),
      is_required_chr          = ifelse(is.na(is_required), "", as.character(is_required)),
      is_primary_key_chr       = ifelse(is.na(is_primary_key), "", as.character(is_primary_key)),
      ordinal_position_chr     = ifelse(is.na(ordinal_position), "", as.character(ordinal_position)),
      source_type_chr          = ifelse(is.na(source_type), "", source_type),
      source_table_name_chr    = ifelse(is.na(source_table_name), "", source_table_name),
      source_variable_name_chr = ifelse(is.na(source_variable_name), "", source_variable_name),
      type_descriptor_chr      = ifelse(is.na(type_descriptor), "", type_descriptor)
    )
  
  hashed <- hash_safe %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      column_schema_hash = digest::digest(
        paste(
          schema_version_chr,
          table_schema_chr,
          lake_table_name_chr,
          lake_variable_name_chr,
          data_type_chr,
          udt_name_chr,
          length_chr,
          precision_chr,
          scale_chr,
          default_value_chr,
          is_nullable_chr,
          is_required_chr,
          is_primary_key_chr,
          ordinal_position_chr,
          source_type_chr,
          source_table_name_chr,
          source_variable_name_chr,
          type_descriptor_chr,
          sep = "|"
        ),
        algo = "sha256"
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(schema_version, lake_table_name, table_schema) %>%
    dplyr::mutate(
      table_schema_hash = digest::digest(
        paste(
          lake_variable_name_chr,
          column_schema_hash,
          sep = "||"
        ),
        algo = "sha256"
      )
    ) %>%
    dplyr::ungroup()
  
  # =============================================================================
  # SECTION 7 — Final column selection
  # =============================================================================
  out <- hashed %>%
    dplyr::transmute(
      schema_version,
      effective_from,
      effective_to,
      table_schema,
      lake_table_name,
      lake_variable_name,
      data_type,
      udt_name,
      length,
      precision,
      scale,
      default_value,
      is_nullable,
      is_required,
      is_primary_key,
      ordinal_position,
      type_descriptor,
      source_type,
      source_table_name,
      source_variable_name,
      column_schema_hash,
      table_schema_hash
    )
  
  return(out)
}
