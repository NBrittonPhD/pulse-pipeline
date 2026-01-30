# =============================================================================
# PULSE — BULK LIGHT DE-IDENTIFICATION (ALL FOLDERS + ALL TABLE FILES)
# -----------------------------------------------------------------------------
# Purpose:
#   Walk a raw/toy data folder tree, de-identify supported tabular files, and
#   write outputs while PRESERVING THE ORIGINAL FOLDER STRUCTURE.
#
# Applies across:
#   • All data folders under INPUT_ROOTS (recursive)
#   • All supported file types found
#
# De-ID rules (lightweight; for dev/toy only):
#   1) ID-like columns (if present in any table):
#        Cisir Id, MRN, Trauma No., Account Number, MEDRECNO
#      → add a deterministic per-column numeric offset (same everywhere).
#
#   2) DOB columns:
#      → shift dates by a deterministic per-row number of days derived from a
#        stable key (preferred) or row index (fallback).
#
#   3) Name columns (FNAME, LNAME):
#      → replace with deterministic pseudorandom character strings derived from
#        original value + salt (stable across tables).
#
# Folder structure:
#   OUT_ROOT/<relative path under INPUT_ROOT>/<filename stem>__deid.<ext>
#
# Supported types:
#   • .csv, .tsv, .txt
#   • .xlsx, .xls (reads first sheet; writes xlsx if writexl installed, else csv)
#   • .rds (data.frame only)
#   • .parquet (requires {arrow})
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(readxl)
  library(stringr)
  library(fs)
  library(tibble)
})

# =============================================================================
# USER INPUTS (edit these)
# =============================================================================

# Input roots to scan (pick ONE or more)
# Examples:
#   file.path("raw")
#   file.path("toy_data")
INPUT_ROOTS <- c(
  file.path("raw")
)

# Output root (de-identified mirror tree)
OUT_ROOT <- file.path("raw", "deid_samples")

# Rows to keep (optional):
#   • If you are de-identifying toy files, keep this as Inf.
#   • If you want to ALSO downsample large raw files, set to 25, 100, etc.
N_MAX_ROWS <- Inf

# Include /archive/ files?
INCLUDE_ARCHIVE <- TRUE

# Global salt (change this if you want a different de-id transform)
DEID_SALT <- "PULSE_DEID_20260107"

# Primary key candidates (used to derive stable per-person DOB shifts).
# First existing one will be used as a "stable key" if found.
STABLE_KEY_CANDIDATES <- c("MRN", "MEDRECNO", "ACCOUNT_NUMBER", "ACCOUNT NUMBER", "TRAUMA NO.", "TRAUMA_NO")

# Target columns (matched by normalized name; case/punct insensitive)
TARGET_ID_COLUMNS <- c("Cisir Id", "MRN", "Trauma No.", "Account Number", "MEDRECNO")
TARGET_NAME_COLUMNS <- c("FNAME", "LNAME")
TARGET_DOB_COLUMNS  <- c("DOB", "DATE_OF_BIRTH", "BIRTH_DATE", "BIRTHDATE")

# Deterministic offsets per ID column (set once, applied everywhere)
# If NULL, they will be generated from DEID_SALT.
ID_OFFSETS <- NULL

# DOB day-shift range
DOB_SHIFT_RANGE <- c(-3650L, 3650L)   # +/- 10 years

# Replacement name string length
NAME_STRING_LENGTH <- 10L

# =============================================================================
# MAIN FUNCTION
# =============================================================================

bulk_deidentify_files <- function(input_roots,
                                  out_root,
                                  n_max_rows = Inf,
                                  include_archive = TRUE,
                                  deid_salt = "PULSE_DEID",
                                  stable_key_candidates = STABLE_KEY_CANDIDATES,
                                  target_id_columns = TARGET_ID_COLUMNS,
                                  target_name_columns = TARGET_NAME_COLUMNS,
                                  target_dob_columns = TARGET_DOB_COLUMNS,
                                  id_offsets = ID_OFFSETS,
                                  dob_shift_range = DOB_SHIFT_RANGE,
                                  name_string_length = NAME_STRING_LENGTH) {

  # ---- helpers ---------------------------------------------------------------

  normalize_name <- function(x) {
    x %>%
      toupper() %>%
      stringr::str_replace_all("[^A-Z0-9]+", "") %>%
      stringr::str_trim()
  }

  is_supported_ext <- function(ext) {
    ext %in% c("csv", "tsv", "txt", "xlsx", "xls", "rds", "parquet")
  }

  # Simple deterministic integer hash (no extra deps)
  # Produces a non-negative integer based on (salt + value)
  hash_int <- function(value, salt, mod = 2147483647L) {
    s <- paste0(salt, "||", as.character(value))
    ints <- utf8ToInt(s)
    # A little mixing; keep it stable
    h <- sum((ints * seq_along(ints)) %% 1000003L)
    as.integer(abs(h) %% mod)
  }

  # Deterministic per-column offsets
  if (is.null(id_offsets)) {
    id_offsets <- setNames(
      vapply(target_id_columns, function(nm) {
        # offset in [100000, 999999]
        100000L + (hash_int(nm, deid_salt, mod = 900000L))
      }, integer(1)),
      target_id_columns
    )
  } else {
    if (is.null(names(id_offsets))) stop("If ID_OFFSETS is provided, it must be a named vector.")
  }

  # Read first N rows depending on file type
  read_first_n <- function(path, ext, n) {
    ext <- tolower(ext)

    if (ext %in% c("csv", "tsv", "txt")) {
      if (ext == "csv") {
        return(readr::read_csv(path, n_max = n, show_col_types = FALSE, progress = FALSE))
      }
      if (ext == "tsv") {
        return(readr::read_tsv(path, n_max = n, show_col_types = FALSE, progress = FALSE))
      }
      # .txt: try comma then tab
      x <- tryCatch(
        readr::read_delim(path, delim = ",", n_max = n, show_col_types = FALSE, progress = FALSE),
        error = function(e) NULL
      )
      if (!is.null(x)) return(x)
      return(readr::read_delim(path, delim = "\t", n_max = n, show_col_types = FALSE, progress = FALSE))
    }

    if (ext %in% c("xlsx", "xls")) {
      sheets <- readxl::excel_sheets(path)
      if (length(sheets) == 0) stop("No sheets found.")
      return(readxl::read_excel(path, sheet = sheets[[1]], n_max = n))
    }

    if (ext == "rds") {
      obj <- readRDS(path)
      if (!inherits(obj, "data.frame")) stop("RDS is not a data.frame; cannot de-identify safely.")
      if (is.finite(n)) obj <- dplyr::slice_head(obj, n = n)
      return(obj)
    }

    if (ext == "parquet") {
      if (!requireNamespace("arrow", quietly = TRUE)) stop("Package {arrow} required for parquet.")
      df <- arrow::read_parquet(path)
      if (!inherits(df, "data.frame")) df <- as.data.frame(df)
      if (is.finite(n)) df <- dplyr::slice_head(df, n = n)
      return(df)
    }

    stop("Unsupported file extension.")
  }

  write_like_input <- function(df, out_path, ext) {
    ext <- tolower(ext)
    fs::dir_create(fs::path_dir(out_path), recurse = TRUE)

    if (ext == "csv") {
      readr::write_csv(df, out_path)
      return(out_path)
    }
    if (ext == "tsv") {
      readr::write_tsv(df, out_path)
      return(out_path)
    }
    if (ext == "txt") {
      # keep .txt extension but write comma-delimited
      readr::write_delim(df, out_path, delim = ",")
      return(out_path)
    }
    if (ext %in% c("xlsx", "xls")) {
      if (requireNamespace("writexl", quietly = TRUE)) {
        writexl::write_xlsx(df, out_path)
        return(out_path)
      }
      csv_fallback <- fs::path_ext_set(out_path, "csv")
      readr::write_csv(df, csv_fallback)
      return(csv_fallback)
    }
    if (ext == "rds") {
      saveRDS(df, out_path)
      return(out_path)
    }
    if (ext == "parquet") {
      if (!requireNamespace("arrow", quietly = TRUE)) stop("Package {arrow} required for parquet writing.")
      arrow::write_parquet(df, out_path)
      return(out_path)
    }

    stop("Unsupported output extension.")
  }

  # Build deterministic pseudoname based on original
  make_pseudoname <- function(x, salt, len) {
    # treat NA as NA
    ifelse(
      is.na(x),
      NA_character_,
      vapply(x, function(v) {
        h <- hash_int(v, salt, mod = 2147483647L)
        # turn hash into base-52-ish characters
        alphabet <- c(letters, LETTERS)
        chars <- character(0)
        y <- h + 1L
        for (k in seq_len(len)) {
          idx <- (y %% length(alphabet)) + 1L
          chars <- c(chars, alphabet[[idx]])
          y <- (y %/% length(alphabet)) + k
        }
        paste0(chars, collapse = "")
      }, character(1))
    )
  }

  # Apply de-id to a data frame based on column presence (normalized matching)
  deidentify_df <- function(df) {

    nm_norm <- normalize_name(names(df))

    # Map normalized target -> actual column name(s)
    target_id_norm <- normalize_name(target_id_columns)
    names(target_id_norm) <- target_id_columns

    target_name_norm <- normalize_name(target_name_columns)
    names(target_name_norm) <- target_name_columns

    target_dob_norm <- normalize_name(target_dob_columns)
    names(target_dob_norm) <- target_dob_columns

    # ---- stable key selection (for DOB shift) --------------------------------
    stable_keys_norm <- normalize_name(stable_key_candidates)
    stable_key_idx <- which(nm_norm %in% stable_keys_norm)[1]
    stable_key_col <- if (!is.na(stable_key_idx)) names(df)[[stable_key_idx]] else NA_character_

    # ---- ID offsets ----------------------------------------------------------
    for (j in seq_along(target_id_norm)) {
      tgt <- names(target_id_norm)[[j]]
      tgt_norm <- target_id_norm[[j]]

      idx <- which(nm_norm == tgt_norm)
      if (length(idx) == 0) next

      col <- names(df)[[idx[1]]]
      offset <- id_offsets[[tgt]]

      # Add offset: numeric if possible; else append
      suppressWarnings(num <- as.numeric(df[[col]]))

      if (all(is.na(num))) {
        df[[col]] <- ifelse(is.na(df[[col]]), NA_character_, paste0(df[[col]], offset))
      } else {
        df[[col]] <- num + offset
      }
    }

    # ---- DOB shift -----------------------------------------------------------
    dob_idx <- which(nm_norm %in% target_dob_norm)[1]
    if (!is.na(dob_idx)) {
      dob_col <- names(df)[[dob_idx]]
      dob_vals <- suppressWarnings(as.Date(df[[dob_col]]))

      if (!all(is.na(dob_vals))) {

        if (!is.na(stable_key_col)) {
          key_vals <- df[[stable_key_col]]
          shifts <- vapply(key_vals, function(k) {
            h <- hash_int(k, deid_salt, mod = (dob_shift_range[2] - dob_shift_range[1] + 1L))
            as.integer(dob_shift_range[1] + h)
          }, integer(1))
        } else {
          shifts <- vapply(seq_along(dob_vals), function(i) {
            h <- hash_int(i, deid_salt, mod = (dob_shift_range[2] - dob_shift_range[1] + 1L))
            as.integer(dob_shift_range[1] + h)
          }, integer(1))
        }

        df[[dob_col]] <- dob_vals + shifts
      }
    }

    # ---- Name replacement ----------------------------------------------------
    for (j in seq_along(target_name_norm)) {
      tgt <- names(target_name_norm)[[j]]
      tgt_norm <- target_name_norm[[j]]

      idx <- which(nm_norm == tgt_norm)
      if (length(idx) == 0) next

      col <- names(df)[[idx[1]]]
      df[[col]] <- make_pseudoname(df[[col]], salt = paste0(deid_salt, "||", tgt), len = name_string_length)
    }

    df
  }

  # ---- discover files --------------------------------------------------------

  input_roots <- fs::path(input_roots)
  input_roots <- input_roots[fs::dir_exists(input_roots)]
  if (length(input_roots) == 0) stop("No INPUT_ROOTS exist on disk. Check INPUT_ROOTS.")

  file_index <- tibble::tibble(input_path = character(0), root = character(0))

  for (root in input_roots) {
    files <- fs::dir_ls(root, recurse = TRUE, type = "file")
    if (length(files) == 0) next
    file_index <- dplyr::bind_rows(file_index, tibble::tibble(
      input_path = as.character(files),
      root = as.character(root)
    ))
  }

  if (nrow(file_index) == 0) stop("No files found under INPUT_ROOTS.")

  if (!include_archive) {
    file_index <- file_index %>% filter(!stringr::str_detect(input_path, "[/\\\\]archive[/\\\\]"))
  }

  file_index <- file_index %>%
    mutate(ext = tolower(fs::path_ext(input_path))) %>%
    filter(vapply(ext, is_supported_ext, logical(1)))

  # ---- process ---------------------------------------------------------------

  manifest <- tibble::tibble(
    input_path = character(0),
    output_path = character(0),
    status = character(0),
    n_rows_written = integer(0),
    notes = character(0)
  )

  for (i in seq_len(nrow(file_index))) {
    in_path <- file_index$input_path[[i]]
    root    <- file_index$root[[i]]
    ext     <- file_index$ext[[i]]

    rel_path <- fs::path_rel(in_path, start = root)
    rel_dir  <- fs::path_dir(rel_path)

    in_base <- fs::path_file(in_path)
    stem <- fs::path_ext_remove(in_base)

    out_path <- file.path(out_root, rel_dir, paste0(stem, "__deid.", ext))

    res <- tryCatch({
      df <- read_first_n(in_path, ext, n_max_rows)

      df2 <- deidentify_df(df)

      out_written <- write_like_input(df2, out_path, ext)

      tibble::tibble(
        input_path = as.character(in_path),
        output_path = as.character(out_written),
        status = "wrote",
        n_rows_written = if (inherits(df2, "data.frame")) nrow(df2) else 0L,
        notes = if (ext %in% c("xlsx", "xls") && !requireNamespace("writexl", quietly = TRUE)) {
          "Excel write fallback: writexl not installed; wrote CSV instead."
        } else {
          ""
        }
      )
    }, error = function(e) {
      tibble::tibble(
        input_path = as.character(in_path),
        output_path = "",
        status = "skipped",
        n_rows_written = 0L,
        notes = paste0("ERROR: ", conditionMessage(e))
      )
    })

    manifest <- dplyr::bind_rows(manifest, res)
  }

  # ---- write manifest --------------------------------------------------------

  fs::dir_create(out_root, recurse = TRUE)
  manifest_path <- file.path(
    out_root,
    paste0("deid_manifest__", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
  )
  readr::write_csv(manifest, manifest_path)

  message("De-identified extracts written under: ", out_root)
  message("Manifest written: ", manifest_path)

  invisible(list(manifest = manifest, manifest_path = manifest_path, id_offsets = id_offsets))
}

# =============================================================================
# RUN
# =============================================================================

bulk_deidentify_files(
  input_roots = INPUT_ROOTS,
  out_root = OUT_ROOT,
  n_max_rows = N_MAX_ROWS,
  include_archive = INCLUDE_ARCHIVE,
  deid_salt = DEID_SALT
)
