# =============================================================================
# PULSE — TOY RAW EXTRACT BUILDER (PRESERVE FOLDER STRUCTURE)
# -----------------------------------------------------------------------------
# Purpose:
#   Create small "toy" versions of raw data files by keeping the first N rows
#   (default: 25) from each file, then writing those samples to disk while
#   PRESERVING THE ORIGINAL FOLDER STRUCTURE (e.g., <source_id>/incoming/...).
#
# Output structure:
#   OUT_ROOT/<relative path under RAW_ROOT>/<original filename stem>__toyN.<ext>
#
# Example:
#   Input:  raw/CLARITY/incoming/encounters/enc.csv
#   Output: toy_data/raw_samples/CLARITY/incoming/encounters/enc__toy25.csv
#
# Supported input types:
#   • .csv, .tsv, .txt (delimited text)
#   • .xlsx, .xls      (writes xlsx/xls if writexl installed; else CSV fallback)
#   • .rds
#   • .parquet         (requires {arrow})
#
# Behavior:
#   • Does NOT fully read large text files (uses n_max = N).
#   • Writes a manifest CSV describing what was created / skipped.
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

# Root(s) where raw files live.
RAW_ROOTS <- c(
  file.path("raw")  # scans everything under ./raw
)

# Where to write toy outputs
OUT_ROOT <- file.path("toy_data", "raw_samples")

# How many rows to keep from each dataset
N_ROWS <- 25

# Whether to also include files under /archive/ (TRUE) or only /incoming/ (FALSE)
INCLUDE_ARCHIVE <- TRUE

# For Excel files: choose a sheet strategy
#   "first" = first sheet only
EXCEL_SHEETS <- "first"

# =============================================================================
# MAIN FUNCTION
# =============================================================================

make_toy_raw_extracts_preserve_structure <- function(raw_roots,
                                                    out_root,
                                                    n_rows = 25,
                                                    include_archive = TRUE,
                                                    excel_sheets = c("first")) {

  excel_sheets <- match.arg(excel_sheets)

  # ---- helpers ---------------------------------------------------------------

  is_supported_ext <- function(ext) {
    ext %in% c("csv", "tsv", "txt", "xlsx", "xls", "rds", "parquet")
  }

  guess_delim <- function(ext) {
    dplyr::case_when(
      ext == "csv" ~ ",",
      ext == "tsv" ~ "\t",
      TRUE ~ ","   # for .txt default to comma; if it fails we will try tab
    )
  }

  read_first_n <- function(path, ext, n) {
    ext <- tolower(ext)

    if (ext %in% c("csv", "tsv", "txt")) {

      delim <- guess_delim(ext)

      # For .txt, try comma then tab if comma fails
      if (ext == "txt") {
        x <- tryCatch(
          readr::read_delim(path, delim = ",", n_max = n, show_col_types = FALSE, progress = FALSE),
          error = function(e) NULL
        )
        if (!is.null(x)) return(x)

        return(readr::read_delim(path, delim = "\t", n_max = n, show_col_types = FALSE, progress = FALSE))
      }

      return(readr::read_delim(path, delim = delim, n_max = n, show_col_types = FALSE, progress = FALSE))
    }

    if (ext %in% c("xlsx", "xls")) {
      sheets <- readxl::excel_sheets(path)
      if (length(sheets) == 0) stop("No sheets found.")
      return(readxl::read_excel(path, sheet = sheets[[1]], n_max = n))
    }

    if (ext == "rds") {
      obj <- readRDS(path)
      if (!inherits(obj, "data.frame")) stop("RDS is not a data.frame; cannot take first N rows safely.")
      return(dplyr::slice_head(obj, n = n))
    }

    if (ext == "parquet") {
      if (!requireNamespace("arrow", quietly = TRUE)) {
        stop("Package {arrow} is required for parquet support.")
      }
      df <- arrow::read_parquet(path)
      if (!inherits(df, "data.frame")) df <- as.data.frame(df)
      return(dplyr::slice_head(df, n = n))
    }

    stop("Unsupported file extension.")
  }

  write_toy <- function(df, in_path, out_path, ext) {
    ext <- tolower(ext)
    fs::dir_create(fs::path_dir(out_path), recurse = TRUE)

    if (ext %in% c("csv", "tsv", "txt")) {
      # Preserve the file type and delimiter style where possible
      if (ext == "csv") {
        readr::write_csv(df, out_path)
      } else if (ext == "tsv") {
        readr::write_tsv(df, out_path)
      } else {
        # .txt: write with comma delimiter (most common); keep .txt extension
        readr::write_delim(df, out_path, delim = ",")
      }
      return(invisible(out_path))
    }

    if (ext %in% c("xlsx", "xls")) {
      # Prefer writing Excel if possible to preserve extension.
      # Use {writexl} (simple, no Java). If not installed, fall back to CSV.
      if (requireNamespace("writexl", quietly = TRUE)) {
        writexl::write_xlsx(df, out_path)
        return(invisible(out_path))
      }

      csv_fallback <- fs::path_ext_set(out_path, "csv")
      readr::write_csv(df, csv_fallback)
      return(invisible(csv_fallback))
    }

    if (ext == "rds") {
      saveRDS(df, out_path)
      return(invisible(out_path))
    }

    if (ext == "parquet") {
      if (!requireNamespace("arrow", quietly = TRUE)) {
        stop("Package {arrow} is required for parquet writing.")
      }
      arrow::write_parquet(df, out_path)
      return(invisible(out_path))
    }

    stop("Unsupported output extension.")
  }

  # ---- discover files --------------------------------------------------------

  raw_roots <- fs::path(raw_roots)
  raw_roots <- raw_roots[fs::dir_exists(raw_roots)]

  if (length(raw_roots) == 0) {
    stop("No RAW_ROOTS exist on disk. Check RAW_ROOTS inputs.")
  }

  # Collect files per root so we can compute a clean relative path for each
  file_index <- tibble::tibble(input_path = character(0), root = character(0))

  for (root in raw_roots) {
    files <- fs::dir_ls(root, recurse = TRUE, type = "file")
    if (length(files) == 0) next
    file_index <- dplyr::bind_rows(file_index, tibble::tibble(input_path = as.character(files), root = as.character(root)))
  }

  if (nrow(file_index) == 0) {
    stop("No files found under RAW_ROOTS.")
  }

  # Optionally drop /archive/ files
  if (!include_archive) {
    file_index <- file_index %>% filter(!stringr::str_detect(input_path, "[/\\\\]archive[/\\\\]"))
  }

  # Keep only supported types
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

    # Relative path under RAW_ROOT (preserves <source_id>/incoming/... structure)
    rel_path <- fs::path_rel(in_path, start = root)
    rel_dir  <- fs::path_dir(rel_path)

    in_base <- fs::path_file(in_path)
    stem <- fs::path_ext_remove(in_base)

    # Output filename keeps extension (with possible Excel->CSV fallback handled in write_toy())
    out_path <- file.path(out_root, rel_dir, paste0(stem, "__toy", n_rows, ".", ext))

    res <- tryCatch({
      toy_df <- read_first_n(in_path, ext, n_rows)
      out_written <- write_toy(toy_df, in_path, out_path, ext)

      tibble::tibble(
        input_path = as.character(in_path),
        output_path = as.character(out_written),
        status = "wrote",
        n_rows_written = nrow(toy_df),
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
    paste0("toy_extract_manifest__", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
  )
  readr::write_csv(manifest, manifest_path)

  message("Toy extracts written under: ", out_root)
  message("Manifest written: ", manifest_path)

  invisible(list(manifest = manifest, manifest_path = manifest_path))
}

# =============================================================================
# RUN
# =============================================================================

make_toy_raw_extracts_preserve_structure(
  raw_roots = RAW_ROOTS,
  out_root = OUT_ROOT,
  n_rows = N_ROWS,
  include_archive = INCLUDE_ARCHIVE,
  excel_sheets = EXCEL_SHEETS
)
