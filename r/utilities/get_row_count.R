# r/utilities/get_row_count.R

get_row_count <- function(file_path) {
  # Ensure data.table is available
  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required for get_row_count().", call. = FALSE)
  }
  
  # 1. Validate file_path
  if (is.null(file_path) || !is.character(file_path) || length(file_path) != 1) {
    stop("get_row_count(): 'file_path' must be a single character string.", call. = FALSE)
  }
  
  # 2. Check file existence
  if (!file.exists(file_path)) {
    stop(glue::glue("get_row_count(): File does not exist: {file_path}"), call. = FALSE)
  }
  
  # 3. Handle empty files (0 bytes)
  file_info <- file.info(file_path)
  if (file_info$size == 0) {
    return(0)
  }
  
  # 4. Read file safely
  dt <- tryCatch(
    data.table::fread(
      file = file_path,
      nThread = 1,          # deterministic behavior
      showProgress = FALSE,
      na.strings = c("", "NA")
    ),
    error = function(e) {
      stop(
        glue::glue("get_row_count(): Failed to read file '{file_path}': {e$message}"),
        call. = FALSE
      )
    }
  )
  
  # 5. Determine row count
  # fread() returns a data.table with 0 rows if only header is present
  n_rows <- nrow(dt)
  
  return(as.numeric(n_rows))
}
