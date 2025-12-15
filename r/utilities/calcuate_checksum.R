# r/utilities/calculate_checksum.R

calculate_checksum <- function(file_path) {
  # Ensure required package is available
  if (!requireNamespace("digest", quietly = TRUE)) {
    stop("Package 'digest' is required for calculate_checksum().", call. = FALSE)
  }
  
  # 1. Validate input
  if (is.null(file_path) || !is.character(file_path) || length(file_path) != 1) {
    stop("calculate_checksum(): 'file_path' must be a single character string.", call. = FALSE)
  }
  
  # 2. Ensure file exists
  if (!file.exists(file_path)) {
    stop(glue::glue("calculate_checksum(): File does not exist: {file_path}"), call. = FALSE)
  }
  
  # 3. Compute MD5 checksum
  # MD5 was chosen because:
  #  - it is stable across OS
  #  - it is deterministic for our lineage needs
  #  - it is used commonly in data governance pipelines
  checksum <- digest::digest(file = file_path, algo = "md5", serialize = FALSE)
  
  # 4. Return checksum
  return(checksum)
}
