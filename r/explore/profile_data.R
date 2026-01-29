library(dplyr)
library(purrr)
library(stringr)
library(tidyr)

profile_one_table <- function(df, table_name) {
  
  tibble(
    table_name = table_name,
    variable   = names(df)
  ) %>%
    mutate(
      n_rows = nrow(df),
      
      n_missing = map_int(variable, ~ {
        x <- df[[.x]]
        sum(is.na(x) | x == "")
      }),
      
      n_non_missing = n_rows - n_missing,
      
      pct_missing = n_missing / n_rows * 100,
      
      any_values = n_non_missing > 0,
      
      n_distinct_non_missing = map_int(variable, ~ {
        x <- df[[.x]]
        n_distinct(x[!is.na(x) & x != ""])
      }),
      
      non_numeric_count = map_int(variable, ~ {
        x <- df[[.x]]
        sum(
          !is.na(x) &
            x != "" &
            !str_detect(x, "^-?\\d+(\\.\\d+)?$")
        )
      }),
      
      all_numeric_like = non_numeric_count == 0 & n_non_missing > 0,
      
      decimal_count = map_int(variable, ~ {
        x <- df[[.x]]
        sum(str_detect(x, "\\."), na.rm = TRUE)
      }),
      
      any_negative = map_lgl(variable, ~ {
        x <- df[[.x]]
        any(str_detect(x, "^-"), na.rm = TRUE)
      }),
      
      boolean_like = map_lgl(variable, ~ {
        x <- df[[.x]]
        x2 <- unique(x[!is.na(x) & x != ""])
        length(x2) > 0 &&
          all(x2 %in% c("0","1","Y","N","Yes","No","TRUE","FALSE"))
      }),
      
      has_sentinel_values = map_lgl(variable, ~ {
        x <- toupper(df[[.x]])
        any(x %in% c("-9","-99","-999","UNK","UNKNOWN","N/A","NA","."), na.rm = TRUE)
      }),
      
      max_char_length = map_int(variable, ~ {
        x <- df[[.x]]
        if (all(is.na(x))) NA_integer_
        else max(nchar(x), na.rm = TRUE)
      }),
      
      likely_id = (
        n_distinct_non_missing / n_rows > 0.8 &
          max_char_length > 8 &
          !boolean_like
      )
    )
}
