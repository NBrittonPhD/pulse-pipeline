library(dplyr)
library(stringr)
library(writexl)

variable_inventory <- read_csv("~/Documents/PULSE/RAW/ARCHAEOLOGY/variable_inventory.csv")

variable_inventory <- variable_inventory %>%
  mutate(
    lake_table_name = if_else(
      # condition: table name already starts with source_type_
      str_starts(
        tolower(source_table_name),
        paste0(tolower(source_type), "_")
      ),
      # true → keep the table name (lowercase)
      tolower(source_table_name),
      # false → prepend source_type_
      tolower(paste0(source_type, "_", source_table_name))
    ),
    lake_variable_name = source_variable_name %>%
      tolower() %>%
      str_replace_all("[\\.\\-]+", "_") %>%  # replace . and - with _
      str_replace_all("\\s+", "_") %>%      # replace spaces with _
      str_replace_all("_+$", "")            # remove trailing underscores
  )


# Generate date stamp
date_stamp <- format(Sys.Date(), "%Y%m%d")

# Construct file path
file_path <- paste0("metadata/ingest_dictionary_", date_stamp, ".xlsx")

# Write out the updated dictionary
write_xlsx(
  variable_inventory,
  path = file_path
)

write_xlsx(variable_inventory, path = "ingest_dictionary.xlsx")
