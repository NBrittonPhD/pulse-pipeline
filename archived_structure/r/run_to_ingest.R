source("r/action/ingest.R")
con <- connect_to_pulse()

raw_path <- "~/Documents/PULSE/RAW/11.28.23_Data_Pull/DATA_TABLES_CSV"

ingest_id <- ingest(
  source_id = "TR2023",
  raw_path = raw_path,
  con = con
)

dbReadTable(con, "source_registry") %>%
  dplyr::filter(source_id == "TR2023")

dbReadTable(con, "batch_log") %>%
  dplyr::filter(ingest_id == ingest_id)

dbGetQuery(con, "
  SELECT table_name
  FROM information_schema.tables
  WHERE table_schema = 'raw'
  ORDER BY table_name;
")


dbReadTable(con, DBI::Id(schema = "raw", table = "labs")) %>% head()
dbReadTable(con, DBI::Id(schema = "raw", table = "blood")) %>% head()

dbReadTable(con, DBI::Id(schema = "raw", table = "ingest_file_log")) %>%
  dplyr::filter(ingest_id == ingest_id)
