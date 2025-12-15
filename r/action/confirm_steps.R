library(DBI)
library(RPostgres)

con <- connect_to_pulse()

dbGetQuery(
  con,
  "SELECT *
   FROM governance.source_registry
   WHERE source_id = 'your_source_id_here';"
)