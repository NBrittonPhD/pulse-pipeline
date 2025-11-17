
library(DBI)
library(RPostgres)

connect_to_pulse <- function() {
  dbConnect(
    Postgres(),
    dbname   = Sys.getenv("PULSE_DB"),
    host     = Sys.getenv("PULSE_HOST"),
    user     = Sys.getenv("PULSE_USER"),
    password = Sys.getenv("PULSE_PW")
  )
}
