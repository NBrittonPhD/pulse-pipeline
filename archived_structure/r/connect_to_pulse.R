library(DBI)
library(RPostgres)
library(tidyverse)

connect_to_pulse <- function() {
  
  con <- dbConnect(
    Postgres(),
    dbname   = Sys.getenv("PULSE_DB"),
    host     = Sys.getenv("PULSE_HOST"),
    user     = Sys.getenv("PULSE_USER"),
    password = Sys.getenv("PULSE_PW")
  )
  
  # CRITICAL: Set search path immediately after connecting
  dbExecute(con, "SET search_path TO governance, public;")
  
  return(con)
}
