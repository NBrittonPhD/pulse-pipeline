# =============================================================================
# connect_to_pulse.R
# =============================================================================
# Centralized database connection wrapper for the PULSE pipeline.
#
# This function establishes a Postgres connection using environment variables:
#   - PULSE_DB
#   - PULSE_HOST
#   - PULSE_USER
#   - PULSE_PW
#
# These must be exported into your shell or RStudio session before connecting:
#   Sys.setenv(PULSE_DB   = "primeai_lake")
#   Sys.setenv(PULSE_HOST = "your-host")
#   Sys.setenv(PULSE_USER = "your-username")
#   Sys.setenv(PULSE_PW   = "your-password")
#
# The function returns a live DBI connection object.
# =============================================================================

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