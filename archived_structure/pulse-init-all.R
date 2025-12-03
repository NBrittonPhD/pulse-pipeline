# ============================================================================
# pulse-init-all.R
# Initializes full PULSE environment: packages, connection, steps, utilities,
# helpers, and runner.
# ============================================================================

message(">>> Initializing PULSE environment...")

# ------------------------------#
# Load packages
# ------------------------------#
library(DBI)
library(RPostgres)
library(glue)
library(yaml)
library(purrr)
library(dplyr)
library(jsonlite)
library(fs)
library(rmarkdown)
library(uuid)

# ------------------------------#
# Load connection
# ------------------------------#
source("r/connect_to_pulse.R")

# ------------------------------#
# Load utilities
# ------------------------------#
utility_files <- list.files(
  "r/utilities",
  pattern = "\\.R$",
  full.names = TRUE
)
invisible(lapply(utility_files, source))

# ------------------------------#
# Load steps
# ------------------------------#
step_files <- list.files(
  "r/steps",
  pattern = "\\.R$",
  full.names = TRUE
)
invisible(lapply(step_files, source))

# ------------------------------#
# Load runner
# ------------------------------#
source("r/runner.R")

message(">>> PULSE environment ready.")
