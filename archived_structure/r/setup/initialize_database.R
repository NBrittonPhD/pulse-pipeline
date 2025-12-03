# ------------------------------------------------------------
# PULSE: Database Initialization Script
# Creates schemas + executes all DDL files in sql/ddl/
# Safe to run multiple times.
# ------------------------------------------------------------

library(DBI)
library(RPostgres)
library(readr)

source("r/connect_to_pulse.R")
con <- connect_to_pulse()

message("Connected to database: ", Sys.getenv("PULSE_DB"))

# ------------------------------------------------------------
# Step 1 — Run create_SCHEMAS.sql FIRST
# ------------------------------------------------------------

schema_file <- "sql/ddl/create_SCHEMAS.sql"

if (!file.exists(schema_file)) {
  stop("Schema DDL file not found: ", schema_file)
}

schema_sql <- read_file(schema_file)

statements <- unlist(strsplit(schema_sql, ";", fixed = TRUE))

for (stmt in statements) {
  s <- trimws(stmt)
  if (s != "") {
    dbExecute(con, paste0(s, ";"))
  }
}

message("Schemas created / verified.")

# ------------------------------------------------------------
# Step 2 — Verify schema existence
# ------------------------------------------------------------

required_schemas <- c("raw", "staging", "validated", "governance", "reference")

schemas <- dbGetQuery(con, "
  SELECT schema_name
  FROM information_schema.schemata;
")

existing <- schemas$schema_name

missing <- setdiff(required_schemas, existing)

if (length(missing) > 0) {
  stop("Missing required schemas: ", paste(missing, collapse = ", "))
}

message("All required schemas exist.")

# ------------------------------------------------------------
# Step 3 — Execute ALL DDL files except create_SCHEMAS.sql
# ------------------------------------------------------------

ddl_path <- "sql/ddl"

ddl_files <- list.files(
  ddl_path,
  pattern = "^create_.*\\.sql$",
  full.names = TRUE
)

# Remove schema file (already executed)
ddl_files <- ddl_files[!grepl("create_SCHEMAS.sql$", ddl_files)]

message("Executing DDL files:")
print(basename(ddl_files))

for (file in ddl_files) {
  ddl_sql <- read_file(file)
  stmts <- unlist(strsplit(ddl_sql, ";", fixed = TRUE))
  for (stmt in stmts) {
    clean_stmt <- trimws(stmt)
    if (clean_stmt != "") {
      dbExecute(con, paste0(clean_stmt, ";"))
    }
  }
}

message("All DDL executed.")

# ------------------------------------------------------------
# Step 4 — Final verification: list all governance tables
# ------------------------------------------------------------

message("Tables in database:")
print(dbListTables(con))

message("Database initialization complete.")
