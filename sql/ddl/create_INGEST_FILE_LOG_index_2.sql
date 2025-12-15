CREATE INDEX IF NOT EXISTS idx_ingest_file_log_lake_table
   ON governance.ingest_file_log (lake_table_name);
