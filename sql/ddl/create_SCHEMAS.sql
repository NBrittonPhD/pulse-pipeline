-- ============================================================================
-- Create core schemas for the PULSE data platform
-- This file is executed by r/setup/initialize_database.R
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS reference;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS validated;
CREATE SCHEMA IF NOT EXISTS governance;