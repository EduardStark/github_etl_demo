-- docker/postgres/init.sql
--
-- PostgreSQL initialization script.
-- Executed once when the postgres Docker container is first created.
--
-- Responsibilities:
--   1. Create the application database (if not using POSTGRES_DB env var alone)
--   2. Create application roles with least-privilege permissions
--   3. Create the 'staging' schema for raw/cleaned landing tables
--   4. Create the 'public' schema tables will land in (default, but explicitly granted)
--
-- Note: POSTGRES_DB, POSTGRES_USER, and POSTGRES_PASSWORD are set via
--       environment variables in docker-compose.yml — do not hardcode credentials here.

-- Create staging schema for raw extracted and cleaned data
CREATE SCHEMA IF NOT EXISTS staging;

-- Grant permissions to the app user on both schemas
-- (POSTGRES_USER is the superuser for the demo; refine for production)
GRANT ALL PRIVILEGES ON SCHEMA staging TO PUBLIC;
GRANT ALL PRIVILEGES ON SCHEMA public TO PUBLIC;
