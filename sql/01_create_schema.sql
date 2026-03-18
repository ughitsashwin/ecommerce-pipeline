-- =============================================================
-- 01_create_schema.sql
-- E-Commerce Data Pipeline Project
-- Creates the staging and warehouse schemas
-- Run this first before any other SQL scripts
-- =============================================================

-- Drop and recreate schemas for a clean slate (dev only)
-- In production, remove the DROP statements
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS warehouse CASCADE;

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Confirm
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('staging', 'warehouse')
ORDER BY schema_name;