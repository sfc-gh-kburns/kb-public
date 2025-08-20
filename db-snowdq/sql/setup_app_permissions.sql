-- Permissions Setup for Streamlit App Role
-- Run this as ACCOUNTADMIN or a role with appropriate privileges
--
-- This script sets up all necessary permissions for the 
-- Snowflake Data Quality & Documentation application

-- Create role for Streamlit app (if not exists)
CREATE ROLE IF NOT EXISTS STREAMLIT_APP_ROLE
COMMENT = 'Role for Snowflake Data Quality & Documentation Streamlit application';

-- Basic warehouse access (replace COMPUTE_WH with your warehouse)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE STREAMLIT_APP_ROLE;

-- =======================
-- METADATA ACCESS GRANTS
-- =======================

-- Grant access to Snowflake system database for metadata queries
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE STREAMLIT_APP_ROLE;

-- Grant specific database roles for metadata access
GRANT DATABASE ROLE SNOWFLAKE.OBJECT_VIEWER TO ROLE STREAMLIT_APP_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.USAGE_VIEWER TO ROLE STREAMLIT_APP_ROLE;

-- ===================
-- CORTEX ACCESS GRANTS
-- ===================

-- Grant Cortex access for LLM functionality
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE STREAMLIT_APP_ROLE;

-- Enable Cortex models at account level (run as ACCOUNTADMIN if needed)
-- Uncomment the line below if Cortex models are restricted in your account
-- ALTER ACCOUNT SET CORTEX_MODELS_ALLOWLIST = 'claude-3-7-sonnet,reka-core,mistral-large2,llama3-70b,snowflake-arctic';

-- ============================
-- DATA METRIC FUNCTION GRANTS
-- ============================

-- Grant Data Metric Function access for data quality monitoring
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE STREAMLIT_APP_ROLE;
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_LOOKUP TO ROLE STREAMLIT_APP_ROLE;

-- ===========================
-- DB_SNOWTOOLS ACCESS GRANTS
-- ===========================

-- Grant access to the application's tracking database
GRANT USAGE ON DATABASE DB_SNOWTOOLS TO ROLE STREAMLIT_APP_ROLE;
GRANT USAGE ON SCHEMA DB_SNOWTOOLS.PUBLIC TO ROLE STREAMLIT_APP_ROLE;

-- Grant table creation privileges for auto-setup
GRANT CREATE TABLE ON SCHEMA DB_SNOWTOOLS.PUBLIC TO ROLE STREAMLIT_APP_ROLE;

-- Grant DML privileges on existing and future tables
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA DB_SNOWTOOLS.PUBLIC TO ROLE STREAMLIT_APP_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DB_SNOWTOOLS.PUBLIC TO ROLE STREAMLIT_APP_ROLE;

-- ===========================
-- USER DATA ACCESS GRANTS
-- ===========================

-- IMPORTANT: You must grant SELECT access to your data databases/schemas
-- Replace the placeholders below with your actual database and schema names
-- These grants allow the app to query your data for description generation

-- Example grants (UNCOMMENT and MODIFY as needed):
-- GRANT USAGE ON DATABASE YOUR_DATA_DATABASE TO ROLE STREAMLIT_APP_ROLE;
-- GRANT USAGE ON ALL SCHEMAS IN DATABASE YOUR_DATA_DATABASE TO ROLE STREAMLIT_APP_ROLE;
-- GRANT SELECT ON ALL TABLES IN DATABASE YOUR_DATA_DATABASE TO ROLE STREAMLIT_APP_ROLE;
-- GRANT SELECT ON ALL VIEWS IN DATABASE YOUR_DATA_DATABASE TO ROLE STREAMLIT_APP_ROLE;
-- GRANT SELECT ON FUTURE TABLES IN DATABASE YOUR_DATA_DATABASE TO ROLE STREAMLIT_APP_ROLE;
-- GRANT SELECT ON FUTURE VIEWS IN DATABASE YOUR_DATA_DATABASE TO ROLE STREAMLIT_APP_ROLE;

-- For more granular access, grant on specific schemas:
-- GRANT USAGE ON SCHEMA YOUR_DATA_DATABASE.YOUR_SCHEMA TO ROLE STREAMLIT_APP_ROLE;
-- GRANT SELECT ON ALL TABLES IN SCHEMA YOUR_DATA_DATABASE.YOUR_SCHEMA TO ROLE STREAMLIT_APP_ROLE;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA YOUR_DATA_DATABASE.YOUR_SCHEMA TO ROLE STREAMLIT_APP_ROLE;

-- For the app to update object descriptions, you also need COMMENT privileges:
-- GRANT MODIFY ON ALL TABLES IN DATABASE YOUR_DATA_DATABASE TO ROLE STREAMLIT_APP_ROLE;
-- GRANT MODIFY ON ALL VIEWS IN DATABASE YOUR_DATA_DATABASE TO ROLE STREAMLIT_APP_ROLE;

-- =======================
-- ASSIGN ROLE TO USER
-- =======================

-- Grant the role to the user who will run the Streamlit app
-- Replace YOUR_USERNAME with the actual username
-- GRANT ROLE STREAMLIT_APP_ROLE TO USER YOUR_USERNAME;

-- Optionally set as default role for the user
-- ALTER USER YOUR_USERNAME SET DEFAULT_ROLE = STREAMLIT_APP_ROLE;

-- =======================
-- VERIFICATION
-- =======================

-- Show the role and its grants
SHOW GRANTS TO ROLE STREAMLIT_APP_ROLE;

-- Display completion message
SELECT 'Permissions setup completed! Remember to:' AS MESSAGE
UNION ALL
SELECT '1. Uncomment and modify the data access grants above' AS MESSAGE
UNION ALL  
SELECT '2. Grant the role to your Streamlit app user' AS MESSAGE
UNION ALL
SELECT '3. Ensure Cortex models are enabled in your account' AS MESSAGE; 