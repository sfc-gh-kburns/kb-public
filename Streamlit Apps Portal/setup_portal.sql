-- Streamlit Apps Portal Setup Script
-- This script sets up the necessary roles and permissions for the portal

-- Create the StreamlitPortalAdmins role
CREATE ROLE IF NOT EXISTS StreamlitPortalAdmins
    COMMENT = 'Role for administrators of the Streamlit Apps Portal';

-- Grant necessary permissions to the role
GRANT USAGE ON DATABASE StreamlitPortal TO ROLE StreamlitPortalAdmins;
GRANT USAGE ON SCHEMA StreamlitPortal.PUBLIC TO ROLE StreamlitPortalAdmins;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA StreamlitPortal.PUBLIC TO ROLE StreamlitPortalAdmins;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA StreamlitPortal.PUBLIC TO ROLE StreamlitPortalAdmins;


CREATE STAGE StreamlitPortal.PUBLIC.portal_images  	DIRECTORY = ( ENABLE = true ) 	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' ) 
	COMMENT = 'to house the images for the portal links';
-- Grant stage permissions for image uploads
GRANT READ, WRITE ON STAGE StreamlitPortal.PUBLIC.portal_images TO ROLE StreamlitPortalAdmins;

-- Grant permission to show streamlits in account (needed for app discovery)
GRANT MONITOR ON ACCOUNT TO ROLE StreamlitPortalAdmins;

-- Add KBURNS to the StreamlitPortalAdmins role
GRANT ROLE StreamlitPortalAdmins TO USER KBURNS;

-- Grant necessary permissions to AccountAdmin role as well
GRANT USAGE ON DATABASE StreamlitPortal TO ROLE AccountAdmin;
GRANT USAGE ON SCHEMA StreamlitPortal.PUBLIC TO ROLE AccountAdmin;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA StreamlitPortal.PUBLIC TO ROLE AccountAdmin;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA StreamlitPortal.PUBLIC TO ROLE AccountAdmin;
GRANT READ, WRITE ON STAGE StreamlitPortal.PUBLIC.portal_images TO ROLE AccountAdmin;

-- Show confirmation
SELECT 'StreamlitPortalAdmins role created, KBURNS added, and AccountAdmin permissions granted successfully' AS setup_status; 

GRANT USAGE ON DATABASE StreamlitPortal TO ROLE AccountAdmin;
GRANT USAGE ON SCHEMA StreamlitPortal.PUBLIC TO ROLE AccountAdmin;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA StreamlitPortal.PUBLIC TO ROLE AccountAdmin;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA StreamlitPortal.PUBLIC TO ROLE AccountAdmin;
GRANT READ, WRITE ON STAGE StreamlitPortal.PUBLIC.portal_images TO ROLE AccountAdmin;