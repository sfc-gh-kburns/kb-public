# Streamlit Apps Portal

A comprehensive portal application for managing and launching Streamlit applications within Snowflake environments.

## Features

- **App Portal**: Grid-based interface showing all accessible Streamlit applications
- **Admin Configuration**: Full administrative interface for managing apps and permissions
- **Role-based Access Control**: Secure access management using Snowflake roles and users (supports both StreamlitPortalAdmins and AccountAdmin for administration)
- **Multi-page Navigation**: Clean, organized interface with multiple functional areas
- **User Documentation**: Built-in help and documentation

## Documentation

[Documentation](https://docs.google.com/document/d/1P5-YC5QnFuWzh1sD6NPNUQy2zgYilMcrRwBheQn9opg/edit?tab=t.0)  <--This links to a Google Sheet that has setup information.  


## Support

This is a personal project and does not come with any official support.  This is not backed by any company or individual.   It will be maintained and enhanced, but there is no set timetable.



## Tech Stack

This is designed to be run in Snowflake using Streamlit.   It has been tested both as a standalone app and using Streamlit in Snowflake, but the percieved value is in Snowflake. 



## Usage

### For End Users
1. Access the portal to view available applications
2. Click "Launch" on any app card to open the application
3. Access is automatically controlled based on your Snowflake roles

### For Administrators
1. Ensure you have the `StreamlitPortalAdmins` or `AccountAdmin` role
2. Use the "Portal Configuration" page to:
   - Add/remove applications from the portal
   - Manage user and role-based access permissions
   - View portal statistics

## Technical Architecture

- **Database**: StreamlitPortal database with tables for apps and access control
- **Authentication**: Uses Snowflake native authentication and roles
- **Storage**: Images stored in Snowflake stages
- **Framework**: Built with Streamlit and Snowpark

## File Structure

- `StreamlitPortal.py` - Main portal application
- `portal_config.py` - Administrative configuration module
- `setup_portal.sql` - Database and role setup script
- `requirements.txt` - Python dependencies
- `requirements.md` - Original requirements document

## Support

For issues or questions, contact your Streamlit administrator or refer to the built-in documentation within the portal.
