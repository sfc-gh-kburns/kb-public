# Streamlit Apps Portal

A comprehensive portal application for managing and launching Streamlit applications within Snowflake environments.

## Features

- **App Portal**: Grid-based interface showing all accessible Streamlit applications
- **Admin Configuration**: Full administrative interface for managing apps and permissions
- **Role-based Access Control**: Secure access management using Snowflake roles and users (supports both StreamlitPortalAdmins and AccountAdmin for administration)
- **Multi-page Navigation**: Clean, organized interface with multiple functional areas
- **User Documentation**: Built-in help and documentation

## Setup Instructions

### 1. Database Setup
Ensure you have a database called `StreamlitPortal` in your Snowflake account.

### 2. Role and Permission Setup
Run the setup script to create the admin role and permissions:
```sql
-- Execute the contents of setup_portal.sql in your Snowflake environment
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Connection
- For local development: Update the connection details in `get_snowflake_connection()` function
- For Streamlit in Snowflake: No additional configuration needed

### 5. Deploy and Run
- **Local**: `streamlit run StreamlitPortal.py`
- **Snowflake**: Deploy as a Streamlit in Snowflake application

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