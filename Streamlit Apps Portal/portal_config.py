import streamlit as st
import pandas as pd
from PIL import Image
import base64
from io import BytesIO

def show_portal_config(conn, user_info):
    """Display the portal configuration interface for administrators"""
    
    # Create tabs for different admin functions
    tab1, tab2, tab3, tab4 = st.tabs(["Manage Applications", "Manage Access", "Access Overview", "Settings"])
    
    with tab1:
        st.subheader("📱 Manage Applications")
        manage_applications(conn)
    
    with tab2:
        st.subheader("🔐 Manage Access Permissions")
        manage_access_permissions(conn)
    
    with tab3:
        st.subheader("👥 Access Overview")
        show_access_overview(conn)
    
    with tab4:
        st.subheader("⚙️ Portal Settings")
        manage_portal_settings(conn, user_info)

def get_all_streamlit_apps(conn):
    """Get all Streamlit apps from the Snowflake account"""
    try:
        if hasattr(conn, 'sql'):  # Snowpark session
            df = conn.sql("SHOW STREAMLITS IN ACCOUNT").to_pandas()
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute("SHOW STREAMLITS IN ACCOUNT")
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(cursor.fetchall(), columns=columns)
            cursor.close()
        
        # Normalize column names to lowercase for consistent handling
        if not df.empty:
            df.columns = df.columns.astype(str).str.lower()
        
        # Return simplified dataframe with key columns including URL info
        if not df.empty:
            # Columns have been normalized and mapped if needed
            
            # Select the columns we need based on actual SHOW STREAMLITS structure
            expected_cols = ['name', 'title', 'comment', 'database_name', 'schema_name', 'url_id']
            cols_to_select = []
            
            # Only add columns that actually exist
            for col in expected_cols:
                if col in df.columns:
                    cols_to_select.append(col)
            
            # If no expected columns found, handle fallback properly
            if not cols_to_select:
                # SHOW STREAMLITS returns numeric columns, select the ones we need
                all_cols = list(df.columns)
                cols_to_select = []
                # Select specific columns we need: 1:name, 4:title, 5:comment, 2:database_name, 3:schema_name, 8:url_id
                for col in ['1', '4', '5', '2', '3', '8']:
                    if col in all_cols:
                        cols_to_select.append(col)
                # Note: Using numeric column indices from SHOW STREAMLITS output
                
                # Create mapping for numeric columns based on SHOW STREAMLITS order:
                # 0:created_on, 1:name, 2:database_name, 3:schema_name, 4:title, 5:comment, 6:owner, 7:query_warehouse, 8:url_id, 9:owner_role_type
                rename_dict = {}
                if '1' in cols_to_select:  # name -> app_name
                    rename_dict['1'] = 'app_name'
                if '4' in cols_to_select:  # title -> app_title
                    rename_dict['4'] = 'app_title'
                if '5' in cols_to_select:  # comment -> description
                    rename_dict['5'] = 'description'
                if '2' in cols_to_select:  # database_name -> database_name
                    rename_dict['2'] = 'database_name'
                if '3' in cols_to_select:  # schema_name -> schema_name
                    rename_dict['3'] = 'schema_name'
                if '8' in cols_to_select:  # url_id -> url_id
                    rename_dict['8'] = 'url_id'
            else:
                # Use standard rename mapping for properly named columns
                rename_dict = {
                    'name': 'app_name',
                    'title': 'app_title', 
                    'comment': 'description',
                    'url_id': 'url_id',
                    'database_name': 'database_name',
                    'schema_name': 'schema_name'
                }
                # Only keep mappings for columns that exist
                rename_dict = {k: v for k, v in rename_dict.items() if k in cols_to_select}
            
            result_df = df[cols_to_select].copy()
            
            result_df = result_df.rename(columns=rename_dict)
            
            # Handle null values and ensure required columns exist
            if 'description' in result_df.columns:
                result_df['description'] = result_df['description'].fillna('')
            else:
                result_df['description'] = ''
                
            if 'app_title' in result_df.columns and 'app_name' in result_df.columns:
                result_df['app_title'] = result_df['app_title'].fillna(result_df['app_name'])
            elif 'app_name' in result_df.columns:
                result_df['app_title'] = result_df['app_name']
            else:
                result_df['app_title'] = 'Unknown App'
            
            # Ensure URL-related columns exist
            for col in ['url_id', 'database_name', 'schema_name']:
                if col not in result_df.columns:
                    result_df[col] = ''
            
            return result_df
        
        return pd.DataFrame(columns=['app_name', 'app_title', 'description', 'url_id', 'database_name', 'schema_name'])
    except Exception as e:
        st.error(f"Error getting Streamlit apps: {str(e)}")
        return pd.DataFrame(columns=['app_name', 'app_title', 'description', 'url_id', 'database_name', 'schema_name'])

def get_portal_apps(conn):
    """Get apps currently configured in the portal"""
    try:
        if hasattr(conn, 'sql'):  # Snowpark session
            df = conn.sql("SELECT * FROM portal_apps ORDER BY app_title").to_pandas()
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM portal_apps ORDER BY app_title")
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(cursor.fetchall(), columns=columns)
            cursor.close()
        
        # Ensure consistent column names (handle case differences)
        if not df.empty:
            # Create a mapping for column names (handle both lowercase and uppercase)
            column_mapping = {}
            for col in df.columns:
                if col.upper() == 'APP_ID':
                    column_mapping[col] = 'app_id'
                elif col.upper() == 'APP_NAME':
                    column_mapping[col] = 'app_name'
                elif col.upper() == 'APP_TITLE':
                    column_mapping[col] = 'app_title'
                elif col.upper() == 'DESCRIPTION':
                    column_mapping[col] = 'description'
                elif col.upper() == 'IS_ACTIVE':
                    column_mapping[col] = 'is_active'
                elif col.upper() == 'CREATED_AT':
                    column_mapping[col] = 'created_at'
                elif col.upper() == 'UPDATED_AT':
                    column_mapping[col] = 'updated_at'
            
            # Rename columns if mapping exists
            if column_mapping:
                df = df.rename(columns=column_mapping)
        
        return df
    except Exception as e:
        st.error(f"Error getting portal apps: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def get_all_users(_conn):
    """Get all users from Snowflake using account_usage view"""
    try:
        sql_query = "select distinct name from snowflake.account_usage.users where owner is not null"
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            df = _conn.sql(sql_query).to_pandas()
        else:  # Regular connection
            cursor = _conn.cursor()
            cursor.execute(sql_query)
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(cursor.fetchall(), columns=columns)
            cursor.close()
        
        # Extract usernames from the 'name' column
        if not df.empty and 'name' in df.columns:
            return sorted(df['name'].tolist())
        elif not df.empty and 'NAME' in df.columns:
            return sorted(df['NAME'].tolist())
        return []
    except Exception as e:
        st.error(f"Error getting users: {str(e)}")
        return []

@st.cache_data(ttl=600)
def get_all_roles(_conn):
    """Get all roles from Snowflake using account_usage view"""
    try:
        sql_query = " select distinct name from snowflake.account_usage.roles;"
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            df = _conn.sql(sql_query).to_pandas()
        else:  # Regular connection
            cursor = _conn.cursor()
            cursor.execute(sql_query)
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(cursor.fetchall(), columns=columns)
            cursor.close()
        
        # Extract role names from the 'name' column
        if not df.empty and 'name' in df.columns:
            return sorted(df['name'].tolist())
        elif not df.empty and 'NAME' in df.columns:
            return sorted(df['NAME'].tolist())
        return ['PUBLIC']  # At minimum, PUBLIC should exist
    except Exception as e:
        st.error(f"Error getting roles: {str(e)}")
        return ['PUBLIC']

def manage_applications(conn):
    """Manage applications in the portal"""
    st.markdown("Add or remove Streamlit applications from the portal.")
    
    # Initialize session state for tracking changes
    if 'changes_saved' not in st.session_state:
        st.session_state.changes_saved = False
    
    # Get all available Streamlit apps
    with st.spinner("Loading Streamlit applications..."):
        all_apps = get_all_streamlit_apps(conn)
        portal_apps = get_portal_apps(conn)
    
    if all_apps.empty:
        st.warning("No Streamlit applications found in this account.")
        return
    
    try:
        # Create a combined view showing which apps are in portal
        combined_df = all_apps.copy()
        
        # Handle empty portal_apps case
        if portal_apps.empty:
            # No apps in portal yet, so all apps are not in portal and inactive
            combined_df['in_portal'] = False
            combined_df['active'] = False
        else:
            # Check if portal_apps has the expected columns
            if 'app_name' not in portal_apps.columns:
                st.error("Portal apps table is missing 'app_name' column. Please check database schema.")
                return
                
            combined_df['in_portal'] = combined_df['app_name'].isin(portal_apps['app_name'].values)
            combined_df['active'] = combined_df['app_name'].apply(
                lambda x: portal_apps[portal_apps['app_name'] == x]['is_active'].iloc[0] 
                if x in portal_apps['app_name'].values else False
            )
    except Exception as e:
        st.error(f"Error creating combined view: {str(e)}")
        return
    
    st.markdown("### Available Streamlit Applications")
    st.markdown("Check the boxes to add/remove applications from the portal:")
    
    # Use data_editor to allow selection
    edited_df = st.data_editor(
        combined_df,
        column_config={
            "in_portal": st.column_config.CheckboxColumn(
                "In Portal",
                help="Check to include this app in the portal",
                default=False,
            ),
            "active": st.column_config.CheckboxColumn(
                "Active",
                help="Check to make this app active in the portal",
                default=True,
            ),
            "app_name": st.column_config.TextColumn(
                "App Name",
                help="Internal Streamlit app name",
                disabled=True,
            ),
            "app_title": st.column_config.TextColumn(
                "Display Title",
                help="Title shown in portal",
                max_chars=100,
            ),
            "database_name": st.column_config.TextColumn(
                "Database",
                help="Database where the app is located",
                disabled=True,
            ),
            "schema_name": st.column_config.TextColumn(
                "Schema", 
                help="Schema where the app is located",
                disabled=True,
            ),
        },
        column_order=["in_portal", "active", "app_title", "app_name", "database_name", "schema_name"],
        hide_index=True,
        use_container_width=True,
        key="app_editor"
    )
    
    # Check if there are any changes to save
    has_changes = not combined_df.equals(edited_df)
    
    # Save changes button with status
    col1, col2 = st.columns([1, 3])
    with col1:
        save_clicked = st.button("💾 Save Changes", type="primary", disabled=not has_changes)
    with col2:
        if has_changes:
            st.info("📝 You have unsaved changes")
        elif st.session_state.changes_saved:
            st.success("✅ All changes saved!")
            # Reset the flag after showing the message
            st.session_state.changes_saved = False
    
    if save_clicked:
        save_application_changes(conn, combined_df, edited_df)

def save_application_changes(conn, original_df, edited_df):
    """Save changes to portal applications"""
    try:
        changes_made = False
        
        for index, row in edited_df.iterrows():
            original_row = original_df.iloc[index]
            app_name = row['app_name']
            
            # Check if app was added to portal
            if row['in_portal'] and not original_row['in_portal']:
                # Insert new app
                url_id = row.get('url_id', '')
                database_name = row.get('database_name', '')
                schema_name = row.get('schema_name', '')
                
                if hasattr(conn, 'sql'):  # Snowpark session
                    conn.sql(f"""
                        INSERT INTO portal_apps (app_id, app_name, app_title, description, url_id, database_name, schema_name, is_active)
                        VALUES ('{app_name}', '{app_name}', '{row['app_title']}', '{row['description']}', '{url_id}', '{database_name}', '{schema_name}', {row['active']})
                    """).collect()
                else:  # Regular connection
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO portal_apps (app_id, app_name, app_title, description, url_id, database_name, schema_name, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (app_name, app_name, row['app_title'], row['description'], url_id, database_name, schema_name, row['active']))
                    cursor.close()
                changes_made = True
                
            # Check if app was removed from portal
            elif not row['in_portal'] and original_row['in_portal']:
                # Delete app and its access records
                if hasattr(conn, 'sql'):  # Snowpark session
                    conn.sql(f"DELETE FROM app_access WHERE app_id = '{app_name}'").collect()
                    conn.sql(f"DELETE FROM portal_apps WHERE app_id = '{app_name}'").collect()
                else:  # Regular connection
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM app_access WHERE app_id = %s", (app_name,))
                    cursor.execute("DELETE FROM portal_apps WHERE app_id = %s", (app_name,))
                    cursor.close()
                changes_made = True
                
            # Check if app details were updated
            elif row['in_portal'] and original_row['in_portal']:
                if (row['app_title'] != original_row['app_title'] or 
                    row['description'] != original_row['description'] or 
                    row['active'] != original_row['active']):
                    # Update existing app
                    if hasattr(conn, 'sql'):  # Snowpark session
                        conn.sql(f"""
                            UPDATE portal_apps 
                            SET app_title = '{row['app_title']}', 
                                description = '{row['description']}', 
                                is_active = {row['active']},
                                updated_at = CURRENT_TIMESTAMP()
                            WHERE app_id = '{app_name}'
                        """).collect()
                    else:  # Regular connection
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE portal_apps 
                            SET app_title = %s, description = %s, is_active = %s, updated_at = CURRENT_TIMESTAMP()
                            WHERE app_id = %s
                        """, (row['app_title'], row['description'], row['active'], app_name))
                        cursor.close()
                    changes_made = True
        
        if changes_made:
            # Set session state flag to show success message
            st.session_state.changes_saved = True
            # Clear cache to ensure fresh data on next load
            st.cache_data.clear()
            st.rerun()
        else:
            st.info("No changes detected.")
            
    except Exception as e:
        st.error(f"Error saving changes: {str(e)}")

def manage_access_permissions(conn):
    """Manage access permissions for portal applications"""
    st.markdown("Configure who can access each application in the portal.")
    
    # Get portal apps
    portal_apps = get_portal_apps(conn)
    
    if portal_apps.empty:
        st.info("No applications are configured in the portal yet. Please add some applications first.")
        return
    
    # ============ SECTION 1: SELECT APPLICATION ============
    # st.markdown("---")
    st.markdown("### 🔍 Select Application")
    selected_app = st.selectbox(
        "Choose an application to manage access for:",
        options=portal_apps['app_title'].tolist(),
        format_func=lambda x: x
    )
    
    if selected_app:
        app_row = portal_apps[portal_apps['app_title'] == selected_app].iloc[0]
        app_id = app_row['app_id']
        
        # ============ SECTION 2: MANAGE ACCESS ============
        st.markdown("---")
        st.markdown(f"### ⚙️ Manage Access for: **{selected_app}**")
        
        # Get current access permissions
        current_access = get_app_access(conn, app_id)
        
        # Load users and roles with caching for performance
        with st.spinner("Loading users and roles..."):
            all_users = get_all_users(conn)
            all_roles = get_all_roles(conn)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 👤 Add User Access")
            if all_users:
                selected_user = st.selectbox(
                    "Select User",
                    options=[""] + all_users,
                    format_func=lambda x: "Choose a user..." if x == "" else x,
                    key="user_select"
                )
                if st.button("Add User Access", disabled=not selected_user, type="primary"):
                    if selected_user:
                        add_access_permission(conn, app_id, 'USER', selected_user.upper())
            else:
                st.warning("No users found or unable to load users.")
            
        with col2:
            st.markdown("#### 🔑 Add Role Access")
            if all_roles:
                selected_role = st.selectbox(
                    "Select Role",
                    options=[""] + all_roles,
                    format_func=lambda x: "Choose a role..." if x == "" else x,
                    key="role_select"
                )
                if st.button("Add Role Access", disabled=not selected_role, type="primary"):
                    if selected_role:
                        add_access_permission(conn, app_id, 'ROLE', selected_role.upper())
            else:
                st.warning("No roles found or unable to load roles.")
        
        # ============ SECTION 3: CURRENT PERMISSIONS ============
        st.markdown("---")
        st.markdown("### 📋 Current Access Permissions")
        if not current_access.empty:
            # Create a more organized display
            st.markdown("#### 👤 Users with Access:")
            user_permissions = current_access[current_access['access_type'] == 'USER']
            if not user_permissions.empty:
                for index, row in user_permissions.iterrows():
                    col1, col2, col3 = st.columns([3, 2, 1])
                    with col1:
                        st.text(f"👤 {row['access_value']}")
                    with col2:
                        st.text(row['created_at'].strftime('%Y-%m-%d') if pd.notna(row['created_at']) else '')
                    with col3:
                        if st.button("🗑️", key=f"delete_user_{row['access_id']}", help="Remove user access"):
                            delete_access_permission(conn, row['access_id'])
                            st.rerun()
            else:
                st.info("No user permissions configured")
            
            st.markdown("#### 🔑 Roles with Access:")
            role_permissions = current_access[current_access['access_type'] == 'ROLE']
            if not role_permissions.empty:
                for index, row in role_permissions.iterrows():
                    col1, col2, col3 = st.columns([3, 2, 1])
                    with col1:
                        st.text(f"🔑 {row['access_value']}")
                    with col2:
                        st.text(row['created_at'].strftime('%Y-%m-%d') if pd.notna(row['created_at']) else '')
                    with col3:
                        if st.button("🗑️", key=f"delete_role_{row['access_id']}", help="Remove role access"):
                            delete_access_permission(conn, row['access_id'])
                            st.rerun()
            else:
                st.info("No role permissions configured")
        else:
            st.info("No access permissions configured for this application.")

def get_app_access(conn, app_id):
    """Get current access permissions for an app"""
    try:
        if hasattr(conn, 'sql'):  # Snowpark session
            df = conn.sql(f"""
                SELECT access_id, access_type, access_value, created_at
                FROM app_access 
                WHERE app_id = '{app_id}'
                ORDER BY access_type, access_value
            """).to_pandas()
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute("""
                SELECT access_id, access_type, access_value, created_at
                FROM app_access 
                WHERE app_id = %s
                ORDER BY access_type, access_value
            """, (app_id,))
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(cursor.fetchall(), columns=columns)
            cursor.close()
        
        # Handle column name casing like in get_portal_apps
        if not df.empty:
            column_mapping = {}
            for col in df.columns:
                if col.upper() == 'ACCESS_ID':
                    column_mapping[col] = 'access_id'
                elif col.upper() == 'ACCESS_TYPE':
                    column_mapping[col] = 'access_type'
                elif col.upper() == 'ACCESS_VALUE':
                    column_mapping[col] = 'access_value'
                elif col.upper() == 'CREATED_AT':
                    column_mapping[col] = 'created_at'
            
            if column_mapping:
                df = df.rename(columns=column_mapping)
        
        return df
    except Exception as e:
        st.error(f"Error getting app access: {str(e)}")
        return pd.DataFrame()

def add_access_permission(conn, app_id, access_type, access_value):
    """Add new access permission"""
    try:
        if hasattr(conn, 'sql'):  # Snowpark session
            conn.sql(f"""
                INSERT INTO app_access (app_id, access_type, access_value)
                VALUES ('{app_id}', '{access_type}', '{access_value}')
            """).collect()
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO app_access (app_id, access_type, access_value)
                VALUES (%s, %s, %s)
            """, (app_id, access_type, access_value))
            cursor.close()
        
        st.success(f"✅ Added {access_type.lower()} access for {access_value}")
        st.rerun()
        
    except Exception as e:
        if "duplicate key" in str(e).lower():
            st.error(f"Access permission already exists for {access_value}")
        else:
            st.error(f"Error adding access permission: {str(e)}")

def delete_access_permission(conn, access_id):
    """Delete access permission"""
    try:
        if hasattr(conn, 'sql'):  # Snowpark session
            conn.sql(f"DELETE FROM app_access WHERE access_id = {access_id}").collect()
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute("DELETE FROM app_access WHERE access_id = %s", (access_id,))
            cursor.close()
        
        st.success("✅ Access permission deleted")
        
    except Exception as e:
        st.error(f"Error deleting access permission: {str(e)}")

def show_access_overview(conn):
    """Show comprehensive overview of all apps and their access permissions"""
    st.markdown("Complete overview of all portal applications and their access permissions.")
    
    # Get all portal apps and their access permissions
    portal_apps = get_portal_apps(conn)
    
    if portal_apps.empty:
        st.info("No applications are configured in the portal yet.")
        return
    
    # Create comprehensive access overview
    overview_data = []
    
    for _, app in portal_apps.iterrows():
        app_id = app['app_id']
        app_title = app['app_title']
        app_name = app['app_name']
        is_active = app['is_active']
        
        # Get access permissions for this app
        access_permissions = get_app_access(conn, app_id)
        
        if access_permissions.empty:
            # App has no permissions
            overview_data.append({
                'App Title': app_title,
                'App Name': app_name,
                'Status': '🟢 Active' if is_active else '🔴 Inactive',
                'Access Type': 'None',
                'Access Value': 'No permissions configured',
                'Created': ''
            })
        else:
            # App has permissions
            for _, perm in access_permissions.iterrows():
                icon = '👤' if perm['access_type'] == 'USER' else '🔑'
                overview_data.append({
                    'App Title': app_title,
                    'App Name': app_name,
                    'Status': '🟢 Active' if is_active else '🔴 Inactive',
                    'Access Type': f"{icon} {perm['access_type']}",
                    'Access Value': perm['access_value'],
                    'Created': perm['created_at'].strftime('%Y-%m-%d') if pd.notna(perm['created_at']) else ''
                })
    
    if overview_data:
        overview_df = pd.DataFrame(overview_data)
        
        # Add filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status_filter = st.selectbox(
                "Filter by Status",
                options=["All", "Active Only", "Inactive Only"]
            )
        
        with col2:
            access_filter = st.selectbox(
                "Filter by Access Type",
                options=["All", "Users Only", "Roles Only", "No Permissions"]
            )
        
        with col3:
            app_filter = st.selectbox(
                "Filter by App",
                options=["All"] + sorted(overview_df['App Title'].unique().tolist())
            )
        
        # Apply filters
        filtered_df = overview_df.copy()
        
        if status_filter == "Active Only":
            filtered_df = filtered_df[filtered_df['Status'].str.contains('Active')]
        elif status_filter == "Inactive Only":
            filtered_df = filtered_df[filtered_df['Status'].str.contains('Inactive')]
        
        if access_filter == "Users Only":
            filtered_df = filtered_df[filtered_df['Access Type'].str.contains('USER')]
        elif access_filter == "Roles Only":
            filtered_df = filtered_df[filtered_df['Access Type'].str.contains('ROLE')]
        elif access_filter == "No Permissions":
            filtered_df = filtered_df[filtered_df['Access Value'] == 'No permissions configured']
        
        if app_filter != "All":
            filtered_df = filtered_df[filtered_df['App Title'] == app_filter]
        
        # Display the table
        st.markdown(f"### Access Overview ({len(filtered_df)} entries)")
        st.dataframe(
            filtered_df,
            use_container_width=True,
            hide_index=True
        )
        
        # Summary statistics
        st.markdown("### Summary Statistics")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_apps = len(portal_apps)
            st.metric("Total Apps", total_apps)
        
        with col2:
            active_apps = len(portal_apps[portal_apps['is_active'] == True])
            st.metric("Active Apps", active_apps)
        
        with col3:
            total_permissions = len([row for row in overview_data if row['Access Value'] != 'No permissions configured'])
            st.metric("Total Permissions", total_permissions)
        
        with col4:
            apps_without_permissions = len(portal_apps) - len(overview_df[overview_df['Access Value'] != 'No permissions configured']['App Title'].unique())
            st.metric("Apps Without Access", apps_without_permissions)

def manage_portal_settings(conn, user_info):
    """Manage general portal settings"""
    st.markdown("Configure general portal settings and maintenance tasks.")
    
    # Get portal apps for image management
    portal_apps = get_portal_apps(conn)
    
    # Image Management Section
    try:
        from simple_image_manager import show_simple_image_management
        show_simple_image_management(conn, portal_apps)
    except ImportError:
        st.error("Image management module not found. Please ensure simple_image_manager.py exists.")
    except Exception as e:
        st.error(f"Error loading image management: {str(e)}")
    
    st.markdown("---")
    
    # Database Maintenance Section
    st.markdown("### 🔧 Database Maintenance")
    
    # First row of maintenance functions
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Refresh Cache"):
            # Clear Streamlit cache
            st.cache_data.clear()
            st.cache_resource.clear()
            st.success("✅ Cache cleared successfully!")
    
    with col2:
        if st.button("📊 View Portal Statistics"):
            show_portal_statistics(conn)
    
    st.markdown("---")
    
    # User Roles Troubleshooting Section
    st.markdown("### 🔍 User Roles Troubleshooting")
    st.markdown("Display current user's roles for troubleshooting access issues.")
    
    # Display user information (using user_info passed from main app)
    st.markdown(f"**Current User:** {user_info['username']}")
    
    if user_info['roles']:
        st.markdown("**Your Assigned Roles:**")
        for i, role in enumerate(user_info['roles'], 1):
            st.markdown(f"{i}. `{role}`")
    else:
        st.warning("No roles found for current user.")
    
    # Show whether user is admin (check admin roles in user's role list)
    admin_roles = ['STREAMLITPORTALADMINS', 'ACCOUNTADMIN']
    user_roles_upper = [role.upper() for role in user_info['roles']]
    is_admin = any(admin_role in user_roles_upper for admin_role in admin_roles)
    admin_status = "✅ Yes" if is_admin else "❌ No"
    st.markdown(f"**Portal Administrator:** {admin_status}")
    
    if st.button("🔄 Refresh User Roles"):
        # Clear any cached user info and refresh
        st.cache_data.clear()
        st.rerun()
    


def show_portal_statistics(conn):
    """Show portal usage statistics"""
    try:
        if hasattr(conn, 'sql'):  # Snowpark session
            stats_query = """
                SELECT 
                    COUNT(*) as total_apps,
                    SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_apps,
                    (SELECT COUNT(*) FROM app_access) as total_permissions
                FROM portal_apps
            """
            stats_df = conn.sql(stats_query).to_pandas()
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_apps,
                    SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_apps,
                    (SELECT COUNT(*) FROM app_access) as total_permissions
                FROM portal_apps
            """)
            stats_df = pd.DataFrame(cursor.fetchall(), columns=['total_apps', 'active_apps', 'total_permissions'])
            cursor.close()
        
        if not stats_df.empty:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Apps", stats_df.iloc[0]['total_apps'])
            with col2:
                st.metric("Active Apps", stats_df.iloc[0]['active_apps'])
            with col3:
                st.metric("Access Permissions", stats_df.iloc[0]['total_permissions'])
        
    except Exception as e:
        st.error(f"Error getting portal statistics: {str(e)}") 