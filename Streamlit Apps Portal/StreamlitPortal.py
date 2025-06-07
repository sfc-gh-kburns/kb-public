import streamlit as st
import streamlit.components.v1
import pandas as pd
import snowflake.connector
import tomli
from snowflake.snowpark.context import get_active_session
import io
from PIL import Image
import base64
from io import BytesIO

# Determine initial sidebar state based on previous admin status
initial_sidebar_state = "expanded" if st.session_state.get("is_admin", False) else "collapsed"

# Set page config
st.set_page_config(
    page_title="Streamlit Apps Portal",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state=initial_sidebar_state
)

# Function to get Snowflake connection - cached to prevent multiple MFA requests
@st.cache_resource(show_spinner="Connecting to Snowflake...")
def get_snowflake_connection():
    # First try to get active session (for Streamlit in Snowflake)
    print("Getting active session")
    try:
        session = get_active_session()
        if session:
            # We're in Streamlit in Snowflake
            st.session_state.is_sis = True
            return session
    except Exception:
        # If get_active_session fails, continue to local connection
        # We're NOT in Streamlit in Snowflake
        st.session_state.is_sis = False
        pass
            
    # Try local connection
    try:
        print("Getting Connections")
        with open('/Users/kburns/.snowflake/connections.toml', 'rb') as f:
            config = tomli.load(f)
            print(f'config: {config}')
        print("Getting Config")

        # Get the default connection name
        default_conn = config.get('kb_demo')
        print(f'default_conn: {default_conn}')
        if not default_conn:
            print("No default connection specified in connections.toml")
            return None

        conn = snowflake.connector.connect(**default_conn)
        return conn
        
    except Exception as e:
        print(f"Failed to connect to Snowflake using local config: {str(e)}")
        return None

# Function to initialize database schema
@st.cache_resource
def initialize_database_schema(_conn):
    """Initialize database schema for portal - create tables if they don't exist"""
    try:
        if hasattr(_conn, 'sql'):  # Snowpark session
            # Only use USE DATABASE if we're NOT in Streamlit in Snowflake
            if not st.session_state.get('is_sis', False):
                _conn.sql("USE DATABASE STREAMLITPORTAL").collect()
            
            # Create portal_apps table with all required columns
            _conn.sql("""
                CREATE TABLE IF NOT EXISTS portal_apps (
                    app_id VARCHAR(255) PRIMARY KEY,
                    app_name VARCHAR(255) NOT NULL,
                    app_title VARCHAR(255) NOT NULL,
                    description TEXT,
                    image_path TEXT,
                    url_id VARCHAR(255),
                    database_name VARCHAR(255),
                    schema_name VARCHAR(255),
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """).collect()
            
            # Migrate existing image_path column if needed (for existing installations)
            try:
                _conn.sql("ALTER TABLE portal_apps ALTER COLUMN image_path SET DATA TYPE TEXT").collect()
            except:
                pass  # Column might already be TEXT or table might not exist yet
            
            # Create app_access table
            _conn.sql("""
                CREATE TABLE IF NOT EXISTS app_access (
                    access_id NUMBER IDENTITY(1,1) PRIMARY KEY,
                    app_id VARCHAR(255) NOT NULL,
                    access_type VARCHAR(20) NOT NULL, -- 'USER' or 'ROLE'
                    access_value VARCHAR(255) NOT NULL, -- username or role name
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    FOREIGN KEY (app_id) REFERENCES portal_apps(app_id)
                )
            """).collect()
            
            # Note: Images are now stored as base64 data in the database
            # No stage creation needed
            
        else:  # Regular connection
            cursor = _conn.cursor()
            # Only use USE DATABASE if we're NOT in Streamlit in Snowflake
            if not st.session_state.get('is_sis', False):
                cursor.execute("USE DATABASE STREAMLITPORTAL")
            
            # Create portal_apps table with all required columns
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS portal_apps (
                    app_id VARCHAR(255) PRIMARY KEY,
                    app_name VARCHAR(255) NOT NULL,
                    app_title VARCHAR(255) NOT NULL,
                    description TEXT,
                    image_path TEXT,
                    url_id VARCHAR(255),
                    database_name VARCHAR(255),
                    schema_name VARCHAR(255),
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            
            # Migrate existing image_path column if needed (for existing installations)
            try:
                cursor.execute("ALTER TABLE portal_apps ALTER COLUMN image_path SET DATA TYPE TEXT")
            except:
                pass  # Column might already be TEXT or table might not exist yet
            
            # Create app_access table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS app_access (
                    access_id NUMBER IDENTITY(1,1) PRIMARY KEY,
                    app_id VARCHAR(255) NOT NULL,
                    access_type VARCHAR(20) NOT NULL, -- 'USER' or 'ROLE'
                    access_value VARCHAR(255) NOT NULL, -- username or role name
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    FOREIGN KEY (app_id) REFERENCES portal_apps(app_id)
                )
            """)
            
            # Note: Images are now stored as base64 data in the database
            # No stage creation needed
            
            cursor.close()
            
        return True
    except Exception as e:
        st.error(f"Error initializing database schema: {str(e)}")
        return False



def load_image_from_database(conn, image_path):
    """Load image from base64 data stored in database"""
    try:
        if not image_path:
            return None
        
        # Handle base64 data only - clean and fast
        if image_path.startswith('base64:'):
            base64_data = image_path.replace('base64:', '')
            try:
                # Decode base64 data
                image_bytes = base64.b64decode(base64_data)
                # Convert to PIL Image for display
                image = Image.open(io.BytesIO(image_bytes))
                return image
            except Exception as e:
                print(f"Could not decode base64 image data: {str(e)}")
                return None
        
        # No image found
        return None
                
    except Exception as e:
        print(f"Could not load image: {str(e)}")
        return None

# Function to get current user info
def get_current_user_info(_conn):
    """Get current user and all their assigned roles"""
    try:
        if hasattr(_conn, 'sql'):  # Snowpark session
            # Get current user
            user_info = _conn.sql("SELECT CURRENT_USER() as username").to_pandas()
            
            # Get all roles assigned to this user from account_usage
            roles_query = """
                SELECT DISTINCT ROLE 
                FROM snowflake.account_usage.grants_to_users 
                WHERE granted_to = 'USER' AND GRANTEE_NAME = CURRENT_USER()
            """
            roles_df = _conn.sql(roles_query).to_pandas()
            
        else:  # Regular connection
            cursor = _conn.cursor()
            # Get current user
            cursor.execute("SELECT CURRENT_USER() as username")
            user_info = pd.DataFrame(cursor.fetchall(), columns=['username'])
            
            # Get all roles assigned to this user from account_usage
            roles_query = """
                SELECT DISTINCT ROLE 
                FROM snowflake.account_usage.grants_to_users 
                WHERE granted_to = 'USER' AND GRANTEE_NAME = CURRENT_USER()
            """
            cursor.execute(roles_query)
            roles_data = cursor.fetchall()
            roles_df = pd.DataFrame(roles_data, columns=['ROLE'])
            cursor.close()
            
        # Build roles list from database
        all_roles = ['PUBLIC']  # Everyone has PUBLIC role
        
        if not roles_df.empty:
            # Normalize column names for consistency
            roles_df.columns = roles_df.columns.astype(str).str.upper()
            user_roles = roles_df['ROLE'].tolist()
            
            # Add discovered roles
            for role in user_roles:
                if role and role not in all_roles:
                    all_roles.append(role)
            
        # Handle username column case differences  
        if 'username' in user_info.columns:
            username = user_info.iloc[0]['username']
        elif 'USERNAME' in user_info.columns:
            username = user_info.iloc[0]['USERNAME']
        else:
            username = 'UNKNOWN'  # Fallback
            
        return {
            'username': username,
            'roles': all_roles
        }
    except Exception as e:
        st.error(f"Error getting user info: {str(e)}")
        return {'username': 'UNKNOWN', 'roles': ['PUBLIC']}

# Function to check if user is admin
def is_portal_admin(user_info):
    """Check if user is part of StreamlitPortalAdmins or AccountAdmin groups"""
    admin_roles = ['STREAMLITPORTALADMINS', 'ACCOUNTADMIN']
    user_roles_upper = [role.upper() for role in user_info['roles']]
    return any(admin_role in user_roles_upper for admin_role in admin_roles)

# Function to get accessible apps for current user
def get_accessible_apps(_conn, user_info):
    """Get apps accessible to current user based on roles and username"""
    try:
        # Build query to get apps user has access to
        user_roles_str = "'" + "','".join(user_info['roles']) + "'"
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            query = f"""
                SELECT DISTINCT pa.app_id, pa.app_name, pa.app_title, pa.description, pa.image_path, pa.url_id, pa.database_name, pa.schema_name
                FROM portal_apps pa
                INNER JOIN app_access aa ON pa.app_id = aa.app_id
                WHERE pa.is_active = TRUE
                AND (
                    (aa.access_type = 'USER' AND UPPER(aa.access_value) = UPPER('{user_info['username']}'))
                    OR (aa.access_type = 'ROLE' AND UPPER(aa.access_value) IN ({user_roles_str.upper()}))
                )
                ORDER BY pa.app_title
            """
            df = _conn.sql(query).to_pandas()
        else:  # Regular connection
            cursor = _conn.cursor()
            query = f"""
                SELECT DISTINCT pa.app_id, pa.app_name, pa.app_title, pa.description, pa.image_path, pa.url_id, pa.database_name, pa.schema_name
                FROM portal_apps pa
                INNER JOIN app_access aa ON pa.app_id = aa.app_id
                WHERE pa.is_active = TRUE
                AND (
                    (aa.access_type = 'USER' AND UPPER(aa.access_value) = UPPER('{user_info['username']}'))
                    OR (aa.access_type = 'ROLE' AND UPPER(aa.access_value) IN ({user_roles_str.upper()}))
                )
                ORDER BY pa.app_title
            """
            cursor.execute(query)
            df = pd.DataFrame(cursor.fetchall(), columns=['app_id', 'app_name', 'app_title', 'description', 'image_path', 'url_id', 'database_name', 'schema_name'])
            cursor.close()
        
        # Normalize column names for SiS compatibility
        if not df.empty:
            df.columns = df.columns.astype(str).str.lower()
            
        return df
    except Exception as e:
        st.error(f"Error getting accessible apps: {str(e)}")
        return pd.DataFrame()

# Function to get current account and organization info for Streamlit apps
@st.cache_data(ttl=3600)
def get_snowflake_app_info(_conn):
    """Get the current Snowflake organization and account info for app URLs"""
    try:
        if hasattr(_conn, 'sql'):  # Snowpark session
            result = _conn.sql("SELECT CURRENT_ORGANIZATION_NAME() as ORGANIZATION, CURRENT_ACCOUNT_NAME() as ACCOUNT").to_pandas()
        else:  # Regular connection
            cursor = _conn.cursor()
            cursor.execute("SELECT CURRENT_ORGANIZATION_NAME() as ORGANIZATION, CURRENT_ACCOUNT_NAME() as ACCOUNT")
            result = pd.DataFrame(cursor.fetchall(), columns=['ORGANIZATION', 'ACCOUNT'])
            cursor.close()
        
        if not result.empty:
            organization = result.iloc[0]['ORGANIZATION']
            account = result.iloc[0]['ACCOUNT']
            return organization, account
        return None, None
    except Exception as e:
        st.error(f"Error getting Snowflake app info: {str(e)}")
        return None, None

# Function to construct Streamlit app URL
def construct_streamlit_url(organization, account, database, schema, app_name):
    """Construct the full URL for a Streamlit app in Snowflake"""
    if not organization or not account:
        return None
    
    # Snowflake app URL format: https://app.snowflake.com/{organization}/{account}/#/streamlit-apps/{database}.{schema}.{app_name}
    if app_name and database and schema:
        return f"https://app.snowflake.com/{organization}/{account}/#/streamlit-apps/{database}.{schema}.{app_name}"
    else:
        # No URL can be constructed without required components
        return None

# Function to create JavaScript for opening URL in new tab
def create_launch_button(app_title, app_url, button_key):
    """Create a button that launches the app in a new tab"""
    if not app_url:
        if st.button(f"Launch", key=button_key, use_container_width=True):
            st.error("Unable to generate launch URL for this application")
        return
    
    # Create HTML/JavaScript to open in new tab
    launch_html = f"""
    <button onclick="window.open('{app_url}', '_blank')" 
            style="
                width: 100%;
                padding: 0.5rem 1rem;
                background-color: #ff4b4b;
                color: white;
                border: none;
                border-radius: 0.5rem;
                cursor: pointer;
                font-size: 14px;
                font-weight: 500;
            ">
        🚀 Launch
    </button>
    """
    
    # Also provide a regular Streamlit button as fallback
    col1, col2 = st.columns([3, 1])
    with col1:
        st.components.v1.html(launch_html, height=40)
    with col2:
        if st.button("📋", key=f"copy_{button_key}", help="Copy URL"):
            st.code(app_url)

# Function to display app grid
def display_app_grid(apps_df, conn):
    """Display apps in a 4-column grid layout"""
    if apps_df.empty:
        st.info("No applications are currently available to you. Please contact your administrator.")
        return
    
    # Get organization and account info for constructing app URLs
    organization, account = get_snowflake_app_info(conn)
    
    # Display apps in responsive grid (4 columns on desktop, fewer on mobile)
    for i in range(0, len(apps_df), 4):
        cols = st.columns(4, gap="medium")
        for j, col in enumerate(cols):
            if i + j < len(apps_df):
                app = apps_df.iloc[i + j]
                with col:
                    # Create app card with natural spacing
                    with st.container(border=True):
                        
                        # Construct app URL first
                        database_name = app.get('database_name', '')
                        schema_name = app.get('schema_name', '')
                        app_name = app['app_name']
                        
                        app_url = construct_streamlit_url(
                            organization,
                            account,
                            database_name,
                            schema_name,
                            app_name
                        )
                        
                        # Create clickable image
                        image_placeholder_url = "https://via.placeholder.com/200x200?text=No+Image"
                        
                        if app['image_path']:
                            try:
                                # Try to load the actual image from database
                                image_data = load_image_from_database(conn, app['image_path'])
                                if image_data:
                                    # Resize image to consistent dimensions (200x200) maintaining aspect ratio
                                    from PIL import Image as PILImage
                                    # Resize image to fit in 200x200 box while maintaining aspect ratio
                                    image_data.thumbnail((200, 200), PILImage.Resampling.LANCZOS)
                                    
                                    # Create a new 200x200 image with light gray background and center the resized image
                                    new_image = PILImage.new('RGB', (200, 200), (248, 249, 250))
                                    # Calculate position to center the image
                                    x = (200 - image_data.width) // 2
                                    y = (200 - image_data.height) // 2
                                    # Handle transparency by converting to RGB if needed
                                    if image_data.mode in ('RGBA', 'LA') or (image_data.mode == 'P' and 'transparency' in image_data.info):
                                        # Create a white background for transparent images
                                        background = PILImage.new('RGB', image_data.size, (248, 249, 250))
                                        if image_data.mode == 'P':
                                            image_data = image_data.convert('RGBA')
                                        background.paste(image_data, mask=image_data.split()[-1])  # Use alpha channel as mask
                                        new_image.paste(background, (x, y))
                                    else:
                                        new_image.paste(image_data, (x, y))
                                    
                                    # Convert PIL image to base64 for HTML embedding
                                    import io
                                    import base64
                                    img_buffer = io.BytesIO()
                                    new_image.save(img_buffer, format='PNG')
                                    img_str = base64.b64encode(img_buffer.getvalue()).decode()
                                    image_src = f"data:image/png;base64,{img_str}"
                                else:
                                    image_src = "https://via.placeholder.com/200x200?text=Custom+Image"
                            except Exception as e:
                                image_src = "https://via.placeholder.com/200x200?text=Image+Error"
                        else:
                            image_src = image_placeholder_url
                        
                        # Create clickable image and title HTML with accessibility and hover effects
                        if app_url:
                            # Escape title for HTML safety
                            safe_title = app['app_title'].replace('"', '&quot;').replace("'", "&#39;")
                            clickable_content = f"""
                            <div style="
                                text-align: center; 
                                cursor: pointer; 
                                transition: transform 0.2s ease, box-shadow 0.2s ease;
                                border-radius: 8px;
                                padding: 8px;
                            " 
                            onclick="window.open('{app_url}', '_blank')"
                            onmouseover="this.style.transform='scale(1.02)'; this.style.boxShadow='0 4px 12px rgba(0,0,0,0.1)'"
                            onmouseout="this.style.transform='scale(1)'; this.style.boxShadow='none'"
                            role="button"
                            tabindex="0"
                            aria-label="Launch {safe_title} application">
                                <img src="{image_src}" 
                                     style="width: 200px; height: 200px; border-radius: 2px; margin-bottom: 2px;"
                                     alt="Icon for {safe_title}"
                                     title="Click to launch {safe_title}" />
                                <div style="font-weight: bold; font-size: 26px; margin-bottom: 4px; color: #333;">
                                    {safe_title}
                                </div>
                            </div>
                            """
                            st.components.v1.html(clickable_content, height=280)
                        else:
                            # Fallback for apps without valid URLs
                            st.image(image_src, width=200)
                            st.markdown(f"**{app['app_title']}**")
                        
                        # Create launch button (still functional as backup)
                        create_launch_button(
                            app['app_title'], 
                            app_url, 
                            f"launch_{app['app_id']}"
                        )

# Main application
def main():
    # Get Snowflake connection
    conn = get_snowflake_connection()
    if not conn:
        st.error("Failed to connect to Snowflake. Please check your connection parameters.")
        st.stop()
    
    # Initialize database schema
    if not initialize_database_schema(conn):
        st.error("Failed to initialize database schema.")
        st.stop()
    
    # Get current user information
    user_info = get_current_user_info(conn)
    
    # Store admin status in session state for future sidebar state determination
    st.session_state.is_admin = is_portal_admin(user_info)
    
    # Sidebar navigation
    st.sidebar.title("🚀 Streamlit Apps Portal")
    st.sidebar.markdown(f"Welcome, **{user_info['username']}**")
    
    # Show helpful note for non-admin users
    if not st.session_state.is_admin:
        st.sidebar.info("💡 You can collapse this sidebar by clicking the arrow (→) at the top.")
    
    # Initialize session state for page navigation
    if 'current_page' not in st.session_state:
        st.session_state.current_page = "Portal"
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Navigation")
    
    # Navigation buttons
    if st.sidebar.button("🏠 Portal", use_container_width=True, type="primary" if st.session_state.current_page == "Portal" else "secondary"):
        st.session_state.current_page = "Portal"
        st.rerun()
    
    if is_portal_admin(user_info):
        if st.sidebar.button("⚙️ Portal Configuration", use_container_width=True, type="primary" if st.session_state.current_page == "Portal Configuration" else "secondary"):
            st.session_state.current_page = "Portal Configuration"
            st.rerun()
    
    if st.sidebar.button("📚 Documentation", use_container_width=True, type="primary" if st.session_state.current_page == "Documentation" else "secondary"):
        st.session_state.current_page = "Documentation"
        st.rerun()
    
    selected_page = st.session_state.current_page
    
    # Main content area
    if selected_page == "Portal":
        st.title("🚀 Streamlit Apps Portal")
        st.markdown("Welcome to your personalized application portal. Click on any app below to launch it.")
        
        # Get and display accessible apps
        with st.spinner("Loading your applications..."):
            apps_df = get_accessible_apps(conn, user_info)
            

            
            display_app_grid(apps_df, conn)
    
    elif selected_page == "Portal Configuration":
        st.title("⚙️ Portal Configuration")
        st.markdown("Configure applications and access permissions for the portal.")
        # Import and display portal configuration
        try:
            from portal_config import show_portal_config
            show_portal_config(conn, user_info)
        except ImportError:
            st.error("Portal Configuration module not found. Please ensure portal_config.py exists.")
    
    elif selected_page == "Documentation":
        st.title("📚 User Documentation")
        st.markdown("""
        ## How to Use the Streamlit Apps Portal
        
        ### For End Users:
        1. **Portal Page**: View all applications you have access to
        2. **Launching Apps**: Click on the app image, title, or "Launch" button to open the application in a new tab
        3. **Access**: You can only see applications that you have been granted access to based on your username or Snowflake roles
        
        ### For Administrators:
        If you are a member of the `StreamlitPortalAdmins` or `AccountAdmin` role, you will see an additional "Portal Configuration" page where you can:
        
        1. **Add Applications**: Select from all available Streamlit applications in your Snowflake account
        2. **Manage Access**: Grant access to applications based on:
           - Individual usernames
           - Snowflake roles (including PUBLIC for all users)
        3. **Upload Images**: Add custom images for each application to make the portal more visually appealing
        4. **Enable/Disable Apps**: Control which applications are active in the portal
        
        ### Security:
        - Access is controlled through Snowflake roles and individual user permissions
        - Only users with the `StreamlitPortalAdmins` or `AccountAdmin` roles can modify portal configuration
        - All users automatically have the PUBLIC role for applications that should be accessible to everyone
        
        ### Support:
        If you need access to additional applications or have questions about the portal, please contact your Streamlit administrator.
        """)

if __name__ == "__main__":
    main() 