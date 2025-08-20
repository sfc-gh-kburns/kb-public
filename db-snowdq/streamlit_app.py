"""
Snowflake Data Quality & Documentation App - Single Page Version
A comprehensive Streamlit application for managing data documentation and quality monitoring in Snowflake.

Optimized for Streamlit in Snowflake (SiS) deployment with fallback support for local development.

SiS Compatibility Features:
- Uses INFORMATION_SCHEMA queries instead of SHOW commands for better permission compatibility
- Implements fallback mechanisms for environments with restricted SHOW command access
- Handles Owner's Rights Model limitations in SiS environments
"""

import streamlit as st
import pandas as pd
import snowflake.connector
import tomli
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta
import signal
import functools
import warnings
import os
import time
from typing import List

# Suppress pandas SQLAlchemy warning for Snowflake connections
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable.*')
from typing import Any, List, Dict, Optional, Tuple
import re
import time

# ========================================================================================
# PAGE CONFIG AND STYLING
# ========================================================================================

st.set_page_config(
    page_title="Snowflake Data Quality & Documentation",
    page_icon="🔺",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Professional CSS styling
st.markdown("""
<style>
    /* Import modern fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Root variables for consistent theming */
    :root {
        --primary-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        --secondary-gradient: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        --accent-gradient: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        --surface-color: #ffffff;
        --surface-secondary: #f8fafc;
        --text-primary: #1e293b;
        --text-secondary: #64748b;
        --text-muted: #94a3b8;
        --border-color: #e2e8f0;
        --border-light: #f1f5f9;
        --success-color: #10b981;
        --warning-color: #f59e0b;
        --error-color: #ef4444;
        --info-color: #3b82f6;
    }
    
    /* Global styles */
    .stApp {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        background: var(--surface-color);
    }
    
    /* Header styling */
    .main-header {
        background: #ffffff;
        color: black;
        margin: -1rem -1rem 2rem -1rem;
        text-align: center;
        padding: 2rem;
        border-bottom: 1px solid var(--border-light);
    }
    
    .main-header h1 {
        font-size: 3rem;
        font-weight: 700;
        background: var(--primary-gradient);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin: 0;
        text-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    
    .main-header p {
        font-size: 1.1rem;
        margin: 0.5rem 0 0 0;
        color: var(--text-secondary);
        font-weight: 400;
    }
    
    /* Metric cards */
    div[data-testid="metric-container"] {
        background: var(--surface-color);
        border: 1px solid var(--border-light);
        border-radius: 1rem;
        padding: 1.5rem;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        transition: all 0.2s ease;
    }
    
    div[data-testid="metric-container"]:hover {
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        transform: translateY(-1px);
    }
    
    /* Navigation button styling */
    .stButton > button {
        border-radius: 0.75rem;
        padding: 0.75rem 1.5rem;
        font-weight: 600;
        font-size: 0.95rem;
        transition: all 0.3s ease;
        border: 2px solid transparent;
        position: relative;
    }
    
    /* Primary (active) navigation buttons */
    .stButton > button[kind="primary"] {
        background: var(--primary-gradient) !important;
        color: white !important;
        border: 2px solid #5a67d8 !important;
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3) !important;
        transform: translateY(-2px) !important;
    }
    
    /* Secondary (inactive) navigation buttons */
    .stButton > button[kind="secondary"] {
        background: var(--surface-secondary) !important;
        color: var(--text-primary) !important;
        border: 2px solid var(--border-color) !important;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1) !important;
    }
    
    /* Hover effects */
    .stButton > button:hover {
        transform: translateY(-1px) !important;
        box-shadow: 0 6px 16px rgba(0,0,0,0.15) !important;
    }
    
    .stButton > button[kind="primary"]:hover {
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4) !important;
        transform: translateY(-3px) !important;
    }
    
    /* Compact layout adjustments */
    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 1rem !important;
    }
    
    /* Reduce spacing between elements */
    .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
        margin-top: 0.5rem !important;
        margin-bottom: 0.75rem !important;
    }
    
    /* Navigation bar spacing */
    div[data-testid="column"] {
        padding: 0 0.25rem !important;
    }
    
    /* DataFrame styling */
    .stDataFrame {
        border-radius: 0.75rem;
        overflow: hidden;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
        background-color: var(--surface-secondary);
        border-radius: 0.5rem;
        color: var(--text-secondary);
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background: var(--primary-gradient);
        color: white;
    }
    
    /* Alert styling */
    .stSuccess {
        background: linear-gradient(135deg, #ecfdf5 0%, #f0fdf4 100%);
        border: 1px solid #bbf7d0;
        border-radius: 0.75rem;
        color: var(--success-color);
    }
    
    .stInfo {
        background: linear-gradient(135deg, #eff6ff 0%, #f0f9ff 100%);
        border: 1px solid #bfdbfe;
        border-radius: 0.75rem;
        color: var(--info-color);
    }
    
    .stWarning {
        background: linear-gradient(135deg, #fffbeb 0%, #fefce8 100%);
        border: 1px solid #fed7aa;
        border-radius: 0.75rem;
        color: var(--warning-color);
    }
    
    .stError {
        background: linear-gradient(135deg, #fef2f2 0%, #fef1f1 100%);
        border: 1px solid #fecaca;
        border-radius: 0.75rem;
        color: var(--error-color);
    }
</style>
""", unsafe_allow_html=True)

# ========================================================================================
# UTILITY CLASSES AND FUNCTIONS
# ========================================================================================

class TimeoutError(Exception):
    """Custom timeout exception."""
    pass

def timeout(seconds=30, error_message="Query timed out"):
    """
    Decorator to add timeout to function calls.
    Note: This only works on Unix-like systems (not Windows).
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Skip timeout on Windows or in environments where signal is not available
            try:
                def timeout_handler(signum, frame):
                    raise TimeoutError(error_message)
                
                # Set the signal handler and alarm
                old_handler = signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(seconds)
                
                try:
                    result = func(*args, **kwargs)
                finally:
                    # Reset the alarm and handler
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, old_handler)
                
                return result
                
            except (AttributeError, ValueError):
                # signal.SIGALRM not available (Windows) or other signal issues
                # Just run the function without timeout
                return func(*args, **kwargs)
            except TimeoutError:
                st.warning(f"Query timed out after {seconds} seconds. Using fallback data.")
                raise
                
        return wrapper
    return decorator

# ========================================================================================
# SNOWFLAKE CONNECTION AND UTILITIES
# ========================================================================================

@st.cache_resource(show_spinner="Connecting to Snowflake...")
def get_snowflake_connection():
    # First try to get active session (for Streamlit in Snowflake)
    print("Getting active session")
    try:
        session = get_active_session()
        if session:
            # Note: ALTER SESSION commands are not supported in SiS due to security restrictions
            # Execute identification query
            try:
                result = session.sql("SELECT 'SNOWDQ_SIS_LAUNCH' AS launch_type").collect()
            except Exception as e:
                print(f"Failed to execute SiS identification query: {str(e)}")
            return session
    except Exception:
        # If get_active_session fails, continue to local connection
        pass
            
    # Try local connection
    try:
        with open(os.path.expanduser('~/.snowflake/connections.toml'), 'rb') as f:
            config = tomli.load(f)

        # Get the default connection name
        default_conn = config.get('kb_demo')
        # print(f'default_conn: {default_conn}')
        if not default_conn:
            print("No default connection specified in connections.toml")
            return None

        conn = snowflake.connector.connect(**default_conn)
        
        # Set query tag for OSS Streamlit
        try:
            cursor = conn.cursor()
            cursor.execute("ALTER SESSION SET QUERY_TAG = 'APP: SNOWDQ_OSS_STREAMLIT'")
        except Exception as e:
            print(f"Failed to set query tag for OSS: {str(e)}")
        
        # Execute identification query
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 'SNOWDQ_OSS_STREAMLIT_LAUNCH' AS launch_type")
            result = cursor.fetchone()
        except Exception as e:
            print(f"Failed to execute OSS identification query: {str(e)}")
        
        return conn
        
    except Exception as e:
        print(f"Failed to connect to Snowflake using local config: {str(e)}")
        return None

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_databases(_conn: Any) -> List[str]:
    """Get list of accessible databases."""
    try:
        query = """
        SELECT DATABASE_NAME 
        FROM INFORMATION_SCHEMA.DATABASES 
        ORDER BY DATABASE_NAME
        """
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            result = _conn.sql(query).to_pandas()
            databases = result['DATABASE_NAME'].tolist()
        else:  # Regular connection
            cursor = _conn.cursor()
            cursor.execute(query)
            databases = [row[0] for row in cursor.fetchall()]
        
        # Filter out system databases
        filtered_databases = [db for db in databases if db not in ['SNOWFLAKE', 'INFORMATION_SCHEMA']]
        return filtered_databases
            
    except Exception as e:
        st.error(f"Error fetching databases: {str(e)}")
        return []

@st.cache_data(ttl=300)
def get_schemas(_conn: Any, database_name: str) -> List[str]:
    """Get list of schemas in a database."""
    try:
        # Try using INFORMATION_SCHEMA first for better SiS compatibility
        info_schema_query = f"""
        SELECT SCHEMA_NAME
        FROM {quote_identifier(database_name)}.INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
        ORDER BY SCHEMA_NAME
        """
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            result = _conn.sql(info_schema_query).to_pandas()
            schemas = result['SCHEMA_NAME'].tolist() if not result.empty else []
        else:  # Regular connection
            result = pd.read_sql(info_schema_query, _conn)
            schemas = result['SCHEMA_NAME'].tolist() if not result.empty else []
        
        if schemas:
            return schemas
            
    except Exception as e:
        # If INFORMATION_SCHEMA fails, fall back to SHOW SCHEMAS
        st.warning(f"Could not access INFORMATION_SCHEMA for database {database_name}, trying SHOW command...")
        
        try:
            quoted_database = quote_identifier(database_name)
            query = f"SHOW SCHEMAS IN DATABASE {quoted_database}"
            
            if hasattr(_conn, 'sql'):  # Snowpark session
                result = _conn.sql(query).to_pandas()
                if 'name' in result.columns:
                    schemas = result['name'].tolist()
                elif 'NAME' in result.columns:
                    schemas = result['NAME'].tolist()
                else:
                    schemas = result.iloc[:, 1].tolist() if len(result.columns) > 1 else []
            else:  # Regular connection
                cursor = _conn.cursor()
                cursor.execute(query)
                schemas = [row[1] for row in cursor.fetchall()]
            
            # Filter out system schemas
            filtered_schemas = [schema for schema in schemas if schema not in ['INFORMATION_SCHEMA']]
            return filtered_schemas
                
        except Exception as e2:
            st.error(f"Error fetching schemas: {str(e2)}")
            return []

@st.cache_data(ttl=300)
def get_tables_and_views(_conn: Any, database_name: str, schema_name: str = None, _refresh_key: str = None) -> pd.DataFrame:
    """Get tables and views with their descriptions. If schema_name is None, gets from all schemas."""
    try:
        tables_data = []
        
        # Determine schemas to process
        if schema_name:
            schemas_to_process = [schema_name]
        else:
            # Get all schemas in the database
            schemas_to_process = get_schemas(_conn, database_name)
        
        for current_schema in schemas_to_process:
            try:
                # Use INFORMATION_SCHEMA instead of SHOW commands for better SiS compatibility
                # Get both tables and views in one query using INFORMATION_SCHEMA.TABLES
                info_schema_query = f"""
                SELECT 
                    TABLE_NAME as name,
                    COMMENT as comment,
                    TABLE_TYPE
                FROM {quote_identifier(database_name)}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{current_schema.upper()}'
                  AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                ORDER BY TABLE_NAME
                """
                
                if hasattr(_conn, 'sql'):  # Snowpark session
                    info_schema_result = _conn.sql(info_schema_query).to_pandas()
                else:  # Regular connection
                    info_schema_result = pd.read_sql(info_schema_query, _conn)
                
                for _, row in info_schema_result.iterrows():
                    name = row.get('name', row.get('NAME', ''))
                    comment = row.get('comment', row.get('COMMENT', ''))
                    table_type = row.get('TABLE_TYPE', 'BASE TABLE')
                    
                    # Skip if name is empty
                    if not name:
                        continue
                    
                    # For views, check if they're secure by trying to access them
                    if table_type == 'VIEW':
                        try:
                            # Try a simple query to check if view is accessible
                            test_query = f"SELECT 1 FROM {quote_identifier(database_name)}.{quote_identifier(current_schema)}.{quote_identifier(name)} LIMIT 0"
                            if hasattr(_conn, 'sql'):
                                _conn.sql(test_query).collect()
                            else:
                                pd.read_sql(test_query, _conn)
                        except Exception:
                            # Skip inaccessible views (likely secure views)
                            continue
                    
                    table_data = {
                        'OBJECT_NAME': name,
                        'OBJECT_TYPE': table_type,
                        'CURRENT_DESCRIPTION': comment if comment else None,
                        'HAS_DESCRIPTION': 'Yes' if comment and comment.strip() else 'No'
                    }
                    
                    # Add schema column if showing multiple schemas
                    if not schema_name:
                        table_data['SCHEMA_NAME'] = current_schema
                    
                    tables_data.append(table_data)
                    
            except Exception as e:
                # If INFORMATION_SCHEMA fails, fall back to SHOW commands
                st.warning(f"Could not access INFORMATION_SCHEMA for schema {current_schema}, trying SHOW commands...")
                
                # Fallback: Get tables using SHOW TABLES
                schema_qualified = f"{quote_identifier(database_name)}.{quote_identifier(current_schema)}"
                tables_query = f"SHOW TABLES IN SCHEMA {schema_qualified}"
                try:
                    if hasattr(_conn, 'sql'):  # Snowpark session
                        tables_result = _conn.sql(tables_query).to_pandas()
                    else:  # Regular connection
                        tables_result = pd.read_sql(tables_query, _conn)
                    
                    for _, row in tables_result.iterrows():
                        name = row.get('name', row.get('NAME', ''))
                        comment = row.get('comment', row.get('COMMENT', ''))
                        
                        if name:  # Only add if name exists
                            table_data = {
                                'OBJECT_NAME': name,
                                'OBJECT_TYPE': 'BASE TABLE',
                                'CURRENT_DESCRIPTION': comment if comment else None,
                                'HAS_DESCRIPTION': 'Yes' if comment and comment.strip() else 'No'
                            }
                            
                            # Add schema column if showing multiple schemas
                            if not schema_name:
                                table_data['SCHEMA_NAME'] = current_schema
                            
                            tables_data.append(table_data)
                except Exception:
                    continue  # Skip schemas we can't access
                
                # Fallback: Get views using SHOW VIEWS
                views_query = f"SHOW VIEWS IN SCHEMA {schema_qualified}"
                try:
                    if hasattr(_conn, 'sql'):  # Snowpark session
                        views_result = _conn.sql(views_query).to_pandas()
                    else:  # Regular connection
                        views_result = pd.read_sql(views_query, _conn)
                    
                    for _, row in views_result.iterrows():
                        name = row.get('name', row.get('NAME', ''))
                        comment = row.get('comment', row.get('COMMENT', ''))
                        
                        # Skip secure views
                        is_secure = (
                            row.get('is_secure', '') or 
                            row.get('IS_SECURE', '') or
                            row.get('secure', '') or
                            row.get('SECURE', '')
                        )
                        
                        is_secure_str = str(is_secure).upper()
                        if is_secure_str in ['YES', 'TRUE', 'Y', '1']:
                            continue
                        
                        if name:  # Only add if name exists
                            view_data = {
                                'OBJECT_NAME': name,
                                'OBJECT_TYPE': 'VIEW',
                                'CURRENT_DESCRIPTION': comment if comment else None,
                                'HAS_DESCRIPTION': 'Yes' if comment and comment.strip() else 'No'
                            }
                            
                            # Add schema column if showing multiple schemas
                            if not schema_name:
                                view_data['SCHEMA_NAME'] = current_schema
                            
                            tables_data.append(view_data)
                except Exception:
                    continue  # Skip schemas we can't access
        
        if tables_data:
            df = pd.DataFrame(tables_data)
            if schema_name:
                return df.sort_values('OBJECT_NAME')
            else:
                return df.sort_values(['SCHEMA_NAME', 'OBJECT_NAME'])
        else:
            columns = ['OBJECT_NAME', 'OBJECT_TYPE', 'CURRENT_DESCRIPTION', 'HAS_DESCRIPTION']
            if not schema_name:
                columns.insert(0, 'SCHEMA_NAME')
            return pd.DataFrame(columns=columns)
            
    except Exception as e:
        st.error(f"Error fetching tables/views: {str(e)}")
        columns = ['OBJECT_NAME', 'OBJECT_TYPE', 'CURRENT_DESCRIPTION', 'HAS_DESCRIPTION']
        if not schema_name:
            columns.insert(0, 'SCHEMA_NAME')
        return pd.DataFrame(columns=columns)

@st.cache_data(ttl=300)
def get_columns(_conn: Any, database_name: str, schema_name: str, table_name: str, _refresh_key: str = None) -> pd.DataFrame:
    """Get columns for a specific table/view."""
    try:
        # Try using INFORMATION_SCHEMA first for better SiS compatibility
        info_schema_query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            COMMENT,
            ORDINAL_POSITION
        FROM {quote_identifier(database_name)}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name.upper()}'
          AND TABLE_NAME = '{table_name.upper()}'
        ORDER BY ORDINAL_POSITION
        """
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            result = _conn.sql(info_schema_query).to_pandas()
        else:  # Regular connection
            result = pd.read_sql(info_schema_query, _conn)
        
        columns_data = []
        
        for _, row in result.iterrows():
            column_name = row.get('COLUMN_NAME', '')
            data_type = row.get('DATA_TYPE', '')
            comment = row.get('COMMENT', '')
            
            # Handle null/empty comments
            if pd.isna(comment) or comment == 'null' or comment == 'NULL' or comment == '':
                comment = None
            
            columns_data.append({
                'COLUMN_NAME': column_name,
                'DATA_TYPE': data_type,
                'CURRENT_DESCRIPTION': comment,
                'HAS_DESCRIPTION': 'Yes' if comment and str(comment).strip() else 'No'
            })
        
        if columns_data:
            return pd.DataFrame(columns_data)
            
    except Exception as e:
        # If INFORMATION_SCHEMA fails, fall back to DESC TABLE
        st.warning(f"Could not access INFORMATION_SCHEMA for {table_name}, trying DESC TABLE...")
        
        try:
            fully_qualified_table = get_fully_qualified_name(database_name, schema_name, table_name)
            desc_query = f"DESC TABLE {fully_qualified_table}"
            
            if hasattr(_conn, 'sql'):  # Snowpark session
                result = _conn.sql(desc_query).to_pandas()
            else:  # Regular connection
                result = pd.read_sql(desc_query, _conn)
            
            # Debug: Print column information to understand the structure
            st.info(f"DESC TABLE returned {len(result.columns)} columns: {list(result.columns)}")
            st.info(f"Data shape: {result.shape}")
            
            columns_data = []
            
            for _, row in result.iterrows():
                # Try different possible column names that DESC TABLE might return
                column_name = (
                    row.get('name') or row.get('NAME') or 
                    row.get('column_name') or row.get('COLUMN_NAME') or
                    row.get('Field') or row.get('FIELD') or ''
                )
                
                data_type = (
                    row.get('type') or row.get('TYPE') or 
                    row.get('data_type') or row.get('DATA_TYPE') or
                    row.get('Type') or ''
                )
                
                comment = (
                    row.get('comment') or row.get('COMMENT') or 
                    row.get('Comment') or row.get('description') or
                    row.get('DESCRIPTION') or ''
                )
                
                # Handle null/empty comments
                if pd.isna(comment) or comment == 'null' or comment == 'NULL' or comment == '':
                    comment = None
                
                # Only add if we have at least a column name
                if column_name:
                    columns_data.append({
                        'COLUMN_NAME': column_name,
                        'DATA_TYPE': data_type,
                        'CURRENT_DESCRIPTION': comment,
                        'HAS_DESCRIPTION': 'Yes' if comment and str(comment).strip() else 'No'
                    })
            
            if columns_data:
                return pd.DataFrame(columns_data)
            else:
                st.warning(f"No column data could be extracted from DESC TABLE result for {table_name}")
                
        except Exception as e2:
            st.error(f"Error fetching columns for {table_name}: {str(e2)}")
    
    # Return empty DataFrame with correct structure if all methods fail
    return pd.DataFrame(columns=['COLUMN_NAME', 'DATA_TYPE', 'CURRENT_DESCRIPTION', 'HAS_DESCRIPTION'])

def execute_comment_sql(_conn: Any, sql_command: str, object_type: str = None) -> bool:
    """Execute a COMMENT ON statement."""
    try:
        if hasattr(_conn, 'sql'):  # Snowpark session
            _conn.sql(sql_command).collect()
        else:  # Regular connection
            cursor = _conn.cursor()
            cursor.execute(sql_command)
        return True
        
    except Exception as e:
        st.error(f"Error executing comment SQL: {str(e)}")
        return False

def quote_identifier(identifier: str) -> str:
    """Quote a Snowflake identifier if it contains spaces or special characters."""
    if identifier is None or identifier == "":
        return identifier
    
    # If identifier already has quotes, return as-is
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier
    
    # Quote if it contains spaces, special characters, or is a reserved word
    if (' ' in identifier or 
        any(char in identifier for char in ['-', '.', '+', '/', '*', '(', ')', '[', ']', '{', '}']) or
        identifier.upper() in ['TABLE', 'COLUMN', 'VIEW', 'DATABASE', 'SCHEMA', 'SELECT', 'FROM', 'WHERE']):
        return f'"{identifier}"'
    
    return identifier

def get_fully_qualified_name(database: str, schema: str, table: str) -> str:
    """Create a fully qualified table name with proper quoting."""
    return f"{quote_identifier(database)}.{quote_identifier(schema)}.{quote_identifier(table)}"

def get_current_user(_conn: Any) -> str:
    """Get the current Snowflake user."""
    try:
        query = "SELECT CURRENT_USER() as current_user"
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            result = _conn.sql(query).to_pandas()
            return result.iloc[0]['CURRENT_USER']
        else:  # Regular connection
            cursor = _conn.cursor()
            cursor.execute(query)
            return cursor.fetchone()[0]
            
    except Exception:
        return "Unknown"

@st.cache_data(ttl=300)
def get_all_contacts(_conn: Any) -> List[str]:
    """Get all contacts in the account with their fully qualified names."""
    try:
        # First try SHOW CONTACTS command
        query = "SHOW CONTACTS IN ACCOUNT"
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            result = _conn.sql(query).to_pandas()
        else:  # Regular connection
            result = pd.read_sql(query, _conn)
        
        contact_options = ["None"]
        
        for _, contact in result.iterrows():
            # Handle different column names from SHOW CONTACTS
            contact_name = (contact.get('name') or contact.get('NAME') or 
                          contact.get('contact_name') or contact.get('CONTACT_NAME'))
            
            db_name = (contact.get('database_name') or contact.get('DATABASE_NAME') or 
                     contact.get('contact_database') or contact.get('CONTACT_DATABASE'))
            
            schema_name = (contact.get('schema_name') or contact.get('SCHEMA_NAME') or
                         contact.get('contact_schema') or contact.get('CONTACT_SCHEMA'))
            
            if contact_name and db_name and schema_name:
                # Create fully qualified name
                full_path = f'{db_name}.{schema_name}."{contact_name}"'
                contact_options.append(full_path)
        
        return contact_options
        
    except Exception as e:
        # If SHOW CONTACTS fails, return empty list with helpful message
        st.warning(f"Unable to retrieve contacts: {str(e)}")
        st.info("You may need permissions to view contacts in the account.")
        return ["None"]

@st.cache_data(ttl=300)
def get_table_contacts(_conn: Any, database: str, schema: str, table: str, _refresh_key: str = None) -> Dict[str, str]:
    """Get existing contacts assigned to a table."""
    try:
        # Query to get table contacts from ACCOUNT_USAGE.CONTACT_REFERENCES
        query = f"""
        SELECT 
            CONTACT_NAME,
            CONTACT_DATABASE,
            CONTACT_SCHEMA,
            CONTACT_PURPOSE
        FROM SNOWFLAKE.ACCOUNT_USAGE.CONTACT_REFERENCES 
        WHERE OBJECT_DATABASE = '{database}'
          AND OBJECT_SCHEMA = '{schema}'
          AND OBJECT_NAME = '{table}'
          AND OBJECT_DELETED IS NULL
        """
        
        contacts = {
            'STEWARD': None,
            'SUPPORT': None,
            'ACCESS_APPROVAL': None
        }
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            result = _conn.sql(query).to_pandas()
        else:  # Regular connection
            result = pd.read_sql(query, _conn)
        
        for _, row in result.iterrows():
            contact_purpose = row['CONTACT_PURPOSE']
            contact_name = row['CONTACT_NAME']
            contact_db = row['CONTACT_DATABASE']
            contact_schema = row['CONTACT_SCHEMA']
            
            if contact_purpose in contacts and contact_name:
                # Create fully qualified contact name to match dropdown options
                full_contact_name = f'{contact_db}.{contact_schema}."{contact_name}"'
                contacts[contact_purpose] = full_contact_name
        
        return contacts
        
    except Exception as e:
        st.warning(f"Could not retrieve table contacts: {str(e)}")
        return {
            'STEWARD': None,
            'SUPPORT': None,
            'ACCESS_APPROVAL': None
        }

# ========================================================================================
# HISTORY TRACKING UTILITIES
# ========================================================================================

def log_description_history(conn, database: str, schema: str, object_name: str, object_type: str, 
                          old_description: str, new_description: str, updated_by: str = "Streamlit App"):
    """Log description changes to the history table."""
    try:
        # Insert into history table
        history_insert = f"""
        INSERT INTO DB_SNOWTOOLS.PUBLIC.DATA_DESCRIPTION_HISTORY (
            DATABASE_NAME,
            SCHEMA_NAME,
            OBJECT_NAME,
            OBJECT_TYPE,
            BEFORE_DESCRIPTION,
            AFTER_DESCRIPTION,
            UPDATED_BY
        ) VALUES (
            '{database}',
            '{schema}',
            '{object_name}',
            '{object_type}',
            {f"'{old_description.replace(chr(39), chr(39)+chr(39))}'" if old_description else 'NULL'},
            '{new_description.replace(chr(39), chr(39)+chr(39))}',
            '{updated_by}'
        )
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            conn.sql(history_insert).collect()
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute(history_insert)
            
        return True
        
    except Exception as e:
        st.warning(f"Could not log description history: {str(e)}")
        return False

def log_dmf_history(conn, database: str, schema: str, table_name: str, dmf_type: str, 
                   column_name: str = None, action: str = "ADDED", updated_by: str = "Streamlit App"):
    """Log DMF configuration changes to the description history table."""
    try:
        # Use the DATA_DESCRIPTION_HISTORY table to track DMF configuration changes
        # Set OBJECT_TYPE to indicate this is a DMF configuration change
        object_type = f"DMF_{dmf_type}"
        if column_name:
            object_type += f"_COLUMN"
        
        # Create a description of the change
        if action == "ADDED":
            description = f"Added {dmf_type} data quality metric"
            if column_name:
                description += f" to column {column_name}"
        else:
            description = f"{action} {dmf_type} data quality metric"
            if column_name:
                description += f" on column {column_name}"
        
        history_insert = f"""
        INSERT INTO DB_SNOWTOOLS.PUBLIC.DATA_DESCRIPTION_HISTORY (
            DATABASE_NAME,
            SCHEMA_NAME,
            OBJECT_NAME,
            COLUMN_NAME,
            OBJECT_TYPE,
            BEFORE_DESCRIPTION,
            AFTER_DESCRIPTION,
            UPDATED_BY
        ) VALUES (
            '{database}',
            '{schema}',
            '{table_name}',
            {f"'{column_name}'" if column_name else 'NULL'},
            '{object_type}',
            NULL,
            '{description}',
            '{updated_by}'
        )
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            conn.sql(history_insert).collect()
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute(history_insert)
            
        return True
        
    except Exception as e:
        st.warning(f"Could not log DMF history: {str(e)}")
        return False

def log_contact_history(conn, database: str, schema: str, table_name: str, contact_type: str,
                       old_contact: str, new_contact: str, updated_by: str = "Streamlit App"):
    """Log contact assignment changes to the history table."""
    try:
        # For now, we'll log contact changes in the description history table with a special object type
        history_insert = f"""
        INSERT INTO DB_SNOWTOOLS.PUBLIC.DATA_DESCRIPTION_HISTORY (
            DATABASE_NAME,
            SCHEMA_NAME,
            OBJECT_NAME,
            OBJECT_TYPE,
            BEFORE_DESCRIPTION,
            AFTER_DESCRIPTION,
            UPDATED_BY
        ) VALUES (
            '{database}',
            '{schema}',
            '{table_name}',
            'CONTACT_{contact_type}',
            {f"'{old_contact}'" if old_contact and old_contact != 'None' else 'NULL'},
            {f"'{new_contact}'" if new_contact and new_contact != 'None' else 'NULL'},
            '{updated_by}'
        )
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            conn.sql(history_insert).collect()
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute(history_insert)
            
        return True
        
    except Exception as e:
        st.warning(f"Could not log contact history: {str(e)}")
        return False

# ========================================================================================
# AI/CORTEX UTILITIES
# ========================================================================================

AVAILABLE_MODELS = [
    'claude-4-sonnet',
    'mistral-large2', 
    'llama3-70b',
    'snowflake-arctic',
    'snowflake-llama-3.1-405b'
]

def get_available_models() -> List[str]:
    """Get list of available LLM models for Cortex COMPLETE."""
    return AVAILABLE_MODELS

def generate_table_description(conn: Any, model: str, database_name: str, schema_name: str, 
                             table_name: str, table_type: str = 'TABLE') -> Optional[str]:
    """Generate a description for a table or view using Cortex COMPLETE."""
    try:
        # Get column information for context
        columns_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = '{database_name.upper()}'
        AND TABLE_SCHEMA = '{schema_name.upper()}'
        AND TABLE_NAME = '{table_name.upper()}'
        ORDER BY ORDINAL_POSITION
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            columns_result = conn.sql(columns_query).to_pandas()
        else:  # Regular connection
            columns_result = pd.read_sql(columns_query, conn)
        
        # Build column metadata string
        column_info = []
        for _, row in columns_result.iterrows():
            col_desc = f"- {row['COLUMN_NAME']} ({row['DATA_TYPE']})"
            if row['COMMENT']:
                col_desc += f": {row['COMMENT']}"
            column_info.append(col_desc)
        
        columns_text = "\n".join(column_info) if column_info else "No columns found"
        
        # Get sample data
        sample_query = f"""
        SELECT *
        FROM {database_name}.{schema_name}.{table_name}
        LIMIT 5
        """
        
        try:
            if hasattr(conn, 'sql'):  # Snowpark session
                sample_result = conn.sql(sample_query).to_pandas()
            else:  # Regular connection
                sample_result = pd.read_sql(sample_query, conn)
            
            sample_data = sample_result.to_string(index=False, max_rows=5)
        except Exception:
            sample_data = "Unable to sample data"
        
        # Build the prompt
        prompt = f"""You are an expert data steward and have been tasked with writing descriptions for tables and columns in an enterprise data warehouse. 
Use the provided metadata and the first few rows of data to write a concise, meaningful, and business-centric description. 
This description should be helpful to both business analysts and technical analysts. 
Focus on the purpose of the data, its key contents, and any important context. 
Output only the description text. Keep the description to 150 characters or less.

---METADATA---
{table_type} Name: {table_name}
Schema: {schema_name}
Database: {database_name}
Columns:
{columns_text}

---SAMPLE DATA (LIMIT 5 ROWS)---
{sample_data}

---TASK---
Generate a description for the {table_type.lower()} named {table_name}."""
        
        # Call Cortex COMPLETE
        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            $${prompt}$$
        ) as generated_description
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            result = conn.sql(cortex_query).to_pandas()
        else:  # Regular connection
            result = pd.read_sql(cortex_query, conn)
        
        description = result.iloc[0]['GENERATED_DESCRIPTION']
        
        # Clean up the description
        description = description.strip()
        if description.startswith('"') and description.endswith('"'):
            description = description[1:-1]
        
        return description
        
    except Exception as e:
        st.error(f"Error generating table description: {str(e)}")
        return None

def generate_column_description(conn: Any, model: str, database_name: str, schema_name: str, 
                              table_name: str, column_name: str, data_type: str) -> Optional[str]:
    """Generate a description for a column using Cortex COMPLETE."""
    try:
        # Get sample data for the specific column
        fully_qualified_table = get_fully_qualified_name(database_name, schema_name, table_name)
        quoted_column = quote_identifier(column_name)
        sample_query = f"""
        SELECT {quoted_column}
        FROM {fully_qualified_table}
        SAMPLE  (10 ROWS);
        """
        
        try:
            if hasattr(conn, 'sql'):  # Snowpark session
                sample_result = conn.sql(sample_query).to_pandas()
            else:  # Regular connection
                sample_result = pd.read_sql(sample_query, conn)
            
            sample_data = sample_result.to_string(index=False, max_rows=10)
        except Exception:
            sample_data = "Unable to sample data"
        
        # Build the prompt
        prompt = f"""You are an expert data steward and have been tasked with writing descriptions for tables and columns in an enterprise data warehouse. 
Use the provided metadata and the first few rows of data to write a concise, meaningful, and business-centric description. 
This description should be helpful to both business analysts and technical analysts. 
Focus on the purpose of the data, its key contents, and any important context. 
Output only the description text. Keep the description to 100 characters or less.

---METADATA---
Column Name: {column_name}
Table Name: {table_name}
Schema: {schema_name}
Database: {database_name}
Data Type: {data_type}

---SAMPLE DATA (LIMIT 10 ROWS)---
{sample_data}

---TASK---
Generate a description for the column named {column_name}."""
        
        # Call Cortex COMPLETE
        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            $${prompt}$$
        ) as generated_description
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            result = conn.sql(cortex_query).to_pandas()
        else:  # Regular connection
            result = pd.read_sql(cortex_query, conn)
        
        description = result.iloc[0]['GENERATED_DESCRIPTION']
        
        # Clean up the description
        description = description.strip()
        if description.startswith('"') and description.endswith('"'):
            description = description[1:-1]
        
        return description
        
    except Exception as e:
        st.error(f"Error generating column description: {str(e)}")
        return None

# ========================================================================================
# DATABASE SETUP UTILITIES
# ========================================================================================

def check_database_exists(conn: Any, database_name: str = "DB_SNOWTOOLS") -> bool:
    """Check if the specified database exists."""
    try:
        query = f"""
        SELECT COUNT(*) as db_count 
        FROM INFORMATION_SCHEMA.DATABASES 
        WHERE DATABASE_NAME = '{database_name.upper()}'
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            result = conn.sql(query).collect()
            return result[0]['DB_COUNT'] > 0
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] > 0
            
    except Exception:
        return False

def setup_database_objects(conn: Any) -> bool:
    """Complete setup of all required database objects."""
    database_name = "DB_SNOWTOOLS"
    schema_name = "PUBLIC"
    
    setup_actions = []
    
    # Check if database exists
    if not check_database_exists(conn, database_name):
        try:
            # Create database and schema
            create_db_sql = f"CREATE DATABASE IF NOT EXISTS {database_name}"
            create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}"
            
            if hasattr(conn, 'sql'):  # Snowpark session
                conn.sql(create_db_sql).collect()
                conn.sql(create_schema_sql).collect()
            else:  # Regular connection
                cursor = conn.cursor()
                cursor.execute(create_db_sql)
                cursor.execute(create_schema_sql)
            
            setup_actions.append(f"Created database {database_name}")
        except Exception as e:
            st.error(f"Error creating database: {str(e)}")
            return False
    
    # Create tracking tables
    try:
        # DATA_DESCRIPTION_HISTORY table
        history_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.DATA_DESCRIPTION_HISTORY (
            HISTORY_ID NUMBER AUTOINCREMENT PRIMARY KEY,
            DATABASE_NAME VARCHAR(255) NOT NULL,
            SCHEMA_NAME VARCHAR(255) NOT NULL,
            OBJECT_TYPE VARCHAR(50) NOT NULL,
            OBJECT_NAME VARCHAR(255) NOT NULL,
            COLUMN_NAME VARCHAR(255),
            BEFORE_DESCRIPTION TEXT,
            AFTER_DESCRIPTION TEXT,
            SQL_EXECUTED TEXT,
            UPDATED_BY VARCHAR(255) DEFAULT CURRENT_USER(),
            UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        
        # DATA_QUALITY_RESULTS table
        quality_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.DATA_QUALITY_RESULTS (
            RESULT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
            MONITOR_NAME VARCHAR(255) NOT NULL,
            DATABASE_NAME VARCHAR(255) NOT NULL,
            SCHEMA_NAME VARCHAR(255) NOT NULL,
            TABLE_NAME VARCHAR(255) NOT NULL,
            COLUMN_NAME VARCHAR(255),
            METRIC_VALUE NUMBER,
            METRIC_UNIT VARCHAR(50),
            THRESHOLD_MIN NUMBER,
            THRESHOLD_MAX NUMBER,
            STATUS VARCHAR(20),
            MEASUREMENT_TIME TIMESTAMP_LTZ,
            RECORD_INSERTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            SQL_EXECUTED TEXT
        )
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            conn.sql(history_table_sql).collect()
            conn.sql(quality_table_sql).collect()
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute(history_table_sql)
            cursor.execute(quality_table_sql)
        
        setup_actions.append("Created tracking tables")
        
    except Exception as e:
        st.error(f"Error creating tables: {str(e)}")
        return False
    
    # Show setup messages if something was created
    # Return True - setup status is now shown in sidebar
    return True

# ========================================================================================
# MAIN APPLICATION
# ========================================================================================

def initialize_session_state():
    """Initialize all session state variables to prevent tab jumping on first interaction."""
    
    # Data Descriptions tab session state
    if 'desc_database' not in st.session_state:
        st.session_state.desc_database = ""
    if 'desc_schema' not in st.session_state:
        st.session_state.desc_schema = ""
    
    # Data Quality tab session state  
    if 'dmf_database' not in st.session_state:
        st.session_state.dmf_database = ""
    if 'dmf_schema' not in st.session_state:
        st.session_state.dmf_schema = ""
    if 'dmf_table' not in st.session_state:
        st.session_state.dmf_table = ""
    
    # Data Contacts tab session state
    if 'contact_database' not in st.session_state:
        st.session_state.contact_database = ""
    if 'contact_schema' not in st.session_state:
        st.session_state.contact_schema = ""
    if 'contact_table' not in st.session_state:
        st.session_state.contact_table = ""
    
    # General session state
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = ""
    
    # Tab state management to prevent jumping
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = "Home"

def main():
    """Main application function."""
    
    # Add global CSS for better spacing and UX
    st.markdown("""
    <style>
    /* Add bottom margin to prevent buttons from being at the very bottom */
    .main .block-container {
        padding-bottom: 5rem !important;
    }
    
    /* Improve button spacing */
    .stButton > button {
        margin-bottom: 0.5rem;
    }
    
    /* Better spacing for expanders */
    .streamlit-expanderHeader {
        margin-bottom: 0.5rem;
    }
    
    /* Add some breathing room for data editors */
    .stDataFrame {
        margin-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Initialize ALL session state variables at the very beginning to prevent tab jumping
    initialize_session_state()
    
    # Compact Header
    st.markdown("""
    <div style="text-align: center; padding: 0.2rem 0; margin-bottom: .2rem;">
        <h2 style="margin: 0; color: #1f77b4; font-size: 1.8rem;">📘 Snowflake Data Quality & Documentation</h2>
        <p style="margin: 0; color: #666; font-size: 0.9rem;">AI-powered data governance and quality monitoring</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Get Snowflake connection
    conn = get_snowflake_connection()
    if not conn:
        st.error("Failed to connect to Snowflake. Please check your connection parameters.")
        st.stop()
    
    # Setup database objects (only shows messages if creation is needed)
    if 'setup_complete' not in st.session_state:
        setup_success = setup_database_objects(conn)
        if setup_success:
            st.session_state.setup_complete = True
    else:
        setup_success = True
    
    if not setup_success:
        st.error("Database setup failed. Please check permissions and try again.")
        st.stop()
    
    # Display current user in sidebar
    with st.sidebar:
        st.markdown("### 🔺 Connection Manager")
        
        try:
            current_user = get_current_user(conn)
            st.success(f"Connected as: **{current_user}**")
        except:
            st.warning("Status: Connected")
    
        # System Information (moved from Home tab)
        st.markdown("---")
        with st.expander("📊 System Information", expanded=False):
            st.markdown("**Connection Details**")
            
            if hasattr(conn, 'sql'):
                st.success("Using Snowpark session (SiS)")
                st.caption("App running within Snowflake's managed environment")
            else:
                st.info("Using standard connector")
                st.caption("Local development mode")
            
            try:
                # Get Snowflake system info
                info_query = "SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_VERSION()"
                if hasattr(conn, 'sql'):
                    result = conn.sql(info_query).to_pandas()
                else:
                    result = pd.read_sql(info_query, conn)
                
                st.markdown("**Environment Details:**")
                st.write(f"• **Account:** {result.iloc[0, 0]}")
                st.write(f"• **Region:** {result.iloc[0, 1]}")
                st.write(f"• **Version:** {result.iloc[0, 2]}")
                    
            except Exception as e:
                st.warning(f"Could not retrieve system info: {str(e)}")
        
        # Platform Overview (moved from Home tab)
        with st.expander("🏗️ Platform Overview", expanded=False):
            st.markdown("""
            **Snowflake Data Quality & Documentation Platform**
            
            A comprehensive solution for:
            • AI-powered data documentation
            • Automated quality monitoring  
            • Contact management & governance
            • Historical tracking & reporting
            
            Built with Streamlit and Snowflake Cortex
            """)
            
            # Quick feature overview
            st.markdown("**Key Features:**")
            st.write("📝 **Data Descriptions** - AI-generated documentation")
            st.write("🔍 **Data Quality** - DMF setup and monitoring")
            st.write("👥 **Data Contacts** - Governance assignments")
            st.write("📈 **History** - Change tracking and reports")
        
        # Database Setup Status (moved from main area)
        if 'setup_complete' in st.session_state and st.session_state.setup_complete:
            with st.expander("🔧 Database Setup Status", expanded=False):
                st.success("✅ All required database objects are ready")
                st.info("DB_SNOWTOOLS database and tracking tables configured")
                st.caption("Setup completed successfully during initialization")
        
        # Quick Actions (moved from Home tab)
        with st.expander("🚀 Quick Actions", expanded=False):
            st.markdown("**Navigate directly to key features:**")
            
            if st.button("📝 Generate Descriptions", use_container_width=True, type="primary", key="sidebar_desc"):
                st.session_state.active_tab = "Data Descriptions"
                st.rerun()
                
            if st.button("🔍 Setup Quality Checks", use_container_width=True, type="secondary", key="sidebar_quality"):
                st.session_state.active_tab = "Data Quality"
                st.rerun()
                
            if st.button("👥 Manage Contacts", use_container_width=True, type="secondary", key="sidebar_contacts"):
                st.session_state.active_tab = "Data Contacts"
                st.rerun()
                
            if st.button("📈 View History", use_container_width=True, type="secondary", key="sidebar_history"):
                st.session_state.active_tab = "History"
                st.rerun()
            
            st.markdown("---")
            st.caption("💡 **Tip:** Use these buttons for quick navigation between features")
    
    # Navigation using radio buttons (more stable than tabs)
    tab_options = [
        "🏠 Home", 
        "📝 Data Descriptions", 
        "🔍 Data Quality", 
        "👥 Data Contacts",
        "📈 History"
    ]
    
    st.markdown("---")

    # Map display names to keys
    tab_keys = ["Home", "Data Descriptions", "Data Quality", "Data Contacts", "History"]
    
    # Find current index
    try:
        current_index = tab_keys.index(st.session_state.active_tab)
    except ValueError:
        current_index = 0
        st.session_state.active_tab = "Home"
    
    # Navigation radio buttons
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        if st.button("🏠 Home", use_container_width=True, type="primary" if st.session_state.active_tab == "Home" else "secondary"):
            st.session_state.active_tab = "Home"
            st.rerun()
    
    with col2:
        if st.button("📝 Data Descriptions", use_container_width=True, type="primary" if st.session_state.active_tab == "Data Descriptions" else "secondary"):
            st.session_state.active_tab = "Data Descriptions"
            st.rerun()
    
    with col3:
        if st.button("🔍 Data Quality", use_container_width=True, type="primary" if st.session_state.active_tab == "Data Quality" else "secondary"):
            st.session_state.active_tab = "Data Quality"
            st.rerun()
    
    with col4:
        if st.button("👥 Data Contacts", use_container_width=True, type="primary" if st.session_state.active_tab == "Data Contacts" else "secondary"):
            st.session_state.active_tab = "Data Contacts"
            st.rerun()
    
    with col5:
        if st.button("📈 History", use_container_width=True, type="primary" if st.session_state.active_tab == "History" else "secondary"):
            st.session_state.active_tab = "History"
            st.rerun()
    
    # Show content based on active tab
    if st.session_state.active_tab == "Home":
        show_home_page(conn)
    elif st.session_state.active_tab == "Data Descriptions":
        show_data_descriptions_page(conn)
    elif st.session_state.active_tab == "Data Quality":
        show_data_quality_page(conn)
    elif st.session_state.active_tab == "Data Contacts":
        show_data_contacts_page(conn)
    elif st.session_state.active_tab == "History":
        show_history_page(conn)

# ========================================================================================
# PAGE FUNCTIONS
# ========================================================================================

@st.cache_data(ttl=300)
def get_kpi_data(_conn: Any) -> Dict[str, Any]:
    """Get comprehensive KPI data for the dashboard."""
    kpis = {
        'databases': 0,
        'schemas': 0,
        'tables': 0,
        'tables_with_descriptions': 0,
        'description_percentage': 0,
        'dmf_count': 0,
        'tables_with_dmfs': 0,
        'contacts_count': 0,
        'tables_with_contacts': 0,
        'error': None
    }
    
    try:
        # Get database count
        databases = get_databases(_conn)
        kpis['databases'] = len(databases)
        
        # Get accurate table counts from ACCOUNT_USAGE (much more efficient than sampling)
        try:
            table_count_query = """
            SELECT COUNT(*) as total_tables, 
                   COUNT(comment) as tables_with_descriptions 
            FROM snowflake.account_usage.tables 
            WHERE table_catalog NOT IN ('SNOWFLAKE') 
              AND table_catalog IS NOT NULL
              AND table_type ILIKE '%table%'  
              AND owner_role_type <> 'APPLICATION'
            """
            
            if hasattr(_conn, 'sql'):
                result = _conn.sql(table_count_query).to_pandas()
                kpis['tables'] = int(result.iloc[0]['TOTAL_TABLES']) if not result.empty else 0
                kpis['tables_with_descriptions'] = int(result.iloc[0]['TABLES_WITH_DESCRIPTIONS']) if not result.empty else 0
            else:
                result = pd.read_sql(table_count_query, _conn)
                kpis['tables'] = int(result.iloc[0, 0]) if not result.empty else 0
                kpis['tables_with_descriptions'] = int(result.iloc[0, 1]) if not result.empty else 0
                
        except Exception as e:
            # Fallback to estimation if ACCOUNT_USAGE query fails
            kpis['tables'] = 0
            kpis['tables_with_descriptions'] = 0
        
        # Get schema count estimate (sample a few databases for performance)
        sample_databases = databases[:3] if databases else []
        total_schemas = 0
        
        for db in sample_databases:
            try:
                schemas = get_schemas(_conn, db)
                total_schemas += len(schemas)
            except:
                continue
        
        # Extrapolate schema count based on sample
        if sample_databases and len(databases) > 0:
            db_ratio = len(databases) / len(sample_databases)
            kpis['schemas'] = int(total_schemas * db_ratio)
        else:
            kpis['schemas'] = total_schemas
        
        # Calculate description percentage
        if kpis['tables'] > 0:
            kpis['description_percentage'] = round((kpis['tables_with_descriptions'] / kpis['tables']) * 100, 1)
        
        # Try to get DMF and contact counts (these might fail due to permissions)
        try:
            # Check for any DMF monitoring results
            dmf_query = "SELECT COUNT(DISTINCT TABLE_DATABASE || TABLE_SCHEMA || METRIC_NAME) as DMF_COUNT FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS"
            if hasattr(_conn, 'sql'):
                result = _conn.sql(dmf_query).to_pandas()
                kpis['dmf_count'] = int(result.iloc[0]['DMF_COUNT']) if not result.empty else 0
            else:
                result = pd.read_sql(dmf_query, _conn)
                kpis['dmf_count'] = int(result.iloc[0, 0]) if not result.empty else 0
        except:
            kpis['dmf_count'] = 0
        
        try:
            # Check for contacts
            contacts_query = "SHOW CONTACTS IN ACCOUNT"
            if hasattr(_conn, 'sql'):
                result = _conn.sql(contacts_query).to_pandas()
                kpis['contacts_count'] = len(result) if not result.empty else 0
            else:
                result = pd.read_sql(contacts_query, _conn)
                kpis['contacts_count'] = len(result) if not result.empty else 0
        except:
            kpis['contacts_count'] = 0
            
        # Estimate tables with DMFs and contacts (simplified for performance)
        kpis['tables_with_dmfs'] = min(kpis['dmf_count'], kpis['tables'])
        kpis['tables_with_contacts'] = min(kpis['contacts_count'], kpis['tables'])
            
    except Exception as e:
        kpis['error'] = str(e)
    
    return kpis

def show_home_page(conn):
    """Display the home page with modern KPI dashboard."""
    
    st.markdown("# 📊 Data Governance Dashboard")
    st.markdown("*Insights into your data quality and documentation coverage*")
    
    # Loading state
    with st.spinner("Loading dashboard metrics..."):
        kpi_data = get_kpi_data(conn)
    
    if kpi_data['error']:
        st.warning(f"Some metrics may be incomplete: {kpi_data['error']}")
    
    # Show refresh confirmation if requested
    if st.session_state.get('kpi_refresh_requested', False):
        st.success("✅ KPIs refreshed with latest data from Snowflake!")
        st.session_state['kpi_refresh_requested'] = False
    
    # Modern KPI Cards with refresh option
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("### 📈 Key Performance Indicators")
    with col2:
        if st.button("🔄 Refresh KPIs", help="Refresh all KPI data from Snowflake", key="refresh_kpis"):
            # Clear the KPI cache to force fresh data
            st.cache_data.clear()
            st.session_state['kpi_refresh_requested'] = True
            st.rerun()
    
    # Row 1: Data Inventory
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div style="background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); padding: 1.5rem; border-radius: 10px; color: white; margin-bottom: 1rem;">
            <h3 style="margin: 0; font-size: 2.5rem; font-weight: bold;">{:,}</h3>
            <p style="margin: 0; font-size: 1.1rem; opacity: 0.9;">Databases</p>
            <p style="margin: 0; font-size: 0.9rem; opacity: 0.7;">Accessible to your role</p>
        </div>
        """.format(kpi_data['databases']), unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="background: linear-gradient(90deg, #f093fb 0%, #f5576c 100%); padding: 1.5rem; border-radius: 10px; color: white; margin-bottom: 1rem;">
            <h3 style="margin: 0; font-size: 2.5rem; font-weight: bold;">{:,}</h3>
            <p style="margin: 0; font-size: 1.1rem; opacity: 0.9;">Schemas</p>
            <p style="margin: 0; font-size: 0.9rem; opacity: 0.7;">Across all databases</p>
        </div>
        """.format(kpi_data['schemas']), unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div style="background: linear-gradient(90deg, #4facfe 0%, #00f2fe 100%); padding: 1.5rem; border-radius: 10px; color: white; margin-bottom: 1rem;">
            <h3 style="margin: 0; font-size: 2.5rem; font-weight: bold;">{:,}</h3>
            <p style="margin: 0; font-size: 1.1rem; opacity: 0.9;">Tables & Views</p>
            <p style="margin: 0; font-size: 0.9rem; opacity: 0.7;">Total data objects</p>
        </div>
        """.format(kpi_data['tables']), unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Row 2: Documentation Coverage
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="background: linear-gradient(90deg, #43e97b 0%, #38f9d7 100%); padding: 1.5rem; border-radius: 10px; color: white; margin-bottom: 1rem;">
            <h3 style="margin: 0; font-size: 2.5rem; font-weight: bold;">{:,}</h3>
            <p style="margin: 0; font-size: 1.1rem; opacity: 0.9;">Tables with Descriptions</p>
            <p style="margin: 0; font-size: 0.9rem; opacity: 0.7;">{}% coverage rate</p>
        </div>
        """.format(kpi_data['tables_with_descriptions'], kpi_data['description_percentage']), unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="background: linear-gradient(90deg, #fa709a 0%, #fee140 100%); padding: 1.5rem; border-radius: 10px; color: white; margin-bottom: 1rem;">
            <h3 style="margin: 0; font-size: 2.5rem; font-weight: bold;">{}%</h3>
            <p style="margin: 0; font-size: 1.1rem; opacity: 0.9;">Documentation Coverage</p>
            <p style="margin: 0; font-size: 0.9rem; opacity: 0.7;">{:,} of {:,} documented</p>
        </div>
        """.format(kpi_data['description_percentage'], kpi_data['tables_with_descriptions'], kpi_data['tables']), unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Row 3: Quality & Governance
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="background: linear-gradient(90deg, #a8edea 0%, #fed6e3 100%); padding: 1.5rem; border-radius: 10px; color: #333; margin-bottom: 1rem;">
            <h3 style="margin: 0; font-size: 2.5rem; font-weight: bold;">{:,}</h3>
            <p style="margin: 0; font-size: 1.1rem; opacity: 0.8;">Data Quality Metrics</p>
            <p style="margin: 0; font-size: 0.9rem; opacity: 0.6;">Active DMF monitors on tables</p>
        </div>
        """.format(kpi_data['dmf_count']), unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="background: linear-gradient(90deg, #d299c2 0%, #fef9d7 100%); padding: 1.5rem; border-radius: 10px; color: #333; margin-bottom: 1rem;">
            <h3 style="margin: 0; font-size: 2.5rem; font-weight: bold;">{:,}</h3>
            <p style="margin: 0; font-size: 1.1rem; opacity: 0.8;">Defined Contacts</p>
            <p style="margin: 0; font-size: 0.9rem; opacity: 0.6;">For governance & support</p>
        </div>
        """.format(kpi_data['contacts_count']), unsafe_allow_html=True)

def show_data_descriptions_page(conn):
    """Display the data descriptions page."""
    
    st.markdown("Generate AI-powered descriptions for your tables, views, and columns using Snowflake Cortex.")
    
    st.info("**Note:** Data Descriptions are generated using Snowflake Cortex and LLMs. Please be aware of cost implications.")
    
    # Database and Schema Selection
    st.markdown("### Database and Schema Selection")
    
    col1, col2 = st.columns(2)
    
    with col1:
        databases = get_databases(conn)
        if not databases:
            st.error("No databases accessible. Please check permissions.")
            return
        
        # Find current database index
        current_db_index = 0
        if st.session_state.desc_database in databases:
            current_db_index = databases.index(st.session_state.desc_database) + 1
            
        selected_db = st.selectbox(
            "Select Database",
            options=[""] + databases,
            index=current_db_index,
            key="desc_db_selector",
            help="Choose a database to explore"
        )
        
        # Update session state if changed
        if selected_db != st.session_state.desc_database:
            st.session_state.desc_database = selected_db
            st.session_state.desc_schema = ""  # Reset schema when database changes
    
    with col2:
        if selected_db:
            schemas = get_schemas(conn, selected_db)
            
            # Find current schema index
            current_schema_index = 0
            if st.session_state.desc_schema in schemas:
                current_schema_index = schemas.index(st.session_state.desc_schema) + 1
                
            selected_schema = st.selectbox(
                "Select Schema",
                options=[""] + schemas,
                index=current_schema_index,
                key="desc_schema_selector",
                help="Choose a schema within the selected database"
            )
            
            # Update session state if changed
            if selected_schema != st.session_state.desc_schema:
                st.session_state.desc_schema = selected_schema
        else:
            selected_schema = ""
            st.selectbox("Select Schema", options=[""], disabled=True, key="desc_schema_disabled", help="Select a database first")
    
    # Object Listing and Filtering
    if selected_db:
        st.markdown("---")
        st.markdown("### Data Objects")
        
        # Get tables and views (all schemas if no schema selected)
        refresh_key = st.session_state.get('last_refresh', '')
        if selected_schema:
            tables_df = get_tables_and_views(conn, selected_db, selected_schema, refresh_key)
        else:
            tables_df = get_tables_and_views(conn, selected_db, None, refresh_key)
        
        if not tables_df.empty:
            # Filter options
            col1, col2 = st.columns(2)
            with col1:
                show_undocumented_only = st.checkbox(
                    "Only show objects without descriptions",
                    help="Filter to show only tables/views that lack descriptions"
                )
            with col2:
                object_type_filter = st.selectbox(
                    "Object Type",
                    options=["All", "BASE TABLE", "VIEW"],
                    help="Filter by object type"
                )
            
            # Apply filters
            filtered_df = tables_df.copy()
            if show_undocumented_only:
                filtered_df = filtered_df[filtered_df['HAS_DESCRIPTION'] == 'No']
            if object_type_filter != "All":
                filtered_df = filtered_df[filtered_df['OBJECT_TYPE'] == object_type_filter]
            
            # Display filtered results
            if not filtered_df.empty:
                st.markdown("### Select Objects for Description Generation")
                
                # Add Select All checkbox and Refresh button
                col1, col2, col3 = st.columns([1, 1, 2])
                with col1:
                    select_all = st.checkbox("Select All", key="select_all_objects")
                with col2:
                    if st.button("🔄 Refresh Data", help="Refresh table and column data from Snowflake", key="refresh_tables_data"):
                        # Clear all caches to force fresh data from Snowflake
                        st.cache_data.clear()
                        st.cache_resource.clear()
                        
                        # Force cache invalidation with new timestamp
                        st.session_state['last_refresh'] = str(time.time())
                        
                        # Show success message and rerun
                        st.success("✅ Data refreshed! Latest descriptions loaded from Snowflake.")
                        st.rerun()
                
                # Add selection column
                df_with_selection = filtered_df.copy()
                df_with_selection.insert(0, "Select", select_all)
                
                # Configure columns based on whether we're showing schema info
                column_config = {
                    "Select": st.column_config.CheckboxColumn("Select", help="Select objects for description generation"),
                    "OBJECT_NAME": "Object Name",
                    "OBJECT_TYPE": "Type", 
                    "CURRENT_DESCRIPTION": st.column_config.TextColumn("Current Description", width="large"),
                    "HAS_DESCRIPTION": "Has Description"
                }
                
                # Add schema column if showing multiple schemas
                if not selected_schema and 'SCHEMA_NAME' in df_with_selection.columns:
                    column_config["SCHEMA_NAME"] = "Schema"
                
                edited_df = st.data_editor(
                    df_with_selection,
                    use_container_width=True,
                    column_config=column_config,
                    hide_index=True,
                    key="object_selection_table"
                )
                
                # Get selected objects
                selected_rows = edited_df[edited_df["Select"] == True]
                selected_objects = selected_rows['OBJECT_NAME'].tolist()
                
                # Show column details for selected objects
                if selected_objects:
                    st.markdown("### Column Details")
                    st.info(f"Selected {len(selected_objects)} object(s): {', '.join(selected_objects)}")
                    
                    for obj_name in selected_objects:
                        # Find the schema for this object if we're in database-level view
                        if selected_schema:
                            obj_schema = selected_schema
                            expander_title = f"Columns in {obj_name}"
                        else:
                            # Find schema from the selected rows
                            obj_row = selected_rows[selected_rows['OBJECT_NAME'] == obj_name]
                            if not obj_row.empty and 'SCHEMA_NAME' in obj_row.columns:
                                obj_schema = obj_row.iloc[0]['SCHEMA_NAME']
                                expander_title = f"Columns in {obj_schema}.{obj_name}"
                            else:
                                continue  # Skip if we can't find the schema
                        
                        with st.expander(expander_title):
                            columns_df = get_columns(conn, selected_db, obj_schema, obj_name, refresh_key)
                            
                            if not columns_df.empty:
                                show_undoc_cols = st.checkbox(
                                    f"Only show columns without descriptions ({obj_name})",
                                    key=f"undoc_cols_{obj_name}"
                                )
                                
                                display_cols_df = columns_df.copy()
                                if show_undoc_cols:
                                    display_cols_df = display_cols_df[display_cols_df['HAS_DESCRIPTION'] == 'No']
                                
                                st.dataframe(
                                    display_cols_df,
                                    use_container_width=True,
                                    column_config={
                                        "COLUMN_NAME": "Column Name",
                                        "DATA_TYPE": "Data Type",
                                        "CURRENT_DESCRIPTION": st.column_config.TextColumn("Current Description", width="large"),
                                        "HAS_DESCRIPTION": "Has Description"
                                    }
                                )
                
                # LLM Model Selection and Actions
                if selected_objects:
                    st.markdown("---")
                    st.markdown("### AI Description Generation")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        available_models = get_available_models()
                        selected_model = st.selectbox(
                            "Select LLM Model",
                            options=available_models,
                            index=0,
                            key="desc_model",
                            help="Choose the Cortex LLM model for description generation"
                        )
                    
                    with col2:
                        if st.button("🧪 Test Model", help="Test if the selected model is available"):
                            with st.spinner("Testing model..."):
                                try:
                                    test_query = f"""
                                    SELECT SNOWFLAKE.CORTEX.COMPLETE(
                                        '{selected_model}',
                                        'Hello, this is a test.'
                                    ) as test_response
                                    """
                                    
                                    if hasattr(conn, 'sql'):
                                        result = conn.sql(test_query).to_pandas()
                                    else:
                                        result = pd.read_sql(test_query, conn)
                                    
                                    st.success(f"✅ Model {selected_model} is working!")
                                    
                                except Exception as e:
                                    st.error(f"❌ Model {selected_model} failed: {str(e)}")
                    
                    # Action buttons
                    st.markdown("### Generate Descriptions")
                    st.caption(f"Generate AI-powered descriptions for {len(selected_objects)} selected object(s)")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if st.button("Generate Table Descriptions", use_container_width=True):
                            generate_descriptions_for_objects(conn, selected_model, selected_db, selected_schema, selected_objects, selected_rows, "table")
                    
                    with col2:
                        if st.button("Generate Column Descriptions", use_container_width=True):
                            generate_descriptions_for_objects(conn, selected_model, selected_db, selected_schema, selected_objects, selected_rows, "column")
                    
                    with col3:
                        if st.button("Generate Both", type="primary", use_container_width=True):
                            generate_descriptions_for_objects(conn, selected_model, selected_db, selected_schema, selected_objects, selected_rows, "both")
                
                else:
                    st.info("✨ Select objects from the table above using the checkboxes to enable description generation.")
            
            else:
                st.info("No objects found matching the current filters.")
                
        else:
            st.info("No tables or views found in the selected schema.")
    
    else:
        st.info("Please select a database and schema to get started.")

def get_view_ddl(conn, database_name: str, schema_name: str, view_name: str) -> str:
    """Get the DDL for a view using GET_DDL function."""
    try:
        fully_qualified_name = get_fully_qualified_name(database_name, schema_name, view_name)
        ddl_query = f"SELECT GET_DDL('VIEW', '{fully_qualified_name}')"
        
        if hasattr(conn, 'sql'):
            result = conn.sql(ddl_query).to_pandas()
            return result.iloc[0, 0] if not result.empty else ""
        else:
            result = pd.read_sql(ddl_query, conn)
            return result.iloc[0, 0] if not result.empty else ""
    except Exception as e:
        st.error(f"Error getting view DDL: {str(e)}")
        return ""

def update_view_descriptions(conn, database, schema, view_name, columns_df, model, generated_descriptions, 
                           view_description=None, generate_columns=True):
    """
    Update view and/or column descriptions by recreating the view with comments.
    This is necessary because Snowflake doesn't support COMMENT ON VIEW or COMMENT ON COLUMN for views.
    
    Args:
        view_description: If provided, use this as the view comment
        generate_columns: If True, generate column descriptions
    """
    try:
        st.info(f"🔍 Step 1: Getting DDL for view {database}.{schema}.{view_name}")
        
        # Get the original view DDL using GET_DDL function
        original_ddl = get_view_ddl(conn, database, schema, view_name)
        if not original_ddl:
            st.error(f"❌ Could not retrieve DDL for view {view_name}")
            return False
            
        st.success(f"✅ Retrieved view DDL ({len(original_ddl)} characters)")
        st.info(f"📄 DDL Preview: {original_ddl[:100]}...")
        
        # Generate column descriptions if requested
        column_descriptions = {}
        if generate_columns:
            st.info(f"🔍 Step 2: Generating column descriptions for view {view_name}")
            
            for _, col_row in columns_df.iterrows():
                col_name = col_row['COLUMN_NAME']
                data_type = col_row['DATA_TYPE']
                
                try:
                    new_col_desc = generate_column_description(
                        conn, model, database, schema, view_name, col_name, data_type
                    )
                    
                    if new_col_desc:
                        column_descriptions[col_name] = new_col_desc
                        # Collect for summary display
                        generated_descriptions.append({
                            'type': 'column',
                            'object': f"{view_name}.{col_name}",
                            'description': new_col_desc
                        })
                        
                except Exception as e:
                    st.warning(f"⚠️ Could not generate description for {view_name}.{col_name}: {str(e)}")
            
            if column_descriptions:
                st.success(f"✅ Generated descriptions for {len(column_descriptions)} columns")
                st.info(f"🔍 Will update comments for: {', '.join(column_descriptions.keys())}")
            else:
                st.warning(f"⚠️ No column descriptions generated for view {view_name}")
        
        # Check if we have anything to update
        if not view_description and not column_descriptions:
            st.warning(f"⚠️ No descriptions to update for view {view_name}")
            return False
        
        st.info(f"🔍 Step 3: Parsing view DDL")
        
        # Parse the DDL to extract the view name and SELECT statement
        import re
        ddl_upper = original_ddl.upper()
        
        # Find view name - look for pattern: CREATE [OR REPLACE] VIEW schema.view_name
        view_pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([^\s\(]+)'
        view_match = re.search(view_pattern, ddl_upper)
        if not view_match:
            st.error("Could not extract view name from DDL")
            st.error(f"View DDL preview: {original_ddl[:200]}...")
            return False
        
        full_view_name = view_match.group(1)
        
        # More robust AS detection - look for the AS that comes after the view definition
        # Since we're searching in ddl_upper (uppercase), use uppercase patterns
        as_patterns = [
            r'\)\s*(?:COMMENT\s*=\s*[\'"][^\'\"]*[\'"])?\s*AS\s*\(',  # ) [COMMENT='...'] AS (
            r'\)\s*AS\s*\(',  # ) AS (
            r'\)\s*(?:COMMENT\s*=\s*[\'"][^\'\"]*[\'"])?\s*AS\s+SELECT',  # ) [COMMENT='...'] AS SELECT
            r'\)\s*AS\s+SELECT',  # ) AS SELECT  
            r'\)\s*AS\s*SELECT'   # ) AS SELECT (no space)
        ]
        
        as_match = None
        for pattern in as_patterns:
            match = re.search(pattern, ddl_upper, re.DOTALL)
            if match:
                as_match = match
                break
        
        if not as_match:
            st.error("Could not find AS clause after view definition")
            st.error(f"View DDL preview: {original_ddl[:500]}...")
            st.error(f"Uppercase DDL preview: {ddl_upper[:500]}...")
            # Try a simpler approach - just find " AS " in the DDL
            simple_as_pos = ddl_upper.find(' AS ')
            if simple_as_pos != -1:
                st.info(f"Found simple ' AS ' at position {simple_as_pos}")
                as_match = type('Match', (), {'end': lambda: simple_as_pos + 4})()
            else:
                return False
        
        # Extract the SELECT statement starting after the AS clause
        as_end_pos = as_match.end()
        
        # Find where the actual SELECT starts (might be after '(' and comments)
        remaining_ddl = original_ddl[as_end_pos:]
        
        # Look for the SELECT keyword
        select_match = re.search(r'SELECT', remaining_ddl.upper())
        if select_match:
            select_start_in_remaining = select_match.start()
            select_statement = remaining_ddl[select_start_in_remaining:]
        else:
            # Fallback - take everything after AS
            select_statement = remaining_ddl
        
        # Clean up the select statement
        select_statement = select_statement.strip()
        
        st.info(f"🔍 Extracted SELECT statement preview: {select_statement[:100]}...")
        
        # Build the column list with comments (existing + new)
        column_definitions = []
        
        for _, row in columns_df.iterrows():
            col_name = row['COLUMN_NAME']
            current_comment = row['CURRENT_DESCRIPTION']
            
            # Check if this column has a new comment
            if col_name.upper() in [c.upper() for c in column_descriptions.keys()]:
                # Find the matching column comment (case insensitive)
                new_comment = None
                for update_col, update_comment in column_descriptions.items():
                    if update_col.upper() == col_name.upper():
                        new_comment = update_comment
                        break
                
                if new_comment:
                    escaped_comment = new_comment.replace("'", "''")
                    column_definitions.append(f"{col_name} COMMENT '{escaped_comment}'")
            # If the column already has a comment, preserve it
            elif current_comment and current_comment.strip():
                escaped_current = current_comment.replace("'", "''")
                column_definitions.append(f"{col_name} COMMENT '{escaped_current}'")
            # Otherwise, just the column name
            else:
                column_definitions.append(col_name)
        
        st.info(f"🔍 Step 4: Building new view DDL with column comments")
        
        # Build the new CREATE OR REPLACE VIEW statement
        column_list = ",\n        ".join(column_definitions)
        
        # Use the fully qualified view name for safety
        fully_qualified_view_name = get_fully_qualified_name(database, schema, view_name)
        
        # Clean the select statement to ensure proper formatting
        clean_select = select_statement.strip()
        
        # Ensure the SELECT statement starts with SELECT keyword
        if not clean_select.upper().startswith('SELECT'):
            clean_select = f"SELECT {clean_select}"
        
        # Remove any trailing semicolons or extra parentheses from the SELECT statement
        clean_select = clean_select.rstrip(';').rstrip(')')
        
        # Build the complete DDL with proper AS clause and view comment
        view_comment_clause = ""
        if view_description:
            escaped_view_desc = view_description.replace("'", "''")
            view_comment_clause = f" COMMENT = '{escaped_view_desc}'"
        
        new_ddl = f"""CREATE OR REPLACE VIEW {fully_qualified_view_name} (
        {column_list}
    ){view_comment_clause} AS (
    {clean_select}
    )"""
    
        st.info(f"🔍 Step 5: Final DDL generated ({len(new_ddl)} characters)")
        
        # Show a preview of the new DDL
        st.code(new_ddl[:400] + "..." if len(new_ddl) > 400 else new_ddl, language="sql")
        
        # Execute the new DDL
        try:
            st.info(f"🔍 Step 6: Executing view recreation SQL...")
            
            if hasattr(conn, 'sql'):  # Snowpark session
                st.info("🔧 Using Snowpark connection to execute DDL")
                result = conn.sql(new_ddl).collect()
                st.info(f"📊 Snowpark execution result: {len(result)} rows returned")
            else:  # Regular connection
                st.info("🔧 Using regular connection to execute DDL")
                cursor = conn.cursor()
                cursor.execute(new_ddl)
                st.info("📊 Regular connection execution completed")
            
            # Build success message based on what was updated
            updates = []
            if view_description:
                updates.append("view description")
            if column_descriptions:
                updates.append(f"column comments for: {', '.join(column_descriptions.keys())}")
            
            update_msg = " and ".join(updates)
            st.success(f"✅ Successfully recreated view {view_name} with {update_msg}")
            
            # Log view description to history if provided
            if view_description:
                log_description_history(conn, database, schema, view_name, 'VIEW', 
                                      None, view_description)
            
            # Log column descriptions to history
            for col_name, col_desc in column_descriptions.items():
                log_description_history(conn, database, schema, f"{view_name}.{col_name}", 'COLUMN', 
                                      None, col_desc)
            
            return True
            
        except Exception as exec_error:
            st.error(f"❌ Error executing view recreation: {str(exec_error)}")
            st.error("🚨 Failed DDL:")
            st.code(new_ddl, language="sql")
            return False
        
    except Exception as e:
        st.error(f"❌ Error updating view column comments: {str(e)}")
        return False

def generate_descriptions_for_objects(conn, model, database, schema, objects, selected_rows, generation_type):
    """Generate descriptions for selected objects."""
    
    total_updates = 0
    generated_descriptions = []  # Track all generated descriptions for summary
    
    # Create expander for processing details
    with st.expander("🔍 View Processing Details", expanded=False):
        with st.spinner("Generating descriptions..."):
            
            for obj_name in objects:
                
                # Determine the schema for this object
                if schema:
                    obj_schema = schema
                    display_name = obj_name
                else:
                    # Find schema from the selected rows
                    obj_row = selected_rows[selected_rows['OBJECT_NAME'] == obj_name]
                    if obj_row.empty or 'SCHEMA_NAME' not in obj_row.columns:
                        st.warning(f"⚠️ Could not find schema for {obj_name}, skipping...")
                        continue
                    obj_schema = obj_row.iloc[0]['SCHEMA_NAME']
                    display_name = f"{obj_schema}.{obj_name}"
                
                # Generate table/view descriptions
                if generation_type in ['table', 'both']:
                    st.write(f"Processing table/view: {display_name}")
                    
                    # Get current description
                    refresh_key = st.session_state.get('last_refresh', '')
                    tables_df = get_tables_and_views(conn, database, obj_schema, refresh_key)
                    current_obj = tables_df[tables_df['OBJECT_NAME'] == obj_name]
                    if current_obj.empty:
                        st.warning(f"⚠️ Could not find {obj_name} in {obj_schema}, skipping...")
                        continue
                    current_obj = current_obj.iloc[0]
                    current_desc = current_obj['CURRENT_DESCRIPTION']
                    object_type = current_obj['OBJECT_TYPE']
                    
                    # Generate description
                    try:
                        new_desc = generate_table_description(
                            conn, model, database, obj_schema, obj_name, 
                            'TABLE' if object_type == 'BASE TABLE' else 'VIEW'
                        )
                        
                        if new_desc:
                            if object_type == 'BASE TABLE':
                                # For tables, use COMMENT ON TABLE
                                escaped_desc = new_desc.replace("'", "''")
                                fully_qualified_name = get_fully_qualified_name(database, obj_schema, obj_name)
                                comment_sql = f"COMMENT ON TABLE {fully_qualified_name} IS '{escaped_desc}';"
                                
                                # Execute the comment
                                if execute_comment_sql(conn, comment_sql, 'TABLE'):
                                    st.success(f"✅ Updated description for {obj_name}")
                                    total_updates += 1
                                    # Log to history
                                    log_description_history(conn, database, obj_schema, obj_name, 'TABLE', 
                                                          current_desc, new_desc)
                                    # Collect for summary display
                                    generated_descriptions.append({
                                        'type': 'table',
                                        'object': obj_name,
                                        'description': new_desc
                                    })
                                else:
                                    st.error(f"❌ Failed to update description for {obj_name}")
                            else:
                                # For views, we need to store the description to use with CREATE OR REPLACE VIEW
                                # We'll handle this when processing columns, or if no columns are being processed
                                st.session_state[f'view_desc_{obj_name}'] = new_desc
                                st.success(f"✅ Generated description for view {obj_name} (will be applied with view recreation)")
                                # Collect for summary display
                                generated_descriptions.append({
                                    'type': 'table',
                                    'object': obj_name,
                                    'description': new_desc
                                })
                        else:
                            st.warning(f"⚠️ No description generated for {obj_name}")
                            
                    except Exception as e:
                        st.error(f"❌ Error processing {obj_name}: {str(e)}")
                    
                    # Handle view descriptions that were generated but not applied (table-only generation)
                    if generation_type == 'table' and object_type == 'VIEW':
                        view_desc = st.session_state.get(f'view_desc_{obj_name}', None)
                        if view_desc:
                            # Apply the view description immediately since no columns will be processed
                            refresh_key = st.session_state.get('last_refresh', '')
                            columns_df = get_columns(conn, database, obj_schema, obj_name, refresh_key)
                            
                            success = update_view_descriptions(
                                conn, database, obj_schema, obj_name, columns_df, model, generated_descriptions,
                                view_description=view_desc, generate_columns=False
                            )
                            if success:
                                total_updates += 1
                                # Clean up the stored view description
                                del st.session_state[f'view_desc_{obj_name}']
            
                # Generate column descriptions
                if generation_type in ['column', 'both']:
                    st.write(f"Processing columns in: {display_name}")
                    
                    refresh_key = st.session_state.get('last_refresh', '')
                    columns_df = get_columns(conn, database, obj_schema, obj_name, refresh_key)
                    
                    # Get object type to handle views differently
                    tables_df = get_tables_and_views(conn, database, obj_schema, refresh_key)
                    current_obj = tables_df[tables_df['OBJECT_NAME'] == obj_name]
                    if current_obj.empty:
                        st.warning(f"⚠️ Could not find {obj_name} in {obj_schema} for column processing, skipping...")
                        continue
                    object_type = current_obj.iloc[0]['OBJECT_TYPE']
                
                    # For views, we need to handle column descriptions differently
                    if object_type == 'VIEW':
                        # Check if we have a stored view description from table generation
                        view_desc = st.session_state.get(f'view_desc_{obj_name}', None)
                        
                        success = update_view_descriptions(
                            conn, database, obj_schema, obj_name, columns_df, model, generated_descriptions,
                            view_description=view_desc, generate_columns=True
                        )
                        if success:
                            # Count updates: view description (if any) + column descriptions
                            update_count = len(columns_df)
                            if view_desc:
                                update_count += 1  # Add 1 for the view description
                            total_updates += update_count
                            
                            # Clean up the stored view description
                            if view_desc:
                                del st.session_state[f'view_desc_{obj_name}']
                    else:
                        # For tables, use the standard column comment approach
                        for _, col_row in columns_df.iterrows():
                            col_name = col_row['COLUMN_NAME']
                            data_type = col_row['DATA_TYPE']
                            current_col_desc = col_row['CURRENT_DESCRIPTION']
                            
                            try:
                                new_col_desc = generate_column_description(
                                    conn, model, database, obj_schema, obj_name, col_name, data_type
                                )
                                
                                if new_col_desc:
                                    # Create COMMENT SQL for column (tables only)
                                    escaped_col_desc = new_col_desc.replace("'", "''")
                                    fully_qualified_name = get_fully_qualified_name(database, obj_schema, obj_name)
                                    quoted_col_name = quote_identifier(col_name)
                                    comment_sql = f"COMMENT ON COLUMN {fully_qualified_name}.{quoted_col_name} IS '{escaped_col_desc}';"
                                    
                                    # Execute the comment
                                    if execute_comment_sql(conn, comment_sql, 'COLUMN'):
                                        st.success(f"✅ Updated description for {obj_name}.{col_name}")
                                        total_updates += 1
                                        # Log to history
                                        log_description_history(conn, database, obj_schema, f"{obj_name}.{col_name}", 'COLUMN', 
                                                              current_col_desc, new_col_desc)
                                        # Collect for summary display
                                        generated_descriptions.append({
                                            'type': 'column',
                                            'object': f"{obj_name}.{col_name}",
                                            'description': new_col_desc
                                        })
                                    else:
                                        st.error(f"❌ Failed to update description for {obj_name}.{col_name}")
                                else:
                                    st.warning(f"⚠️ No description generated for {obj_name}.{col_name}")
                                    
                            except Exception as e:
                                st.error(f"Error processing {obj_name}.{col_name}: {str(e)}")
    
    st.success(f"Description generation complete! Updated {total_updates} descriptions.")
    
    # Show generated descriptions in a collapsible section
    if generated_descriptions:
        with st.expander("📝 View Generated Descriptions", expanded=False):
            st.markdown("**All generated descriptions from this session:**")
            
            # Group by type for better organization
            table_descriptions = [desc for desc in generated_descriptions if desc['type'] == 'table']
            column_descriptions = [desc for desc in generated_descriptions if desc['type'] == 'column']
            
            if table_descriptions:
                st.markdown("### 📋 Table/View Descriptions")
                for desc in table_descriptions:
                    st.markdown(f"**{desc['object']}:**")
                    st.markdown(f"> {desc['description']}")
                    st.markdown("---")
            
            if column_descriptions:
                st.markdown("### 📊 Column Descriptions")
                for desc in column_descriptions:
                    st.markdown(f"**{desc['object']}:**")
                    st.markdown(f"> {desc['description']}")
                    st.markdown("---")
            
            st.info(f"💡 **Summary:** Generated {len(table_descriptions)} table descriptions and {len(column_descriptions)} column descriptions")

def show_data_quality_page(conn):
    """Display the modern single-page data quality configuration interface."""
    
    st.markdown("# 🔍 Data Quality Monitoring")
    st.markdown("Configure Snowflake Data Metric Functions (DMFs) with smart data type filtering and bulk operations.")
    
    # Quick access to documentation
    with st.expander("📚 **Quick Reference & Documentation**", expanded=False):
        show_dmf_quick_reference()
    
    st.markdown("---")
    
    # Main configuration interface
    show_modern_dmf_interface(conn)

# ========================================================================================
# SYSTEM DMF DEFINITIONS AND DATA TYPE MAPPINGS
# ========================================================================================

# Comprehensive system DMFs with their supported data types
SYSTEM_DMFS = {
    # Table-level DMFs
    'ROW_COUNT': {
        'label': 'Row Count',
        'description': 'Total number of rows in the table',
        'level': 'table',
        'data_types': [],  # Table-level, no column data types
        'help': 'Monitors the total row count in the table'
    },
    'FRESHNESS': {
        'label': 'Data Freshness',
        'description': 'Data freshness based on timestamp column',
        'level': 'column',
        'data_types': ['DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ'],
        'help': 'Measures how recent the data is based on a timestamp column'
    },
    
    # Column-level DMFs - Basic Quality
    'NULL_COUNT': {
        'label': 'Null Count',
        'description': 'Count of NULL values in a column',
        'level': 'column',
        'data_types': ['ALL'],  # Works with all data types
        'help': 'Counts the number of NULL values in the column'
    },
    'DUPLICATE_COUNT': {
        'label': 'Duplicate Count',
        'description': 'Count of duplicate values in a column',
        'level': 'column',
        'data_types': ['ALL'],  # Works with all data types
        'help': 'Counts the number of duplicate values in the column'
    },
    'UNIQUE_COUNT': {
        'label': 'Unique Count',
        'description': 'Count of unique, non-NULL values in a column',
        'level': 'column',
        'data_types': ['ALL'],  # Works with all data types
        'help': 'Counts the number of unique, non-NULL values in the column'
    },
    
    # Note: Only including verified working system DMFs
    # Additional DMFs can be added here as they are tested and confirmed
}

def get_compatible_dmfs_for_data_type(data_type: str) -> List[str]:
    """Get list of DMF keys that are compatible with the given data type."""
    compatible_dmfs = []
    data_type_upper = data_type.upper()
    
    for dmf_key, dmf_info in SYSTEM_DMFS.items():
        if dmf_info['level'] == 'table':
            continue  # Skip table-level DMFs
            
        # Check if DMF supports all data types
        if 'ALL' in dmf_info['data_types']:
            compatible_dmfs.append(dmf_key)
            continue
            
        # Check if data type matches any of the supported types
        for supported_type in dmf_info['data_types']:
            if supported_type in data_type_upper:
                compatible_dmfs.append(dmf_key)
                break
    
    return compatible_dmfs

def show_dmf_quick_reference():
    """Show quick reference documentation in a collapsible section."""
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🔐 Required Permissions")
        st.code("""
-- Grant required database roles
           GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE your_role;
           GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_LOOKUP TO ROLE your_role;
        """, language="sql")
        
        st.markdown("### 📊 Available System DMFs")
        st.markdown("""
        **Table-Level:**
        - `ROW_COUNT` - Total rows in table
        - `FRESHNESS` - Data freshness (requires timestamp column)
        
        **Column-Level (All Types):**
        - `NULL_COUNT` - Count of NULL values
        - `DUPLICATE_COUNT` - Count of duplicate values  
        - `UNIQUE_COUNT` - Count of unique values
        """)
    
    with col2:
        st.markdown("### 📅 Schedule Examples")
        st.code("""
-- Every 30 minutes
'30 MINUTE'

-- Every 4 hours  
'USING CRON 0 */4 * * * UTC'

-- Daily at 6 AM UTC
'USING CRON 0 6 * * * UTC'

-- On data changes
'TRIGGER_ON_CHANGES'
        """, language="sql")
        
        st.markdown("### 🔍 View Results")
        st.code("""
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
ORDER BY MEASUREMENT_TIME DESC;
        """, language="sql")

def show_modern_dmf_interface(conn):
    """Modern single-page DMF configuration interface."""
    
    # Step 1: Database and Schema Selection
    st.markdown("## 🎯 Step 1: Select Database and Schema")
    
    col1, col2 = st.columns(2)
    
    with col1:
        databases = get_databases(conn)
        if not databases:
            st.error("❌ No databases accessible. Please check your permissions.")
            return
        
        selected_db = st.selectbox(
            "📁 Database",
            options=[""] + databases,
            key="modern_dmf_database",
            help="Choose a database to explore tables"
        )
    
    with col2:
        if selected_db:
            schemas = get_schemas(conn, selected_db)
            selected_schema = st.selectbox(
                "📂 Schema", 
                options=[""] + schemas,
                key="modern_dmf_schema",
                help="Choose a schema within the selected database"
            )
        else:
            selected_schema = ""
            st.selectbox("📂 Schema", options=[""], disabled=True, help="Select a database first")
    
    if not selected_db or not selected_schema:
        st.info("👆 Please select both a database and schema to continue.")
        return
    
    # Step 2: Table Selection with Modern Grid
    st.markdown("---")
    st.markdown("## 📋 Step 2: Select Tables for Data Quality Monitoring")
    
    # Get tables
    refresh_key = st.session_state.get('last_refresh', '')
    tables_df = get_tables_and_views(conn, selected_db, selected_schema, refresh_key)
    
    if tables_df.empty:
        st.warning(f"No tables found in `{selected_db}.{selected_schema}`. Please check permissions or try a different schema.")
        return
    
    # Table filtering and selection controls
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        show_only_tables = st.checkbox(
            "📊 Tables only", 
            value=True,
            help="Show only base tables (exclude views)",
            key="modern_show_tables_only"
        )
    
    with col2:
        search_term = st.text_input(
            "🔍 Search tables",
            placeholder="Filter by name...",
            key="modern_table_search"
        )
    
    with col3:
        if st.button("🔄 Refresh", help="Refresh table list from Snowflake"):
            st.cache_data.clear()
            st.session_state['last_refresh'] = str(time.time())
            st.rerun()
    
    with col4:
        select_all = st.checkbox("✅ Select All", key="modern_select_all")
    
    # Apply filters
    filtered_df = tables_df.copy()
    
    if show_only_tables:
        filtered_df = filtered_df[filtered_df['OBJECT_TYPE'] == 'BASE TABLE']
    
    if search_term:
        filtered_df = filtered_df[
            filtered_df['OBJECT_NAME'].str.contains(search_term, case=False, na=False)
        ]
    
    if filtered_df.empty:
        st.info("No tables match your current filters. Try adjusting the search term or filters.")
        return
    
    # Add selection column
    filtered_df.insert(0, "Select", select_all)
    
    # Modern table selection grid
    st.markdown(f"**Found {len(filtered_df)} table(s) matching your criteria:**")
    
    edited_df = st.data_editor(
        filtered_df,
        use_container_width=True,
        column_config={
            "Select": st.column_config.CheckboxColumn(
                "Select",
                help="Select tables for DMF configuration",
                default=False
            ),
            "OBJECT_NAME": st.column_config.TextColumn(
                "Table Name",
                help="Name of the table",
                width="medium"
            ),
            "OBJECT_TYPE": st.column_config.TextColumn(
                "Type",
                help="Object type (TABLE or VIEW)",
                width="small"
            ),
            "CURRENT_DESCRIPTION": st.column_config.TextColumn(
                "Description",
                help="Current table description",
                width="large"
            ),
            "HAS_DESCRIPTION": st.column_config.CheckboxColumn(
                "Has Desc",
                help="Whether table has a description",
                width="small"
            )
        },
        hide_index=True,
        key="modern_table_selection_grid"
    )
    
    # Get selected tables
    selected_tables = edited_df[edited_df["Select"] == True]
    
    if selected_tables.empty:
        st.info("👆 Select one or more tables above to configure data quality metrics.")
        return
    
    # Step 3: Configuration for Selected Tables
    st.markdown("---")
    st.markdown(f"## ⚙️ Step 3: Configure DMFs for {len(selected_tables)} Selected Table(s)")
    
    # Show selected tables summary
    with st.expander(f"📋 **Selected Tables ({len(selected_tables)})**", expanded=False):
        for _, table in selected_tables.iterrows():
            st.markdown(f"• **{table['OBJECT_NAME']}** ({table['OBJECT_TYPE']})")
            if table['CURRENT_DESCRIPTION']:
                st.caption(f"  ↳ {table['CURRENT_DESCRIPTION']}")
    
    # Bulk Schedule Configuration
    st.markdown("### 📅 Monitoring Schedule")
    st.markdown("Set the monitoring schedule that will apply to all selected tables.")
    
    schedule_config = configure_monitoring_schedule("modern_bulk")
    
    if not schedule_config:
        st.info("👆 Please configure a monitoring schedule to continue.")
        return
    
    st.success(f"📅 **Schedule**: {schedule_config['description']}")
    
    # Individual Table Configuration
    st.markdown("---")
    st.markdown("### 🔧 Individual Table Configuration")
    st.markdown("Configure specific DMFs for each selected table. Each table shows only compatible metrics based on its column data types.")
    
    # Store all configurations
    table_configurations = {}
    
    # Create expander for each selected table
    for _, table_row in selected_tables.iterrows():
        table_name = table_row['OBJECT_NAME']
        
        with st.expander(f"🏷️ **{table_name}** - Configure DMFs", expanded=True):
            config = configure_table_dmfs(
                conn, selected_db, selected_schema, table_name, 
                key_prefix=f"modern_{table_name}"
            )
            
            if config:
                table_configurations[table_name] = config
    
    # Step 4: Generate and Execute
    if table_configurations:
        st.markdown("---")
        st.markdown("## 🚀 Step 4: Apply Configuration")
        
        # Generate SQL for all tables
        sql_commands = generate_bulk_dmf_sql(
            selected_db, selected_schema, schedule_config, table_configurations
        )
        
        # Show SQL preview
        with st.expander("📄 **Preview Generated SQL**", expanded=False):
            st.code(sql_commands, language="sql")
        
        # Action buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.download_button(
                label="📥 Download SQL",
                data=sql_commands,
                file_name=f"dmf_setup_{len(table_configurations)}_tables.sql",
                mime="text/sql",
                help="Download the generated SQL for manual execution"
            )
        
        with col2:
            if st.button(
                "🔧 Apply All DMFs", 
                type="primary",
                help=f"Execute SQL to configure DMFs on {len(table_configurations)} table(s)"
            ):
                execute_bulk_dmf_configuration(
                    conn, selected_db, selected_schema, sql_commands, table_configurations
                )
        
        with col3:
            if st.button("🧪 Test Connection", help="Test database connection and permissions"):
                test_dmf_permissions(conn, selected_db, selected_schema)

def configure_monitoring_schedule(key_prefix: str) -> dict:
    """Configure monitoring schedule with modern UI."""
    
    schedule_type = st.radio(
        "Choose schedule type:",
        options=["⏱️ Periodic", "📅 Daily", "🔄 On Changes"],
        key=f"{key_prefix}_schedule_type",
        horizontal=True,
        help="How often should data quality checks run?"
    )
    
    if schedule_type == "⏱️ Periodic":
        col1, col2 = st.columns(2)
        with col1:
            interval_type = st.selectbox(
                "Interval",
                options=["Minutes", "Hours"],
                key=f"{key_prefix}_interval_type"
            )
        with col2:
            if interval_type == "Minutes":
                interval = st.selectbox(
                    "Every X minutes",
                    options=[5, 15, 30, 60],
                    index=2,
                    key=f"{key_prefix}_minutes"
                )
                return {
                    'schedule_expression': f'{interval} MINUTE',
                    'description': f'Every {interval} minutes'
                }
            else:
                interval = st.selectbox(
                    "Every X hours", 
                    options=[1, 2, 4, 6, 8, 12, 24],
                    index=2,
                    key=f"{key_prefix}_hours"
                )
                return {
                    'schedule_expression': f'USING CRON 0 */{interval} * * * UTC',
                    'description': f'Every {interval} hours'
                }
    
    elif schedule_type == "📅 Daily":
        col1, col2 = st.columns(2)
        with col1:
            hour = st.selectbox(
                "Hour (24h format)",
                options=list(range(24)),
                index=6,
                key=f"{key_prefix}_hour"
            )
        with col2:
            minute = st.selectbox(
                "Minute",
                options=[0, 15, 30, 45],
                key=f"{key_prefix}_minute"
            )
        return {
            'schedule_expression': f'USING CRON {minute} {hour} * * * UTC',
            'description': f'Daily at {hour:02d}:{minute:02d} UTC'
        }
    
    else:  # On Changes
        st.info("💡 Triggers when table data changes (INSERT, UPDATE, DELETE)")
        return {
            'schedule_expression': 'TRIGGER_ON_CHANGES',
            'description': 'When data changes'
        }

def configure_table_dmfs(conn, database: str, schema: str, table_name: str, key_prefix: str) -> dict:
    """Configure DMFs for a specific table with smart data type filtering."""
    
    # Get table columns
    refresh_key = st.session_state.get('last_refresh', '')
    columns_df = get_columns(conn, database, schema, table_name, refresh_key)
    
    if columns_df.empty:
        st.warning(f"Could not retrieve columns for {table_name}")
        return None
    
    st.markdown(f"**Table Info:** {len(columns_df)} columns")
    
    config = {
        'table_dmfs': {},
        'column_dmfs': {}
    }
    
    # Table-level DMFs
    st.markdown("##### 🏢 Table-Level Metrics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        config['table_dmfs']['ROW_COUNT'] = st.checkbox(
            f"✅ {SYSTEM_DMFS['ROW_COUNT']['label']}",
            help=SYSTEM_DMFS['ROW_COUNT']['help'],
            key=f"{key_prefix}_row_count"
        )
    
    with col2:
        # Check for timestamp columns
        timestamp_cols = [
            col for col in columns_df['COLUMN_NAME'] 
            if any(word in col.upper() for word in ['DATE', 'TIME', 'TIMESTAMP', 'CREATED', 'UPDATED'])
        ]
        
        if timestamp_cols:
            config['table_dmfs']['FRESHNESS'] = st.checkbox(
                f"✅ {SYSTEM_DMFS['FRESHNESS']['label']}",
                help=SYSTEM_DMFS['FRESHNESS']['help'],
                key=f"{key_prefix}_freshness"
            )
            
            if config['table_dmfs']['FRESHNESS']:
                config['freshness_column'] = st.selectbox(
                    "Timestamp column",
                    options=timestamp_cols,
                    key=f"{key_prefix}_freshness_col"
                )
        else:
            st.info("💡 No timestamp columns found for freshness monitoring")
    
    # Column-level DMFs
    st.markdown("##### 📊 Column-Level Metrics")
    
    # Group columns by data type for better organization
    column_groups = {}
    for _, col_row in columns_df.iterrows():
        col_name = col_row['COLUMN_NAME']
        data_type = col_row['DATA_TYPE']
        
        if data_type not in column_groups:
            column_groups[data_type] = []
        column_groups[data_type].append(col_name)
    
    # Individual column DMF selection - flexible approach
    if column_groups:
        st.markdown("**Configure metrics for individual columns:**")
        
        # Create tabs for each data type for better organization
        data_types = list(column_groups.keys())
        
        if len(data_types) == 1:
            # Single data type - show columns directly
            data_type = data_types[0]
            columns = column_groups[data_type]
            compatible_dmfs = get_compatible_dmfs_for_data_type(data_type)
            
            if compatible_dmfs:
                st.markdown(f"**{data_type}** columns - Select metrics for each column individually:")
                
                # Show each column with its own DMF selection
                for col_name in columns:
                    st.markdown(f"##### 📊 {col_name}")
                    
                    selected_dmfs = []
                    dmf_cols = st.columns(min(len(compatible_dmfs), 4))
                    
                    for i, dmf_key in enumerate(compatible_dmfs):
                        dmf_info = SYSTEM_DMFS[dmf_key]
                        col_idx = i % len(dmf_cols)
                        
                        with dmf_cols[col_idx]:
                            if st.checkbox(
                                dmf_info['label'],
                                help=dmf_info['help'],
                                key=f"{key_prefix}_{col_name}_{dmf_key}"
                            ):
                                selected_dmfs.append(dmf_key)
                    
                    # Store individual column selections
                    if selected_dmfs:
                        config['column_dmfs'][col_name] = selected_dmfs
                        st.success(f"✅ {col_name}: {', '.join([SYSTEM_DMFS[dmf]['label'] for dmf in selected_dmfs])}")
                    else:
                        st.caption(f"No metrics selected for {col_name}")
        
        else:
            # Multiple data types - use tabs with individual column selection
            tab_names = [f"{dt} ({len(column_groups[dt])})" for dt in data_types]
            tabs = st.tabs(tab_names)
            
            for i, data_type in enumerate(data_types):
                with tabs[i]:
                    columns = column_groups[data_type]
                    compatible_dmfs = get_compatible_dmfs_for_data_type(data_type)
                    
                    if not compatible_dmfs:
                        st.info(f"No compatible metrics available for {data_type} columns")
                        continue
                    
                    st.caption(f"**Available metrics for {data_type}:** {', '.join([SYSTEM_DMFS[dmf]['label'] for dmf in compatible_dmfs])}")
                    
                    st.markdown("**Configure each column individually:**")
                    
                    # Show each column with its own DMF selection
                    for col_name in columns:
                        st.markdown(f"##### 📊 {col_name}")
                        
                        selected_dmfs = []
                        dmf_cols = st.columns(min(len(compatible_dmfs), 4))
                        
                        for j, dmf_key in enumerate(compatible_dmfs):
                            dmf_info = SYSTEM_DMFS[dmf_key]
                            col_idx = j % len(dmf_cols)
                            
                            with dmf_cols[col_idx]:
                                # Check if this DMF was already selected (from bulk action or previous selection)
                                default_value = col_name in config['column_dmfs'] and dmf_key in config['column_dmfs'][col_name]
                                
                                if st.checkbox(
                                    dmf_info['label'],
                                    value=default_value,
                                    help=dmf_info['help'],
                                    key=f"{key_prefix}_{col_name}_{dmf_key}"
                                ):
                                    selected_dmfs.append(dmf_key)
                        
                        # Store individual column selections
                        if selected_dmfs:
                            config['column_dmfs'][col_name] = selected_dmfs
                            st.success(f"✅ {col_name}: {', '.join([SYSTEM_DMFS[dmf]['label'] for dmf in selected_dmfs])}")
                        else:
                            # Remove from config if no metrics selected
                            if col_name in config['column_dmfs']:
                                del config['column_dmfs'][col_name]
                            st.caption(f"No metrics selected for {col_name}")
                        
                        st.markdown("")  # Add some spacing between columns
    else:
        st.info("No columns found for this table")
    
    # Show summary
    total_table_dmfs = sum(1 for v in config['table_dmfs'].values() if v)
    total_column_dmfs = len(config['column_dmfs'])
    
    if total_table_dmfs > 0 or total_column_dmfs > 0:
        st.success(f"📊 **Configuration**: {total_table_dmfs} table-level + {total_column_dmfs} column-level metrics")
        return config
    else:
        st.info("No metrics selected for this table")
        return None

def generate_bulk_dmf_sql(database: str, schema: str, schedule_config: dict, table_configs: dict) -> str:
    """Generate SQL for bulk DMF configuration."""
    
    sql_lines = [
        f"-- Bulk DMF Configuration for {len(table_configs)} table(s)",
        f"-- Database: {database}",
        f"-- Schema: {schema}",
        f"-- Schedule: {schedule_config['description']}",
        f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        ""
    ]
    
    for table_name, config in table_configs.items():
        full_table_name = get_fully_qualified_name(database, schema, table_name)
        
        sql_lines.extend([
            f"-- ========================================",
            f"-- Configuration for {table_name}",
            f"-- ========================================",
            "",
            "-- Step 1: Set monitoring schedule",
            f"ALTER TABLE {full_table_name} SET DATA_METRIC_SCHEDULE = '{schedule_config['schedule_expression']}';",
            "",
            "-- Step 2: Add Data Metric Functions"
        ])
        
        # Table-level DMFs
        if config['table_dmfs'].get('ROW_COUNT'):
            sql_lines.append(f"ALTER TABLE {full_table_name} ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();")
        
        if config['table_dmfs'].get('FRESHNESS') and 'freshness_column' in config:
            quoted_col = quote_identifier(config['freshness_column'])
            sql_lines.append(f"ALTER TABLE {full_table_name} ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ({quoted_col});")
        
        # Column-level DMFs
        if config['column_dmfs']:
            sql_lines.append("")
            sql_lines.append("-- Column-level DMFs")
            
            for col_name, dmf_list in config['column_dmfs'].items():
                quoted_col = quote_identifier(col_name)
                for dmf_key in dmf_list:
                    sql_lines.append(f"ALTER TABLE {full_table_name} ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.{dmf_key} ON ({quoted_col});")
        
        sql_lines.extend(["", ""])
    
    sql_lines.extend([
        "-- View results with:",
        "-- SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS",
        "-- ORDER BY MEASUREMENT_TIME DESC;"
    ])
    
    return "\n".join(sql_lines)

def execute_bulk_dmf_configuration(conn, database: str, schema: str, sql_commands: str, table_configs: dict):
    """Execute bulk DMF configuration with progress tracking."""
    
    sql_lines = [line.strip() for line in sql_commands.split('\n') if line.strip() and not line.strip().startswith('--')]
    
    if not sql_lines:
        st.error("No SQL commands to execute")
        return
    
    # Progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    success_count = 0
    error_count = 0
    total_commands = len(sql_lines)
    
    # Execute commands
    for i, sql_line in enumerate(sql_lines):
        progress = (i + 1) / total_commands
        progress_bar.progress(progress)
        status_text.text(f"Executing command {i + 1} of {total_commands}...")
        
        try:
            if execute_comment_sql(conn, sql_line, 'DMF'):
                success_count += 1
                
                # Log DMF history
                if 'ADD DATA METRIC FUNCTION' in sql_line.upper():
                    log_dmf_execution(conn, database, schema, sql_line)
            else:
                error_count += 1
                st.error(f"❌ Failed: {sql_line}")
                
        except Exception as e:
            error_count += 1
            st.error(f"❌ Error in: {sql_line}")
            st.error(f"Details: {str(e)}")
    
    # Final results
    progress_bar.progress(1.0)
    status_text.empty()
    
    if error_count == 0:
        st.success(f"🎉 **Success!** Applied {success_count} DMF configurations to {len(table_configs)} table(s)")
        # st.balloons()
        st.info("💡 View results: `SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS;`")
    else:
        if success_count > 0:
            st.warning(f"⚠️ **Partial Success**: {success_count} succeeded, {error_count} failed")
        else:
            st.error(f"❌ **Failed**: All {error_count} commands failed. Check permissions and table ownership.")

def log_dmf_execution(conn, database: str, schema: str, sql_line: str):
    """Log DMF execution to history table."""
    try:
        import re
        
        # Extract table name
        table_match = re.search(r'ALTER TABLE\s+(?:"?[^".\s]+"?\.)?(?:"?[^".\s]+"?\.)?"?([^".\s]+)"?\s+ADD', sql_line.upper())
        table_name = table_match.group(1).strip('"') if table_match else "UNKNOWN_TABLE"
        
        # Extract DMF type
        dmf_match = re.search(r'SNOWFLAKE\.CORE\.(\w+)', sql_line.upper())
        dmf_type = dmf_match.group(1) if dmf_match else "UNKNOWN_DMF"
        
        # Extract column name if present
        column_match = re.search(r'ON \(([^)]+)\)', sql_line)
        column_name = None
        if column_match and column_match.group(1).strip():
            column_name = column_match.group(1).strip().strip('"').strip("'")
        
        # Log to history
        log_dmf_history(conn, database, schema, table_name, dmf_type, column_name, "ADDED")
        
    except Exception as e:
        # Don't fail the main operation if logging fails
        st.warning(f"Could not log DMF history: {str(e)}")

def test_dmf_permissions(conn, database: str, schema: str):
    """Test DMF permissions and setup."""
    
    with st.spinner("Testing permissions and setup..."):
        results = []
        
        # Test 1: Database roles
        try:
            test_query = "SELECT CURRENT_ROLE()"
            result = pd.read_sql(test_query, conn)
            current_role = result.iloc[0, 0]
            results.append(("✅", "Connection", f"Connected as role: {current_role}"))
        except Exception as e:
            results.append(("❌", "Connection", f"Failed: {str(e)}"))
        
        # Test 2: Database access
        try:
            test_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}'"
            result = pd.read_sql(test_query, conn)
            table_count = result.iloc[0, 0]
            results.append(("✅", "Database Access", f"Can access {table_count} tables in {database}.{schema}"))
        except Exception as e:
            results.append(("❌", "Database Access", f"Failed: {str(e)}"))
        
        # Test 3: DMF monitoring results access
        try:
            test_query = "SELECT COUNT(*) FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS LIMIT 1"
            pd.read_sql(test_query, conn)
            results.append(("✅", "DMF Results Access", "Can access monitoring results"))
        except Exception as e:
            results.append(("❌", "DMF Results Access", f"Failed: {str(e)}"))
    
    # Display results
    st.markdown("### 🧪 Permission Test Results")
    
    for status, test_name, message in results:
        if status == "✅":
            st.success(f"{status} **{test_name}**: {message}")
        else:
            st.error(f"{status} **{test_name}**: {message}")
    
    # Show recommendations
    failed_tests = [r for r in results if r[0] == "❌"]
    if failed_tests:
        st.markdown("### 💡 Recommendations")
        st.markdown("""
        If you see permission errors, ask your Snowflake administrator to run:
           ```sql
        GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE your_role;
        GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_LOOKUP TO ROLE your_role;
           ```
        """)
    
def show_data_contacts_page(conn):
    """Display the data contacts page."""
    
    st.markdown("Manage contacts and assignments for data governance, support, and access control.")
    
    st.info("This feature integrates with Snowflake's contact system for data governance workflows.")
    
    with st.expander("📋 **Contact Types**", expanded=False):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**🔍 Data Steward**")
            st.markdown("Responsible for data accuracy, quality, and governance")
        
        with col2:
            st.markdown("**🛠️ Technical Support**")
            st.markdown("Handles technical issues and system maintenance")
        
        with col3:
            st.markdown("**🔐 Access Control**")
            st.markdown("Manages data access permissions and security")
    
    st.markdown("---")
    
    # Database/Schema/Table selection for contact assignment
    st.markdown("### Assign Contacts to Tables")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        databases = get_databases(conn)
        
        # Find current database index
        current_db_index = 0
        if st.session_state.get('contacts_database', '') in databases:
            current_db_index = databases.index(st.session_state.contacts_database) + 1
        
        selected_db = st.selectbox(
            "Database",
            options=[""] + databases,
            index=current_db_index,
            key="contacts_db_selector",
            help="Choose a database"
        )
        
        # Update session state if changed
        if selected_db != st.session_state.get('contacts_database', ''):
            st.session_state.contacts_database = selected_db
            st.session_state.contacts_schema = ""  # Reset schema when database changes
    
    with col2:
        if selected_db:
            schemas = get_schemas(conn, selected_db)
            
            # Find current schema index
            current_schema_index = 0
            if st.session_state.dmf_schema in schemas:
                current_schema_index = schemas.index(st.session_state.dmf_schema) + 1
            
            selected_schema = st.selectbox(
                "Select Schema",
                options=[""] + schemas,
                index=current_schema_index,
                key="single_dmf_schema_selector",
                help="Choose a schema within the selected database"
            )
            
            # Update session state if changed
            if selected_schema != st.session_state.dmf_schema:
                st.session_state.dmf_schema = selected_schema
        else:
            selected_schema = ""
            st.selectbox("Select Schema", options=[""], disabled=True, key="single_dmf_schema_disabled", help="Select a database first")
    
    with col3:
        if selected_db and selected_schema:
            refresh_key = st.session_state.get('last_refresh', '')
            tables_df = get_tables_and_views(conn, selected_db, selected_schema, refresh_key)
            table_names = tables_df['OBJECT_NAME'].tolist() if not tables_df.empty else []
            
            selected_table = st.selectbox(
                "Select Table",
                options=[""] + table_names,
                key="single_dmf_table_selector",
                help="Choose a table to configure with data quality metrics"
            )
        else:
            selected_table = ""
            st.selectbox("Select Table", options=[""], disabled=True, key="single_dmf_table_disabled", help="Select database and schema first")
    
    if selected_db and selected_schema and selected_table:
        st.markdown(f"### 🎯 Configure DMFs for {selected_table}")
        
        # Get columns for the selected table
        refresh_key = st.session_state.get('last_refresh', '')
        columns_df = get_columns(conn, selected_db, selected_schema, selected_table, refresh_key)
        
        if not columns_df.empty:
            # Schedule Configuration Section
            st.markdown("#### 📅 Set Monitoring Schedule")
            st.markdown("**Important**: A schedule must be set before adding DMFs to the table.")
        
        schedule_type = st.radio(
            "Choose schedule type:",
            options=["Periodic (Minutes/Hours)", "Daily at specific time", "On data changes"],
            help="How often should the data quality checks run?",
                key="single_dmf_schedule_type"
        )
        
        schedule_config = {}
        
        if schedule_type == "Periodic (Minutes/Hours)":
            col1, col2 = st.columns(2)
            with col1:
                interval_type = st.selectbox(
                    "Interval type",
                    options=["Minutes", "Hours"],
                    help="Run every X minutes or hours",
                        key="single_dmf_interval_type"
                )
            with col2:
                if interval_type == "Minutes":
                    interval_value = st.selectbox(
                        "Every X minutes",
                        options=[5, 15, 30, 60],
                        index=2,  # Default to 30 minutes
                        help="Minimum interval is 5 minutes",
                            key="single_dmf_minute_interval"
                    )
                    schedule_config = {
                        'schedule_expression': f'{interval_value} MINUTE',
                        'description': f'Every {interval_value} minutes'
                    }
                else:  # Hours
                    interval_value = st.selectbox(
                        "Every X hours",
                        options=[1, 2, 3, 4, 6, 8, 12, 24],
                        index=3,  # Default to 4 hours
                        help="Run every X hours",
                            key="single_dmf_hour_interval"
                    )
                    schedule_config = {
                        'schedule_expression': f'USING CRON 0 */{interval_value} * * * UTC',
                        'description': f'Every {interval_value} hours'
                    }
        
        elif schedule_type == "Daily at specific time":
            col1, col2 = st.columns(2)
            with col1:
                hour = st.selectbox(
                    "Hour (24-hour format)",
                    options=list(range(24)),
                    index=6,  # Default to 6 AM
                    help="Hour of the day to run (0-23)",
                        key="single_dmf_daily_hour"
                )
            with col2:
                minute = st.selectbox(
                    "Minute",
                    options=[0, 15, 30, 45],
                    index=0,  # Default to top of hour
                    help="Minute of the hour to run",
                        key="single_dmf_daily_minute"
                )
            
            schedule_config = {
                'schedule_expression': f'USING CRON {minute} {hour} * * * UTC',
                'description': f'Daily at {hour:02d}:{minute:02d} UTC'
            }
        
        else:  # On data changes
            schedule_config = {
                'schedule_expression': 'TRIGGER_ON_CHANGES',
                'description': 'When data in the table changes (INSERT, UPDATE, DELETE)'
            }
            st.info("💡 **Note**: This triggers DMFs whenever the table data changes. Use for frequently updated tables where you need immediate quality feedback.")
        
        # Display selected schedule
        if schedule_config:
            st.success(f"📅 **Selected Schedule**: {schedule_config['description']}")
            with st.expander("View schedule details"):
                fully_qualified_table = get_fully_qualified_name(selected_db, selected_schema, selected_table)
                st.code(f"ALTER TABLE {fully_qualified_table} SET DATA_METRIC_SCHEDULE = '{schedule_config['schedule_expression']}';", language="sql")
        
            st.markdown("---")
            
                # DMF Configuration
            st.markdown("#### 🔍 Data Quality Metrics Configuration")
                
                # Table-level DMFs
            st.markdown("##### Table-Level Metrics")
            table_dmf_selections = {}
                
            col1, col2 = st.columns(2)
            with col1:
                table_dmf_selections['ROW_COUNT'] = st.checkbox(
                    f"✅ {SYSTEM_DMFS['ROW_COUNT']['label']}", 
                    help=SYSTEM_DMFS['ROW_COUNT']['help'],
                    key="single_row_count"
                )
                
            with col2:
                # Freshness requires a timestamp column
                timestamp_columns = [col for col in columns_df['COLUMN_NAME'] 
                                    if any(word in col.upper() for word in ['DATE', 'TIME', 'TIMESTAMP', 'CREATED', 'UPDATED'])]
                
                if timestamp_columns:
                    table_dmf_selections['FRESHNESS'] = st.checkbox(
                        f"✅ {SYSTEM_DMFS['FRESHNESS']['label']}", 
                        help=SYSTEM_DMFS['FRESHNESS']['help'],
                        key="single_freshness"
                    )
                    if table_dmf_selections['FRESHNESS']:
                        freshness_column = st.selectbox("Timestamp Column", timestamp_columns, key="single_freshness_col")
                else:
                    st.info("💡 **Data Freshness**: No timestamp columns detected in this table")
            
            # Column-level DMFs with smart data type filtering
                st.markdown("---")
                st.markdown("##### Column-Level Metrics")
                st.markdown("*Smart filtering: Only compatible metrics shown for each column's data type*")
                
                column_dmf_assignments = {}
                    
                for _, col_row in columns_df.iterrows():
                    col_name = col_row['COLUMN_NAME']
                    data_type = col_row['DATA_TYPE']
                    
                    # Get compatible DMFs for this data type
                    compatible_dmfs = get_compatible_dmfs_for_data_type(data_type)
                    
                    if compatible_dmfs:
                        with st.expander(f"📊 {col_name} ({data_type})", expanded=False):
                            st.caption(f"**Data Type:** {data_type}")
                            st.caption(f"**Compatible Metrics:** {len(compatible_dmfs)} available")
                            
                            selected_dmfs = []
                            for dmf_key in compatible_dmfs:
                                dmf_info = SYSTEM_DMFS[dmf_key]
                                if st.checkbox(
                                    f"✅ {dmf_info['label']}", 
                                    help=dmf_info['help'],
                                    key=f"single_{col_name}_{dmf_key}"
                                ):
                                        selected_dmfs.append(dmf_key)
                            
                            if selected_dmfs:
                                column_dmf_assignments[col_name] = selected_dmfs
                                st.success(f"Selected: {', '.join([SYSTEM_DMFS[dmf]['label'] for dmf in selected_dmfs])}")
                    else:
                        with st.expander(f"📊 {col_name} ({data_type})", expanded=False):
                            st.caption(f"**Data Type:** {data_type}")
                            st.info("No compatible data quality metrics available for this data type")
                
                # Generate and execute SQL
                has_table_dmfs = any(table_dmf_selections.values())
                has_column_dmfs = bool(column_dmf_assignments)
                    
                if has_table_dmfs or has_column_dmfs:
                    st.markdown("---")
                    st.markdown("#### 🚀 Apply Data Quality Metrics")
                    
                    # Generate SQL
                    sql_commands = []
                    full_table_name = get_fully_qualified_name(selected_db, selected_schema, selected_table)
                    
                    sql_commands.append(f"-- DMF setup for {full_table_name}")
                    sql_commands.append("")
                    
                    # Set schedule
                    if schedule_config:
                        sql_commands.append("-- Step 1: Set monitoring schedule (required)")
                        sql_commands.append(f"ALTER TABLE {full_table_name} SET DATA_METRIC_SCHEDULE = '{schedule_config['schedule_expression']}';")
                        sql_commands.append("")
                    
                    # Add DMFs
                    sql_commands.append("-- Step 2: Add Data Metric Functions")
                    
                    # Table-level DMFs
                    if table_dmf_selections.get('ROW_COUNT'):
                        sql_commands.append(f"ALTER TABLE {full_table_name} ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();")
                    
                    if table_dmf_selections.get('FRESHNESS') and 'freshness_column' in locals():
                        quoted_freshness_col = quote_identifier(freshness_column)
                        sql_commands.append(f"ALTER TABLE {full_table_name} ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ({quoted_freshness_col});")
                    
                    # Column-level DMFs
                    if column_dmf_assignments:
                        sql_commands.append("")
                        sql_commands.append("-- Column-level DMFs")
                        
                        for col_name, dmf_list in column_dmf_assignments.items():
                            quoted_col_name = quote_identifier(col_name)
                            sql_commands.append(f"-- DMFs for column: {quoted_col_name}")
                            for dmf_key in dmf_list:
                                    sql_commands.append(f"ALTER TABLE {full_table_name} ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.{dmf_key} ON ({quoted_col_name});")
                            sql_commands.append("")
                    
                    sql_commands.append("")
                    sql_commands.append("-- View results with:")
                    sql_commands.append("-- SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS;")
                    
                    generated_sql = "\n".join(sql_commands)
                    
                    # Display SQL and actions
                    with st.expander("📄 View Generated SQL Commands", expanded=False):
                        st.code(generated_sql, language="sql")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.download_button(
                            label="📥 Download SQL File",
                            data=generated_sql,
                            file_name=f"dmf_setup_{selected_table}.sql",
                            mime="text/sql"
                        )
                    
                    with col2:
                        if st.button("🔧 Apply Data Quality Metrics", type="primary", help="Execute the generated SQL to add DMFs to the table", key="single_apply_dmfs"):
                            with st.spinner(f"Applying DMFs to {selected_table}..."):
                                success_count = 0
                                error_count = 0
                                
                                # Execute each SQL statement individually
                                for sql_line in sql_commands:
                                    if sql_line.strip() and not sql_line.strip().startswith('--'):
                                        try:
                                            if execute_comment_sql(conn, sql_line, 'DMF'):
                                                success_count += 1
                                                # Log DMF history
                                                if 'ADD DATA METRIC FUNCTION' in sql_line.upper():
                                                    import re
                                                    # Extract DMF type
                                                    dmf_match = re.search(r'SNOWFLAKE\.CORE\.(\w+)', sql_line.upper())
                                                    if dmf_match:
                                                        dmf_type = dmf_match.group(1)
                                                        # Extract column name if present
                                                        column_match = re.search(r'ON \(([^)]+)\)', sql_line)
                                                        if column_match and column_match.group(1).strip():
                                                            column_name = column_match.group(1).strip().strip('"').strip("'")
                                                        else:
                                                            column_name = None
                                                        # Log to history
                                                        log_dmf_history(conn, selected_db, selected_schema, selected_table, 
                                                                    dmf_type, column_name, "ADDED")
                                            else:
                                                error_count += 1
                                                st.error(f"❌ Failed to execute: {sql_line}")
                                        except Exception as e:
                                            error_count += 1
                                            st.error(f"❌ Error executing: {sql_line}")
                                            st.error(f"Error details: {str(e)}")
                                
                                # Show final results
                                if error_count == 0:
                                    st.success(f"✅ Successfully applied {success_count} DMF(s) to {selected_table}")
                                    st.info("💡 View results with: `SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS;`")
                                else:
                                    if success_count > 0:
                                        st.warning(f"⚠️ Partially successful: {success_count} succeeded, {error_count} failed")
                                    else:
                                        st.error(f"❌ All {error_count} DMF(s) failed to apply. Check your permissions and table ownership.")
                        else:
                            st.info("👆 Select data quality metrics above to generate SQL commands")
                else:
                    st.info("No columns found in the selected table.")
        else:
            st.info("Please select a database, schema, and table to get started.")

# ========================================================================================
# DATA CONTACTS PAGE - COMPLETE IMPLEMENTATION BELOW
# ========================================================================================
    
    with col2:
        if selected_db:
            schemas = get_schemas(conn, selected_db)
            selected_schema = st.selectbox(
                "Filter by Schema (Optional)",
                options=["All Schemas"] + schemas,
                key="multi_dmf_schema_filter",
                help="Optionally filter tables by schema"
            )
        else:
            selected_schema = ""
            st.selectbox("Filter by Schema", options=[""], disabled=True, key="multi_dmf_schema_disabled")
    
    if selected_db:
        st.markdown("---")
        st.markdown("### 📋 Select Tables for Data Quality Monitoring")
        
        # Get tables based on selection
        refresh_key = st.session_state.get('last_refresh', '')
        
        if selected_schema == "All Schemas":
            # Get tables from all schemas
            all_tables = []
            schemas = get_schemas(conn, selected_db)
            for schema in schemas:
                try:
                    schema_tables = get_tables_and_views(conn, selected_db, schema, refresh_key)
                    if not schema_tables.empty:
                        schema_tables['SCHEMA_NAME'] = schema  # Add schema column
                        all_tables.append(schema_tables)
                except:
                    continue  # Skip schemas we can't access
            
            if all_tables:
                tables_df = pd.concat(all_tables, ignore_index=True)
            else:
                tables_df = pd.DataFrame()
        else:
            # Get tables from specific schema
            tables_df = get_tables_and_views(conn, selected_db, selected_schema, refresh_key)
            if not tables_df.empty:
                tables_df['SCHEMA_NAME'] = selected_schema
        
        if not tables_df.empty:
            # Filter and selection options
            col1, col2, col3 = st.columns(3)
            with col1:
                show_only_tables = st.checkbox("Only show tables (exclude views)", help="Filter to show only base tables", key="multi_show_tables_only")
            with col2:
                if st.button("🔄 Refresh Tables", help="Refresh table list from Snowflake", key="multi_refresh_tables"):
                    st.cache_data.clear()
                    st.session_state['last_refresh'] = str(time.time())
                    st.rerun()
            with col3:
                select_all_tables = st.checkbox("Select All Tables", key="multi_select_all_tables")
            
            # Apply filters
            filtered_tables_df = tables_df.copy()
            if show_only_tables:
                filtered_tables_df = filtered_tables_df[filtered_tables_df['OBJECT_TYPE'] == 'BASE TABLE']
            
            if not filtered_tables_df.empty:
                # Add selection column
                filtered_tables_df.insert(0, "Select", select_all_tables)
                
                # Reorder columns for better display
                column_order = ["Select", "OBJECT_NAME", "SCHEMA_NAME", "OBJECT_TYPE", "CURRENT_DESCRIPTION", "HAS_DESCRIPTION"]
                filtered_tables_df = filtered_tables_df[column_order]
                
                # Table selection interface
                st.markdown("**Select tables to configure with data quality metrics:**")
                
                edited_tables_df = st.data_editor(
                    filtered_tables_df,
                    use_container_width=True,
                    column_config={
                        "Select": st.column_config.CheckboxColumn("Select", help="Select tables for DMF configuration"),
                        "OBJECT_NAME": "Table Name",
                        "SCHEMA_NAME": "Schema",
                        "OBJECT_TYPE": "Type",
                        "CURRENT_DESCRIPTION": st.column_config.TextColumn("Description", width="large"),
                        "HAS_DESCRIPTION": "Has Description"
                    },
                    hide_index=True,
                    key="multi_dmf_table_selection"
                )
                
                # Get selected tables
                selected_tables_df = edited_tables_df[edited_tables_df["Select"] == True]
                
                if not selected_tables_df.empty:
                    st.success(f"✅ Selected {len(selected_tables_df)} table(s)")
                    
                    # Show selected tables grouped by schema
                    schemas_with_tables = selected_tables_df.groupby('SCHEMA_NAME')['OBJECT_NAME'].apply(list).to_dict()
                    for schema, tables in schemas_with_tables.items():
                        st.info(f"**{schema}**: {', '.join(tables)}")
                    
                    st.markdown("---")
                    st.markdown("### 🎯 Configure Table-Level DMFs")
                    
                    # Table-level DMF selection
                    col1, col2 = st.columns(2)
                    
                    table_dmf_selections = {}
                    
                    with col1:
                        table_dmf_selections['ROW_COUNT'] = st.checkbox(
                            f"✅ {SYSTEM_DMFS['ROW_COUNT']['label']}", 
                            help=f"{SYSTEM_DMFS['ROW_COUNT']['help']} - Will be applied to all selected tables",
                            key="multi_row_count"
                        )
                    
                    with col2:
                        table_dmf_selections['FRESHNESS'] = st.checkbox(
                            f"✅ {SYSTEM_DMFS['FRESHNESS']['label']}", 
                            help=f"{SYSTEM_DMFS['FRESHNESS']['help']} - Requires timestamp column selection",
                            key="multi_freshness"
                        )
                        
                        if table_dmf_selections['FRESHNESS']:
                            freshness_column = st.text_input(
                                "Timestamp Column Name",
                                placeholder="e.g., CREATED_DATE, UPDATED_AT",
                                help="Enter the name of the timestamp column (must exist in all selected tables)",
                                key="multi_freshness_col"
                            )
                    
                    # Schedule configuration
                    if any(table_dmf_selections.values()):
                        st.markdown("---")
                        st.markdown("#### 📅 Set Monitoring Schedule")
                        
                        schedule_type = st.radio(
                            "Choose schedule type:",
                            options=["Periodic (Hours)", "Daily at specific time", "On data changes"],
                            help="How often should the data quality checks run?",
                            key="multi_dmf_schedule_type"
                        )
                        
                        schedule_config = {}
                        
                        if schedule_type == "Periodic (Hours)":
                            interval_value = st.selectbox(
                                "Every X hours",
                                options=[1, 2, 3, 4, 6, 8, 12, 24],
                                index=3,  # Default to 4 hours
                                help="Run every X hours",
                                key="multi_dmf_hour_interval"
                            )
                            schedule_config = {
                                'schedule_expression': f'USING CRON 0 */{interval_value} * * * UTC',
                                'description': f'Every {interval_value} hours'
                            }
                        
                        elif schedule_type == "Daily at specific time":
                            col1, col2 = st.columns(2)
                            with col1:
                                hour = st.selectbox(
                                    "Hour (24-hour format)",
                                    options=list(range(24)),
                                    index=6,  # Default to 6 AM
                                    help="Hour of the day to run (0-23)",
                                    key="multi_dmf_daily_hour"
                                )
                            with col2:
                                minute = st.selectbox(
                                    "Minute",
                                    options=[0, 15, 30, 45],
                                    index=0,  # Default to top of hour
                                    help="Minute of the hour to run",
                                    key="multi_dmf_daily_minute"
                                )
                            
                            schedule_config = {
                                'schedule_expression': f'USING CRON {minute} {hour} * * * UTC',
                                'description': f'Daily at {hour:02d}:{minute:02d} UTC'
                            }
                        
                        else:  # On data changes
                            schedule_config = {
                                'schedule_expression': 'TRIGGER_ON_CHANGES',
                                'description': 'When data in the table changes (INSERT, UPDATE, DELETE)'
                            }
                            st.info("💡 **Note**: This triggers DMFs whenever table data changes. Use for frequently updated tables.")
                        
                        # Generate and execute SQL
                        if schedule_config:
                            st.markdown("---")
                            st.markdown("#### 🚀 Apply Data Quality Metrics")
                            
                            # Generate SQL for all selected tables
                            sql_commands = []
                            
                            sql_commands.append(f"-- Multi-table DMF setup for {len(selected_tables_df)} table(s)")
                            sql_commands.append(f"-- Schedule: {schedule_config['description']}")
                            sql_commands.append("")
                            
                            for _, row in selected_tables_df.iterrows():
                                table_name = row['OBJECT_NAME']
                                schema_name = row['SCHEMA_NAME']
                                full_table_name = get_fully_qualified_name(selected_db, schema_name, table_name)
                                
                                sql_commands.append(f"-- ========================================")
                                sql_commands.append(f"-- DMF Configuration for {schema_name}.{table_name}")
                                sql_commands.append(f"-- ========================================")
                                sql_commands.append("")
                                
                                # Set schedule
                                sql_commands.append("-- Step 1: Set monitoring schedule")
                                sql_commands.append(f"ALTER TABLE {full_table_name} SET DATA_METRIC_SCHEDULE = '{schedule_config['schedule_expression']}';")
                                sql_commands.append("")
                                
                                # Add DMFs
                                sql_commands.append("-- Step 2: Add Data Metric Functions")
                                
                                if table_dmf_selections.get('ROW_COUNT'):
                                    sql_commands.append(f"ALTER TABLE {full_table_name} ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();")
                                
                                if table_dmf_selections.get('FRESHNESS') and 'freshness_column' in locals() and freshness_column:
                                    quoted_freshness_col = quote_identifier(freshness_column)
                                    sql_commands.append(f"ALTER TABLE {full_table_name} ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ({quoted_freshness_col});")
                                
                                sql_commands.append("")
                            
                            sql_commands.append("-- View results with:")
                            sql_commands.append("-- SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS;")
                            
                            generated_sql = "\n".join(sql_commands)
                            
                            # Display SQL and actions
                            with st.expander("📄 View Generated SQL Commands", expanded=False):
                                st.code(generated_sql, language="sql")
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.download_button(
                                    label="📥 Download SQL File",
                                    data=generated_sql,
                                    file_name=f"multi_dmf_setup_{len(selected_tables_df)}_tables.sql",
                                    mime="text/sql"
                                )
                            
                            with col2:
                                if st.button("🔧 Apply Data Quality Metrics", type="primary", help="Execute the generated SQL to add DMFs to all selected tables", key="multi_apply_dmfs"):
                                    with st.spinner(f"Applying DMFs to {len(selected_tables_df)} table(s)..."):
                                        success_count = 0
                                        error_count = 0
                                        
                                        # Execute each SQL statement individually
                                        for sql_line in sql_commands:
                                            if sql_line.strip() and not sql_line.strip().startswith('--'):
                                                try:
                                                    if execute_comment_sql(conn, sql_line, 'DMF'):
                                                        success_count += 1
                                                        # Log DMF history
                                                        if 'ADD DATA METRIC FUNCTION' in sql_line.upper():
                                                            import re
                                                            # Extract table name from SQL
                                                            table_match = re.search(r'ALTER TABLE\s+(?:"?[^".\s]+"?\.)?(?:"?[^".\s]+"?\.)?"?([^".\s]+)"?\s+ADD', sql_line.upper())
                                                            schema_match = re.search(r'ALTER TABLE\s+(?:"?[^".\s]+"?\.)?(?:"?([^".\s]+)"?\.)', sql_line.upper())
                                                            
                                                            if table_match:
                                                                table_name_from_sql = table_match.group(1).strip('"')
                                                            else:
                                                                table_name_from_sql = "UNKNOWN_TABLE"
                                                            
                                                            if schema_match:
                                                                schema_name_from_sql = schema_match.group(1).strip('"')
                                                            else:
                                                                schema_name_from_sql = "UNKNOWN_SCHEMA"
                                                            
                                                            # Extract DMF type
                                                            dmf_match = re.search(r'SNOWFLAKE\.CORE\.(\w+)', sql_line.upper())
                                                            if dmf_match:
                                                                dmf_type = dmf_match.group(1)
                                                                # Extract column name if present
                                                                column_match = re.search(r'ON \(([^)]+)\)', sql_line)
                                                                if column_match and column_match.group(1).strip():
                                                                    column_name = column_match.group(1).strip().strip('"').strip("'")
                                                                else:
                                                                    column_name = None
                                                                # Log to history
                                                                log_dmf_history(conn, selected_db, schema_name_from_sql, table_name_from_sql, 
                                                                               dmf_type, column_name, "ADDED")
                                                    else:
                                                        error_count += 1
                                                        st.error(f"❌ Failed to execute: {sql_line}")
                                                except Exception as e:
                                                    error_count += 1
                                                    st.error(f"❌ Error executing: {sql_line}")
                                                    st.error(f"Error details: {str(e)}")
                                        
                                        # Show final results
                                        if error_count == 0:
                                            st.success(f"✅ Successfully applied {success_count} DMF(s) to {len(selected_tables_df)} table(s)")
                                            st.info("💡 View results with: `SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS;`")
                                        else:
                                            if success_count > 0:
                                                st.warning(f"⚠️ Partially successful: {success_count} succeeded, {error_count} failed")
                                            else:
                                                st.error(f"❌ All {error_count} DMF(s) failed to apply. Check your permissions and table ownership.")
                else:
                    st.info("👆 Select one or more tables above to configure data quality metrics")
            else:
                st.info("No tables found matching the current filters.")
        else:
            st.info("No tables found in the selected database/schema.")
    else:
        st.info("Please select a database to get started.")

def show_dmf_documentation_and_samples():
    """Tab 3: Documentation and Sample Code."""
    st.markdown("### 📚 Data Quality Documentation & Sample Code")
    st.markdown("Comprehensive documentation, permissions setup, and code samples for Snowflake Data Metric Functions.")
    
    # Permission Setup Section
    with st.expander("🔐 **Required Permissions and Setup**", expanded=True):
        st.markdown("""
        ### Prerequisites
        
        To use Data Metric Functions, you need the following permissions and setup:
        
        #### 1. Database Roles
        ```sql
        -- Grant the required database roles to your role
        GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE your_role;
        GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_LOOKUP TO ROLE your_role;
        ```
        
        #### 2. Table Ownership or Privileges
        - You need **ownership** or appropriate **privileges** on tables to add DMFs
        - Alternative: `MODIFY` privilege on the table
        
        #### 3. Warehouse Access
        - Access to a warehouse to execute the DMF setup commands
        - The warehouse will be used for monitoring execution
        """)
    
    # System DMFs Documentation
    with st.expander("📊 **Complete System DMF Reference**", expanded=False):
        st.markdown("### Available System Data Metric Functions")
        
        # Table-level DMFs
        st.markdown("#### Table-Level Metrics")
        table_dmfs = {k: v for k, v in SYSTEM_DMFS.items() if v['level'] == 'table'}
        
        for dmf_key, dmf_info in table_dmfs.items():
            st.markdown(f"""
            **{dmf_info['label']}** (`{dmf_key}`)
            - **Description**: {dmf_info['description']}
            - **Usage**: `ALTER TABLE table_name ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.{dmf_key} ON ();`
            """)
        
        st.markdown("---")
        
        # Column-level DMFs by category
        st.markdown("#### Column-Level Metrics")
        
        # Group by data type compatibility
        universal_dmfs = {k: v for k, v in SYSTEM_DMFS.items() if v['level'] == 'column' and 'ALL' in v['data_types']}
        numeric_dmfs = {k: v for k, v in SYSTEM_DMFS.items() if v['level'] == 'column' and 'NUMBER' in str(v['data_types'])}
        string_dmfs = {k: v for k, v in SYSTEM_DMFS.items() if v['level'] == 'column' and 'VARCHAR' in str(v['data_types'])}
        timestamp_dmfs = {k: v for k, v in SYSTEM_DMFS.items() if v['level'] == 'column' and 'TIMESTAMP' in str(v['data_types'])}
        
        st.markdown("##### Universal (All Data Types)")
        for dmf_key, dmf_info in universal_dmfs.items():
            st.markdown(f"""
            **{dmf_info['label']}** (`{dmf_key}`)
            - **Description**: {dmf_info['description']}
            - **Compatible Types**: All data types
            - **Usage**: `ALTER TABLE table_name ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.{dmf_key} ON (column_name);`
            """)
        
        if numeric_dmfs:
            st.markdown("##### Numeric Data Types")
            for dmf_key, dmf_info in numeric_dmfs.items():
                if dmf_key not in universal_dmfs:  # Avoid duplicates
                    st.markdown(f"""
                    **{dmf_info['label']}** (`{dmf_key}`)
                    - **Description**: {dmf_info['description']}
                    - **Compatible Types**: {', '.join(dmf_info['data_types'][:5])}{'...' if len(dmf_info['data_types']) > 5 else ''}
                    - **Usage**: `ALTER TABLE table_name ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.{dmf_key} ON (column_name);`
                    """)
        
        if string_dmfs:
            st.markdown("##### String Data Types")
            for dmf_key, dmf_info in string_dmfs.items():
                if dmf_key not in universal_dmfs:  # Avoid duplicates
                    st.markdown(f"""
                    **{dmf_info['label']}** (`{dmf_key}`)
                    - **Description**: {dmf_info['description']}
                    - **Compatible Types**: {', '.join(dmf_info['data_types'])}
                    - **Usage**: `ALTER TABLE table_name ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.{dmf_key} ON (column_name);`
                    """)
        
        if timestamp_dmfs:
            st.markdown("##### Timestamp Data Types")
            for dmf_key, dmf_info in timestamp_dmfs.items():
                if dmf_key not in universal_dmfs:  # Avoid duplicates
                    st.markdown(f"""
                    **{dmf_info['label']}** (`{dmf_key}`)
                    - **Description**: {dmf_info['description']}
                    - **Compatible Types**: {', '.join(dmf_info['data_types'])}
                    - **Usage**: `ALTER TABLE table_name ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.{dmf_key} ON (column_name);`
                    """)
    
    # Sample Code Section
    with st.expander("💻 **Sample Code and Examples**", expanded=False):
        st.markdown("### Complete Setup Example")
        
        st.code("""
-- Step 1: Grant required permissions (run as ACCOUNTADMIN)
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE DATA_ENGINEER;
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_LOOKUP TO ROLE DATA_ENGINEER;

-- Step 2: Set monitoring schedule (required before adding DMFs)
ALTER TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE 
SET DATA_METRIC_SCHEDULE = 'USING CRON 0 6 * * * UTC';  -- Daily at 6 AM UTC

-- Step 3: Add table-level DMFs
ALTER TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE 
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

-- Step 4: Add column-level DMFs
ALTER TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE 
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (customer_id);

ALTER TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE 
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (email);

ALTER TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE 
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON (created_date);

-- Step 5: View results
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE TABLE_NAME = 'MY_TABLE'
ORDER BY MEASUREMENT_TIME DESC;
        """, language="sql")
        
        st.markdown("### Schedule Options")
        
        st.code("""
-- Periodic schedules
ALTER TABLE table_name SET DATA_METRIC_SCHEDULE = '30 MINUTE';  -- Every 30 minutes
ALTER TABLE table_name SET DATA_METRIC_SCHEDULE = 'USING CRON 0 */4 * * * UTC';  -- Every 4 hours

-- Daily schedules
ALTER TABLE table_name SET DATA_METRIC_SCHEDULE = 'USING CRON 0 6 * * * UTC';  -- Daily at 6 AM UTC
ALTER TABLE table_name SET DATA_METRIC_SCHEDULE = 'USING CRON 30 14 * * * UTC';  -- Daily at 2:30 PM UTC

-- Trigger on data changes
ALTER TABLE table_name SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
        """, language="sql")
        
        st.markdown("### Viewing Results")
        
        st.code("""
-- View all monitoring results
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
ORDER BY MEASUREMENT_TIME DESC;

-- Filter by specific table
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE TABLE_DATABASE = 'MY_DB' 
  AND TABLE_SCHEMA = 'MY_SCHEMA'
  AND TABLE_NAME = 'MY_TABLE'
ORDER BY MEASUREMENT_TIME DESC;

-- Filter by metric type
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE METRIC_NAME = 'NULL_COUNT'
ORDER BY MEASUREMENT_TIME DESC;

-- Get latest results per table/metric
SELECT 
    TABLE_DATABASE,
    TABLE_SCHEMA,
    TABLE_NAME,
    METRIC_NAME,
    VALUE,
    MEASUREMENT_TIME
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY TABLE_DATABASE, TABLE_SCHEMA, TABLE_NAME, METRIC_NAME ORDER BY MEASUREMENT_TIME DESC) = 1
ORDER BY TABLE_NAME, METRIC_NAME;
        """, language="sql")
    
    # Troubleshooting Section
    with st.expander("🔧 **Troubleshooting Common Issues**", expanded=False):
        st.markdown("""
        ### Common Error Messages and Solutions
        
        #### 1. Permission Errors
        **Error**: `Insufficient privileges to operate on table`
        **Solution**: 
        - Ensure you have ownership or MODIFY privileges on the table
        - Grant required database roles: `SNOWFLAKE.DATA_METRIC_USER` and `SNOWFLAKE.DATA_QUALITY_MONITORING_LOOKUP`
        
        #### 2. Schedule Not Set
        **Error**: `Data metric schedule must be set before adding DMFs`
        **Solution**: 
        - Always set a schedule first: `ALTER TABLE table_name SET DATA_METRIC_SCHEDULE = 'schedule_expression';`
        
        #### 3. Invalid Data Type
        **Error**: `Function 'FUNCTION_NAME' does not exist or not authorized`
        **Solution**: 
        - Check that the DMF is compatible with your column's data type
        - Use this app's Single Table Setup for automatic data type filtering
        
        #### 4. Column Not Found
        **Error**: `Column 'COLUMN_NAME' does not exist`
        **Solution**: 
        - Verify column names are spelled correctly
        - Use proper case sensitivity (Snowflake is case-sensitive for quoted identifiers)
        
        #### 5. No Results in Monitoring View
        **Issue**: `SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS` is empty
        **Solution**: 
        - Wait for the first scheduled execution
        - Check that the schedule expression is valid
        - Verify the table has data to measure
        """)
    
    # Best Practices Section
    with st.expander("✨ **Best Practices**", expanded=False):
        st.markdown("""
        ### Data Quality Monitoring Best Practices
        
        #### 1. Start Small
        - Begin with a few critical tables and basic metrics (ROW_COUNT, NULL_COUNT)
        - Gradually expand to more tables and sophisticated metrics
        
        #### 2. Choose Appropriate Schedules
        - **High-frequency tables**: Use TRIGGER_ON_CHANGES or frequent intervals (15-30 minutes)
        - **Batch-loaded tables**: Use daily schedules aligned with your ETL processes
        - **Reference tables**: Use less frequent schedules (daily or weekly)
        
        #### 3. Focus on Business-Critical Columns
        - Prioritize columns used in joins, calculations, and business logic
        - Monitor primary keys, foreign keys, and required business fields
        
        #### 4. Set Up Alerts
        - Use Snowflake's notification features to alert on quality issues
        - Create views or dashboards to visualize quality trends
        
        #### 5. Regular Review
        - Periodically review monitoring results
        - Adjust schedules and metrics based on actual usage patterns
        - Remove monitoring for deprecated tables/columns
        
        #### 6. Performance Considerations
        - Avoid over-monitoring with too many metrics or too frequent schedules
        - Consider the impact on warehouse usage and costs
        - Use appropriate warehouse sizes for monitoring workloads
        """)
    
    # Links and Resources
    st.markdown("---")
    st.markdown("### 📖 Additional Resources")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Official Documentation**
        - [Snowflake Data Quality Overview](https://docs.snowflake.com/en/user-guide/data-quality-intro)
        - [System DMFs Reference](https://docs.snowflake.com/en/user-guide/data-quality-system-dmfs)
        - [Data Quality Monitoring](https://docs.snowflake.com/en/user-guide/data-quality-monitoring)
        """)
    
    with col2:
        st.markdown("""
        **Related Features**
        - [Data Classification](https://docs.snowflake.com/en/user-guide/data-classification)
        - [Data Governance](https://docs.snowflake.com/en/user-guide/governance-overview)
        - [Snowflake Cortex](https://docs.snowflake.com/en/user-guide/snowflake-cortex/overview)
        """)


def show_history_page(conn):
    """Display the history page."""
    
    st.markdown("View historical tracking data for description changes and data quality monitoring.")
    
    # Tab selection for different history types
    history_tab1, history_tab2 = st.tabs(["📝 Description History", "🔍 Quality History"])
    
    with history_tab1:
        st.markdown("### Description Changes History")
        
        try:
            # Try to get description history (exclude DMF and contact entries)
            history_query = """
            SELECT 
                DATABASE_NAME,
                SCHEMA_NAME,
                OBJECT_TYPE,
                OBJECT_NAME,
                COLUMN_NAME,
                BEFORE_DESCRIPTION,
                AFTER_DESCRIPTION,
                UPDATED_BY,
                UPDATED_AT
            FROM DB_SNOWTOOLS.PUBLIC.DATA_DESCRIPTION_HISTORY
            WHERE OBJECT_TYPE NOT LIKE 'DMF_%' 
              AND OBJECT_TYPE NOT LIKE 'CONTACT_%'
            ORDER BY UPDATED_AT DESC
            LIMIT 1000
            """
            
            if hasattr(conn, 'sql'):
                history_df = conn.sql(history_query).to_pandas()
            else:
                history_df = pd.read_sql(history_query, conn)
            
            if not history_df.empty:
                # Summary metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Changes", len(history_df))
                with col2:
                    st.metric("Unique Objects", history_df['OBJECT_NAME'].nunique())
                with col3:
                    st.metric("Unique Users", history_df['UPDATED_BY'].nunique())
                
                # Display history
                st.dataframe(
                    history_df,
                    use_container_width=True,
                    column_config={
                        "DATABASE_NAME": "Database",
                        "SCHEMA_NAME": "Schema",
                        "OBJECT_TYPE": "Type",
                        "OBJECT_NAME": "Object",
                        "COLUMN_NAME": "Column",
                        "BEFORE_DESCRIPTION": st.column_config.TextColumn("Before", width="medium"),
                        "AFTER_DESCRIPTION": st.column_config.TextColumn("After", width="medium"),
                        "UPDATED_BY": "Updated By",
                        "UPDATED_AT": "Updated At"
                    }
                )
                
                # Export option
                if st.button("📊 Export Description History to CSV"):
                    csv = history_df.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"description_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            else:
                st.info("No description history found. Start documenting objects to see changes here!")
                
        except Exception as e:
            st.warning("Description history tracking is not yet available.")
            st.info("This will be populated as you use the Data Descriptions feature to update object descriptions.")
    
    with history_tab2:
        # DMF Configuration History Section
        st.markdown("### 🔧 Data Quality Configuration History")
        st.markdown("Track when data quality metrics (DMFs) were added or modified.")
        
        try:
            # Get DMF configuration history
            dmf_history_query = """
            SELECT 
                DATABASE_NAME,
                SCHEMA_NAME,
                OBJECT_TYPE,
                OBJECT_NAME,
                COLUMN_NAME,
                AFTER_DESCRIPTION as ACTION_DESCRIPTION,
                UPDATED_BY,
                UPDATED_AT
            FROM DB_SNOWTOOLS.PUBLIC.DATA_DESCRIPTION_HISTORY
            WHERE OBJECT_TYPE LIKE 'DMF_%'
            ORDER BY UPDATED_AT DESC
            LIMIT 500
            """
            
            if hasattr(conn, 'sql'):
                dmf_history_df = conn.sql(dmf_history_query).to_pandas()
            else:
                dmf_history_df = pd.read_sql(dmf_history_query, conn)
            
            if not dmf_history_df.empty:
                # Summary metrics for DMF history
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("DMF Changes", len(dmf_history_df))
                with col2:
                    st.metric("Tables Configured", dmf_history_df['OBJECT_NAME'].nunique())
                with col3:
                    st.metric("Unique Metrics", dmf_history_df['OBJECT_TYPE'].nunique())
                
                # Display DMF configuration history
                st.dataframe(
                    dmf_history_df,
                    use_container_width=True,
                    column_config={
                        "DATABASE_NAME": "Database",
                        "SCHEMA_NAME": "Schema",
                        "OBJECT_TYPE": "DMF Type",
                        "OBJECT_NAME": "Table",
                        "COLUMN_NAME": "Column",
                        "ACTION_DESCRIPTION": st.column_config.TextColumn("Action", width="medium"),
                        "UPDATED_BY": "Updated By",
                        "UPDATED_AT": st.column_config.DatetimeColumn("Updated At")
                    }
                )
                
                # Export DMF history
                if st.button("📊 Export DMF Configuration History to CSV"):
                    csv = dmf_history_df.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"dmf_configuration_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            else:
                st.info("No DMF configuration history found. Configure data quality metrics to see changes here!")
                
        except Exception as e:
            st.warning("DMF configuration history is not yet available.")
            st.info("This will be populated as you use the Data Quality feature to configure monitoring.")
        
        st.markdown("---")
        
        # Existing Quality Monitoring Dashboard
        st.markdown("### 📊 Data Quality Monitoring Dashboard")
        st.markdown("Monitor and analyze your data quality metrics across all databases and schemas.")
        
        # Filters Section
        with st.expander("🔍 Filters & Settings", expanded=True):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Get available databases for filtering
                try:
                    databases = get_databases(conn)
                    selected_dbs = st.multiselect(
                        "Filter by Database(s)",
                        options=databases,
                        default=[],
                        help="Select specific databases to filter results"
                    )
                except:
                    selected_dbs = []
                    st.info("Could not load databases for filtering")
            
            with col2:
                # Schema filter (populated based on selected databases)
                selected_schemas = []
                if selected_dbs:
                    try:
                        all_schemas = []
                        for db in selected_dbs:
                            schemas = get_schemas(conn, db)
                            all_schemas.extend([f"{db}.{schema}" for schema in schemas])
                        
                        selected_schemas = st.multiselect(
                            "Filter by Schema(s)",
                            options=all_schemas,
                            default=[],
                            help="Select specific schemas to filter results"
                        )
                    except:
                        st.info("Select databases first to filter schemas")
            
            with col3:
                # Time range filter
                time_range = st.selectbox(
                    "Time Range",
                    options=["Last 24 Hours", "Last 7 Days", "Last 30 Days", "All Time"],
                    index=1,
                    help="Filter results by time period"
                )
        
        # Build filter conditions
        filter_conditions = []
        if selected_dbs:
            db_list = "', '".join(selected_dbs)
            filter_conditions.append(f"DATABASE_NAME IN ('{db_list}')")
        
        if selected_schemas:
            schema_conditions = []
            for schema_full in selected_schemas:
                db, schema = schema_full.split('.', 1)
                schema_conditions.append(f"(DATABASE_NAME = '{db}' AND SCHEMA_NAME = '{schema}')")
            filter_conditions.append(f"({' OR '.join(schema_conditions)})")
        
        # Time filter
        if time_range == "Last 24 Hours":
            filter_conditions.append("MEASUREMENT_TIME >= DATEADD(hour, -24, CURRENT_TIMESTAMP())")
        elif time_range == "Last 7 Days":
            filter_conditions.append("MEASUREMENT_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())")
        elif time_range == "Last 30 Days":
            filter_conditions.append("MEASUREMENT_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())")
        
        where_clause = " AND ".join(filter_conditions) if filter_conditions else "1=1"
        
        try:
            # Get quality monitoring results using the actual table structure
            st.markdown("#### 🎯 Data Quality Monitoring Results")
            
            # Build the WHERE clause for MEASUREMENT_TIME instead of UPDATED_AT
            measurement_where_clause = where_clause
            if "MEASUREMENT_TIME" not in where_clause and where_clause != "1=1":
                # Replace any time filters to use MEASUREMENT_TIME
                if "DATEADD" in where_clause:
                    measurement_where_clause = where_clause.replace("UPDATED_AT", "MEASUREMENT_TIME")
            
            # Main quality results query - using Snowflake's native DMF results
            # Extract column names from ARGUMENT_NAMES array when available
            quality_results_query = f"""
            SELECT 
                METRIC_NAME as MONITOR_NAME,
                TABLE_DATABASE as DATABASE_NAME,
                TABLE_SCHEMA as SCHEMA_NAME,
                TABLE_NAME,
                CASE 
                    WHEN ARGUMENT_TYPES IS NOT NULL AND ARRAY_SIZE(ARGUMENT_TYPES) > 0 
                         AND ARGUMENT_TYPES[0]::STRING = 'COLUMN'
                         AND ARGUMENT_NAMES IS NOT NULL AND ARRAY_SIZE(ARGUMENT_NAMES) > 0
                    THEN ARGUMENT_NAMES[0]::STRING
                    ELSE NULL
                END as COLUMN_NAME,
                VALUE as METRIC_VALUE,
                'numeric' as METRIC_UNIT,
                NULL as THRESHOLD_MIN,
                NULL as THRESHOLD_MAX,
                CASE 
                    WHEN VALUE IS NOT NULL THEN 'MEASURED'
                    ELSE 'UNKNOWN'
                END as STATUS,
                MEASUREMENT_TIME,
                MEASUREMENT_TIME as RECORD_INSERTED_AT,
                ARGUMENT_TYPES,
                ARGUMENT_NAMES
            FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
            WHERE {measurement_where_clause.replace('DATABASE_NAME', 'TABLE_DATABASE').replace('SCHEMA_NAME', 'TABLE_SCHEMA')}
            ORDER BY MEASUREMENT_TIME DESC
            LIMIT 1000
            """
            
            if hasattr(conn, 'sql'):
                quality_results_df = conn.sql(quality_results_query).to_pandas()
            else:
                quality_results_df = pd.read_sql(quality_results_query, conn)
            
            # Create a summary of monitored objects from the results
            dmf_config_df = pd.DataFrame()
            if not quality_results_df.empty:
                # Create a summary of what's being monitored based on the results
                dmf_config_df = quality_results_df.groupby(['DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'MONITOR_NAME']).agg({
                    'MEASUREMENT_TIME': 'max',
                    'STATUS': 'last',
                    'RECORD_INSERTED_AT': 'max'
                }).reset_index()
                dmf_config_df.columns = ['DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'MONITOR_TYPE', 'LAST_CHECK', 'LAST_STATUS', 'CONFIGURED_AT']
            
            # Summary KPIs
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_monitors = len(dmf_config_df) if not dmf_config_df.empty else 0
                st.metric(
                    "📊 Active Monitors", 
                    total_monitors,
                    help="Total number of active quality monitors"
                )
            
            with col2:
                unique_tables = quality_results_df['TABLE_NAME'].nunique() if not quality_results_df.empty else 0
                st.metric(
                    "📋 Tables Monitored", 
                    unique_tables,
                    help="Number of unique tables with quality monitoring"
                )
            
            with col3:
                total_checks = len(quality_results_df) if not quality_results_df.empty else 0
                st.metric(
                    "🔍 Quality Checks", 
                    total_checks,
                    help="Total quality check results in selected time range"
                )
            
            with col4:
                if not quality_results_df.empty and 'STATUS' in quality_results_df.columns:
                    pass_rate = (quality_results_df['STATUS'] == 'PASS').mean() * 100
                    st.metric(
                        "✅ Pass Rate", 
                        f"{pass_rate:.1f}%",
                        help="Percentage of quality checks that passed"
                    )
                else:
                    st.metric("✅ Pass Rate", "N/A")
            
            # Active Monitors Overview
            with st.expander("🔧 Active Quality Monitors", expanded=False):
                if not dmf_config_df.empty:
                    st.markdown(f"**{len(dmf_config_df)} active quality monitors**")
                    
                    # Group by monitor type for better visualization
                    monitor_type_counts = dmf_config_df['MONITOR_TYPE'].value_counts()
                    
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.markdown("**Monitor Types Distribution:**")
                        for monitor_type, count in monitor_type_counts.items():
                            st.write(f"• **{monitor_type}**: {count}")
                    
                    with col2:
                        # Show recent activity
                        st.markdown("**Recently Active:**")
                        recent_monitors = dmf_config_df.sort_values('LAST_CHECK', ascending=False).head(5)
                        for _, row in recent_monitors.iterrows():
                            col_info = f".{row['COLUMN_NAME']}" if pd.notna(row['COLUMN_NAME']) else ""
                            status_emoji = "✅" if row['LAST_STATUS'] == 'PASS' else "❌" if row['LAST_STATUS'] == 'FAIL' else "⚠️"
                            st.write(f"• {status_emoji} {row['MONITOR_TYPE']} on {row['TABLE_NAME']}{col_info}")
                    
                    # Full monitors table
                    st.markdown("**All Active Monitors:**")
                    st.dataframe(
                        dmf_config_df,
                        use_container_width=True,
                        column_config={
                            "DATABASE_NAME": "Database",
                            "SCHEMA_NAME": "Schema", 
                            "TABLE_NAME": "Table",
                            "COLUMN_NAME": "Column",
                            "MONITOR_TYPE": "Monitor Type",
                            "LAST_CHECK": st.column_config.DatetimeColumn("Last Check"),
                            "LAST_STATUS": "Last Status",
                            "CONFIGURED_AT": st.column_config.DatetimeColumn("First Seen")
                        }
                    )
                else:
                    st.info("No active quality monitors found. Visit the Data Quality page to set up monitoring.")
            
            # Quality Results Details
            with st.expander("📈 Quality Check Results", expanded=False):
                if not quality_results_df.empty:
                    st.markdown(f"**{len(quality_results_df)} quality check results in selected time range**")
                    
                    # Status distribution
                    if 'STATUS' in quality_results_df.columns:
                        status_counts = quality_results_df['STATUS'].value_counts()
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**Status Distribution:**")
                            for status, count in status_counts.items():
                                emoji = "✅" if status == "PASS" else "❌" if status == "FAIL" else "⚠️"
                                st.write(f"{emoji} **{status}**: {count}")
                        
                        with col2:
                            # Recent failures (if any)
                            if 'FAIL' in status_counts:
                                st.markdown("**Recent Failures:**")
                                failures = quality_results_df[quality_results_df['STATUS'] == 'FAIL'].head(3)
                                for _, row in failures.iterrows():
                                    st.write(f"❌ {row['TABLE_NAME']} - {row.get('MONITOR_NAME', 'Unknown')}")
                    
                    # Full results table
                    st.markdown("**All Quality Check Results:**")
                    st.dataframe(
                        quality_results_df,
                    use_container_width=True,
                    column_config={
                        "MONITOR_NAME": "Monitor",
                        "DATABASE_NAME": "Database",
                        "SCHEMA_NAME": "Schema",
                        "TABLE_NAME": "Table",
                        "COLUMN_NAME": "Column",
                        "METRIC_VALUE": "Value",
                        "METRIC_UNIT": "Unit",
                            "THRESHOLD_MIN": "Min Threshold",
                            "THRESHOLD_MAX": "Max Threshold",
                        "STATUS": "Status",
                            "MEASUREMENT_TIME": st.column_config.DatetimeColumn("Measured At"),
                            "RECORD_INSERTED_AT": st.column_config.DatetimeColumn("Recorded At"),
                            "ARGUMENT_TYPES": st.column_config.TextColumn("Arg Types", width="small"),
                            "ARGUMENT_NAMES": st.column_config.TextColumn("Arg Names", width="medium")
                        }
                    )
                else:
                    st.info("No quality check results found for the selected filters and time range.")
            
            # Tables with Quality Monitoring
            with st.expander("📊 Tables & Columns with Quality Monitoring", expanded=False):
                if not dmf_config_df.empty:
                    # Group by table to show what's monitored
                    table_summary = dmf_config_df.groupby(['DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME']).agg({
                        'MONITOR_TYPE': lambda x: ', '.join(sorted(set(x))),
                        'COLUMN_NAME': lambda x: len([c for c in x if pd.notna(c)]),
                        'LAST_CHECK': 'max',
                        'LAST_STATUS': lambda x: 'MIXED' if len(set(x)) > 1 else x.iloc[0]
                    }).reset_index()
                    
                    table_summary.columns = ['Database', 'Schema', 'Table', 'Monitor Types', 'Columns Monitored', 'Last Check', 'Overall Status']
                    
                    st.markdown(f"**{len(table_summary)} tables have quality monitoring configured**")
                    
                    st.dataframe(
                        table_summary,
                        use_container_width=True,
                        column_config={
                            "Last Check": st.column_config.DatetimeColumn("Last Check"),
                            "Overall Status": "Status"
                        }
                    )
                else:
                    st.info("No tables have quality monitoring configured yet.")
            
            # Export Options
            st.markdown("#### 📥 Export Options")
            col1, col2 = st.columns(2)
            
            with col1:
                if not dmf_config_df.empty:
                    csv_config = dmf_config_df.to_csv(index=False)
                    st.download_button(
                        label="📊 Export Monitor Summary",
                        data=csv_config,
                        file_name=f"quality_monitors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        help="Download active quality monitors as CSV"
                    )
            
            with col2:
                if not quality_results_df.empty:
                    csv_results = quality_results_df.to_csv(index=False)
                    st.download_button(
                        label="📈 Export Quality Results", 
                        data=csv_results,
                        file_name=f"quality_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        help="Download quality check results as CSV"
                    )
                
        except Exception as e:
            st.warning("⚠️ Could not load data quality information.")
            st.error(f"Error details: {str(e)}")
            st.info("This may be because:")
            st.info("• No DMFs have been configured yet")
            st.info("• The quality monitoring tables don't exist")
            st.info("• There are permission issues accessing the data")

# ========================================================================================
# RUN APPLICATION
# ========================================================================================

if __name__ == "__main__":
    main()
