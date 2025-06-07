#!/usr/bin/env python3
"""
Test and Setup Script for Streamlit Apps Portal
This script helps verify the setup and optionally adds sample data for testing.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from StreamlitPortal import get_snowflake_connection, initialize_database_schema

def test_connection():
    """Test the Snowflake connection"""
    print("Testing Snowflake connection...")
    try:
        conn = get_snowflake_connection()
        if conn:
            print("✅ Connection successful!")
            return conn
        else:
            print("❌ Connection failed!")
            return None
    except Exception as e:
        print(f"❌ Connection error: {str(e)}")
        return None

def test_database_setup(conn):
    """Test database schema initialization"""
    print("Testing database schema setup...")
    try:
        result = initialize_database_schema(conn)
        if result:
            print("✅ Database schema initialized successfully!")
            return True
        else:
            print("❌ Database schema setup failed!")
            return False
    except Exception as e:
        print(f"❌ Database setup error: {str(e)}")
        return False

def add_sample_app_data(conn):
    """Add sample application data for testing"""
    print("Adding sample application data...")
    try:
        # Sample app data
        sample_apps = [
            {
                'app_id': 'SAMPLE_DASHBOARD',
                'app_name': 'SAMPLE_DASHBOARD',
                'app_title': 'Sample Dashboard',
                'description': 'A sample dashboard application for testing the portal'
            },
            {
                'app_id': 'SAMPLE_ANALYTICS',
                'app_name': 'SAMPLE_ANALYTICS', 
                'app_title': 'Analytics Tool',
                'description': 'Sample analytics tool for data visualization'
            }
        ]
        
        for app in sample_apps:
            if hasattr(conn, 'sql'):  # Snowpark session
                # Check if app already exists
                existing = conn.sql(f"SELECT COUNT(*) as cnt FROM portal_apps WHERE app_id = '{app['app_id']}'").to_pandas()
                if existing.iloc[0]['cnt'] == 0:
                    conn.sql(f"""
                        INSERT INTO portal_apps (app_id, app_name, app_title, description, is_active)
                        VALUES ('{app['app_id']}', '{app['app_name']}', '{app['app_title']}', '{app['description']}', TRUE)
                    """).collect()
                    
                    # Add PUBLIC access for testing
                    conn.sql(f"""
                        INSERT INTO app_access (app_id, access_type, access_value)
                        VALUES ('{app['app_id']}', 'ROLE', 'PUBLIC')
                    """).collect()
                    print(f"✅ Added sample app: {app['app_title']}")
                else:
                    print(f"ℹ️  Sample app already exists: {app['app_title']}")
            else:  # Regular connection
                cursor = conn.cursor()
                # Check if app already exists
                cursor.execute(f"SELECT COUNT(*) as cnt FROM portal_apps WHERE app_id = %s", (app['app_id'],))
                if cursor.fetchone()[0] == 0:
                    cursor.execute("""
                        INSERT INTO portal_apps (app_id, app_name, app_title, description, is_active)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (app['app_id'], app['app_name'], app['app_title'], app['description'], True))
                    
                    # Add PUBLIC access for testing
                    cursor.execute("""
                        INSERT INTO app_access (app_id, access_type, access_value)
                        VALUES (%s, %s, %s)
                    """, (app['app_id'], 'ROLE', 'PUBLIC'))
                    print(f"✅ Added sample app: {app['app_title']}")
                else:
                    print(f"ℹ️  Sample app already exists: {app['app_title']}")
                cursor.close()
        
        print("✅ Sample data setup complete!")
        return True
        
    except Exception as e:
        print(f"❌ Error adding sample data: {str(e)}")
        return False

def verify_role_setup(conn):
    """Verify that StreamlitPortalAdmins role exists and check AccountAdmin permissions"""
    print("Verifying admin role setup...")
    try:
        # Check StreamlitPortalAdmins role
        if hasattr(conn, 'sql'):  # Snowpark session
            roles_df = conn.sql("SHOW ROLES LIKE 'STREAMLITPORTALADMINS'").to_pandas()
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute("SHOW ROLES LIKE 'STREAMLITPORTALADMINS'")
            roles_data = cursor.fetchall()
            cursor.close()
            
        if (hasattr(conn, 'sql') and not roles_df.empty) or (not hasattr(conn, 'sql') and roles_data):
            print("✅ StreamlitPortalAdmins role exists!")
            streamlit_role_exists = True
        else:
            print("❌ StreamlitPortalAdmins role not found!")
            streamlit_role_exists = False
        
        # AccountAdmin should always exist in Snowflake
        print("✅ AccountAdmin role available (built-in Snowflake role)")
        
        if streamlit_role_exists:
            print("✅ Admin role setup complete!")
            return True
        else:
            print("❌ Please run the setup_portal.sql script to create StreamlitPortalAdmins role.")
            print("   Note: AccountAdmin users can still access admin features.")
            return False
            
    except Exception as e:
        print(f"❌ Error checking roles: {str(e)}")
        return False

def main():
    """Main test function"""
    print("=" * 50)
    print("Streamlit Apps Portal - Setup & Test Script")
    print("=" * 50)
    
    # Test connection
    conn = test_connection()
    if not conn:
        print("\n❌ Cannot proceed without a valid connection.")
        return False
    
    print()
    
    # Test database setup
    if not test_database_setup(conn):
        print("\n❌ Database setup failed.")
        return False
    
    print()
    
    # Verify role setup
    role_ok = verify_role_setup(conn)
    if not role_ok:
        print("\n⚠️  Role setup incomplete, but continuing with other tests...")
    
    print()
    
    # Ask if user wants to add sample data
    add_samples = input("Do you want to add sample application data for testing? (y/n): ").lower().strip()
    if add_samples in ['y', 'yes']:
        print()
        add_sample_app_data(conn)
    
    print("\n" + "=" * 50)
    print("Setup and testing complete!")
    print("=" * 50)
    
    print("\nNext Steps:")
    print("1. Run 'streamlit run StreamlitPortal.py' to start the portal")
    if not role_ok:
        print("2. Execute setup_portal.sql to create the admin role")
    print("3. Access the portal and test the functionality")
    
    return True

if __name__ == "__main__":
    main() 