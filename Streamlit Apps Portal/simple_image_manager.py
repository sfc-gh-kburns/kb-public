import streamlit as st
import pandas as pd
import io
from PIL import Image
import time
import base64

class SimpleImageManager:
    """Simple image manager for Streamlit Apps Portal - MVP version"""
    
    def __init__(self, conn):
        self.conn = conn
        
    def show_image_management(self, portal_apps):
        """Show simple image management interface"""
        st.markdown("### 🖼️ Image Management")
        st.markdown("Upload and manage images for your portal applications.")
        
        if portal_apps.empty:
            st.info("No applications in portal yet. Add some applications first.")
            return
        

        
        # Select app to manage
        st.markdown("### 🎯 Select an Application")
        selected_app = st.selectbox(
            "Choose the application you want to manage images for",
            options=portal_apps['app_title'].tolist(),
            format_func=lambda x: x,
            key="image_app_select",
            help="Select an application to view or modify its image"
        )
        
        if selected_app:
            app_row = portal_apps[portal_apps['app_title'] == selected_app].iloc[0]
            app_id = app_row['app_id']
            app_name = app_row['app_name']
            
            # st.markdown("---")
            st.markdown(f"## **Manage Image for: {selected_app}**")
            
            # Show current image
            self.show_current_image(app_id)
            
            # Upload new image
            st.markdown("### 📁 Upload New Image")
            
            self.file_upload_interface(app_id, app_name)
    
    def show_current_image(self, app_id):
        """Show the current image for an app"""
        current_image_path = self.get_current_image_path(app_id)
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            if current_image_path:
                st.markdown("**Current Image:**")
                # Try to load the actual image from database
                image_data = self.load_image_from_database(current_image_path)
                if image_data:
                    st.image(image_data, width=200)
                else:
                    st.image("https://via.placeholder.com/200x200?text=Image+Found", width=200)
            else:
                st.markdown("**Current Image:**")
                st.image("https://via.placeholder.com/200x200?text=No+Image", width=200)
        
        with col2:
            if current_image_path:
                st.markdown("**Actions:**")
                
                if st.button("🗑️ Remove Current Image", key=f"remove_{app_id}"):
                    if self.remove_image(app_id):
                        st.success("✅ Image removed successfully!")
                        st.rerun()
                    else:
                        st.error("❌ Failed to remove image")
            else:
                st.info("No image currently set for this application")
    
    def file_upload_interface(self, app_id, app_name):
        """Simple file upload interface"""
        st.markdown("Upload an image file (PNG, JPG, JPEG, GIF, WEBP)")
        
        # Check for upload success state
        success_key = f"upload_success_{app_id}"
        if success_key in st.session_state and st.session_state[success_key]:
            st.success("✅ Image uploaded successfully!")
            if st.button("📁 Upload Another Image", key=f"reset_upload_{app_id}"):
                # Clear only the success state - don't touch file uploader state
                if success_key in st.session_state:
                    del st.session_state[success_key]
                st.rerun()
            return  # Don't show upload interface when in success state
        
        # File uploader - don't try to control via session state
        uploaded_file = st.file_uploader(
            "Choose an image file",
            type=['png', 'jpg', 'jpeg', 'gif', 'webp'],
            key=f"upload_{app_id}",
            help="Select an image file from your computer"
        )
        
        if uploaded_file is not None:
            # Show preview
            try:
                image = Image.open(uploaded_file)
                st.markdown("**Preview:**")
                
                # Show original size info
                st.text(f"Original size: {image.size[0]} x {image.size[1]} pixels")
                
                # Resize for preview while maintaining aspect ratio
                preview_image = self.resize_image_for_preview(image)
                st.image(preview_image, width=200)
                
            except Exception as e:
                st.error(f"❌ Error processing image: {str(e)}")
        
        # Show save button only if there's a file selected
        if uploaded_file is not None:
            if st.button("💾 Save Image", key=f"save_upload_{app_id}", type="primary"):
                if self.save_image_to_database(uploaded_file, app_id, app_name):
                    st.success("✅ Image uploaded successfully!")
                    # Use a success flag instead of clearing session state directly
                    st.session_state[f"upload_success_{app_id}"] = True
                    st.rerun()
                else:
                    st.error("❌ Failed to upload image")
    

    
    def resize_image_for_preview(self, image, max_size=(200, 200)):
        """Resize image for preview while maintaining aspect ratio"""
        if isinstance(image, bytes):
            image = Image.open(io.BytesIO(image))
        
        # Create a copy to avoid modifying the original
        preview = image.copy()
        preview.thumbnail(max_size, Image.Resampling.LANCZOS)
        return preview
    
    def save_image_to_database(self, image_data, app_id, app_name):
        """Save image as compressed base64 data in database"""
        try:
            # Convert to bytes if needed
            if hasattr(image_data, 'read'):
                # Reset file pointer if it's a file-like object
                if hasattr(image_data, 'seek'):
                    image_data.seek(0)
                img_bytes = image_data.read()
            else:
                img_bytes = image_data
            
            # Database storage with compression
            st.info("Compressing and storing image")
            
            # Compress image to reduce base64 size and ensure consistent display
            try:
                # Open image with PIL
                image = Image.open(io.BytesIO(img_bytes))
                
                # Resize to reasonable size for portal display (max 400x400)
                image.thumbnail((400, 400), Image.Resampling.LANCZOS)
                
                # Convert to RGB if needed (removes transparency)
                if image.mode in ('RGBA', 'LA', 'P'):
                    rgb_image = Image.new('RGB', image.size, (255, 255, 255))
                    if image.mode == 'P':
                        image = image.convert('RGBA')
                    rgb_image.paste(image, mask=image.split()[-1] if image.mode == 'RGBA' else None)
                    image = rgb_image
                
                # Save as compressed JPEG to reduce size
                output_buffer = io.BytesIO()
                image.save(output_buffer, format='JPEG', quality=85, optimize=True)
                compressed_bytes = output_buffer.getvalue()
                
                st.info(f"Image compressed: {len(img_bytes)} bytes → {len(compressed_bytes)} bytes")
                
                # Convert compressed image to base64
                img_base64 = base64.b64encode(compressed_bytes).decode('utf-8')
                
            except Exception as e:
                st.warning(f"Could not compress image, using original: {str(e)}")
                # Fallback to original if compression fails
                img_base64 = base64.b64encode(img_bytes).decode('utf-8')
            
            # Store base64 data directly in image_path column with special prefix
            image_path = f"base64:{img_base64}"
            
            if hasattr(self.conn, 'sql'):  # Snowpark session
                # Use proper SQL escaping for safety
                escaped_image_path = image_path.replace("'", "''")  # Escape single quotes
                escaped_app_id = app_id.replace("'", "''")  # Escape single quotes
                self.conn.sql(f"""
                    UPDATE portal_apps 
                    SET image_path = '{escaped_image_path}', updated_at = CURRENT_TIMESTAMP()
                    WHERE app_id = '{escaped_app_id}'
                """).collect()
            else:  # Regular connection
                cursor = self.conn.cursor()
                cursor.execute("""
                    UPDATE portal_apps 
                    SET image_path = %s, updated_at = CURRENT_TIMESTAMP()
                    WHERE app_id = %s
                """, (image_path, app_id))
                cursor.close()
            
            st.success("✅ Image saved successfully!")
            return True
                
        except Exception as e:
            st.error(f"Error saving image: {str(e)}")
            return False
    
    def remove_image(self, app_id):
        """Remove image for an app"""
        try:
            if hasattr(self.conn, 'sql'):  # Snowpark session
                # Use proper SQL escaping for safety
                escaped_app_id = app_id.replace("'", "''")  # Escape single quotes
                self.conn.sql(f"""
                    UPDATE portal_apps 
                    SET image_path = NULL, updated_at = CURRENT_TIMESTAMP()
                    WHERE app_id = '{escaped_app_id}'
                """).collect()
            else:  # Regular connection
                cursor = self.conn.cursor()
                cursor.execute("""
                    UPDATE portal_apps 
                    SET image_path = %s, updated_at = CURRENT_TIMESTAMP()
                    WHERE app_id = %s
                """, (None, app_id))
                cursor.close()
            
            return True
            
        except Exception as e:
            st.error(f"Error removing image: {str(e)}")
            return False
    
    def get_current_image_path(self, app_id):
        """Get current image path for an app"""
        try:
            if hasattr(self.conn, 'sql'):  # Snowpark session
                # Use proper SQL escaping for safety
                escaped_app_id = app_id.replace("'", "''")  # Escape single quotes
                result = self.conn.sql(f"""
                    SELECT image_path FROM portal_apps WHERE app_id = '{escaped_app_id}'
                """).to_pandas()
                
                if not result.empty:
                    # Normalize column names for SiS compatibility
                    result.columns = result.columns.astype(str).str.lower()
                    return result.iloc[0]['image_path']
                return None
            else:  # Regular connection
                cursor = self.conn.cursor()
                cursor.execute("SELECT image_path FROM portal_apps WHERE app_id = %s", (app_id,))
                result = cursor.fetchone()
                cursor.close()
                
                if result and result[0]:
                    return result[0]
                return None
                
        except Exception as e:
            st.error(f"Error getting image path: {str(e)}")
            return None
    
    def load_image_from_database(self, image_path):
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
                    st.warning(f"Could not decode base64 image data: {str(e)}")
                    return None
            
            # No image found
            return None
                    
        except Exception as e:
            st.warning(f"Could not load image: {str(e)}")
            return None

def show_simple_image_management(conn, portal_apps):
    """Show the simple image management interface"""
    image_manager = SimpleImageManager(conn)
    image_manager.show_image_management(portal_apps) 