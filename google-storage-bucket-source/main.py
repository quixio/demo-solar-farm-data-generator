# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

def test_google_storage_connection():
    """Test connection to Google Storage Bucket and read 10 sample items."""
    
    # Load environment variables
    load_dotenv()
    
    try:
        # Get environment variables
        bucket_name = os.environ.get('GS_BUCKET')
        project_id = os.environ.get('GS_PROJECT_ID')
        credentials_json = os.environ.get('GS_SECRET_KEY')
        folder_path = os.environ.get('GS_FOLDER_PATH', '/')
        
        # Validate required environment variables
        if not bucket_name:
            raise ValueError("GS_BUCKET environment variable is required")
        if not project_id:
            raise ValueError("GS_PROJECT_ID environment variable is required")
        if not credentials_json:
            raise ValueError("GS_SECRET_KEY environment variable is required")
        
        print(f"Connecting to Google Storage Bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        
        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in credentials")
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # Check if bucket exists
        if not bucket.exists():
            raise ValueError(f"Bucket '{bucket_name}' does not exist or is not accessible")
        
        print(f"Successfully connected to bucket: {bucket_name}")
        
        # Clean folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        # List blobs (files) in the specified folder
        blobs = list(bucket.list_blobs(prefix=folder_path, max_results=10))
        
        if not blobs:
            print(f"No files found in folder: {folder_path}")
            return
        
        print(f"\nFound {len(blobs)} files. Reading up to 10 samples:")
        print("-" * 50)
        
        # Read and display up to 10 files
        for i, blob in enumerate(blobs[:10], 1):
            try:
                print(f"\n[{i}] File: {blob.name}")
                print(f"    Size: {blob.size} bytes")
                print(f"    Content Type: {blob.content_type}")
                print(f"    Created: {blob.time_created}")
                
                # Only read content for small files (< 1MB) to avoid memory issues
                if blob.size and blob.size < 1024 * 1024:
                    content = blob.download_as_text()
                    # Show first 200 characters of content
                    preview = content[:200] + "..." if len(content) > 200 else content
                    print(f"    Content preview: {repr(preview)}")
                else:
                    print(f"    Content: [File too large to preview - {blob.size} bytes]")
                    
            except Exception as e:
                print(f"    Error reading file {blob.name}: {str(e)}")
        
        print(f"\nSuccessfully processed {min(len(blobs), 10)} files from Google Storage Bucket")
        
    except Exception as e:
        print(f"Error connecting to Google Storage Bucket: {str(e)}")
        raise

if __name__ == "__main__":
    test_google_storage_connection()