# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_google_storage_connection():
    try:
        # Get credentials from environment variable
        credentials_json = os.environ.get('GS_SECRET_KEY')
        if not credentials_json:
            raise ValueError("GS_SECRET_KEY environment variable not found")
        
        # Parse the JSON credentials
        credentials_info = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        
        # Get other configuration from environment variables
        bucket_name = os.environ.get('GS_BUCKET')
        project_id = os.environ.get('GS_PROJECT_ID')
        folder_path = os.environ.get('GS_FOLDER_PATH', '/')
        file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
        
        if not bucket_name:
            raise ValueError("GS_BUCKET environment variable not found")
        if not project_id:
            raise ValueError("GS_PROJECT_ID environment variable not found")
        
        print(f"Connecting to Google Storage bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"Expected file format: {file_format}")
        print("-" * 50)
        
        # Initialize the Google Cloud Storage client
        client = storage.Client(project=project_id, credentials=credentials)
        bucket = client.bucket(bucket_name)
        
        # Clean up folder path for prefix search
        prefix = folder_path.strip('/')
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        
        # List blobs with the specified prefix
        blobs = list(bucket.list_blobs(prefix=prefix if prefix != '/' else ''))
        
        if not blobs:
            print("No files found in the specified folder path")
            return
        
        print(f"Found {len(blobs)} total files in bucket")
        print("Reading first 10 files:")
        print("-" * 50)
        
        count = 0
        for blob in blobs:
            if count >= 10:
                break
                
            # Skip directories (blobs ending with '/')
            if blob.name.endswith('/'):
                continue
                
            try:
                print(f"\nFile {count + 1}: {blob.name}")
                print(f"Size: {blob.size} bytes")
                print(f"Content Type: {blob.content_type}")
                print(f"Created: {blob.time_created}")
                
                # Download and display content preview (first 500 characters)
                if blob.size and blob.size > 0:
                    content = blob.download_as_text(encoding='utf-8')
                    preview = content[:500] if len(content) > 500 else content
                    print(f"Content preview:\n{preview}")
                    if len(content) > 500:
                        print("... (truncated)")
                else:
                    print("File is empty")
                
                print("-" * 30)
                count += 1
                
            except Exception as e:
                print(f"Error reading file {blob.name}: {str(e)}")
                print("-" * 30)
                count += 1
                continue
        
        if count == 0:
            print("No readable files found (all files may be directories or empty)")
        else:
            print(f"\nSuccessfully read {count} files from Google Storage bucket")
            
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in GS_SECRET_KEY")
    except Exception as e:
        print(f"Connection test failed: {str(e)}")
    finally:
        print("\nConnection test completed")

if __name__ == "__main__":
    test_google_storage_connection()