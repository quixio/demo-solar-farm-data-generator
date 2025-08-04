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
    """Test connection to Google Storage Bucket and read sample files."""
    
    try:
        # Get credentials from environment variable
        credentials_json = os.environ.get('GS_SECRET_KEY')
        if not credentials_json:
            raise ValueError("GS_SECRET_KEY environment variable not found")
        
        # Parse the JSON credentials
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in credentials")
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize the Google Cloud Storage client
        client = storage.Client(
            credentials=credentials,
            project=os.environ.get('GS_PROJECT_ID')
        )
        
        # Get the bucket
        bucket_name = os.environ.get('GS_BUCKET')
        bucket = client.bucket(bucket_name)
        
        print(f"Successfully connected to Google Storage bucket: {bucket_name}")
        
        # Get folder path (remove leading slash if present)
        folder_path = os.environ.get('GS_FOLDER_PATH', '/')
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        
        # List blobs in the specified folder
        blobs = bucket.list_blobs(prefix=folder_path)
        
        # Read exactly 10 sample items
        items_read = 0
        target_items = 10
        
        print(f"\nReading {target_items} sample items from bucket...")
        print("-" * 60)
        
        for blob in blobs:
            if items_read >= target_items:
                break
                
            # Skip directories (blobs ending with '/')
            if blob.name.endswith('/'):
                continue
                
            try:
                print(f"\nItem {items_read + 1}:")
                print(f"File Name: {blob.name}")
                print(f"Size: {blob.size} bytes")
                print(f"Created: {blob.time_created}")
                print(f"Content Type: {blob.content_type}")
                
                # Read file content (limit to first 500 characters for display)
                content = blob.download_as_text()
                if len(content) > 500:
                    print(f"Content Preview: {content[:500]}...")
                else:
                    print(f"Content: {content}")
                
                items_read += 1
                
            except Exception as e:
                print(f"Error reading blob {blob.name}: {str(e)}")
                continue
        
        if items_read == 0:
            print("No files found in the specified bucket/folder path")
        else:
            print(f"\nSuccessfully read {items_read} items from Google Storage bucket")
            
    except Exception as e:
        print(f"Connection test failed: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    print("Testing Google Storage Bucket connection...")
    success = test_google_storage_connection()
    
    if success:
        print("\n✅ Connection test completed successfully!")
    else:
        print("\n❌ Connection test failed!")