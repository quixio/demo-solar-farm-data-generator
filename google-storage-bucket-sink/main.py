# DEPENDENCIES:
# pip install google-cloud-storage
# pip install pandas
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

def test_google_storage_connection():
    """Test connection to Google Storage Bucket and read sample data."""
    
    # Load environment variables
    load_dotenv()
    
    try:
        # Get configuration from environment variables
        bucket_name = os.getenv('GS_BUCKET')
        project_id = os.getenv('GS_PROJECT_ID')
        folder_path = os.getenv('GS_FOLDER_PATH', '/')
        file_format = os.getenv('GS_FILE_FORMAT', 'csv')
        file_compression = os.getenv('GS_COMPRESSION', 'none')
        secret_key_name = os.getenv('GS_SECRET_KEY')
        
        print(f"Connecting to Google Storage Bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"File compression: {file_compression}")
        
        # Get credentials from secret
        if not secret_key_name:
            raise ValueError("GS_SECRET_KEY environment variable not set")
            
        # Read credentials from the secret (assuming it contains JSON credentials)
        credentials_json = os.getenv(secret_key_name)
        if not credentials_json:
            raise ValueError(f"Secret {secret_key_name} not found in environment variables")
        
        # Parse credentials JSON
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Get bucket
        bucket = client.bucket(bucket_name)
        
        # List files in the specified folder
        folder_prefix = folder_path.strip('/')
        if folder_prefix and not folder_prefix.endswith('/'):
            folder_prefix += '/'
        elif folder_prefix == '/':
            folder_prefix = ''
            
        print(f"\nLooking for {file_format} files in folder: '{folder_prefix}'")
        
        # Get list of files matching the criteria
        blobs = list(bucket.list_blobs(prefix=folder_prefix))
        
        # Filter files by format
        matching_files = []
        for blob in blobs:
            if blob.name.endswith(f'.{file_format}'):
                matching_files.append(blob)
        
        if not matching_files:
            print(f"No {file_format} files found in the specified folder")
            return
            
        print(f"Found {len(matching_files)} {file_format} files")
        
        # Read from the first matching file
        target_blob = matching_files[0]
        print(f"\nReading from file: {target_blob.name}")
        print(f"File size: {target_blob.size} bytes")
        print(f"Last modified: {target_blob.time_created}")
        
        # Download file content
        file_content = target_blob.download_as_text()
        
        # Process based on file format
        items_read = 0
        max_items = 10
        
        if file_format.lower() == 'csv':
            # Use pandas to read CSV
            from io import StringIO
            df = pd.read_csv(StringIO(file_content))
            
            print(f"\nCSV has {len(df)} rows and {len(df.columns)} columns")
            print(f"Columns: {list(df.columns)}")
            print("\n" + "="*50)
            print("SAMPLE DATA (first 10 rows):")
            print("="*50)
            
            for idx, (index, row) in enumerate(df.iterrows()):
                if items_read >= max_items:
                    break
                print(f"\nRecord {items_read + 1}:")
                for col in df.columns:
                    print(f"  {col}: {row[col]}")
                items_read += 1
                
        elif file_format.lower() == 'json':
            # Process JSON file
            json_data = json.loads(file_content)
            
            if isinstance(json_data, list):
                print(f"\nJSON file contains {len(json_data)} items")
                print("\n" + "="*50)
                print("SAMPLE DATA (first 10 items):")
                print("="*50)
                
                for idx, item in enumerate(json_data):
                    if items_read >= max_items:
                        break
                    print(f"\nRecord {items_read + 1}:")
                    print(f"  {json.dumps(item, indent=2)}")
                    items_read += 1
            else:
                print(f"\nJSON file contains a single object:")
                print("="*50)
                print("SAMPLE DATA:")
                print("="*50)
                print(json.dumps(json_data, indent=2))
                items_read = 1
                
        else:
            # For other formats, just show first few lines
            lines = file_content.split('\n')
            print(f"\nFile contains {len(lines)} lines")
            print("\n" + "="*50)
            print("SAMPLE DATA (first 10 lines):")
            print("="*50)
            
            for idx, line in enumerate(lines):
                if items_read >= max_items or not line.strip():
                    if items_read >= max_items:
                        break
                    continue
                print(f"Line {items_read + 1}: {line}")
                items_read += 1
        
        print(f"\n" + "="*50)
        print(f"Successfully read {items_read} sample records from Google Storage Bucket")
        print("="*50)
        
    except Exception as e:
        print(f"Error connecting to Google Storage Bucket: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_google_storage_connection()