# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# pip install pandas
# END_DEPENDENCIES

import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
from io import StringIO, BytesIO

# Load environment variables
load_dotenv()

def test_google_storage_connection():
    try:
        # Get environment variables
        bucket_name = os.environ.get('GS_BUCKET')
        project_id = os.environ.get('GS_PROJECT_ID')
        folder_path = os.environ.get('GS_FOLDER_PATH', '/')
        file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
        file_compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
        credentials_json = os.environ.get('GS_SECRET_KEY')
        
        # Validate required environment variables
        if not bucket_name:
            raise ValueError("GS_BUCKET environment variable is required")
        if not project_id:
            raise ValueError("GS_PROJECT_ID environment variable is required")
        if not credentials_json:
            raise ValueError("GS_SECRET_KEY environment variable is required")
        
        print(f"Connecting to Google Storage bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {file_compression}")
        
        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON format in GS_SECRET_KEY")
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # List files in the specified folder
        blobs = list(bucket.list_blobs(prefix=folder_path.lstrip('/')))
        
        if not blobs:
            print("No files found in the specified folder")
            return
        
        print(f"Found {len(blobs)} files in the bucket")
        
        # Filter files by format if specified
        if file_format != 'none':
            filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format.lower()}')]
            if filtered_blobs:
                blobs = filtered_blobs
            else:
                print(f"No {file_format} files found, using all available files")
        
        # Process files and read sample data
        items_read = 0
        target_items = 10
        
        for blob in blobs:
            if items_read >= target_items:
                break
                
            # Skip directories
            if blob.name.endswith('/'):
                continue
                
            print(f"\nReading from file: {blob.name}")
            
            try:
                # Download file content
                file_content = blob.download_as_bytes()
                
                # Handle compression
                if file_compression.lower() == 'gzip':
                    import gzip
                    file_content = gzip.decompress(file_content)
                elif file_compression.lower() == 'bz2':
                    import bz2
                    file_content = bz2.decompress(file_content)
                
                # Process based on file format
                if file_format.lower() == 'csv':
                    # Read CSV content
                    df = pd.read_csv(BytesIO(file_content))
                    remaining_items = target_items - items_read
                    sample_rows = min(len(df), remaining_items)
                    
                    for idx, row in df.head(sample_rows).iterrows():
                        print(f"Item {items_read + 1}: {row.to_dict()}")
                        items_read += 1
                        if items_read >= target_items:
                            break
                            
                elif file_format.lower() == 'json':
                    # Read JSON content
                    content_str = file_content.decode('utf-8')
                    try:
                        # Try parsing as JSON array
                        json_data = json.loads(content_str)
                        if isinstance(json_data, list):
                            for item in json_data:
                                if items_read >= target_items:
                                    break
                                print(f"Item {items_read + 1}: {item}")
                                items_read += 1
                        else:
                            # Single JSON object
                            print(f"Item {items_read + 1}: {json_data}")
                            items_read += 1
                    except json.JSONDecodeError:
                        # Try reading as JSONL (newline-delimited JSON)
                        lines = content_str.strip().split('\n')
                        for line in lines:
                            if items_read >= target_items:
                                break
                            if line.strip():
                                try:
                                    item = json.loads(line)
                                    print(f"Item {items_read + 1}: {item}")
                                    items_read += 1
                                except json.JSONDecodeError:
                                    continue
                                    
                elif file_format.lower() == 'txt':
                    # Read text content line by line
                    content_str = file_content.decode('utf-8')
                    lines = content_str.strip().split('\n')
                    for line in lines:
                        if items_read >= target_items:
                            break
                        if line.strip():
                            print(f"Item {items_read + 1}: {line.strip()}")
                            items_read += 1
                            
                else:
                    # For other formats, treat as binary and show first few bytes
                    print(f"Item {items_read + 1}: Binary content (first 100 bytes): {file_content[:100]}")
                    items_read += 1
                    
            except Exception as e:
                print(f"Error reading file {blob.name}: {str(e)}")
                continue
        
        if items_read == 0:
            print("No readable items found in the bucket")
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
        print("\nConnection test completed successfully!")
    else:
        print("\nConnection test failed!")