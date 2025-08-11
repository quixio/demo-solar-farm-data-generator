# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# pip install pandas
# END_DEPENDENCIES

import os
import json
import io
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
import pandas as pd

def main():
    # Load environment variables
    load_dotenv()
    
    try:
        # Get credentials from environment variable
        credentials_json = os.environ.get('GOOGLE_STORAGE_SECRET_KEY')
        if not credentials_json:
            raise ValueError("GOOGLE_STORAGE_SECRET_KEY environment variable is required")
        
        # Parse credentials JSON
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials)
        
        # Get bucket
        bucket_name = os.environ.get('GOOGLE_STORAGE_BUCKET')
        if not bucket_name:
            raise ValueError("GOOGLE_STORAGE_BUCKET environment variable is required")
        
        bucket = client.bucket(bucket_name)
        
        # Get configuration
        folder_path = os.environ.get('GOOGLE_STORAGE_FOLDER_PATH', '/')
        file_format = os.environ.get('GOOGLE_STORAGE_FILE_FORMAT', 'csv')
        
        # Clean folder path (remove leading slash if present)
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        print(f"Connecting to Google Storage bucket: {bucket_name}")
        print(f"Looking for {file_format} files in folder: {folder_path or 'root'}")
        
        # List files in the specified folder
        blobs = list(bucket.list_blobs(prefix=folder_path))
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if blob.name.lower().endswith(f'.{file_format.lower()}'):
                target_files.append(blob)
        
        if not target_files:
            print(f"No {file_format} files found in the specified folder")
            return
        
        print(f"Found {len(target_files)} {file_format} file(s)")
        
        items_read = 0
        target_items = 10
        
        # Read files until we get 10 items
        for blob in target_files:
            if items_read >= target_items:
                break
                
            print(f"\nReading from file: {blob.name}")
            
            try:
                # Download file content
                content = blob.download_as_text()
                
                if file_format.lower() == 'csv':
                    # Parse CSV content
                    df = pd.read_csv(io.StringIO(content))
                    
                    # Read rows until we reach our target
                    for index, row in df.iterrows():
                        if items_read >= target_items:
                            break
                        
                        items_read += 1
                        print(f"Item {items_read}:")
                        print(f"  File: {blob.name}")
                        print(f"  Row {index + 1}: {row.to_dict()}")
                        print()
                
                elif file_format.lower() == 'json':
                    # Parse JSON content
                    try:
                        # Try to parse as JSON array
                        data = json.loads(content)
                        if isinstance(data, list):
                            for item in data:
                                if items_read >= target_items:
                                    break
                                items_read += 1
                                print(f"Item {items_read}:")
                                print(f"  File: {blob.name}")
                                print(f"  Data: {item}")
                                print()
                        else:
                            # Single JSON object
                            if items_read < target_items:
                                items_read += 1
                                print(f"Item {items_read}:")
                                print(f"  File: {blob.name}")
                                print(f"  Data: {data}")
                                print()
                    except json.JSONDecodeError:
                        # Try parsing as JSONL (newline-delimited JSON)
                        lines = content.strip().split('\n')
                        for line in lines:
                            if items_read >= target_items:
                                break
                            if line.strip():
                                try:
                                    item = json.loads(line)
                                    items_read += 1
                                    print(f"Item {items_read}:")
                                    print(f"  File: {blob.name}")
                                    print(f"  Data: {item}")
                                    print()
                                except json.JSONDecodeError:
                                    continue
                
                else:
                    # For other formats, read as text lines
                    lines = content.strip().split('\n')
                    for line in lines:
                        if items_read >= target_items:
                            break
                        if line.strip():
                            items_read += 1
                            print(f"Item {items_read}:")
                            print(f"  File: {blob.name}")
                            print(f"  Data: {line}")
                            print()
                            
            except Exception as e:
                print(f"Error reading file {blob.name}: {str(e)}")
                continue
        
        print(f"\nSuccessfully read {items_read} items from Google Storage bucket")
        
    except Exception as e:
        print(f"Error connecting to Google Storage: {str(e)}")
        raise

if __name__ == "__main__":
    main()