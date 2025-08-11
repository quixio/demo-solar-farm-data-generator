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

def test_google_storage_connection():
    """Test connection to Google Cloud Storage and read 10 sample records."""
    
    # Load environment variables
    load_dotenv()
    
    try:
        # Get credentials from environment variable
        credentials_json = os.environ.get('GOOGLE_STORAGE_SECRET_KEY')
        if not credentials_json:
            raise ValueError("GOOGLE_STORAGE_SECRET_KEY environment variable not found")
        
        # Parse credentials JSON
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Get configuration from environment variables
        bucket_name = os.environ.get('GOOGLE_STORAGE_BUCKET')
        folder_path = os.environ.get('GOOGLE_STORAGE_FOLDER_PATH', '/')
        file_format = os.environ.get('GOOGLE_STORAGE_FILE_FORMAT', 'csv')
        
        if not bucket_name:
            raise ValueError("GOOGLE_STORAGE_BUCKET environment variable not found")
        
        print(f"Connecting to Google Cloud Storage bucket: {bucket_name}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        
        # Initialize the storage client
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)
        
        # Clean up folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        # List files in the bucket/folder
        blobs = list(bucket.list_blobs(prefix=folder_path))
        
        if not blobs:
            print(f"No files found in bucket '{bucket_name}' with prefix '{folder_path}'")
            return
        
        # Filter files by format
        target_files = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format.lower()}')]
        
        if not target_files:
            print(f"No {file_format} files found in the specified location")
            return
        
        print(f"Found {len(target_files)} {file_format} file(s)")
        
        records_read = 0
        target_records = 10
        
        # Read files until we get 10 records
        for blob in target_files:
            if records_read >= target_records:
                break
                
            print(f"\nReading from file: {blob.name}")
            
            try:
                # Download file content
                content = blob.download_as_text()
                
                if file_format.lower() == 'csv':
                    # Read CSV content
                    df = pd.read_csv(io.StringIO(content))
                    
                    # Get remaining records needed
                    remaining = target_records - records_read
                    records_to_read = min(len(df), remaining)
                    
                    # Print records
                    for i in range(records_to_read):
                        print(f"Record {records_read + 1}: {df.iloc[i].to_dict()}")
                        records_read += 1
                        
                elif file_format.lower() == 'json':
                    # Handle JSON files
                    try:
                        # Try to parse as JSON array
                        data = json.loads(content)
                        if isinstance(data, list):
                            remaining = target_records - records_read
                            records_to_read = min(len(data), remaining)
                            
                            for i in range(records_to_read):
                                print(f"Record {records_read + 1}: {data[i]}")
                                records_read += 1
                        else:
                            # Single JSON object
                            print(f"Record {records_read + 1}: {data}")
                            records_read += 1
                    except json.JSONDecodeError:
                        # Handle JSONL (newline-delimited JSON)
                        lines = content.strip().split('\n')
                        remaining = target_records - records_read
                        records_to_read = min(len(lines), remaining)
                        
                        for i in range(records_to_read):
                            try:
                                record = json.loads(lines[i])
                                print(f"Record {records_read + 1}: {record}")
                                records_read += 1
                            except json.JSONDecodeError as e:
                                print(f"Error parsing JSON line {i+1}: {e}")
                                
                else:
                    # Handle other text formats
                    lines = content.strip().split('\n')
                    remaining = target_records - records_read
                    records_to_read = min(len(lines), remaining)
                    
                    for i in range(records_to_read):
                        print(f"Record {records_read + 1}: {lines[i]}")
                        records_read += 1
                        
            except Exception as e:
                print(f"Error reading file {blob.name}: {e}")
                continue
        
        print(f"\nSuccessfully read {records_read} records from Google Cloud Storage")
        
    except json.JSONDecodeError as e:
        print(f"Error parsing credentials JSON: {e}")
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {e}")

if __name__ == "__main__":
    test_google_storage_connection()