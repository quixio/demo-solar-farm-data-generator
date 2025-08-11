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
        # Get configuration from environment variables
        bucket_name = os.environ['GOOGLE_STORAGE_BUCKET']
        folder_path = os.environ['GOOGLE_STORAGE_FOLDER_PATH'].strip('/')
        file_format = os.environ['GOOGLE_STORAGE_FILE_FORMAT'].lower()
        credentials_json_str = os.environ['GOOGLE_STORAGE_SECRET_KEY']
        
        print(f"Connecting to Google Cloud Storage bucket: {bucket_name}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        
        # Parse credentials JSON
        try:
            credentials_info = json.loads(credentials_json_str)
        except json.JSONDecodeError as e:
            print(f"Error parsing credentials JSON: {e}")
            return
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        
        # Initialize the Google Cloud Storage client
        client = storage.Client(credentials=credentials)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        print(f"Successfully connected to bucket: {bucket_name}")
        
        # List files in the specified folder
        prefix = folder_path + '/' if folder_path else ''
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            print(f"No files found in folder: {folder_path}")
            return
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if blob.name.lower().endswith(f'.{file_format}'):
                target_files.append(blob)
        
        if not target_files:
            print(f"No {file_format} files found in the specified folder")
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
                file_content = blob.download_as_text()
                
                if file_format == 'csv':
                    # Read CSV data
                    df = pd.read_csv(io.StringIO(file_content))
                    
                    # Read remaining records needed
                    records_to_read = min(len(df), target_records - records_read)
                    
                    for i in range(records_to_read):
                        record = df.iloc[i].to_dict()
                        print(f"Record {records_read + 1}: {record}")
                        records_read += 1
                        
                elif file_format == 'json':
                    # Handle JSON files
                    try:
                        # Try to parse as JSON array
                        data = json.loads(file_content)
                        if isinstance(data, list):
                            records_to_read = min(len(data), target_records - records_read)
                            for i in range(records_to_read):
                                print(f"Record {records_read + 1}: {data[i]}")
                                records_read += 1
                        else:
                            # Single JSON object
                            print(f"Record {records_read + 1}: {data}")
                            records_read += 1
                    except json.JSONDecodeError:
                        # Handle JSONL (newline-delimited JSON)
                        lines = file_content.strip().split('\n')
                        records_to_read = min(len(lines), target_records - records_read)
                        for i in range(records_to_read):
                            try:
                                record = json.loads(lines[i])
                                print(f"Record {records_read + 1}: {record}")
                                records_read += 1
                            except json.JSONDecodeError as e:
                                print(f"Error parsing JSON line {i+1}: {e}")
                                
                elif file_format == 'txt':
                    # Handle text files
                    lines = file_content.strip().split('\n')
                    records_to_read = min(len(lines), target_records - records_read)
                    for i in range(records_to_read):
                        print(f"Record {records_read + 1}: {lines[i]}")
                        records_read += 1
                        
                else:
                    print(f"Unsupported file format: {file_format}")
                    return
                    
            except Exception as e:
                print(f"Error reading file {blob.name}: {e}")
                continue
        
        print(f"\nSuccessfully read {records_read} records from Google Cloud Storage")
        
    except KeyError as e:
        print(f"Missing required environment variable: {e}")
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {e}")

if __name__ == "__main__":
    test_google_storage_connection()