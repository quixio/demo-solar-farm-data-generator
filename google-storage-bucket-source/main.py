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
from io import StringIO

def main():
    # Load environment variables
    load_dotenv()
    
    try:
        # Get configuration from environment variables
        bucket_name = os.environ['GCS_BUCKET']
        folder_path = os.environ['GCS_FOLDER_PATH'].strip('/')
        file_format = os.environ['GCS_FILE_FORMAT'].lower()
        file_compression = os.environ['GCS_FILE_COMPRESSION']
        credentials_key = os.environ['GCP_CREDENTIALS_KEY']
        
        print(f"Connecting to Google Cloud Storage bucket: {bucket_name}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {file_compression}")
        
        # Get credentials from environment variable
        credentials_json = os.environ.get(credentials_key)
        if not credentials_json:
            raise ValueError("GCP credentials not found in environment variable")
        
        # Parse credentials JSON
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize GCS client
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)
        
        print(f"Successfully connected to bucket: {bucket_name}")
        
        # List files in the specified folder
        prefix = folder_path + '/' if folder_path else None
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            print("No files found in the specified folder")
            return
        
        # Filter files by format
        if file_format == 'csv':
            target_files = [blob for blob in blobs if blob.name.lower().endswith('.csv')]
        elif file_format == 'json':
            target_files = [blob for blob in blobs if blob.name.lower().endswith('.json')]
        else:
            target_files = blobs  # Include all files if format is not specified
        
        if not target_files:
            print(f"No {file_format} files found in the specified folder")
            return
        
        print(f"Found {len(target_files)} {file_format} files")
        
        # Read and display samples from files
        items_read = 0
        target_items = 10
        
        for blob in target_files:
            if items_read >= target_items:
                break
                
            print(f"\n--- Reading from file: {blob.name} ---")
            
            try:
                # Download file content
                content = blob.download_as_text()
                
                if file_format == 'csv':
                    # Parse CSV content
                    df = pd.read_csv(StringIO(content))
                    
                    # Read available rows up to remaining target
                    rows_to_read = min(len(df), target_items - items_read)
                    
                    for i in range(rows_to_read):
                        print(f"Item {items_read + 1}: {df.iloc[i].to_dict()}")
                        items_read += 1
                        
                elif file_format == 'json':
                    # Handle different JSON formats
                    try:
                        # Try parsing as JSON Lines (JSONL)
                        lines = content.strip().split('\n')
                        for line in lines:
                            if items_read >= target_items:
                                break
                            if line.strip():
                                item = json.loads(line)
                                print(f"Item {items_read + 1}: {item}")
                                items_read += 1
                    except json.JSONDecodeError:
                        try:
                            # Try parsing as single JSON object
                            data = json.loads(content)
                            if isinstance(data, list):
                                # JSON array
                                for item in data:
                                    if items_read >= target_items:
                                        break
                                    print(f"Item {items_read + 1}: {item}")
                                    items_read += 1
                            else:
                                # Single JSON object
                                print(f"Item {items_read + 1}: {data}")
                                items_read += 1
                        except json.JSONDecodeError as e:
                            print(f"Error parsing JSON file {blob.name}: Invalid JSON format")
                            continue
                
                else:
                    # For other formats, just display raw content (limited)
                    lines = content.split('\n')
                    for line in lines:
                        if items_read >= target_items:
                            break
                        if line.strip():
                            print(f"Item {items_read + 1}: {line.strip()}")
                            items_read += 1
                            
            except Exception as e:
                print(f"Error reading file {blob.name}: {str(e)}")
                continue
            
            if items_read >= target_items:
                break
        
        print(f"\n--- Successfully read {items_read} items from Google Cloud Storage ---")
        
    except KeyError as e:
        print(f"Missing required environment variable: {e}")
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in credentials")
    except Exception as e:
        print(f"Connection test failed: {str(e)}")

if __name__ == "__main__":
    main()