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
        # Get configuration from environment variables
        bucket_name = os.environ['GOOGLE_STORAGE_BUCKET']
        region = os.environ['GOOGLE_STORAGE_REGION']
        credentials_json = os.environ['GOOGLE_STORAGE_SECRET_KEY']
        folder_path = os.environ['GOOGLE_STORAGE_FOLDER_PATH']
        file_format = os.environ['GOOGLE_STORAGE_FILE_FORMAT']
        compression = os.environ['GOOGLE_STORAGE_FILE_COMPRESSION']
        
        # Parse credentials JSON
        if not credentials_json:
            raise ValueError("Google Cloud credentials not found in environment variable")
        
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials)
        
        print(f"Connecting to Google Storage bucket: {bucket_name}")
        print(f"Region: {region}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {compression}")
        print("-" * 50)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # Clean up folder path
        prefix = folder_path.strip('/')
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        
        # List files in the bucket with the specified prefix
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            print(f"No files found in bucket {bucket_name} with prefix '{prefix}'")
            return
        
        print(f"Found {len(blobs)} files in the bucket")
        
        # Filter files by format if specified
        if file_format.lower() != 'none':
            filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format.lower()}')]
            if not filtered_blobs:
                print(f"No {file_format} files found")
                return
            blobs = filtered_blobs
        
        items_read = 0
        target_items = 10
        
        # Process files until we get 10 items
        for blob in blobs:
            if items_read >= target_items:
                break
                
            print(f"\nProcessing file: {blob.name}")
            
            try:
                # Download file content
                content = blob.download_as_bytes()
                
                # Handle compression
                if compression.lower() == 'gzip':
                    import gzip
                    content = gzip.decompress(content)
                elif compression.lower() == 'bz2':
                    import bz2
                    content = bz2.decompress(content)
                
                # Process based on file format
                if file_format.lower() == 'csv':
                    # Read CSV content
                    df = pd.read_csv(io.BytesIO(content))
                    
                    # Print up to remaining items needed
                    remaining_items = target_items - items_read
                    rows_to_show = min(len(df), remaining_items)
                    
                    for i in range(rows_to_show):
                        print(f"Item {items_read + 1}: {df.iloc[i].to_dict()}")
                        items_read += 1
                        
                elif file_format.lower() == 'json':
                    # Read JSON content
                    content_str = content.decode('utf-8')
                    
                    # Try to parse as JSON array first
                    try:
                        json_data = json.loads(content_str)
                        if isinstance(json_data, list):
                            remaining_items = target_items - items_read
                            items_to_show = min(len(json_data), remaining_items)
                            
                            for i in range(items_to_show):
                                print(f"Item {items_read + 1}: {json_data[i]}")
                                items_read += 1
                        else:
                            # Single JSON object
                            print(f"Item {items_read + 1}: {json_data}")
                            items_read += 1
                    except json.JSONDecodeError:
                        # Try parsing as JSONL (newline-delimited JSON)
                        lines = content_str.strip().split('\n')
                        remaining_items = target_items - items_read
                        lines_to_show = min(len(lines), remaining_items)
                        
                        for i in range(lines_to_show):
                            try:
                                json_obj = json.loads(lines[i])
                                print(f"Item {items_read + 1}: {json_obj}")
                                items_read += 1
                            except json.JSONDecodeError:
                                print(f"Item {items_read + 1}: {lines[i]}")
                                items_read += 1
                                
                elif file_format.lower() == 'txt':
                    # Read text content line by line
                    content_str = content.decode('utf-8')
                    lines = content_str.strip().split('\n')
                    
                    remaining_items = target_items - items_read
                    lines_to_show = min(len(lines), remaining_items)
                    
                    for i in range(lines_to_show):
                        print(f"Item {items_read + 1}: {lines[i]}")
                        items_read += 1
                        
                else:
                    # For other formats, just show raw content (limited)
                    content_str = content.decode('utf-8', errors='ignore')
                    print(f"Item {items_read + 1}: {content_str[:200]}...")
                    items_read += 1
                    
            except Exception as e:
                print(f"Error processing file {blob.name}: {str(e)}")
                continue
        
        print(f"\n" + "=" * 50)
        print(f"Successfully read {items_read} items from Google Storage bucket")
        
    except KeyError as e:
        print(f"Missing required environment variable: {e}")
    except json.JSONDecodeError:
        print("Invalid JSON format in credentials")
    except Exception as e:
        print(f"Connection error: {str(e)}")

if __name__ == "__main__":
    main()