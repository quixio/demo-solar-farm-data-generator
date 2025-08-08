# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import io
import csv
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

def main():
    # Load environment variables
    load_dotenv()
    
    try:
        # Get configuration from environment variables
        bucket_name = os.environ['GCS_BUCKET_NAME']
        folder_path = os.environ['GCS_FOLDER_PATH'].strip('/')
        file_format = os.environ['GCS_FILE_FORMAT'].lower()
        project_id = os.environ['GCP_PROJECT_ID']
        credentials_json_str = os.environ['GCP_SECRET_NAME_KEY']
        
        print(f"Connecting to Google Cloud Storage bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        
        # Parse credentials JSON
        credentials_info = json.loads(credentials_json_str)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        
        # Initialize GCS client
        client = storage.Client(credentials=credentials, project=project_id)
        bucket = client.bucket(bucket_name)
        
        print(f"\nSuccessfully connected to bucket: {bucket_name}")
        
        # List files in the specified folder
        prefix = folder_path + '/' if folder_path else ''
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            print(f"No files found in folder: {folder_path}")
            return
        
        print(f"Found {len(blobs)} files in the bucket")
        
        # Filter files by format if specified
        target_files = []
        if file_format and file_format != 'none':
            target_files = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format}')]
        else:
            target_files = blobs
        
        if not target_files:
            print(f"No files with format '{file_format}' found")
            return
        
        print(f"Found {len(target_files)} files matching format '{file_format}'")
        
        # Read sample data from files
        items_read = 0
        max_items = 10
        
        for blob in target_files:
            if items_read >= max_items:
                break
                
            print(f"\nReading from file: {blob.name}")
            
            try:
                # Download file content
                content = blob.download_as_bytes()
                
                if file_format == 'csv':
                    # Parse CSV content
                    content_str = content.decode('utf-8')
                    csv_reader = csv.DictReader(io.StringIO(content_str))
                    
                    for row_num, row in enumerate(csv_reader):
                        if items_read >= max_items:
                            break
                        items_read += 1
                        print(f"Item {items_read}: {dict(row)}")
                        
                elif file_format == 'json':
                    # Parse JSON content
                    content_str = content.decode('utf-8')
                    try:
                        # Try to parse as JSON array
                        json_data = json.loads(content_str)
                        if isinstance(json_data, list):
                            for item in json_data:
                                if items_read >= max_items:
                                    break
                                items_read += 1
                                print(f"Item {items_read}: {item}")
                        else:
                            # Single JSON object
                            items_read += 1
                            print(f"Item {items_read}: {json_data}")
                    except json.JSONDecodeError:
                        # Try to parse as JSONL (newline-delimited JSON)
                        lines = content_str.strip().split('\n')
                        for line in lines:
                            if items_read >= max_items:
                                break
                            if line.strip():
                                try:
                                    json_obj = json.loads(line)
                                    items_read += 1
                                    print(f"Item {items_read}: {json_obj}")
                                except json.JSONDecodeError:
                                    print(f"Skipping invalid JSON line: {line[:100]}...")
                
                else:
                    # For other formats, just show raw content (limited)
                    try:
                        content_str = content.decode('utf-8')
                        lines = content_str.split('\n')
                        for line in lines[:max_items - items_read]:
                            if line.strip():
                                items_read += 1
                                print(f"Item {items_read}: {line[:200]}...")  # Limit line length
                    except UnicodeDecodeError:
                        # Binary content
                        items_read += 1
                        print(f"Item {items_read}: Binary content ({len(content)} bytes)")
                
            except Exception as e:
                print(f"Error reading file {blob.name}: {str(e)}")
                continue
        
        print(f"\nSuccessfully read {items_read} items from Google Cloud Storage")
        
    except KeyError as e:
        print(f"Missing required environment variable: {e}")
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in credentials")
    except Exception as e:
        print(f"Connection test failed: {str(e)}")

if __name__ == "__main__":
    main()