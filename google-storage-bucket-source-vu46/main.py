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
        bucket_name = os.environ['GOOGLE_BUCKET_NAME']
        folder_path = os.environ['GOOGLE_FOLDER_PATH'].strip('/')
        file_format = os.environ['GOOGLE_FILE_FORMAT']
        file_compression = os.environ['GOOGLE_FILE_COMPRESSION']
        credentials_json_str = os.environ['GOOGLE_SECRET_KEY']
        
        print(f"Connecting to Google Cloud Storage bucket: {bucket_name}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"File compression: {file_compression}")
        
        # Parse credentials JSON
        credentials_info = json.loads(credentials_json_str)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)
        
        print(f"\nSuccessfully connected to bucket: {bucket_name}")
        
        # List files in the specified folder
        prefix = folder_path + '/' if folder_path else ''
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            print(f"No files found in folder: {folder_path}")
            return
        
        # Filter files by format if specified
        if file_format and file_format != 'none':
            blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format.lower()}')]
        
        if not blobs:
            print(f"No {file_format} files found in folder: {folder_path}")
            return
        
        print(f"\nFound {len(blobs)} file(s) matching criteria")
        
        items_read = 0
        target_items = 10
        
        # Process files to read sample items
        for blob in blobs:
            if items_read >= target_items:
                break
                
            print(f"\nReading from file: {blob.name}")
            
            try:
                # Download file content
                file_content = blob.download_as_text()
                
                if file_format.lower() == 'csv':
                    # Parse CSV content
                    csv_reader = csv.DictReader(io.StringIO(file_content))
                    
                    for row_num, row in enumerate(csv_reader, 1):
                        if items_read >= target_items:
                            break
                        
                        items_read += 1
                        print(f"\nItem {items_read}:")
                        print(f"File: {blob.name}, Row: {row_num}")
                        print(f"Data: {dict(row)}")
                        
                elif file_format.lower() == 'json':
                    # Parse JSON content
                    try:
                        # Try to parse as JSON array
                        data = json.loads(file_content)
                        if isinstance(data, list):
                            for item in data:
                                if items_read >= target_items:
                                    break
                                items_read += 1
                                print(f"\nItem {items_read}:")
                                print(f"File: {blob.name}")
                                print(f"Data: {item}")
                        else:
                            # Single JSON object
                            items_read += 1
                            print(f"\nItem {items_read}:")
                            print(f"File: {blob.name}")
                            print(f"Data: {data}")
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON in file {blob.name}: {str(e)}")
                        continue
                        
                else:
                    # For other formats, treat as text and read lines
                    lines = file_content.strip().split('\n')
                    for line_num, line in enumerate(lines, 1):
                        if items_read >= target_items:
                            break
                        if line.strip():  # Skip empty lines
                            items_read += 1
                            print(f"\nItem {items_read}:")
                            print(f"File: {blob.name}, Line: {line_num}")
                            print(f"Data: {line}")
                            
            except Exception as e:
                print(f"Error reading file {blob.name}: {str(e)}")
                continue
        
        print(f"\n✅ Successfully read {items_read} items from Google Cloud Storage bucket")
        
    except KeyError as e:
        print(f"❌ Missing required environment variable: {e}")
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in credentials: {str(e)}")
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")

if __name__ == "__main__":
    main()