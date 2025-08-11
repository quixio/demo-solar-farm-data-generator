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
        
        print(f"Connecting to Google Cloud Storage bucket: {bucket_name}")
        print(f"Region: {region}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {compression}")
        
        # Parse credentials JSON
        if not credentials_json:
            raise ValueError("Google Cloud Storage credentials not found")
        
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)
        
        print(f"\nSuccessfully connected to bucket: {bucket_name}")
        
        # Clean folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        # List files in the specified folder
        blobs = list(bucket.list_blobs(prefix=folder_path))
        
        if not blobs:
            print(f"No files found in folder: {folder_path}")
            return
        
        # Filter files by format if specified
        if file_format and file_format.lower() != 'none':
            extension = f".{file_format.lower()}"
            blobs = [blob for blob in blobs if blob.name.lower().endswith(extension)]
        
        if not blobs:
            print(f"No {file_format} files found in folder: {folder_path}")
            return
        
        print(f"\nFound {len(blobs)} file(s) in the bucket")
        
        items_read = 0
        target_items = 10
        
        for blob in blobs:
            if items_read >= target_items:
                break
                
            print(f"\nReading from file: {blob.name}")
            
            try:
                # Download file content
                content = blob.download_as_bytes()
                
                # Handle compression
                if compression and compression.lower() != 'none':
                    if compression.lower() == 'gzip':
                        import gzip
                        content = gzip.decompress(content)
                    elif compression.lower() == 'zip':
                        import zipfile
                        with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                            # Get first file from zip
                            file_names = zip_file.namelist()
                            if file_names:
                                content = zip_file.read(file_names[0])
                
                # Process based on file format
                if file_format.lower() == 'csv':
                    df = pd.read_csv(io.BytesIO(content))
                    
                    # Read up to remaining items needed
                    remaining_items = target_items - items_read
                    rows_to_read = min(len(df), remaining_items)
                    
                    for i in range(rows_to_read):
                        print(f"Item {items_read + 1}: {df.iloc[i].to_dict()}")
                        items_read += 1
                        
                elif file_format.lower() == 'json':
                    content_str = content.decode('utf-8')
                    
                    # Try to parse as JSON lines or regular JSON
                    try:
                        # Try JSON lines format first
                        lines = content_str.strip().split('\n')
                        for line in lines:
                            if items_read >= target_items:
                                break
                            if line.strip():
                                item = json.loads(line.strip())
                                print(f"Item {items_read + 1}: {item}")
                                items_read += 1
                    except json.JSONDecodeError:
                        # Try regular JSON format
                        data = json.loads(content_str)
                        if isinstance(data, list):
                            for item in data:
                                if items_read >= target_items:
                                    break
                                print(f"Item {items_read + 1}: {item}")
                                items_read += 1
                        else:
                            print(f"Item {items_read + 1}: {data}")
                            items_read += 1
                            
                elif file_format.lower() == 'txt':
                    content_str = content.decode('utf-8')
                    lines = content_str.strip().split('\n')
                    
                    for line in lines:
                        if items_read >= target_items:
                            break
                        if line.strip():
                            print(f"Item {items_read + 1}: {line.strip()}")
                            items_read += 1
                            
                else:
                    # For other formats, treat as raw content
                    print(f"Item {items_read + 1}: {content[:500]}...")  # Show first 500 chars
                    items_read += 1
                    
            except Exception as e:
                print(f"Error reading file {blob.name}: {str(e)}")
                continue
        
        print(f"\nSuccessfully read {items_read} items from Google Cloud Storage bucket")
        
    except KeyError as e:
        print(f"Missing required environment variable: {e}")
    except json.JSONDecodeError:
        print("Invalid JSON format in credentials")
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {str(e)}")

if __name__ == "__main__":
    main()