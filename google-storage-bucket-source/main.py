# Cached connection test code for google storage bucket
# Generated on 2025-08-13 14:28:08
# Template: Unknown
# This is cached code - delete this file to force regeneration

# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

def main():
    # Load environment variables
    load_dotenv()
    
    try:
        # Get configuration from environment variables
        bucket_uri = os.environ['GOOGLE_BUCKET_URI']
        bucket_name = bucket_uri.replace('gs://', '')
        folder_path = os.environ['GOOGLE_FOLDER_PATH'].strip('/')
        file_format = os.environ['GOOGLE_FILE_FORMAT']
        service_account_key = os.environ['GOOGLE_SERVICE_ACCOUNT_KEY_KEY']
        
        # Parse service account credentials from environment variable
        if not service_account_key:
            raise ValueError("Service account credentials not found in environment variable")
        
        credentials_info = json.loads(service_account_key)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)
        
        print(f"Connected to Google Cloud Storage bucket: {bucket_name}")
        print(f"Looking for {file_format} files in folder: {folder_path}")
        print("-" * 50)
        
        # List blobs in the specified folder with the specified format
        prefix = folder_path + '/' if folder_path else ''
        blobs = bucket.list_blobs(prefix=prefix)
        
        # Filter blobs by file format
        matching_blobs = []
        for blob in blobs:
            if blob.name.endswith(f'.{file_format}') and not blob.name.endswith('/'):
                matching_blobs.append(blob)
        
        if not matching_blobs:
            print(f"No {file_format} files found in the specified folder")
            return
        
        print(f"Found {len(matching_blobs)} {file_format} file(s)")
        
        # Read and display up to 10 sample items
        items_read = 0
        max_items = 10
        
        for blob in matching_blobs:
            if items_read >= max_items:
                break
                
            print(f"\nReading from file: {blob.name}")
            print(f"File size: {blob.size} bytes")
            print(f"Last modified: {blob.updated}")
            
            try:
                # Download blob content
                content = blob.download_as_text()
                
                if file_format.lower() == 'csv':
                    # For CSV files, split by lines and show first few rows
                    lines = content.strip().split('\n')
                    for i, line in enumerate(lines[:min(5, len(lines))]):
                        if items_read >= max_items:
                            break
                        print(f"Item {items_read + 1}: {line}")
                        items_read += 1
                        
                elif file_format.lower() == 'json':
                    # For JSON files, try to parse and show objects
                    try:
                        # Try parsing as JSON array
                        data = json.loads(content)
                        if isinstance(data, list):
                            for item in data[:min(max_items - items_read, len(data))]:
                                print(f"Item {items_read + 1}: {json.dumps(item, indent=2)}")
                                items_read += 1
                        else:
                            # Single JSON object
                            print(f"Item {items_read + 1}: {json.dumps(data, indent=2)}")
                            items_read += 1
                    except json.JSONDecodeError:
                        # Try parsing as JSONL (newline-delimited JSON)
                        lines = content.strip().split('\n')
                        for line in lines[:min(max_items - items_read, len(lines))]:
                            try:
                                item = json.loads(line)
                                print(f"Item {items_read + 1}: {json.dumps(item, indent=2)}")
                                items_read += 1
                            except json.JSONDecodeError:
                                print(f"Item {items_read + 1}: {line}")
                                items_read += 1
                                
                elif file_format.lower() == 'txt':
                    # For text files, split by lines
                    lines = content.strip().split('\n')
                    for line in lines[:min(max_items - items_read, len(lines))]:
                        print(f"Item {items_read + 1}: {line}")
                        items_read += 1
                        
                else:
                    # For other formats, show raw content in chunks
                    chunk_size = 200  # Show first 200 characters per item
                    content_chunks = [content[i:i+chunk_size] for i in range(0, len(content), chunk_size)]
                    for chunk in content_chunks[:min(max_items - items_read, len(content_chunks))]:
                        print(f"Item {items_read + 1}: {chunk}")
                        items_read += 1
                        
            except Exception as e:
                print(f"Error reading file {blob.name}: {str(e)}")
                continue
        
        print(f"\n" + "-" * 50)
        print(f"Successfully read {items_read} items from Google Cloud Storage")
        
    except KeyError as e:
        print(f"Missing required environment variable: {e}")
    except json.JSONDecodeError:
        print("Invalid JSON format in service account credentials")
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {str(e)}")

if __name__ == "__main__":
    main()