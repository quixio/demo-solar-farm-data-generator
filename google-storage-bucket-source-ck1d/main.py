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
        bucket_name = os.environ['GCS_BUCKET']
        folder_path = os.environ['GCS_FOLDER_PATH'].strip('/')
        file_format = os.environ['GCS_FILE_FORMAT']
        project_id = os.environ['GCP_PROJECT_ID']
        credentials_json_str = os.environ['GCP_CREDENTIALS_KEY']
        
        print(f"Connecting to Google Cloud Storage bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        
        # Parse credentials JSON
        try:
            credentials_info = json.loads(credentials_json_str)
        except json.JSONDecodeError as e:
            print(f"Error parsing GCP credentials JSON: {e}")
            return
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        
        # Initialize the Google Cloud Storage client
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # List blobs in the specified folder
        if folder_path:
            prefix = folder_path + '/' if not folder_path.endswith('/') else folder_path
        else:
            prefix = None
            
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            print("No files found in the specified bucket/folder")
            return
        
        print(f"Found {len(blobs)} files in bucket")
        
        items_read = 0
        target_items = 10
        
        # Process files to read sample items
        for blob in blobs:
            if items_read >= target_items:
                break
                
            # Skip directories
            if blob.name.endswith('/'):
                continue
                
            # Filter by file format if specified
            if file_format != 'none' and not blob.name.lower().endswith(f'.{file_format.lower()}'):
                continue
            
            print(f"\nReading from file: {blob.name}")
            
            try:
                # Download blob content
                content = blob.download_as_bytes()
                
                # Process based on file format
                if file_format.lower() == 'csv':
                    # Process CSV file
                    content_str = content.decode('utf-8')
                    csv_reader = csv.DictReader(io.StringIO(content_str))
                    
                    for row in csv_reader:
                        if items_read >= target_items:
                            break
                        items_read += 1
                        print(f"Item {items_read}: {dict(row)}")
                        
                elif file_format.lower() == 'json':
                    # Process JSON file
                    try:
                        content_str = content.decode('utf-8')
                        json_data = json.loads(content_str)
                        
                        # Handle different JSON structures
                        if isinstance(json_data, list):
                            for item in json_data:
                                if items_read >= target_items:
                                    break
                                items_read += 1
                                print(f"Item {items_read}: {item}")
                        else:
                            items_read += 1
                            print(f"Item {items_read}: {json_data}")
                            
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON from {blob.name}: {e}")
                        
                else:
                    # Process as text file
                    try:
                        content_str = content.decode('utf-8')
                        lines = content_str.split('\n')
                        
                        for line in lines:
                            if items_read >= target_items:
                                break
                            if line.strip():  # Skip empty lines
                                items_read += 1
                                print(f"Item {items_read}: {line.strip()}")
                                
                    except UnicodeDecodeError:
                        print(f"Could not decode {blob.name} as text")
                        
            except Exception as e:
                print(f"Error reading file {blob.name}: {e}")
                continue
        
        if items_read == 0:
            print("No readable items found in the bucket")
        else:
            print(f"\nSuccessfully read {items_read} items from Google Cloud Storage")
            
    except KeyError as e:
        print(f"Missing required environment variable: {e}")
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {e}")

if __name__ == "__main__":
    main()