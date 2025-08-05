# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import csv
from io import StringIO
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read 10 sample items"""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    bucket_name = os.environ.get('GCS_BUCKET')
    folder_path = os.environ.get('GCS_FOLDER_PATH', '/')
    file_format = os.environ.get('GCS_FILE_FORMAT', 'csv')
    project_id = os.environ.get('GCP_PROJECT_ID')
    credentials_json = os.environ.get('GCP_CREDENTIALS_KEY')
    
    if not all([bucket_name, project_id, credentials_json]):
        raise ValueError("Missing required environment variables")
    
    try:
        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in credentials")
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize the GCS client
        client = storage.Client(credentials=credentials, project=project_id)
        
        print(f"Successfully connected to Google Cloud Storage")
        print(f"Project ID: {project_id}")
        print(f"Bucket: {bucket_name}")
        print(f"Folder path: {folder_path}")
        print("=" * 50)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # Clean up folder path
        prefix = folder_path.strip('/') + '/' if folder_path and folder_path != '/' else ''
        
        # List blobs in the bucket with the specified prefix
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            print("No files found in the specified path")
            return
        
        # Filter files by format if specified
        if file_format and file_format != 'none':
            blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format.lower()}')]
        
        if not blobs:
            print(f"No {file_format} files found in the specified path")
            return
        
        print(f"Found {len(blobs)} file(s)")
        
        # Read from the first file and extract 10 items
        first_blob = blobs[0]
        print(f"Reading from file: {first_blob.name}")
        print("=" * 50)
        
        # Download the file content
        content = first_blob.download_as_text()
        
        items_read = 0
        
        if file_format.lower() == 'csv':
            # Handle CSV files
            csv_reader = csv.reader(StringIO(content))
            
            for i, row in enumerate(csv_reader):
                if items_read >= 10:
                    break
                print(f"Item {i + 1}: {row}")
                items_read += 1
                
        elif file_format.lower() == 'json':
            # Handle JSON files
            try:
                # Try to parse as JSON array
                data = json.loads(content)
                if isinstance(data, list):
                    for i, item in enumerate(data[:10]):
                        print(f"Item {i + 1}: {item}")
                        items_read += 1
                else:
                    # Single JSON object
                    print(f"Item 1: {data}")
                    items_read = 1
            except json.JSONDecodeError:
                # Handle JSONL (newline-delimited JSON)
                lines = content.strip().split('\n')
                for i, line in enumerate(lines[:10]):
                    try:
                        item = json.loads(line)
                        print(f"Item {i + 1}: {item}")
                        items_read += 1
                    except json.JSONDecodeError:
                        print(f"Item {i + 1}: {line}")
                        items_read += 1
        else:
            # Handle text files or other formats
            lines = content.strip().split('\n')
            for i, line in enumerate(lines[:10]):
                print(f"Item {i + 1}: {line}")
                items_read += 1
        
        print("=" * 50)
        print(f"Successfully read {items_read} items from Google Cloud Storage")
        
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {str(e)}")
        raise

if __name__ == "__main__":
    test_gcs_connection()