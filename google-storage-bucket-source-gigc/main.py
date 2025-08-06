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

load_dotenv()

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read 10 sample items."""
    
    try:
        # Get credentials from environment variable
        credentials_json = os.environ.get('GCP_CREDENTIALS_KEY')
        if not credentials_json:
            raise ValueError("GCP_CREDENTIALS_KEY environment variable is not set")
        
        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON in GCP_CREDENTIALS_KEY") from e
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize GCS client
        client = storage.Client(
            credentials=credentials,
            project=os.environ.get('GCP_PROJECT_ID')
        )
        
        # Get bucket
        bucket_name = os.environ.get('GCS_BUCKET')
        if not bucket_name:
            raise ValueError("GCS_BUCKET environment variable is not set")
        
        bucket = client.bucket(bucket_name)
        
        # Get folder path and file format
        folder_path = os.environ.get('GCS_FOLDER_PATH', '/').strip('/')
        file_format = os.environ.get('GCS_FILE_FORMAT', 'csv').lower()
        
        print(f"Connected to GCS bucket: {bucket_name}")
        print(f"Looking for {file_format} files in folder: {folder_path}")
        print("-" * 50)
        
        # List files in the specified folder
        prefix = folder_path + '/' if folder_path else ''
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if blob.name.lower().endswith(f'.{file_format}'):
                target_files.append(blob)
        
        if not target_files:
            print(f"No {file_format} files found in the specified folder")
            return
        
        print(f"Found {len(target_files)} {file_format} files")
        
        # Read from the first file
        blob = target_files[0]
        print(f"Reading from file: {blob.name}")
        print("-" * 50)
        
        # Download file content
        content = blob.download_as_text()
        
        # Process based on file format
        items_read = 0
        max_items = 10
        
        if file_format == 'csv':
            # Parse CSV content
            csv_reader = csv.DictReader(StringIO(content))
            
            for i, row in enumerate(csv_reader):
                if items_read >= max_items:
                    break
                print(f"Item {items_read + 1}: {dict(row)}")
                items_read += 1
                
        elif file_format == 'json':
            # Parse JSON content
            try:
                # Try to parse as JSON Lines format first
                lines = content.strip().split('\n')
                for line in lines:
                    if items_read >= max_items:
                        break
                    if line.strip():
                        try:
                            item = json.loads(line)
                            print(f"Item {items_read + 1}: {item}")
                            items_read += 1
                        except json.JSONDecodeError:
                            continue
                
                # If no items were read, try parsing as a single JSON array
                if items_read == 0:
                    data = json.loads(content)
                    if isinstance(data, list):
                        for item in data[:max_items]:
                            print(f"Item {items_read + 1}: {item}")
                            items_read += 1
                    else:
                        print(f"Item 1: {data}")
                        items_read = 1
                        
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
                return
                
        elif file_format == 'txt':
            # Parse text file line by line
            lines = content.strip().split('\n')
            for line in lines[:max_items]:
                if line.strip():
                    print(f"Item {items_read + 1}: {line.strip()}")
                    items_read += 1
        else:
            # For other formats, just show raw content in chunks
            lines = content.strip().split('\n')
            for line in lines[:max_items]:
                print(f"Item {items_read + 1}: {line}")
                items_read += 1
        
        print("-" * 50)
        print(f"Successfully read {items_read} items from GCS bucket")
        
    except Exception as e:
        print(f"Error connecting to GCS: {str(e)}")
        raise

if __name__ == "__main__":
    test_gcs_connection()