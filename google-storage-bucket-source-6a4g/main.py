# DEPENDENCIES:
# pip install google-cloud-storage
# pip install pandas
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

def test_google_storage_connection():
    """Test connection to Google Cloud Storage and read 10 sample records."""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    bucket_name = os.getenv('GS_BUCKET')
    region = os.getenv('GS_REGION')
    secret_key_name = os.getenv('GS_SECRET_KEY')
    folder_path = os.getenv('GS_FOLDER_PATH', '/')
    file_format = os.getenv('GS_FILE_FORMAT', 'csv')
    file_compression = os.getenv('GS_FILE_COMPRESSION', 'none')
    project_id = os.getenv('GS_PROJECT_ID')
    
    print(f"Connecting to Google Cloud Storage...")
    print(f"Bucket: {bucket_name}")
    print(f"Region: {region}")
    print(f"Project ID: {project_id}")
    print(f"Folder Path: {folder_path}")
    print(f"File Format: {file_format}")
    print(f"File Compression: {file_compression}")
    print("-" * 50)
    
    client = None
    
    try:
        # Get credentials from secret
        credentials_json = os.getenv(secret_key_name)
        if not credentials_json:
            raise ValueError(f"Credentials not found in environment variable: {secret_key_name}")
        
        # Parse credentials JSON
        credentials_dict = json.loads(credentials_json)
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize the GCS client
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        print("✓ Successfully connected to Google Cloud Storage")
        
        # Clean up folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        # List blobs in the specified folder
        blobs = list(bucket.list_blobs(prefix=folder_path))
        
        # Filter blobs by file format
        filtered_blobs = []
        for blob in blobs:
            if blob.name.endswith(f'.{file_format}'):
                # Handle compression
                if file_compression == 'none':
                    filtered_blobs.append(blob)
                elif blob.name.endswith(f'.{file_compression}'):
                    filtered_blobs.append(blob)
        
        if not filtered_blobs:
            print(f"No {file_format} files found in folder: {folder_path}")
            return
        
        print(f"Found {len(filtered_blobs)} {file_format} files")
        
        # Read from the first file
        first_blob = filtered_blobs[0]
        print(f"Reading from file: {first_blob.name}")
        
        # Download the content
        content = first_blob.download_as_text()
        
        # Read the data based on format
        if file_format.lower() == 'csv':
            # Use pandas to read CSV from string
            from io import StringIO
            df = pd.read_csv(StringIO(content))
            
            # Get exactly 10 records (or fewer if file has less)
            sample_records = df.head(10)
            
            print(f"\nSample records from {first_blob.name}:")
            print("=" * 60)
            
            for idx, row in sample_records.iterrows():
                print(f"Record {idx + 1}:")
                for col, val in row.items():
                    print(f"  {col}: {val}")
                print("-" * 40)
                
        elif file_format.lower() == 'json':
            # Parse JSON content
            lines = content.strip().split('\n')
            records_printed = 0
            
            print(f"\nSample records from {first_blob.name}:")
            print("=" * 60)
            
            for line_num, line in enumerate(lines[:10], 1):
                try:
                    record = json.loads(line.strip())
                    print(f"Record {line_num}:")
                    for key, value in record.items():
                        print(f"  {key}: {value}")
                    print("-" * 40)
                    records_printed += 1
                except json.JSONDecodeError:
                    print(f"Warning: Could not parse line {line_num} as JSON")
                    continue
                    
        else:
            # For other formats, just print raw content lines
            lines = content.strip().split('\n')
            print(f"\nSample lines from {first_blob.name}:")
            print("=" * 60)
            
            for line_num, line in enumerate(lines[:10], 1):
                print(f"Line {line_num}: {line}")
                print("-" * 40)
        
        print("✓ Successfully read sample data from Google Cloud Storage")
        
    except Exception as e:
        print(f"✗ Error connecting to Google Cloud Storage: {str(e)}")
        raise
    
    finally:
        # Cleanup - GCS client doesn't need explicit cleanup
        if client:
            print("Connection cleanup completed")

if __name__ == "__main__":
    test_google_storage_connection()