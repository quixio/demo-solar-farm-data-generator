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
    """Test connection to Google Storage Bucket and read sample data."""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    bucket_name = os.getenv('GS_BUCKET')
    project_id = os.getenv('GS_PROJECT_ID')
    folder_path = os.getenv('GS_FOLDER_PATH', '/')
    file_format = os.getenv('GS_FILE_FORMAT', 'csv')
    file_compression = os.getenv('GS_COMPRESSION', 'none')
    secret_key = os.getenv('GS_SECRET_KEY')
    
    print(f"Connecting to Google Storage Bucket: {bucket_name}")
    print(f"Project ID: {project_id}")
    print(f"Folder path: {folder_path}")
    print(f"File format: {file_format}")
    print(f"File compression: {file_compression}")
    
    client = None
    
    try:
        # Get credentials from secret
        if not secret_key:
            raise ValueError("GS_SECRET_KEY environment variable not found")
        
        # Assume the secret contains the JSON credentials
        credentials_json = os.getenv(secret_key)
        if not credentials_json:
            raise ValueError(f"Credentials not found in secret: {secret_key}")
        
        # Parse credentials JSON
        credentials_dict = json.loads(credentials_json)
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize the Google Cloud Storage client
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        print(f"Successfully connected to bucket: {bucket_name}")
        
        # List files in the specified folder
        folder_prefix = folder_path.strip('/') + '/' if folder_path != '/' else ''
        blobs = list(bucket.list_blobs(prefix=folder_prefix))
        
        if not blobs:
            print(f"No files found in folder: {folder_path}")
            return
        
        # Filter files by format if specified
        if file_format != 'csv':
            filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format.lower()}')]
        else:
            filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith('.csv')]
        
        if not filtered_blobs:
            print(f"No {file_format} files found in folder: {folder_path}")
            return
        
        print(f"Found {len(filtered_blobs)} {file_format} files")
        
        # Read from the first available file
        target_blob = filtered_blobs[0]
        print(f"Reading from file: {target_blob.name}")
        
        # Download file content
        file_content = target_blob.download_as_text()
        
        # Parse CSV content
        if file_format.lower() == 'csv':
            from io import StringIO
            df = pd.read_csv(StringIO(file_content))
            
            # Get exactly 10 sample records (or fewer if file has less)
            sample_count = min(10, len(df))
            sample_data = df.head(sample_count)
            
            print(f"\n=== Reading {sample_count} sample records from {target_blob.name} ===")
            
            # Print each record
            for idx, row in sample_data.iterrows():
                print(f"\nRecord {idx + 1}:")
                for column, value in row.items():
                    print(f"  {column}: {value}")
                print("-" * 50)
            
            print(f"\nSuccessfully read {sample_count} records from Google Storage Bucket")
            
        else:
            # For non-CSV files, just show first 10 lines
            lines = file_content.split('\n')
            sample_count = min(10, len(lines))
            
            print(f"\n=== Reading {sample_count} sample lines from {target_blob.name} ===")
            
            for i, line in enumerate(lines[:sample_count]):
                if line.strip():  # Skip empty lines
                    print(f"Line {i + 1}: {line}")
            
            print(f"\nSuccessfully read {sample_count} lines from Google Storage Bucket")
    
    except Exception as e:
        print(f"Error connecting to Google Storage Bucket: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        raise
    
    finally:
        # Clean up connection (Google Cloud Storage client doesn't require explicit cleanup)
        if client:
            print("Connection cleanup completed")

if __name__ == "__main__":
    test_google_storage_connection()