# DEPENDENCIES:
# pip install google-cloud-storage
# pip install pandas
# END_DEPENDENCIES

import os
import json
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import io

def test_google_storage_connection():
    """Test connection to Google Storage Bucket and read sample data."""
    
    try:
        # Get environment variables
        bucket_name = os.getenv('GS_BUCKET')
        project_id = os.getenv('GS_PROJECT_ID')
        folder_path = os.getenv('GS_FOLDER_PATH', '/')
        file_format = os.getenv('GS_FILE_FORMAT', 'csv')
        secret_key_name = os.getenv('GS_SECRET_KEY')
        
        # Validate required environment variables
        if not bucket_name:
            raise ValueError("GS_BUCKET environment variable is required")
        if not project_id:
            raise ValueError("GS_PROJECT_ID environment variable is required")
        if not secret_key_name:
            raise ValueError("GS_SECRET_KEY environment variable is required")
        
        # Get credentials from environment variable (assumes JSON content is already loaded)
        credentials_json = os.getenv(secret_key_name)
        if not credentials_json:
            raise ValueError("Credentials not found in specified environment variable")
        
        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in credentials")
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials, project=project_id)
        
        print(f"Connecting to Google Storage Bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print("-" * 50)
        
        # Get bucket
        bucket = client.bucket(bucket_name)
        
        # Clean folder path
        prefix = folder_path.strip('/')
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        
        # List files in the bucket with the specified prefix
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            print("No files found in the specified bucket/folder")
            return
        
        # Filter files by format if specified
        if file_format:
            target_files = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format.lower()}')]
        else:
            target_files = blobs
        
        if not target_files:
            print(f"No {file_format} files found in the bucket/folder")
            return
        
        print(f"Found {len(target_files)} {file_format} files")
        
        # Read data from the first suitable file
        blob = target_files[0]
        print(f"Reading from file: {blob.name}")
        print(f"File size: {blob.size} bytes")
        print("-" * 50)
        
        # Download file content
        file_content = blob.download_as_text()
        
        # Process based on file format
        if file_format.lower() == 'csv':
            # Read CSV data
            df = pd.read_csv(io.StringIO(file_content))
            
            # Get exactly 10 sample records
            sample_size = min(10, len(df))
            sample_data = df.head(sample_size)
            
            print(f"Reading {sample_size} sample records from CSV:")
            print("=" * 60)
            
            for idx, row in sample_data.iterrows():
                print(f"Record {idx + 1}:")
                for col, value in row.items():
                    print(f"  {col}: {value}")
                print("-" * 40)
        
        else:
            # For non-CSV files, read first 10 lines
            lines = file_content.split('\n')
            sample_size = min(10, len([line for line in lines if line.strip()]))
            
            print(f"Reading {sample_size} sample lines from {file_format} file:")
            print("=" * 60)
            
            count = 0
            for i, line in enumerate(lines):
                if line.strip() and count < 10:
                    count += 1
                    print(f"Line {count}: {line}")
                    if count >= 10:
                        break
        
        print("=" * 60)
        print("Connection test completed successfully!")
        
    except Exception as e:
        print(f"Error testing Google Storage connection: {str(e)}")
        raise

if __name__ == "__main__":
    test_google_storage_connection()