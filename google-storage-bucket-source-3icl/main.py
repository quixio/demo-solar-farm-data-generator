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
    """Test connection to Google Cloud Storage and read sample data."""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    bucket_name = os.getenv('GS_BUCKET')
    project_id = os.getenv('GS_PROJECT_ID')
    folder_path = os.getenv('GS_FOLDER_PATH', '/')
    file_format = os.getenv('GS_FILE_FORMAT', 'csv')
    file_compression = os.getenv('GS_FILE_COMPRESSION', 'none')
    secret_key_name = os.getenv('GS_SECRET_KEY')
    
    # Validate required environment variables
    if not bucket_name:
        raise ValueError("GS_BUCKET environment variable is required")
    if not project_id:
        raise ValueError("GS_PROJECT_ID environment variable is required")
    if not secret_key_name:
        raise ValueError("GS_SECRET_KEY environment variable is required")
    
    print(f"Connecting to Google Cloud Storage...")
    print(f"Project ID: {project_id}")
    print(f"Bucket: {bucket_name}")
    print(f"Folder Path: {folder_path}")
    print(f"File Format: {file_format}")
    print(f"File Compression: {file_compression}")
    
    client = None
    
    try:
        # Get credentials from the secret
        credentials_json = os.getenv(secret_key_name)
        if not credentials_json:
            raise ValueError(f"Credentials not found in environment variable: {secret_key_name}")
        
        # Parse the JSON credentials
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Create the storage client
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # Check if bucket exists and is accessible
        if not bucket.exists():
            raise ValueError(f"Bucket '{bucket_name}' does not exist or is not accessible")
        
        print(f"Successfully connected to bucket: {bucket_name}")
        
        # Clean up folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        # List files in the bucket with the specified folder path and format
        blobs = list(bucket.list_blobs(prefix=folder_path))
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if blob.name.endswith(f'.{file_format}'):
                # Handle compression
                if file_compression == 'none':
                    target_files.append(blob)
                elif blob.name.endswith(f'.{file_format}.{file_compression}'):
                    target_files.append(blob)
        
        if not target_files:
            print(f"No {file_format} files found in bucket {bucket_name} with path {folder_path}")
            return
        
        print(f"Found {len(target_files)} {file_format} files in the bucket")
        
        # Read sample data from the first file
        sample_blob = target_files[0]
        print(f"Reading sample data from: {sample_blob.name}")
        
        # Download the file content
        file_content = sample_blob.download_as_text()
        
        # Process based on file format
        if file_format.lower() == 'csv':
            # Use pandas to read CSV
            from io import StringIO
            df = pd.read_csv(StringIO(file_content))
            
            # Get exactly 10 sample records
            sample_size = min(10, len(df))
            sample_data = df.head(sample_size)
            
            print(f"\nSample data from {sample_blob.name}:")
            print(f"Total rows in file: {len(df)}")
            print(f"Showing first {sample_size} records:")
            print("-" * 80)
            
            for idx, row in sample_data.iterrows():
                print(f"Record {idx + 1}:")
                for col, value in row.items():
                    print(f"  {col}: {value}")
                print("-" * 40)
        
        else:
            # For other formats, just show raw content lines
            lines = file_content.split('\n')
            sample_size = min(10, len([line for line in lines if line.strip()]))
            
            print(f"\nSample data from {sample_blob.name}:")
            print(f"Total lines in file: {len(lines)}")
            print(f"Showing first {sample_size} non-empty lines:")
            print("-" * 80)
            
            count = 0
            for i, line in enumerate(lines):
                if line.strip() and count < 10:
                    count += 1
                    print(f"Line {i + 1}: {line}")
                    print("-" * 40)
                if count >= 10:
                    break
        
        print("\nConnection test completed successfully!")
        
    except json.JSONDecodeError as e:
        print(f"Error parsing credentials JSON: {e}")
        raise
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {e}")
        raise
    finally:
        # Cleanup is handled automatically by the Google Cloud Storage client
        print("Connection cleanup completed.")

if __name__ == "__main__":
    test_google_storage_connection()