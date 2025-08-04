# DEPENDENCIES:
# pip install google-cloud-storage
# pip install pandas
# END_DEPENDENCIES 

import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from io import StringIO
import sys

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read sample data."""
    
    try:
        # Get environment variables
        bucket_name = os.environ.get('GS_BUCKET')
        project_id = os.environ.get('GS_PROJECT_ID')
        folder_path = os.environ.get('GS_FOLDER_PATH', '/')
        file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
        secret_key_name = os.environ.get('GS_SECRET_KEY')
        
        print(f"Connecting to GCS bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        
        # Get credentials from secret
        if not secret_key_name:
            raise ValueError("GS_SECRET_KEY environment variable not set")
            
        # Read credentials from the secret (assuming it's already loaded as JSON string)
        credentials_json = os.environ.get(secret_key_name)
        if not credentials_json:
            raise ValueError(f"Secret '{secret_key_name}' not found in environment")
        
        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in credentials: {e}")
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize GCS client
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Get bucket
        bucket = client.bucket(bucket_name)
        
        print(f"Successfully connected to bucket: {bucket_name}")
        
        # Clean up folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        # List files in the specified folder
        blobs = list(bucket.list_blobs(prefix=folder_path))
        
        # Filter files by format
        data_files = []
        for blob in blobs:
            if blob.name.lower().endswith(f'.{file_format.lower()}'):
                data_files.append(blob)
        
        if not data_files:
            print(f"No {file_format} files found in folder: {folder_path}")
            print("Available files:")
            for blob in blobs[:10]:  # Show first 10 files
                print(f"  - {blob.name}")
            return
        
        print(f"Found {len(data_files)} {file_format} files")
        
        # Read data from the first file
        first_file = data_files[0]
        print(f"Reading from file: {first_file.name}")
        
        # Download file content
        file_content = first_file.download_as_text()
        
        # Parse based on file format
        sample_count = 0
        max_samples = 10
        
        if file_format.lower() == 'csv':
            # Parse CSV content
            df = pd.read_csv(StringIO(file_content))
            
            print(f"\nCSV file shape: {df.shape}")
            print("Column names:", list(df.columns))
            print("\nFirst 10 records:")
            print("-" * 50)
            
            for idx, row in df.head(max_samples).iterrows():
                print(f"Record {idx + 1}:")
                for col, value in row.items():
                    print(f"  {col}: {value}")
                print("-" * 30)
                sample_count += 1
                
        elif file_format.lower() == 'json':
            # Parse JSON content
            try:
                json_data = json.loads(file_content)
                
                # Handle different JSON structures
                if isinstance(json_data, list):
                    items = json_data[:max_samples]
                elif isinstance(json_data, dict):
                    # If it's a single object, treat it as one item
                    items = [json_data]
                else:
                    items = [json_data]
                
                print(f"\nJSON file contains {len(json_data) if isinstance(json_data, list) else 1} items")
                print("\nFirst 10 records:")
                print("-" * 50)
                
                for idx, item in enumerate(items):
                    print(f"Record {idx + 1}:")
                    print(json.dumps(item, indent=2))
                    print("-" * 30)
                    sample_count += 1
                    
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
                # Fallback: show raw content lines
                lines = file_content.split('\n')[:max_samples]
                for idx, line in enumerate(lines):
                    if line.strip():
                        print(f"Line {idx + 1}: {line[:200]}...")
                        sample_count += 1
                        
        else:
            # For other formats, show raw content lines
            print(f"\nShowing first 10 lines from {file_format} file:")
            print("-" * 50)
            
            lines = file_content.split('\n')
            for idx, line in enumerate(lines[:max_samples]):
                if line.strip():
                    print(f"Line {idx + 1}: {line[:200]}...")
                    sample_count += 1
        
        print(f"\nSuccessfully read {sample_count} sample records from GCS bucket")
        
    except Exception as e:
        print(f"Error testing GCS connection: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        sys.exit(1)

if __name__ == "__main__":
    test_gcs_connection()