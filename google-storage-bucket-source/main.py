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
    secret_key = os.getenv('GS_SECRET_KEY')
    
    print(f"Connecting to Google Cloud Storage...")
    print(f"Project ID: {project_id}")
    print(f"Bucket: {bucket_name}")
    print(f"Folder path: {folder_path}")
    print(f"File format: {file_format}")
    print("-" * 50)
    
    try:
        # Get credentials from secret
        if not secret_key:
            raise ValueError("GS_SECRET_KEY environment variable not set")
            
        # Load credentials from the secret (assuming it contains JSON credentials)
        credentials_json = os.getenv(secret_key)
        if not credentials_json:
            raise ValueError(f"Secret '{secret_key}' not found in environment variables")
        
        # Parse credentials JSON
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        print(f"Successfully connected to bucket: {bucket_name}")
        
        # Clean folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        # List blobs in the specified folder with the specified format
        prefix = folder_path if folder_path != '/' else ''
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        # Filter blobs by file format
        matching_blobs = []
        for blob in blobs:
            if blob.name.lower().endswith(f'.{file_format.lower()}'):
                matching_blobs.append(blob)
        
        if not matching_blobs:
            print(f"No {file_format} files found in folder: {folder_path}")
            return
        
        print(f"Found {len(matching_blobs)} {file_format} file(s)")
        
        # Read sample data from the first file
        first_blob = matching_blobs[0]
        print(f"Reading sample data from: {first_blob.name}")
        print("-" * 50)
        
        # Download blob content
        blob_content = first_blob.download_as_text()
        
        # Process based on file format
        sample_count = 0
        max_samples = 10
        
        if file_format.lower() == 'csv':
            # Use pandas to read CSV
            from io import StringIO
            df = pd.read_csv(StringIO(blob_content))
            
            print(f"CSV file has {len(df)} rows and {len(df.columns)} columns")
            print(f"Columns: {list(df.columns)}")
            print("-" * 50)
            
            # Print first 10 rows
            for idx, row in df.head(max_samples).iterrows():
                sample_count += 1
                print(f"Record {sample_count}:")
                for col in df.columns:
                    print(f"  {col}: {row[col]}")
                print()
                
        elif file_format.lower() == 'json':
            # Handle JSON files
            lines = blob_content.strip().split('\n')
            for line in lines[:max_samples]:
                if line.strip():
                    sample_count += 1
                    try:
                        json_data = json.loads(line)
                        print(f"Record {sample_count}:")
                        for key, value in json_data.items():
                            print(f"  {key}: {value}")
                        print()
                    except json.JSONDecodeError:
                        print(f"Record {sample_count}: {line}")
                        print()
        else:
            # Handle other text formats
            lines = blob_content.strip().split('\n')
            for line in lines[:max_samples]:
                if line.strip():
                    sample_count += 1
                    print(f"Record {sample_count}: {line}")
                    print()
        
        print(f"Successfully read {sample_count} sample records from Google Cloud Storage")
        
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {str(e)}")
        raise

if __name__ == "__main__":
    test_google_storage_connection()