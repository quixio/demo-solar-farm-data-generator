# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# pip install pandas
# END_DEPENDENCIES

import os
import json
import io
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
import pandas as pd

def test_google_storage_connection():
    """Test connection to Google Cloud Storage and read 10 sample records."""
    
    # Load environment variables
    load_dotenv()
    
    try:
        # Get credentials from environment variable
        credentials_json = os.environ.get('GOOGLE_STORAGE_SECRET_KEY')
        if not credentials_json:
            raise ValueError("Google Storage credentials not found in environment variables")
        
        # Parse credentials JSON
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Get configuration from environment variables
        bucket_name = os.environ.get('GOOGLE_STORAGE_BUCKET', 'quix-workflow')
        folder_path = os.environ.get('GOOGLE_STORAGE_FOLDER_PATH', '/')
        file_format = os.environ.get('GOOGLE_STORAGE_FILE_FORMAT', 'csv')
        compression = os.environ.get('GOOGLE_STORAGE_FILE_COMPRESSION', 'none')
        
        print(f"Connecting to Google Cloud Storage bucket: {bucket_name}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {compression}")
        
        # Initialize the client
        client = storage.Client(credentials=credentials)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # List files in the specified folder
        prefix = folder_path.lstrip('/') if folder_path != '/' else ''
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            print("No files found in the specified folder")
            return
        
        print(f"Found {len(blobs)} files in the bucket")
        
        # Find files matching the specified format
        matching_files = []
        for blob in blobs:
            if blob.name.lower().endswith(f'.{file_format.lower()}'):
                matching_files.append(blob)
        
        if not matching_files:
            print(f"No {file_format} files found in the bucket")
            return
        
        print(f"Found {len(matching_files)} {file_format} files")
        
        # Read from the first matching file
        target_blob = matching_files[0]
        print(f"Reading from file: {target_blob.name}")
        
        # Download the file content
        file_content = target_blob.download_as_bytes()
        
        # Handle compression if specified
        if compression.lower() == 'gzip':
            import gzip
            file_content = gzip.decompress(file_content)
        
        # Parse the file based on format
        if file_format.lower() == 'csv':
            # Read CSV file
            df = pd.read_csv(io.BytesIO(file_content))
            
            # Get first 10 records
            sample_records = df.head(10)
            
            print(f"\nFirst 10 records from {target_blob.name}:")
            print("=" * 50)
            
            for index, row in sample_records.iterrows():
                print(f"Record {index + 1}:")
                for column, value in row.items():
                    print(f"  {column}: {value}")
                print("-" * 30)
        
        elif file_format.lower() == 'json':
            # Read JSON file
            content_str = file_content.decode('utf-8')
            
            # Try to parse as JSON Lines or regular JSON
            try:
                # First try as regular JSON
                data = json.loads(content_str)
                if isinstance(data, list):
                    records = data[:10]
                else:
                    records = [data]
            except json.JSONDecodeError:
                # Try as JSON Lines
                lines = content_str.strip().split('\n')
                records = []
                for line in lines[:10]:
                    if line.strip():
                        records.append(json.loads(line))
            
            print(f"\nFirst 10 records from {target_blob.name}:")
            print("=" * 50)
            
            for i, record in enumerate(records):
                print(f"Record {i + 1}:")
                print(json.dumps(record, indent=2))
                print("-" * 30)
        
        else:
            # For other formats, just show raw content (first 1000 chars per record)
            content_str = file_content.decode('utf-8')
            lines = content_str.split('\n')[:10]
            
            print(f"\nFirst 10 lines from {target_blob.name}:")
            print("=" * 50)
            
            for i, line in enumerate(lines):
                if line.strip():
                    print(f"Record {i + 1}: {line[:1000]}")
                    print("-" * 30)
        
        print(f"\nSuccessfully read sample data from Google Cloud Storage bucket: {bucket_name}")
        
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {str(e)}")
        raise

if __name__ == "__main__":
    test_google_storage_connection()