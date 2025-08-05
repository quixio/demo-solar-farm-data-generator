# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# pip install pandas
# END_DEPENDENCIES

import os
import json
import tempfile
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
import pandas as pd
import io

def main():
    # Load environment variables
    load_dotenv()
    
    try:
        # Get environment variables
        bucket_name = os.environ.get('GCS_BUCKET')
        folder_path = os.environ.get('GCS_FOLDER_PATH', '/')
        file_format = os.environ.get('GCS_FILE_FORMAT', 'csv')
        file_compression = os.environ.get('GCS_FILE_COMPRESSION', 'none')
        project_id = os.environ.get('GCP_PROJECT_ID')
        credentials_json = os.environ.get('GCP_CREDENTIALS_KEY')
        
        # Validate required environment variables
        if not bucket_name:
            raise ValueError("GCS_BUCKET environment variable is required")
        if not project_id:
            raise ValueError("GCP_PROJECT_ID environment variable is required")
        if not credentials_json:
            raise ValueError("GCP_CREDENTIALS_KEY environment variable is required")
        
        print(f"Connecting to Google Cloud Storage bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"File compression: {file_compression}")
        
        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in GCP_CREDENTIALS_KEY")
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize the Google Cloud Storage client
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        print(f"\nSuccessfully connected to bucket: {bucket_name}")
        
        # Clean folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        # List files in the bucket with the specified folder path and file format
        blobs = list(bucket.list_blobs(prefix=folder_path))
        
        # Filter files by format
        matching_files = []
        for blob in blobs:
            if blob.name.endswith(f'.{file_format}'):
                matching_files.append(blob)
        
        if not matching_files:
            print(f"No {file_format} files found in folder: {folder_path}")
            return
        
        print(f"\nFound {len(matching_files)} {file_format} files in the bucket")
        
        # Read sample data from the first file
        sample_blob = matching_files[0]
        print(f"\nReading sample data from: {sample_blob.name}")
        
        # Download the file content
        file_content = sample_blob.download_as_bytes()
        
        # Handle compression if needed
        if file_compression != 'none':
            if file_compression == 'gzip':
                import gzip
                file_content = gzip.decompress(file_content)
            elif file_compression == 'bz2':
                import bz2
                file_content = bz2.decompress(file_content)
            else:
                print(f"Warning: Unsupported compression format: {file_compression}")
        
        # Read data based on file format
        if file_format.lower() == 'csv':
            # Read CSV data
            df = pd.read_csv(io.BytesIO(file_content))
            
            # Get first 10 records
            sample_records = df.head(10)
            
            print(f"\nFirst 10 records from {sample_blob.name}:")
            print("=" * 80)
            
            for index, row in sample_records.iterrows():
                print(f"Record {index + 1}:")
                for column, value in row.items():
                    print(f"  {column}: {value}")
                print("-" * 40)
                
        elif file_format.lower() == 'json':
            # Read JSON data
            content_str = file_content.decode('utf-8')
            
            # Try to parse as JSON Lines format first
            try:
                lines = content_str.strip().split('\n')
                records = []
                for line in lines[:10]:  # Get first 10 lines
                    if line.strip():
                        records.append(json.loads(line))
                        
                print(f"\nFirst 10 records from {sample_blob.name}:")
                print("=" * 80)
                
                for i, record in enumerate(records):
                    print(f"Record {i + 1}:")
                    print(json.dumps(record, indent=2))
                    print("-" * 40)
                    
            except json.JSONDecodeError:
                # Try to parse as single JSON array
                try:
                    data = json.loads(content_str)
                    if isinstance(data, list):
                        records = data[:10]  # Get first 10 items
                    else:
                        records = [data]  # Single object
                        
                    print(f"\nFirst 10 records from {sample_blob.name}:")
                    print("=" * 80)
                    
                    for i, record in enumerate(records):
                        print(f"Record {i + 1}:")
                        print(json.dumps(record, indent=2))
                        print("-" * 40)
                        
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON file: {e}")
                    
        else:
            # For other formats, just show raw content (first 10 lines)
            content_str = file_content.decode('utf-8', errors='ignore')
            lines = content_str.split('\n')[:10]
            
            print(f"\nFirst 10 lines from {sample_blob.name}:")
            print("=" * 80)
            
            for i, line in enumerate(lines):
                print(f"Line {i + 1}: {line}")
        
        print(f"\nConnection test completed successfully!")
        print(f"Successfully read sample data from Google Cloud Storage bucket: {bucket_name}")
        
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {str(e)}")
        raise

if __name__ == "__main__":
    main()