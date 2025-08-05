# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# pip install pandas
# END_DEPENDENCIES

import os
import json
import io
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

def test_google_storage_connection():
    """Test connection to Google Storage Bucket and read 10 sample records."""
    
    # Load environment variables
    load_dotenv()
    
    try:
        # Get environment variables
        bucket_name = os.environ.get('GS_BUCKET')
        project_id = os.environ.get('GS_PROJECT_ID')
        credentials_json = os.environ.get('GS_SECRET_KEY')
        folder_path = os.environ.get('GS_FOLDER_PATH', '/')
        file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
        file_compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
        
        # Validate required environment variables
        if not bucket_name:
            raise ValueError("GS_BUCKET environment variable is required")
        if not project_id:
            raise ValueError("GS_PROJECT_ID environment variable is required")
        if not credentials_json:
            raise ValueError("GS_SECRET_KEY environment variable is required")
        
        print(f"Connecting to Google Storage Bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {file_compression}")
        
        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in credentials")
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # Clean folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        print(f"\nListing files in bucket '{bucket_name}' with prefix '{folder_path}'...")
        
        # List blobs with the specified prefix
        blobs = list(client.list_blobs(bucket, prefix=folder_path))
        
        if not blobs:
            print("No files found in the specified path")
            return
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if not blob.name.endswith('/'):  # Skip directories
                if file_format.lower() == 'csv' and blob.name.lower().endswith('.csv'):
                    target_files.append(blob)
                elif file_format.lower() == 'json' and blob.name.lower().endswith('.json'):
                    target_files.append(blob)
                elif file_format.lower() == 'txt' and blob.name.lower().endswith('.txt'):
                    target_files.append(blob)
        
        if not target_files:
            print(f"No {file_format} files found in the specified path")
            return
        
        print(f"Found {len(target_files)} {file_format} files")
        
        # Read from the first suitable file
        blob = target_files[0]
        print(f"\nReading from file: {blob.name}")
        print(f"File size: {blob.size} bytes")
        
        # Download file content
        file_content = blob.download_as_bytes()
        
        # Handle compression
        if file_compression.lower() == 'gzip':
            import gzip
            file_content = gzip.decompress(file_content)
        
        # Parse content based on format
        records_read = 0
        max_records = 10
        
        if file_format.lower() == 'csv':
            # Read CSV file
            df = pd.read_csv(io.BytesIO(file_content))
            print(f"\nCSV file has {len(df)} rows and {len(df.columns)} columns")
            print(f"Columns: {list(df.columns)}")
            print("\n--- Sample Records (first 10) ---")
            
            for idx, row in df.head(max_records).iterrows():
                print(f"Record {records_read + 1}:")
                for col in df.columns:
                    print(f"  {col}: {row[col]}")
                print("-" * 40)
                records_read += 1
                
        elif file_format.lower() == 'json':
            # Read JSON file
            content_str = file_content.decode('utf-8')
            try:
                # Try to parse as JSON array
                data = json.loads(content_str)
                if isinstance(data, list):
                    print(f"\nJSON file contains {len(data)} records")
                    print("\n--- Sample Records (first 10) ---")
                    for i, record in enumerate(data[:max_records]):
                        print(f"Record {i + 1}: {record}")
                        print("-" * 40)
                        records_read += 1
                else:
                    # Single JSON object
                    print("\n--- Single JSON Record ---")
                    print(f"Record 1: {data}")
                    records_read = 1
            except json.JSONDecodeError:
                # Try to parse as JSONL (one JSON per line)
                lines = content_str.strip().split('\n')
                print(f"\nJSONL file contains {len(lines)} lines")
                print("\n--- Sample Records (first 10) ---")
                for i, line in enumerate(lines[:max_records]):
                    try:
                        record = json.loads(line)
                        print(f"Record {i + 1}: {record}")
                        print("-" * 40)
                        records_read += 1
                    except json.JSONDecodeError:
                        print(f"Record {i + 1}: {line}")
                        print("-" * 40)
                        records_read += 1
                        
        elif file_format.lower() == 'txt':
            # Read text file
            content_str = file_content.decode('utf-8')
            lines = content_str.strip().split('\n')
            print(f"\nText file contains {len(lines)} lines")
            print("\n--- Sample Records (first 10) ---")
            for i, line in enumerate(lines[:max_records]):
                print(f"Record {i + 1}: {line}")
                print("-" * 40)
                records_read += 1
        
        print(f"\nSuccessfully read {records_read} sample records from Google Storage Bucket")
        
    except Exception as e:
        print(f"Error connecting to Google Storage Bucket: {str(e)}")
        raise

if __name__ == "__main__":
    test_google_storage_connection()