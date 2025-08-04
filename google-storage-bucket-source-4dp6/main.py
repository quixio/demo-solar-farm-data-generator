# DEPENDENCIES:
# pip install google-cloud-storage
# pip install pandas
# END_DEPENDENCIES

import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
import io

def test_google_storage_connection():
    """Test connection to Google Storage Bucket and read 10 sample records"""
    
    try:
        # Get environment variables
        bucket_name = os.getenv('GS_BUCKET')
        project_id = os.getenv('GS_PROJECT_ID')
        folder_path = os.getenv('GS_FOLDER_PATH', '/')
        file_format = os.getenv('GS_FILE_FORMAT', 'csv')
        compression = os.getenv('GS_FILE_COMPRESSION', 'none')
        credentials_json = os.getenv('GS_SECRET_KEY')
        
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
        print(f"Compression: {compression}")
        
        # Parse credentials JSON
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize storage client
        client = storage.Client(credentials=credentials, project=project_id)
        bucket = client.bucket(bucket_name)
        
        print(f"\nSuccessfully connected to bucket: {bucket_name}")
        
        # Clean folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        # List files in the bucket/folder
        blobs = list(bucket.list_blobs(prefix=folder_path))
        
        if not blobs:
            print(f"No files found in folder: {folder_path}")
            return
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if blob.name.endswith(f'.{file_format}'):
                target_files.append(blob)
        
        if not target_files:
            print(f"No {file_format} files found in folder: {folder_path}")
            return
        
        print(f"\nFound {len(target_files)} {file_format} files")
        
        # Read from the first file
        target_blob = target_files[0]
        print(f"\nReading from file: {target_blob.name}")
        
        # Download file content
        file_content = target_blob.download_as_bytes()
        
        # Handle compression
        if compression == 'gzip':
            import gzip
            file_content = gzip.decompress(file_content)
        
        # Read data based on file format
        if file_format.lower() == 'csv':
            df = pd.read_csv(io.BytesIO(file_content))
        elif file_format.lower() == 'json':
            data = json.loads(file_content.decode('utf-8'))
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = pd.DataFrame([data])
        elif file_format.lower() in ['xlsx', 'xls']:
            df = pd.read_excel(io.BytesIO(file_content))
        else:
            # For other formats, try to read as CSV
            df = pd.read_csv(io.BytesIO(file_content))
        
        print(f"\nFile contains {len(df)} total records")
        print(f"Columns: {list(df.columns)}")
        
        # Get exactly 10 sample records
        sample_size = min(10, len(df))
        sample_df = df.head(sample_size)
        
        print(f"\n=== Reading {sample_size} sample records ===")
        
        for i, (index, row) in enumerate(sample_df.iterrows(), 1):
            print(f"\n--- Record {i} ---")
            record_dict = row.to_dict()
            for key, value in record_dict.items():
                print(f"{key}: {value}")
        
        print(f"\n✅ Successfully read {sample_size} records from Google Storage Bucket")
        
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing credentials JSON: {str(e)}")
    except Exception as e:
        print(f"❌ Error connecting to Google Storage: {str(e)}")
    
    finally:
        print("\n🔄 Connection test completed")

if __name__ == "__main__":
    test_google_storage_connection()