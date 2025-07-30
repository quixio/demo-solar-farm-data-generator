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
import gzip
import io
from typing import List, Any

def create_gcs_client():
    """Create and return a Google Cloud Storage client using service account credentials."""
    try:
        # Get the service account key from environment variable
        key_file_path = os.getenv('GCS_KEY_FILE_PATH')
        project_id = os.getenv('GCS_PROJECT_ID')
        
        if not key_file_path or not project_id:
            raise ValueError("GCS_KEY_FILE_PATH and GCS_PROJECT_ID environment variables are required")
        
        # Check if key_file_path is a JSON string or file path
        try:
            # Try to parse as JSON string first
            credentials_info = json.loads(key_file_path)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
        except json.JSONDecodeError:
            # If not JSON, treat as file path
            credentials = service_account.Credentials.from_service_account_file(key_file_path)
        
        client = storage.Client(credentials=credentials, project=project_id)
        return client
    except Exception as e:
        print(f"Error creating GCS client: {e}")
        raise

def list_files_in_bucket(client: storage.Client, bucket_name: str, folder_path: str, file_format: str) -> List[str]:
    """List files in the GCS bucket matching the specified criteria."""
    try:
        bucket = client.bucket(bucket_name)
        
        # Normalize folder path
        prefix = folder_path.strip('/')
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        
        # List blobs with the specified prefix
        blobs = bucket.list_blobs(prefix=prefix if prefix != '/' else None)
        
        # Filter files by format
        file_list = []
        for blob in blobs:
            if blob.name.endswith(f'.{file_format}') or blob.name.endswith(f'.{file_format}.gz'):
                file_list.append(blob.name)
        
        return file_list
    except Exception as e:
        print(f"Error listing files in bucket: {e}")
        raise

def read_file_from_gcs(client: storage.Client, bucket_name: str, file_name: str, compression: str) -> pd.DataFrame:
    """Read a file from GCS and return as pandas DataFrame."""
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        # Download file content
        content = blob.download_as_bytes()
        
        # Handle compression
        if compression == 'gzip' and file_name.endswith('.gz'):
            content = gzip.decompress(content)
        
        # Convert to DataFrame
        if file_name.endswith('.csv') or file_name.endswith('.csv.gz'):
            df = pd.read_csv(io.BytesIO(content))
        else:
            # For other formats, try to read as CSV by default
            df = pd.read_csv(io.BytesIO(content))
        
        return df
    except Exception as e:
        print(f"Error reading file {file_name}: {e}")
        raise

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read sample data."""
    try:
        # Get environment variables
        bucket_name = os.getenv('GCS_BUCKET_NAME')
        bucket_region = os.getenv('GCS_BUCKET_REGION')
        folder_path = os.getenv('GCS_FOLDER_PATH', '/')
        file_format = os.getenv('GCS_FILE_FORMAT', 'csv')
        compression = os.getenv('GCS_FILE_COMPRESSION', 'none')
        
        print("=== Google Cloud Storage Connection Test ===")
        print(f"Bucket: {bucket_name}")
        print(f"Region: {bucket_region}")
        print(f"Folder Path: {folder_path}")
        print(f"File Format: {file_format}")
        print(f"Compression: {compression}")
        print("=" * 50)
        
        # Create GCS client
        print("Creating GCS client...")
        client = create_gcs_client()
        print("✓ GCS client created successfully")
        
        # List files in bucket
        print(f"\nListing files in bucket '{bucket_name}'...")
        files = list_files_in_bucket(client, bucket_name, folder_path, file_format)
        
        if not files:
            print(f"No {file_format} files found in {folder_path}")
            return
        
        print(f"Found {len(files)} files:")
        for i, file_name in enumerate(files[:5]):  # Show first 5 files
            print(f"  {i+1}. {file_name}")
        
        # Read sample data from the first file
        sample_file = files[0]
        print(f"\nReading sample data from: {sample_file}")
        
        df = read_file_from_gcs(client, bucket_name, sample_file, compression)
        
        print(f"File shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        # Print exactly 10 sample records
        sample_count = min(10, len(df))
        print(f"\n=== First {sample_count} Records ===")
        
        for i in range(sample_count):
            print(f"\nRecord {i+1}:")
            for col in df.columns:
                value = df.iloc[i][col]
                print(f"  {col}: {value}")
            print("-" * 30)
        
        print(f"\n✓ Successfully read {sample_count} records from GCS")
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        raise
    finally:
        print("\n=== Connection test completed ===")

if __name__ == "__main__":
    test_gcs_connection()