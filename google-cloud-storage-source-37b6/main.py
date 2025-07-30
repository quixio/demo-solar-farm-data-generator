# DEPENDENCIES:
# pip install google-cloud-storage
# pip install pandas
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from google.cloud import storage
import pandas as pd
import gzip
from io import StringIO, BytesIO
from dotenv import load_dotenv

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read sample data."""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    bucket_name = os.getenv('GCS_BUCKET_NAME', 'quix-workflow')
    project_id = os.getenv('GCS_PROJECT_ID', 'quix-testing-365012')
    key_file_path = os.getenv('GCS_KEY_FILE_PATH', 'GCLOUD_PK_JSON')
    folder_path = os.getenv('GCS_FOLDER_PATH', '/').strip('/')
    file_format = os.getenv('GCS_FILE_FORMAT', 'csv').lower()
    file_compression = os.getenv('GCS_FILE_COMPRESSION', 'gzip').lower()
    service_account_email = os.getenv('GCS_SERVICE_ACCOUNT_EMAIL')
    
    print(f"Testing GCS connection...")
    print(f"Project ID: {project_id}")
    print(f"Bucket: {bucket_name}")
    print(f"Folder: {folder_path}")
    print(f"File format: {file_format}")
    print(f"Compression: {file_compression}")
    print(f"Service account: {service_account_email}")
    print("-" * 50)
    
    client = None
    try:
        # Initialize GCS client
        if key_file_path and key_file_path != 'GCLOUD_PK_JSON':
            # Use service account key file
            client = storage.Client.from_service_account_json(key_file_path, project=project_id)
        else:
            # Use default credentials (environment variable GOOGLE_APPLICATION_CREDENTIALS)
            client = storage.Client(project=project_id)
        
        # Get bucket
        bucket = client.bucket(bucket_name)
        
        print(f"Successfully connected to GCS bucket: {bucket_name}")
        
        # List files in the specified folder
        prefix = folder_path + '/' if folder_path else ''
        blobs = list(client.list_blobs(bucket_name, prefix=prefix, max_results=50))
        
        if not blobs:
            print(f"No files found in bucket {bucket_name} with prefix '{prefix}'")
            return
        
        print(f"Found {len(blobs)} files in the bucket")
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if blob.name.endswith(f'.{file_format}') or \
               (file_compression == 'gzip' and blob.name.endswith(f'.{file_format}.gz')):
                target_files.append(blob)
        
        if not target_files:
            print(f"No {file_format} files found with compression: {file_compression}")
            print("Available files:")
            for blob in blobs[:10]:  # Show first 10 files
                print(f"  - {blob.name}")
            return
        
        print(f"Found {len(target_files)} {file_format} files")
        
        # Read first file and extract 10 sample records
        sample_file = target_files[0]
        print(f"\nReading sample data from: {sample_file.name}")
        print(f"File size: {sample_file.size} bytes")
        print(f"Created: {sample_file.time_created}")
        print("-" * 50)
        
        # Download file content
        file_content = sample_file.download_as_bytes()
        
        # Handle compression
        if file_compression == 'gzip' or sample_file.name.endswith('.gz'):
            try:
                file_content = gzip.decompress(file_content)
            except Exception as e:
                print(f"Warning: Failed to decompress file, trying as uncompressed: {e}")
        
        # Convert bytes to string
        content_str = file_content.decode('utf-8')
        
        # Parse based on file format
        if file_format == 'csv':
            # Read CSV data
            df = pd.read_csv(StringIO(content_str))
            print(f"CSV file contains {len(df)} rows and {len(df.columns)} columns")
            print(f"Columns: {list(df.columns)}")
            print("\nFirst 10 records:")
            
            for i, (index, row) in enumerate(df.head(10).iterrows()):
                print(f"\nRecord {i+1}:")
                for col, value in row.items():
                    print(f"  {col}: {value}")
                
        elif file_format == 'json':
            # Handle JSON files
            try:
                # Try to parse as JSON lines
                lines = content_str.strip().split('\n')
                json_records = []
                for line in lines[:10]:  # Get first 10 lines
                    if line.strip():
                        json_records.append(json.loads(line))
                
                print(f"JSON Lines file - showing first 10 records:")
                for i, record in enumerate(json_records):
                    print(f"\nRecord {i+1}:")
                    print(json.dumps(record, indent=2))
                    
            except json.JSONDecodeError:
                try:
                    # Try to parse as single JSON array
                    data = json.loads(content_str)
                    if isinstance(data, list):
                        print(f"JSON array with {len(data)} items - showing first 10:")
                        for i, item in enumerate(data[:10]):
                            print(f"\nRecord {i+1}:")
                            print(json.dumps(item, indent=2))
                    else:
                        print("Single JSON object:")
                        print(json.dumps(data, indent=2))
                except Exception as e:
                    print(f"Failed to parse JSON: {e}")
                    print("Raw content preview (first 500 chars):")
                    print(content_str[:500])
        else:
            # Handle other formats as text
            lines = content_str.split('\n')
            print(f"Text file with {len(lines)} lines - showing first 10:")
            for i, line in enumerate(lines[:10]):
                if line.strip():
                    print(f"Line {i+1}: {line}")
        
        print("-" * 50)
        print("Connection test completed successfully!")
        
    except Exception as e:
        print(f"Error during GCS connection test: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        
        # Additional troubleshooting info
        if "could not find default credentials" in str(e).lower():
            print("\nTroubleshooting tips:")
            print("1. Set GOOGLE_APPLICATION_CREDENTIALS environment variable to your service account key file")
            print("2. Or run 'gcloud auth application-default login'")
            print("3. Or ensure your service account key file path is correct")
        elif "access denied" in str(e).lower() or "forbidden" in str(e).lower():
            print("\nTroubleshooting tips:")
            print("1. Check if your service account has proper permissions (Storage Object Viewer/Reader)")
            print("2. Verify the bucket name and project ID are correct")
            print("3. Ensure the service account has access to the specified bucket")
    
    finally:
        if client:
            # GCS client doesn't need explicit cleanup
            pass

if __name__ == "__main__":
    test_gcs_connection()