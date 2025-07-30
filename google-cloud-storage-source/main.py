import os
import json
import gzip
import csv
from io import StringIO
from google.cloud import storage
from google.oauth2 import service_account

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read sample data."""
    
    try:
        # Get environment variables
        bucket_name = os.getenv('GCS_BUCKET_NAME', 'quix-workflow')
        project_id = os.getenv('GCS_PROJECT_ID', 'quix-testing-365012')
        service_account_email = os.getenv('GCS_SERVICE_ACCOUNT_EMAIL')
        key_file_path = os.getenv('GCS_KEY_FILE_PATH', 'GCLOUD_PK_JSON')
        folder_path = os.getenv('GCS_FOLDER_PATH', '/')
        file_format = os.getenv('GCS_FILE_FORMAT', 'csv')
        file_compression = os.getenv('GCS_COMPRESSION', 'gzip')
        
        print(f"Connecting to GCS bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Service Account: {service_account_email}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {file_compression}")
        print("-" * 50)
        
        # Initialize GCS client with service account credentials
        if os.path.exists(key_file_path):
            credentials = service_account.Credentials.from_service_account_file(key_file_path)
        else:
            # Try to parse as JSON string (in case it's stored as env var content)
            try:
                key_data = json.loads(os.getenv('GCLOUD_PK_JSON', '{}'))
                credentials = service_account.Credentials.from_service_account_info(key_data)
            except json.JSONDecodeError:
                raise ValueError("Invalid service account key format")
        
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Get bucket
        bucket = client.bucket(bucket_name)
        
        # List files in the specified folder
        blobs = list(bucket.list_blobs(prefix=folder_path.lstrip('/')))
        
        if not blobs:
            print(f"No files found in folder: {folder_path}")
            return
        
        print(f"Found {len(blobs)} files in bucket")
        
        # Filter for the specified file format
        target_files = [blob for blob in blobs if blob.name.endswith(f'.{file_format}') or 
                       blob.name.endswith(f'.{file_format}.gz')]
        
        if not target_files:
            print(f"No {file_format} files found")
            return
        
        print(f"Found {len(target_files)} {file_format} files")
        
        # Read from the first available file
        target_blob = target_files[0]
        print(f"Reading from file: {target_blob.name}")
        print("-" * 50)
        
        # Download file content
        file_content = target_blob.download_as_bytes()
        
        # Handle compression
        if file_compression == 'gzip' or target_blob.name.endswith('.gz'):
            file_content = gzip.decompress(file_content)
        
        # Decode content
        content_str = file_content.decode('utf-8')
        
        # Parse based on file format
        if file_format.lower() == 'csv':
            csv_reader = csv.DictReader(StringIO(content_str))
            records = []
            
            for i, row in enumerate(csv_reader):
                if i >= 10:  # Read exactly 10 records
                    break
                records.append(row)
                print(f"Record {i + 1}: {row}")
            
            print(f"\nSuccessfully read {len(records)} records from GCS")
            
        elif file_format.lower() == 'json':
            # Handle JSON format
            lines = content_str.strip().split('\n')
            for i, line in enumerate(lines[:10]):  # Read exactly 10 lines
                try:
                    record = json.loads(line)
                    print(f"Record {i + 1}: {record}")
                except json.JSONDecodeError as e:
                    print(f"Record {i + 1}: Invalid JSON - {line[:100]}...")
            
            print(f"\nSuccessfully read {min(len(lines), 10)} records from GCS")
            
        else:
            # Handle plain text or other formats
            lines = content_str.strip().split('\n')
            for i, line in enumerate(lines[:10]):  # Read exactly 10 lines
                print(f"Record {i + 1}: {line}")
            
            print(f"\nSuccessfully read {min(len(lines), 10)} records from GCS")
        
    except Exception as e:
        print(f"Error connecting to GCS: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        return False
    
    return True

def list_bucket_contents():
    """Helper function to list all files in the bucket for debugging."""
    try:
        bucket_name = os.getenv('GCS_BUCKET_NAME', 'quix-workflow')
        project_id = os.getenv('GCS_PROJECT_ID', 'quix-testing-365012')
        key_file_path = os.getenv('GCS_KEY_FILE_PATH', 'GCLOUD_PK_JSON')
        
        # Initialize credentials
        if os.path.exists(key_file_path):
            credentials = service_account.Credentials.from_service_account_file(key_file_path)
        else:
            key_data = json.loads(os.getenv('GCLOUD_PK_JSON', '{}'))
            credentials = service_account.Credentials.from_service_account_info(key_data)
        
        client = storage.Client(credentials=credentials, project=project_id)
        bucket = client.bucket(bucket_name)
        
        print("All files in bucket:")
        for blob in bucket.list_blobs():
            print(f"  - {blob.name} (size: {blob.size} bytes)")
            
    except Exception as e:
        print(f"Error listing bucket contents: {str(e)}")

if __name__ == "__main__":
    print("Testing Google Cloud Storage connection...")
    print("=" * 60)
    
    # First, list all files in bucket for debugging
    list_bucket_contents()
    print("-" * 60)
    
    # Test the main connection
    success = test_gcs_connection()
    
    print("=" * 60)
    if success:
        print("✅ GCS connection test completed successfully!")
    else:
        print("❌ GCS connection test failed!")