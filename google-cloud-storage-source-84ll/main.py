import os
import json
import gzip
import csv
from io import StringIO, BytesIO
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
        folder_path = os.getenv('GCS_FOLDER_PATH', '/').strip('/')
        file_format = os.getenv('GCS_FILE_FORMAT', 'csv').lower()
        file_compression = os.getenv('GCS_FILE_COMPRESSION', 'gzip').lower()
        
        print(f"Connecting to GCS bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {file_compression}")
        print("-" * 50)
        
        # Initialize GCS client with service account credentials
        if os.path.exists(key_file_path):
            # If key_file_path is a file path
            credentials = service_account.Credentials.from_service_account_file(key_file_path)
        else:
            # If key_file_path contains JSON string (common in containerized environments)
            try:
                key_data = json.loads(os.getenv('GCLOUD_PK_JSON', key_file_path))
                credentials = service_account.Credentials.from_service_account_info(key_data)
            except json.JSONDecodeError:
                print(f"Error: Could not parse service account key from {key_file_path}")
                return
        
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        print(f"Successfully connected to bucket: {bucket_name}")
        
        # List files in the specified folder
        prefix = folder_path + '/' if folder_path and not folder_path.endswith('/') else folder_path
        blobs = list(bucket.list_blobs(prefix=prefix, max_results=50))
        
        if not blobs:
            print(f"No files found in folder: {folder_path}")
            return
        
        print(f"Found {len(blobs)} files in the bucket")
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if blob.name.endswith('.csv') or blob.name.endswith('.csv.gz'):
                target_files.append(blob)
        
        if not target_files:
            print(f"No {file_format} files found")
            return
        
        print(f"Found {len(target_files)} {file_format} files")
        
        # Read sample data from the first file
        sample_blob = target_files[0]
        print(f"\nReading sample data from: {sample_blob.name}")
        print(f"File size: {sample_blob.size} bytes")
        print("-" * 50)
        
        # Download file content
        file_content = sample_blob.download_as_bytes()
        
        # Handle compression
        if file_compression == 'gzip' or sample_blob.name.endswith('.gz'):
            try:
                file_content = gzip.decompress(file_content)
                print("Successfully decompressed gzip file")
            except Exception as e:
                print(f"Warning: Could not decompress file: {e}")
        
        # Convert bytes to string
        content_str = file_content.decode('utf-8')
        
        # Read CSV data
        if file_format == 'csv':
            csv_reader = csv.DictReader(StringIO(content_str))
            
            print("Sample records (up to 10):")
            print("=" * 60)
            
            count = 0
            for row in csv_reader:
                if count >= 10:
                    break
                
                print(f"Record {count + 1}:")
                for key, value in row.items():
                    print(f"  {key}: {value}")
                print("-" * 40)
                count += 1
            
            if count == 0:
                print("No records found in the CSV file")
            else:
                print(f"\nSuccessfully read {count} sample records")
        
        else:
            # For non-CSV files, just show first few lines
            lines = content_str.split('\n')[:10]
            print("Sample content (first 10 lines):")
            print("=" * 60)
            for i, line in enumerate(lines, 1):
                print(f"Line {i}: {line}")
        
    except Exception as e:
        print(f"Error connecting to GCS: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        
        # Additional debugging info
        if "credentials" in str(e).lower():
            print("\nCredential troubleshooting:")
            print(f"- Check if service account email is correct: {service_account_email}")
            print(f"- Verify key file path/content: {key_file_path}")
            print("- Ensure the service account has Storage Object Viewer permissions")
        
        if "bucket" in str(e).lower():
            print(f"\nBucket troubleshooting:")
            print(f"- Verify bucket name: {bucket_name}")
            print(f"- Check bucket region: {os.getenv('GCS_BUCKET_REGION', 'us')}")
            print("- Ensure bucket exists and is accessible")

if __name__ == "__main__":
    print("Testing Google Cloud Storage Connection")
    print("=" * 50)
    test_gcs_connection()