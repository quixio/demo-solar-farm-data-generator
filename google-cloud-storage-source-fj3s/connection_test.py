import os
import json
import gzip
import csv
from io import StringIO, TextIOWrapper
from google.cloud import storage
from google.oauth2 import service_account

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read sample data."""
    
    try:
        # Get environment variables
        bucket_name = os.getenv('GCS_BUCKET_NAME', 'quix-workflow')
        project_id = os.getenv('GCS_PROJECT_ID', 'quix-testing-365012')
        key_file_path = os.getenv('GCS_KEY_FILE_PATH', 'GCLOUD_PK_JSON')
        folder_path = os.getenv('GCS_FOLDER_PATH', '/')
        file_format = os.getenv('GCS_FILE_FORMAT', 'csv')
        file_compression = os.getenv('GCS_FILE_COMPRESSION', 'gzip')
        
        print(f"Connecting to GCS bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {file_compression}")
        print("-" * 50)
        
        # Initialize GCS client with service account credentials
        if key_file_path and os.path.exists(key_file_path):
            # Load credentials from JSON file
            credentials = service_account.Credentials.from_service_account_file(key_file_path)
            client = storage.Client(project=project_id, credentials=credentials)
        else:
            # Try using environment variable or default credentials
            try:
                # Check if credentials are in environment variable
                creds_json = os.getenv('GCLOUD_PK_JSON')
                if creds_json:
                    credentials_info = json.loads(creds_json)
                    credentials = service_account.Credentials.from_service_account_info(credentials_info)
                    client = storage.Client(project=project_id, credentials=credentials)
                else:
                    # Use default credentials
                    client = storage.Client(project=project_id)
            except Exception as cred_error:
                print(f"Error loading credentials: {cred_error}")
                return
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # List files in the specified folder
        folder_prefix = folder_path.strip('/') + '/' if folder_path != '/' else ''
        blobs = list(client.list_blobs(bucket, prefix=folder_prefix, max_results=50))
        
        if not blobs:
            print(f"No files found in bucket '{bucket_name}' with prefix '{folder_prefix}'")
            return
        
        print(f"Found {len(blobs)} files in the bucket")
        
        # Find files matching the specified format
        matching_files = []
        for blob in blobs:
            if not blob.name.endswith('/'):  # Skip directories
                if file_compression == 'gzip' and blob.name.endswith('.gz'):
                    matching_files.append(blob)
                elif file_compression != 'gzip' and not blob.name.endswith('.gz'):
                    matching_files.append(blob)
        
        if not matching_files:
            print(f"No files found matching format '{file_format}' with compression '{file_compression}'")
            return
        
        print(f"Found {len(matching_files)} matching files")
        
        # Read from the first matching file
        target_blob = matching_files[0]
        print(f"Reading from file: {target_blob.name}")
        print("-" * 50)
        
        # Download and read the file content
        file_content = target_blob.download_as_bytes()
        
        # Handle compression
        if file_compression == 'gzip':
            file_content = gzip.decompress(file_content)
        
        # Convert bytes to string
        content_str = file_content.decode('utf-8')
        
        # Read based on file format
        if file_format.lower() == 'csv':
            # Parse CSV content
            csv_reader = csv.DictReader(StringIO(content_str))
            
            print("Reading CSV data (first 10 rows):")
            print("=" * 60)
            
            for i, row in enumerate(csv_reader):
                if i >= 10:  # Stop after 10 items
                    break
                print(f"Row {i + 1}: {dict(row)}")
                
        else:
            # For other formats, just print lines
            lines = content_str.strip().split('\n')
            print(f"Reading {file_format.upper()} data (first 10 lines):")
            print("=" * 60)
            
            for i, line in enumerate(lines[:10]):
                print(f"Line {i + 1}: {line}")
        
        print("-" * 50)
        print("Connection test completed successfully!")
        
    except Exception as e:
        print(f"Error during GCS connection test: {e}")
        print(f"Error type: {type(e).__name__}")
        
        # Additional debugging information
        if "credentials" in str(e).lower():
            print("\nCredential troubleshooting:")
            print("- Check if GCS_KEY_FILE_PATH points to a valid JSON file")
            print("- Verify GCLOUD_PK_JSON environment variable contains valid JSON")
            print("- Ensure service account has proper permissions")
        
        if "bucket" in str(e).lower():
            print("\nBucket troubleshooting:")
            print("- Verify bucket name is correct")
            print("- Check if bucket exists and is accessible")
            print("- Ensure service account has Storage Object Viewer permission")

def main():
    """Main function to run the GCS connection test."""
    print("Starting Google Cloud Storage connection test...")
    print("=" * 60)
    
    test_gcs_connection()

if __name__ == "__main__":
    main()