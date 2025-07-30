# DEPENDENCIES:
# pip install google-cloud-storage
# pip install pandas
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import gzip
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read sample data."""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    bucket_name = os.getenv('GCS_BUCKET_NAME', 'quix-workflow')
    project_id = os.getenv('GCS_PROJECT_ID', 'quix-testing-365012')
    service_account_email = os.getenv('GCS_SERVICE_ACCOUNT_EMAIL')
    key_file_path = os.getenv('GCS_KEY_FILE_PATH')
    folder_path = os.getenv('GCS_FOLDER_PATH', '/')
    file_format = os.getenv('GCS_FILE_FORMAT', 'csv')
    file_compression = os.getenv('GCS_FILE_COMPRESSION', 'gzip')
    
    client = None
    
    try:
        print("=" * 60)
        print("GOOGLE CLOUD STORAGE CONNECTION TEST")
        print("=" * 60)
        print(f"Project ID: {project_id}")
        print(f"Bucket Name: {bucket_name}")
        print(f"Service Account: {service_account_email}")
        print(f"Folder Path: {folder_path}")
        print(f"File Format: {file_format}")
        print(f"File Compression: {file_compression}")
        print("-" * 60)
        
        # Initialize GCS client with service account credentials
        if key_file_path and os.path.exists(key_file_path):
            print(f"Using service account key file: {key_file_path}")
            credentials = service_account.Credentials.from_service_account_file(key_file_path)
            client = storage.Client(credentials=credentials, project=project_id)
        else:
            print("Using default credentials or environment-based authentication")
            client = storage.Client(project=project_id)
        
        # Get bucket
        print(f"\nConnecting to bucket: {bucket_name}")
        bucket = client.bucket(bucket_name)
        
        # Check if bucket exists and is accessible
        if not bucket.exists():
            raise Exception(f"Bucket '{bucket_name}' does not exist or is not accessible")
        
        print(f"✓ Successfully connected to bucket: {bucket_name}")
        
        # List files in the specified folder
        folder_prefix = folder_path.strip('/')
        if folder_prefix and not folder_prefix.endswith('/'):
            folder_prefix += '/'
        
        print(f"\nLooking for {file_format} files in folder: {folder_path}")
        
        # Get list of files matching the criteria
        blobs = list(bucket.list_blobs(prefix=folder_prefix if folder_prefix != '/' else None))
        
        # Filter files by format
        matching_files = []
        for blob in blobs:
            blob_name = blob.name.lower()
            if file_format.lower() in blob_name:
                if file_compression == 'gzip' and blob_name.endswith('.gz'):
                    matching_files.append(blob)
                elif file_compression != 'gzip' and not blob_name.endswith('.gz'):
                    matching_files.append(blob)
        
        if not matching_files:
            print(f"⚠ No {file_format} files found in the specified location")
            print("Available files:")
            for blob in blobs[:10]:  # Show first 10 files
                print(f"  - {blob.name}")
            return
        
        print(f"Found {len(matching_files)} matching files")
        
        # Read from the first matching file
        target_blob = matching_files[0]
        print(f"\nReading from file: {target_blob.name}")
        print(f"File size: {target_blob.size} bytes")
        print(f"Last modified: {target_blob.time_created}")
        
        # Download and read the file content
        file_content = target_blob.download_as_bytes()
        
        # Handle compression
        if file_compression == 'gzip':
            print("Decompressing gzip file...")
            file_content = gzip.decompress(file_content)
        
        # Convert to string for processing
        content_str = file_content.decode('utf-8')
        
        print("\n" + "=" * 60)
        print("SAMPLE DATA (First 10 records)")
        print("=" * 60)
        
        # Process based on file format
        if file_format.lower() == 'csv':
            # Use pandas to read CSV data
            from io import StringIO
            csv_data = pd.read_csv(StringIO(content_str))
            
            print(f"CSV Shape: {csv_data.shape} (rows, columns)")
            print(f"Columns: {list(csv_data.columns)}")
            print("-" * 60)
            
            # Print first 10 records
            sample_records = csv_data.head(10)
            for idx, row in sample_records.iterrows():
                print(f"Record {idx + 1}:")
                for col in csv_data.columns:
                    print(f"  {col}: {row[col]}")
                print("-" * 40)
                
        else:
            # For other formats, show raw content lines
            lines = content_str.strip().split('\n')
            print(f"Total lines in file: {len(lines)}")
            print("-" * 60)
            
            for i, line in enumerate(lines[:10], 1):
                print(f"Record {i}:")
                print(f"  Content: {line[:200]}{'...' if len(line) > 200 else ''}")
                print("-" * 40)
        
        print("\n✓ Successfully read sample data from Google Cloud Storage")
        
    except Exception as e:
        print(f"\n❌ Error connecting to Google Cloud Storage:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        
        # Provide troubleshooting hints
        print("\nTroubleshooting hints:")
        print("1. Verify the service account key file exists and is valid")
        print("2. Check that the service account has Storage Object Viewer permissions")
        print("3. Ensure the bucket name and project ID are correct")
        print("4. Verify the folder path and file format settings")
        
    finally:
        if client:
            print("\nConnection cleanup completed.")

if __name__ == "__main__":
    test_gcs_connection()