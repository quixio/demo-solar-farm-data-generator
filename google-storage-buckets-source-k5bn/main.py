# DEPENDENCIES:
# pip install google-cloud-storage
# pip install pandas
# END_DEPENDENCIES

import os
import json
from google.cloud import storage
import pandas as pd
from io import StringIO
import sys

def get_credentials():
    """Get GCP credentials from environment variable."""
    try:
        # Get the secret name from environment variable
        secret_name = os.getenv('GS_API_KEY')
        if not secret_name:
            raise ValueError("GS_API_KEY environment variable not set")
        
        # Read credentials from the secret (assuming it contains JSON)
        credentials_json = os.getenv(secret_name)
        if not credentials_json:
            raise ValueError(f"Credentials not found in environment variable: {secret_name}")
        
        # Parse JSON credentials
        credentials_dict = json.loads(credentials_json)
        return credentials_dict
    except Exception as e:
        print(f"Error getting credentials: {e}")
        raise

def connect_to_gcs():
    """Establish connection to Google Cloud Storage."""
    try:
        # Get credentials
        credentials_dict = get_credentials()
        
        # Create client using credentials dictionary
        client = storage.Client.from_service_account_info(credentials_dict)
        
        print("✓ Successfully connected to Google Cloud Storage")
        return client
    except Exception as e:
        print(f"✗ Failed to connect to Google Cloud Storage: {e}")
        raise

def list_and_read_files(client):
    """List files in bucket and read sample data."""
    try:
        bucket_name = os.getenv('GS_BUCKET')
        folder_path = os.getenv('GS_FOLDER_PATH', '/')
        file_format = os.getenv('GS_FILE_FORMAT', 'csv')
        
        if not bucket_name:
            raise ValueError("GS_BUCKET environment variable not set")
        
        # Get bucket
        bucket = client.bucket(bucket_name)
        print(f"✓ Connected to bucket: {bucket_name}")
        
        # Clean folder path
        prefix = folder_path.strip('/') + '/' if folder_path != '/' else ''
        
        # List files with the specified format
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if blob.name.lower().endswith(f'.{file_format.lower()}'):
                target_files.append(blob)
        
        if not target_files:
            print(f"✗ No {file_format} files found in bucket {bucket_name} with prefix '{prefix}'")
            return
        
        print(f"✓ Found {len(target_files)} {file_format} files")
        
        # Read sample data from the first file
        sample_blob = target_files[0]
        print(f"✓ Reading sample data from: {sample_blob.name}")
        
        # Download file content
        file_content = sample_blob.download_as_text()
        
        # Parse based on file format
        if file_format.lower() == 'csv':
            # Read CSV data
            df = pd.read_csv(StringIO(file_content))
            
            # Get first 10 rows
            sample_rows = df.head(10)
            
            print(f"\n=== SAMPLE DATA (First 10 records from {sample_blob.name}) ===")
            print(f"File size: {sample_blob.size} bytes")
            print(f"Total rows in sample: {len(sample_rows)}")
            print(f"Columns: {list(df.columns)}")
            print("\nData:")
            
            for idx, row in sample_rows.iterrows():
                print(f"Record {idx + 1}:")
                for col in df.columns:
                    print(f"  {col}: {row[col]}")
                print()
                
        else:
            # For non-CSV files, just show first 10 lines
            lines = file_content.split('\n')
            sample_lines = lines[:10]
            
            print(f"\n=== SAMPLE DATA (First 10 lines from {sample_blob.name}) ===")
            print(f"File size: {sample_blob.size} bytes")
            print(f"Total lines in file: {len(lines)}")
            print("\nContent:")
            
            for idx, line in enumerate(sample_lines, 1):
                if line.strip():  # Skip empty lines
                    print(f"Line {idx}: {line}")
                    
    except Exception as e:
        print(f"✗ Error reading files: {e}")
        raise

def main():
    """Main function to test Google Cloud Storage connection."""
    print("=== Google Cloud Storage Connection Test ===")
    print()
    
    try:
        # Display configuration
        print("Configuration:")
        print(f"  Bucket: {os.getenv('GS_BUCKET')}")
        print(f"  Region: {os.getenv('GS_REGION')}")
        print(f"  Project ID: {os.getenv('GS_PROJECT_ID')}")
        print(f"  Folder Path: {os.getenv('GS_FOLDER_PATH')}")
        print(f"  File Format: {os.getenv('GS_FILE_FORMAT')}")
        print(f"  Compression: {os.getenv('GS_FILE_COMPRESSION')}")
        print()
        
        # Connect to GCS
        client = connect_to_gcs()
        
        # List and read sample files
        list_and_read_files(client)
        
        print("✓ Connection test completed successfully!")
        
    except Exception as e:
        print(f"✗ Connection test failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()