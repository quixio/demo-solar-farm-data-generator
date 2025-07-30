#!/usr/bin/env python3
"""
Google Cloud Storage Connection Test
Reads sample files from GCS bucket for connectivity testing.
"""

import os
import gzip
import csv
import json
from google.cloud import storage
from google.oauth2 import service_account
import io

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read sample data."""
    
    # Get environment variables
    bucket_name = os.getenv('GCS_BUCKET_NAME', 'quix-workflow')
    project_id = os.getenv('GCS_PROJECT_ID', 'quix-testing-365012')
    key_file_path = os.getenv('GCS_KEY_FILE_PATH', 'GCLOUD_PK_JSON')
    service_account_email = os.getenv('GCS_SERVICE_ACCOUNT_EMAIL')
    folder_path = os.getenv('GCS_FOLDER_PATH', '/').strip('/')
    file_format = os.getenv('GCS_FILE_FORMAT', 'csv').lower()
    file_compression = os.getenv('GCS_FILE_COMPRESSION', 'gzip').lower()
    
    print("=== Google Cloud Storage Connection Test ===")
    print(f"Project ID: {project_id}")
    print(f"Bucket: {bucket_name}")
    print(f"Service Account: {service_account_email}")
    print(f"Folder Path: {folder_path}")
    print(f"File Format: {file_format}")
    print(f"Compression: {file_compression}")
    print("-" * 50)
    
    client = None
    
    try:
        # Initialize GCS client with service account credentials
        if os.path.exists(key_file_path):
            # Load from file
            credentials = service_account.Credentials.from_service_account_file(key_file_path)
            print(f"✓ Loaded credentials from file: {key_file_path}")
        else:
            # Try to load from environment variable (JSON string)
            try:
                key_json = os.getenv('GCLOUD_PK_JSON')
                if key_json:
                    credentials_info = json.loads(key_json)
                    credentials = service_account.Credentials.from_service_account_info(credentials_info)
                    print("✓ Loaded credentials from environment variable")
                else:
                    raise ValueError("No credentials found")
            except (json.JSONDecodeError, ValueError) as e:
                print(f"✗ Failed to load credentials: {e}")
                return False
        
        # Create client
        client = storage.Client(project=project_id, credentials=credentials)
        print("✓ GCS client initialized successfully")
        
        # Get bucket
        bucket = client.bucket(bucket_name)
        
        # Check if bucket exists and is accessible
        if not bucket.exists():
            print(f"✗ Bucket '{bucket_name}' does not exist or is not accessible")
            return False
        
        print(f"✓ Successfully connected to bucket: {bucket_name}")
        
        # List files in the specified folder
        prefix = folder_path + '/' if folder_path else ''
        blobs = list(client.list_blobs(bucket_name, prefix=prefix, max_results=50))
        
        if not blobs:
            print(f"✗ No files found in folder: {folder_path}")
            return False
        
        print(f"✓ Found {len(blobs)} files in the bucket")
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if file_format in blob.name.lower():
                target_files.append(blob)
        
        if not target_files:
            print(f"✗ No {file_format} files found")
            return False
        
        print(f"✓ Found {len(target_files)} {file_format} files")
        
        # Read sample data from the first suitable file
        sample_blob = target_files[0]
        print(f"\n📄 Reading sample data from: {sample_blob.name}")
        print(f"   Size: {sample_blob.size} bytes")
        print(f"   Created: {sample_blob.time_created}")
        print("-" * 50)
        
        # Download and process the file
        file_content = sample_blob.download_as_bytes()
        
        # Handle compression
        if file_compression == 'gzip' and sample_blob.name.endswith('.gz'):
            file_content = gzip.decompress(file_content)
            print("✓ Decompressed gzip file")
        
        # Process based on file format
        content_str = file_content.decode('utf-8')
        
        if file_format == 'csv':
            # Parse CSV content
            csv_reader = csv.DictReader(io.StringIO(content_str))
            rows = list(csv_reader)
            
            print(f"✓ CSV file contains {len(rows)} rows")
            
            # Print first 10 rows
            for i, row in enumerate(rows[:10], 1):
                print(f"\nRecord {i}:")
                for key, value in row.items():
                    print(f"  {key}: {value}")
        
        elif file_format == 'json':
            # Parse JSON content
            try:
                # Try parsing as JSON lines
                lines = content_str.strip().split('\n')
                json_objects = []
                for line in lines[:10]:
                    if line.strip():
                        json_objects.append(json.loads(line))
                
                print(f"✓ JSON Lines file, showing first {len(json_objects)} records")
                
                for i, obj in enumerate(json_objects, 1):
                    print(f"\nRecord {i}:")
                    print(json.dumps(obj, indent=2))
                    
            except json.JSONDecodeError:
                # Try parsing as single JSON array/object
                try:
                    data = json.loads(content_str)
                    if isinstance(data, list):
                        print(f"✓ JSON array with {len(data)} items")
                        for i, item in enumerate(data[:10], 1):
                            print(f"\nRecord {i}:")
                            print(json.dumps(item, indent=2))
                    else:
                        print("✓ Single JSON object:")
                        print(json.dumps(data, indent=2))
                except json.JSONDecodeError as e:
                    print(f"✗ Failed to parse JSON: {e}")
                    
        else:
            # For other formats, just print raw content
            lines = content_str.split('\n')[:10]
            print(f"✓ Text file, showing first {len(lines)} lines")
            
            for i, line in enumerate(lines, 1):
                print(f"Line {i}: {line}")
        
        print("\n" + "=" * 50)
        print("✓ Connection test completed successfully!")
        return True
        
    except Exception as e:
        print(f"✗ Connection test failed: {str(e)}")
        import traceback
        print("Error details:")
        traceback.print_exc()
        return False
    
    finally:
        # Cleanup (GCS client doesn't require explicit cleanup)
        print("🧹 Cleanup completed")

if __name__ == "__main__":
    success = test_gcs_connection()
    exit(0 if success else 1)