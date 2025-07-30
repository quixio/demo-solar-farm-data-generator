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
from dotenv import load_dotenv
import gzip
import io

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read sample data."""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    bucket_name = os.getenv('GCS_BUCKET_NAME', 'quix-workflow')
    folder_path = os.getenv('GCS_FOLDER_PATH', '/')
    file_format = os.getenv('GCS_FILE_FORMAT', 'csv')
    file_compression = os.getenv('GCS_FILE_COMPRESSION', 'gzip')
    project_id = os.getenv('GCS_PROJECT_ID', 'quix-testing-365012')
    key_file_path = os.getenv('GCS_KEY_FILE_PATH', 'GCLOUD_PK_JSON')
    service_account_email = os.getenv('GCS_SERVICE_ACCOUNT_EMAIL')
    
    print(f"Testing connection to GCS bucket: {bucket_name}")
    print(f"Project ID: {project_id}")
    print(f"Folder path: {folder_path}")
    print(f"File format: {file_format}")
    print(f"Compression: {file_compression}")
    print("-" * 50)
    
    client = None
    
    try:
        # Set up authentication
        if key_file_path and os.path.exists(key_file_path):
            # Use service account key file
            credentials = service_account.Credentials.from_service_account_file(key_file_path)
            client = storage.Client(credentials=credentials, project=project_id)
            print("✓ Authenticated using service account key file")
        else:
            # Try to use default credentials or environment variable
            try:
                # Check if GCLOUD_PK_JSON is actually JSON content
                gcloud_json = os.getenv('GCLOUD_PK_JSON')
                if gcloud_json:
                    key_data = json.loads(gcloud_json)
                    credentials = service_account.Credentials.from_service_account_info(key_data)
                    client = storage.Client(credentials=credentials, project=project_id)
                    print("✓ Authenticated using JSON credentials from environment variable")
                else:
                    # Fall back to default credentials
                    client = storage.Client(project=project_id)
                    print("✓ Authenticated using default credentials")
            except json.JSONDecodeError:
                print("✗ Invalid JSON in GCLOUD_PK_JSON environment variable")
                return
            except Exception as e:
                print(f"✗ Authentication failed: {e}")
                return
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # Check if bucket exists
        if not bucket.exists():
            print(f"✗ Bucket '{bucket_name}' does not exist or is not accessible")
            return
            
        print(f"✓ Successfully connected to bucket '{bucket_name}'")
        
        # Clean up folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
            
        # List files in the specified folder
        print(f"\n📁 Listing files in folder: '{folder_path or 'root'}'")
        
        blobs = list(bucket.list_blobs(prefix=folder_path, max_results=50))
        
        if not blobs:
            print(f"✗ No files found in folder '{folder_path or 'root'}'")
            return
            
        # Filter for the specified file format
        target_files = []
        for blob in blobs:
            if blob.name.endswith(f'.{file_format}') or (file_compression == 'gzip' and blob.name.endswith(f'.{file_format}.gz')):
                target_files.append(blob)
                
        if not target_files:
            print(f"✗ No {file_format} files found in the specified folder")
            print("Available files:")
            for blob in blobs[:10]:
                print(f"  - {blob.name}")
            return
            
        print(f"✓ Found {len(target_files)} {file_format} file(s)")
        
        # Read from the first suitable file
        target_blob = target_files[0]
        print(f"\n📖 Reading sample data from: {target_blob.name}")
        print(f"   File size: {target_blob.size:,} bytes")
        print(f"   Last modified: {target_blob.time_created}")
        
        # Download and read the file content
        file_content = target_blob.download_as_bytes()
        
        # Handle compression
        if file_compression == 'gzip' or target_blob.name.endswith('.gz'):
            print("   Decompressing gzip file...")
            file_content = gzip.decompress(file_content)
            
        # Convert bytes to string for CSV processing
        content_str = file_content.decode('utf-8')
        
        # Read as CSV using pandas
        if file_format.lower() == 'csv':
            df = pd.read_csv(io.StringIO(content_str))
            
            print(f"\n✓ Successfully read CSV file")
            print(f"   Shape: {df.shape[0]} rows, {df.shape[1]} columns")
            print(f"   Columns: {list(df.columns)}")
            
            # Print exactly 10 sample records
            sample_size = min(10, len(df))
            print(f"\n📋 Displaying first {sample_size} records:")
            print("=" * 80)
            
            for i in range(sample_size):
                print(f"\nRecord {i + 1}:")
                record = df.iloc[i].to_dict()
                for key, value in record.items():
                    print(f"  {key}: {value}")
                print("-" * 40)
                
        else:
            # For non-CSV formats, just show raw content lines
            lines = content_str.strip().split('\n')
            sample_size = min(10, len(lines))
            
            print(f"\n📋 Displaying first {sample_size} lines:")
            print("=" * 80)
            
            for i, line in enumerate(lines[:sample_size]):
                print(f"Line {i + 1}: {line}")
                
        print(f"\n✅ Connection test completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error during connection test: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if client:
            print("\n🔒 Connection cleanup completed")

if __name__ == "__main__":
    test_gcs_connection()