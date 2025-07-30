import os
import json
from google.cloud import storage
from google.auth import load_credentials_from_info
import pandas as pd
import gzip
import io

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read sample data"""
    
    try:
        # Load environment variables
        bucket_name = os.getenv('GCS_BUCKET_NAME', 'quix-workflow')
        project_id = os.getenv('GCS_PROJECT_ID', 'quix-testing-365012')
        key_file_path = os.getenv('GCS_KEY_FILE_PATH', 'GCLOUD_PK_JSON')
        folder_path = os.getenv('GCS_FOLDER_PATH', '/').strip('/')
        file_format = os.getenv('GCS_FILE_FORMAT', 'csv').lower()
        compression = os.getenv('GCS_COMPRESSION', 'gzip').lower()
        
        print(f"Testing connection to GCS bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {compression}")
        print("-" * 50)
        
        # Initialize GCS client with service account credentials
        client = None
        
        # Try to load credentials from JSON key file or environment variable
        if os.path.exists(key_file_path):
            print(f"Loading credentials from file: {key_file_path}")
            client = storage.Client.from_service_account_json(key_file_path, project=project_id)
        else:
            # Try to load from environment variable (JSON string)
            try:
                credentials_json = os.getenv('GCLOUD_PK_JSON')
                if credentials_json:
                    print("Loading credentials from environment variable")
                    credentials_info = json.loads(credentials_json)
                    credentials, _ = load_credentials_from_info(credentials_info)
                    client = storage.Client(credentials=credentials, project=project_id)
                else:
                    print("Using default credentials")
                    client = storage.Client(project=project_id)
            except Exception as e:
                print(f"Error loading credentials: {e}")
                print("Using default credentials")
                client = storage.Client(project=project_id)
        
        # Get bucket
        bucket = client.bucket(bucket_name)
        
        # Test bucket access
        if not bucket.exists():
            raise Exception(f"Bucket {bucket_name} does not exist or is not accessible")
        
        print(f"✓ Successfully connected to bucket: {bucket_name}")
        
        # List files in the specified folder
        prefix = folder_path + '/' if folder_path else ''
        blobs = list(bucket.list_blobs(prefix=prefix, max_results=50))
        
        if not blobs:
            print(f"No files found in folder: {folder_path}")
            return
        
        print(f"Found {len(blobs)} files in the bucket")
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if blob.name.lower().endswith(f'.{file_format}') or \
               (compression == 'gzip' and blob.name.lower().endswith(f'.{file_format}.gz')):
                target_files.append(blob)
        
        if not target_files:
            print(f"No {file_format} files found with compression {compression}")
            return
        
        print(f"Found {len(target_files)} {file_format} files")
        
        # Read sample data from the first file
        sample_blob = target_files[0]
        print(f"\nReading sample data from: {sample_blob.name}")
        
        # Download file content
        file_content = sample_blob.download_as_bytes()
        
        # Handle compression
        if compression == 'gzip' and sample_blob.name.lower().endswith('.gz'):
            file_content = gzip.decompress(file_content)
        
        # Convert to text
        text_content = file_content.decode('utf-8')
        
        # Read as CSV
        if file_format == 'csv':
            df = pd.read_csv(io.StringIO(text_content))
            
            print(f"File shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print("\nFirst 10 records:")
            print("=" * 80)
            
            # Print exactly 10 records (or fewer if file has less)
            sample_size = min(10, len(df))
            for i in range(sample_size):
                print(f"Record {i+1}:")
                record = df.iloc[i].to_dict()
                for key, value in record.items():
                    print(f"  {key}: {value}")
                print("-" * 40)
                
        else:
            # For non-CSV files, just print first 10 lines
            lines = text_content.split('\n')
            print(f"Total lines: {len(lines)}")
            print("\nFirst 10 lines:")
            print("=" * 80)
            
            for i, line in enumerate(lines[:10]):
                if line.strip():  # Skip empty lines
                    print(f"Line {i+1}: {line}")
        
        print("\n✓ Connection test completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during connection test: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("\nConnection test finished.")

if __name__ == "__main__":
    # Install required packages:
    # pip install google-cloud-storage pandas
    
    test_gcs_connection()