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
    """
    Test connection to Google Cloud Storage and read sample CSV data.
    """
    try:
        # Load environment variables
        load_dotenv()
        
        # Get configuration from environment variables
        bucket_name = os.getenv('GCS_BUCKET_NAME', 'quix-workflow')
        project_id = os.getenv('GCS_PROJECT_ID', 'quix-testing-365012')
        key_file_path = os.getenv('GCS_KEY_FILE_PATH')
        service_account_email = os.getenv('GCS_SERVICE_ACCOUNT_EMAIL')
        folder_path = os.getenv('GCS_FOLDER_PATH', '/')
        file_format = os.getenv('GCS_FILE_FORMAT', 'csv')
        compression = os.getenv('GCS_COMPRESSION', 'gzip')
        
        print(f"Connecting to GCS bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {compression}")
        
        # Initialize GCS client with service account credentials
        if key_file_path and os.path.exists(key_file_path):
            # Load credentials from JSON file
            credentials = service_account.Credentials.from_service_account_file(key_file_path)
            client = storage.Client(project=project_id, credentials=credentials)
        elif key_file_path:
            # Try to parse as JSON string (for environment variable containing JSON)
            try:
                key_data = json.loads(key_file_path)
                credentials = service_account.Credentials.from_service_account_info(key_data)
                client = storage.Client(project=project_id, credentials=credentials)
            except json.JSONDecodeError:
                print("Error: Invalid JSON in GCS_KEY_FILE_PATH")
                return
        else:
            # Use default credentials (for running on GCP)
            client = storage.Client(project=project_id)
        
        # Get bucket
        bucket = client.bucket(bucket_name)
        print(f"Successfully connected to bucket: {bucket_name}")
        
        # List files in the specified folder
        folder_prefix = folder_path.strip('/') + '/' if folder_path != '/' else ''
        blobs = list(bucket.list_blobs(prefix=folder_prefix))
        
        # Filter for CSV files
        csv_files = [blob for blob in blobs if blob.name.endswith('.csv') or blob.name.endswith('.csv.gz')]
        
        if not csv_files:
            print(f"No CSV files found in folder: {folder_path}")
            return
        
        print(f"Found {len(csv_files)} CSV files")
        
        # Read from the first CSV file found
        target_blob = csv_files[0]
        print(f"Reading from file: {target_blob.name}")
        
        # Download file content
        file_content = target_blob.download_as_bytes()
        
        # Handle compression
        if compression == 'gzip' or target_blob.name.endswith('.gz'):
            file_content = gzip.decompress(file_content)
        
        # Convert bytes to string
        csv_content = file_content.decode('utf-8')
        
        # Read CSV data using pandas
        df = pd.read_csv(io.StringIO(csv_content))
        
        print(f"Successfully loaded CSV with {len(df)} rows and {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Read exactly 10 sample records
        sample_size = min(10, len(df))
        sample_data = df.head(sample_size)
        
        print(f"\n=== Reading {sample_size} sample records ===")
        
        for idx, row in sample_data.iterrows():
            print(f"\nRecord {idx + 1}:")
            for column, value in row.items():
                print(f"  {column}: {value}")
            print("-" * 50)
        
        print(f"\nSuccessfully read {sample_size} records from GCS bucket")
        
    except Exception as e:
        print(f"Error connecting to GCS or reading data: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("Connection test completed")

if __name__ == "__main__":
    test_gcs_connection()