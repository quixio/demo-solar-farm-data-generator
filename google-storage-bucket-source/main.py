# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# pip install pandas
# END_DEPENDENCIES

import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
from io import StringIO, BytesIO

load_dotenv()

def create_gcs_client():
    """Create and return a Google Cloud Storage client using service account credentials."""
    try:
        # Get credentials from environment variable
        credentials_json = os.environ.get('GS_SECRET_KEY')
        if not credentials_json:
            raise ValueError("GS_SECRET_KEY environment variable not found")
        
        # Parse the JSON credentials
        credentials_info = json.loads(credentials_json)
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        
        # Create and return the client
        client = storage.Client(credentials=credentials, project=os.environ.get('GS_PROJECT_ID'))
        return client
        
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON format in credentials")
    except Exception as e:
        raise ValueError(f"Failed to create GCS client: {str(e)}")

def read_csv_from_blob(blob):
    """Read CSV data from a blob and return as DataFrame."""
    try:
        content = blob.download_as_bytes()
        
        # Handle compression if specified
        compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
        if compression and compression.lower() != 'none':
            df = pd.read_csv(BytesIO(content), compression=compression)
        else:
            df = pd.read_csv(BytesIO(content))
        
        return df
    except Exception as e:
        print(f"Error reading CSV from blob {blob.name}: {str(e)}")
        return None

def main():
    """Main function to test Google Cloud Storage connection and read sample data."""
    try:
        # Create GCS client
        print("Creating Google Cloud Storage client...")
        client = create_gcs_client()
        
        # Get bucket
        bucket_name = os.environ.get('GS_BUCKET')
        folder_path = os.environ.get('GS_FOLDER_PATH', '/')
        file_format = os.environ.get('GS_FILE_FORMAT', 'csv').lower()
        
        print(f"Connecting to bucket: {bucket_name}")
        bucket = client.bucket(bucket_name)
        
        # Check if bucket exists
        if not bucket.exists():
            raise ValueError(f"Bucket '{bucket_name}' does not exist or is not accessible")
        
        print(f"Successfully connected to bucket: {bucket_name}")
        print(f"Looking for {file_format} files in folder: {folder_path}")
        
        # Clean folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        # List blobs with the specified prefix and file format
        blobs = list(bucket.list_blobs(prefix=folder_path))
        
        # Filter blobs by file format
        filtered_blobs = []
        for blob in blobs:
            if blob.name.lower().endswith(f'.{file_format}'):
                filtered_blobs.append(blob)
        
        if not filtered_blobs:
            print(f"No {file_format} files found in the specified folder path")
            return
        
        print(f"Found {len(filtered_blobs)} {file_format} files")
        
        total_records_read = 0
        target_records = 10
        
        # Read data from files until we have 10 records
        for blob in filtered_blobs:
            if total_records_read >= target_records:
                break
                
            print(f"\nReading from file: {blob.name}")
            
            if file_format == 'csv':
                df = read_csv_from_blob(blob)
                if df is not None and not df.empty:
                    records_to_read = min(target_records - total_records_read, len(df))
                    
                    for i in range(records_to_read):
                        record_num = total_records_read + 1
                        print(f"\n--- Record {record_num} (from {blob.name}) ---")
                        record_dict = df.iloc[i].to_dict()
                        for key, value in record_dict.items():
                            print(f"{key}: {value}")
                        total_records_read += 1
            else:
                # For non-CSV files, just read raw content
                try:
                    content = blob.download_as_text()
                    lines = content.split('\n')
                    
                    records_to_read = min(target_records - total_records_read, len(lines))
                    
                    for i in range(records_to_read):
                        if lines[i].strip():  # Skip empty lines
                            record_num = total_records_read + 1
                            print(f"\n--- Record {record_num} (from {blob.name}) ---")
                            print(f"Content: {lines[i]}")
                            total_records_read += 1
                            
                except Exception as e:
                    print(f"Error reading text from blob {blob.name}: {str(e)}")
        
        print(f"\n=== Connection Test Complete ===")
        print(f"Successfully read {total_records_read} records from Google Cloud Storage")
        
    except Exception as e:
        print(f"Error during connection test: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("✅ Google Cloud Storage connection test completed successfully")
    else:
        print("❌ Google Cloud Storage connection test failed")