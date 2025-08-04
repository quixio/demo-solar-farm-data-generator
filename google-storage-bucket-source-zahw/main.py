# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# pip install pandas
# END_DEPENDENCIES

import os
import json
import tempfile
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
from dotenv import load_dotenv

def test_google_storage_connection():
    """Test connection to Google Cloud Storage and read 10 sample records."""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    bucket_name = os.getenv('GS_BUCKET')
    project_id = os.getenv('GS_PROJECT_ID')
    credentials_json = os.getenv('GS_SECRET_KEY')
    folder_path = os.getenv('GS_FOLDER_PATH', '/')
    file_format = os.getenv('GS_FILE_FORMAT', 'csv')
    file_compression = os.getenv('GS_FILE_COMPRESSION', 'none')
    
    # Validate required environment variables
    if not bucket_name:
        raise ValueError("GS_BUCKET environment variable is required")
    if not project_id:
        raise ValueError("GS_PROJECT_ID environment variable is required")
    if not credentials_json:
        raise ValueError("GS_SECRET_KEY environment variable is required")
    
    client = None
    
    try:
        print(f"Connecting to Google Cloud Storage...")
        print(f"Project ID: {project_id}")
        print(f"Bucket: {bucket_name}")
        print(f"Folder Path: {folder_path}")
        print(f"File Format: {file_format}")
        print(f"File Compression: {file_compression}")
        
        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON format in credentials")
        
        # Create credentials from JSON
        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        
        # Initialize GCS client
        client = storage.Client(project=project_id, credentials=credentials)
        
        # Get bucket
        bucket = client.bucket(bucket_name)
        
        # Normalize folder path
        if folder_path and folder_path != '/':
            folder_prefix = folder_path.strip('/')
            if folder_prefix:
                folder_prefix += '/'
        else:
            folder_prefix = ''
        
        print(f"\nListing files in bucket '{bucket_name}' with prefix '{folder_prefix}'...")
        
        # List files in the bucket with the specified prefix
        blobs = list(bucket.list_blobs(prefix=folder_prefix))
        
        if not blobs:
            print("No files found in the specified bucket/folder")
            return
        
        # Filter files by format if specified
        filtered_blobs = []
        for blob in blobs:
            if blob.name.endswith('/'):  # Skip folders
                continue
            
            if file_format.lower() == 'csv' and blob.name.lower().endswith('.csv'):
                filtered_blobs.append(blob)
            elif file_format.lower() == 'json' and blob.name.lower().endswith('.json'):
                filtered_blobs.append(blob)
            elif file_format.lower() == 'parquet' and blob.name.lower().endswith('.parquet'):
                filtered_blobs.append(blob)
            elif file_format.lower() not in ['csv', 'json', 'parquet']:
                filtered_blobs.append(blob)  # Include all files if format is not specified or unknown
        
        if not filtered_blobs:
            print(f"No {file_format} files found in the specified bucket/folder")
            return
        
        print(f"Found {len(filtered_blobs)} {file_format} files")
        
        # Read from the first suitable file
        target_blob = filtered_blobs[0]
        print(f"\nReading from file: {target_blob.name}")
        print(f"File size: {target_blob.size} bytes")
        print(f"Content type: {target_blob.content_type}")
        
        # Download file content
        file_content = target_blob.download_as_text()
        
        records_read = 0
        
        if file_format.lower() == 'csv':
            # Read CSV data
            from io import StringIO
            df = pd.read_csv(StringIO(file_content))
            
            print(f"\nCSV has {len(df)} rows and {len(df.columns)} columns")
            print(f"Columns: {list(df.columns)}")
            print("\n--- Reading 10 sample records ---")
            
            for idx, (_, row) in enumerate(df.head(10).iterrows()):
                print(f"\nRecord {idx + 1}:")
                for col in df.columns:
                    print(f"  {col}: {row[col]}")
                records_read += 1
                
        elif file_format.lower() == 'json':
            # Read JSON data
            try:
                import json
                data = json.loads(file_content)
                
                if isinstance(data, list):
                    print(f"\nJSON array has {len(data)} items")
                    print("\n--- Reading 10 sample records ---")
                    
                    for idx, item in enumerate(data[:10]):
                        print(f"\nRecord {idx + 1}:")
                        if isinstance(item, dict):
                            for key, value in item.items():
                                print(f"  {key}: {value}")
                        else:
                            print(f"  Value: {item}")
                        records_read += 1
                        
                elif isinstance(data, dict):
                    print(f"\nJSON object with {len(data)} keys")
                    print("\n--- Reading sample record ---")
                    
                    for key, value in list(data.items())[:10]:
                        print(f"  {key}: {value}")
                    records_read = 1
                    
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
                return
                
        else:
            # For other formats, read as text lines
            lines = file_content.split('\n')
            print(f"\nFile has {len(lines)} lines")
            print("\n--- Reading 10 sample lines ---")
            
            for idx, line in enumerate(lines[:10]):
                if line.strip():  # Skip empty lines
                    print(f"Line {idx + 1}: {line[:200]}{'...' if len(line) > 200 else ''}")
                    records_read += 1
        
        print(f"\n✅ Successfully read {records_read} sample records from Google Cloud Storage")
        
    except Exception as e:
        print(f"❌ Error connecting to Google Cloud Storage: {str(e)}")
        raise
    
    finally:
        # Cleanup - GCS client doesn't need explicit cleanup
        print("\n🔌 Connection test completed")

if __name__ == "__main__":
    test_google_storage_connection()