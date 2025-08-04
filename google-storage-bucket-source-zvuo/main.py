# DEPENDENCIES:
# pip install google-cloud-storage
# pip install pandas
# END_DEPENDENCIES

import os
import json
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import io

def test_google_storage_connection():
    """Test connection to Google Cloud Storage and read sample data."""
    
    try:
        # Get environment variables
        bucket_name = os.getenv('GS_BUCKET')
        project_id = os.getenv('GS_PROJECT_ID')
        credentials_json = os.getenv('GS_SECRET_KEY')
        folder_path = os.getenv('GS_FOLDER_PATH', '/').strip('/')
        file_format = os.getenv('GS_FILE_FORMAT', 'csv').lower()
        compression = os.getenv('GS_COMPRESSION', 'none').lower()
        
        if not bucket_name:
            raise ValueError("GS_BUCKET environment variable is required")
        
        if not project_id:
            raise ValueError("GS_PROJECT_ID environment variable is required")
            
        if not credentials_json:
            raise ValueError("GS_SECRET_KEY environment variable is required")
        
        print(f"Connecting to Google Cloud Storage...")
        print(f"Project ID: {project_id}")
        print(f"Bucket: {bucket_name}")
        print(f"Folder path: {folder_path if folder_path else 'root'}")
        print(f"File format: {file_format}")
        print(f"Compression: {compression}")
        
        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(
                credentials_dict, 
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in credentials")
        except Exception as e:
            raise ValueError(f"Failed to create credentials: {str(e)}")
        
        # Initialize the client
        client = storage.Client(project=project_id, credentials=credentials)
        
        # Get the bucket
        try:
            bucket = client.bucket(bucket_name)
            # Test if bucket exists by trying to get its metadata
            bucket.reload()
            print(f"✓ Successfully connected to bucket '{bucket_name}'")
        except Exception as e:
            raise ValueError(f"Failed to access bucket '{bucket_name}': {str(e)}")
        
        # List files in the specified folder
        prefix = folder_path + '/' if folder_path and not folder_path.endswith('/') else folder_path
        if prefix == '/':
            prefix = ''
            
        blobs = list(client.list_blobs(bucket_name, prefix=prefix, max_results=50))
        
        if not blobs:
            print(f"No files found in bucket '{bucket_name}' with prefix '{prefix}'")
            return
        
        print(f"\nFound {len(blobs)} files in the bucket")
        
        # Filter files by format if specified
        if file_format != 'all':
            filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format}')]
            if not filtered_blobs:
                print(f"No {file_format} files found")
                return
            blobs = filtered_blobs
        
        print(f"Reading sample data from {file_format} files...")
        
        items_read = 0
        max_items = 10
        
        for blob in blobs:
            if items_read >= max_items:
                break
                
            try:
                print(f"\n--- Reading from file: {blob.name} ---")
                
                # Download file content
                content = blob.download_as_bytes()
                
                # Handle compression
                if compression in ['gzip', 'gz']:
                    import gzip
                    content = gzip.decompress(content)
                elif compression in ['bz2', 'bzip2']:
                    import bz2
                    content = bz2.decompress(content)
                
                # Convert bytes to string for text formats
                if file_format in ['csv', 'txt', 'json']:
                    content_str = content.decode('utf-8')
                    
                    if file_format == 'csv':
                        # Read CSV data
                        df = pd.read_csv(io.StringIO(content_str))
                        rows_to_read = min(max_items - items_read, len(df))
                        
                        for i in range(rows_to_read):
                            print(f"Item {items_read + 1}: {df.iloc[i].to_dict()}")
                            items_read += 1
                            if items_read >= max_items:
                                break
                                
                    elif file_format == 'json':
                        # Handle JSON data
                        try:
                            data = json.loads(content_str)
                            if isinstance(data, list):
                                rows_to_read = min(max_items - items_read, len(data))
                                for i in range(rows_to_read):
                                    print(f"Item {items_read + 1}: {data[i]}")
                                    items_read += 1
                                    if items_read >= max_items:
                                        break
                            else:
                                print(f"Item {items_read + 1}: {data}")
                                items_read += 1
                        except json.JSONDecodeError:
                            print(f"Item {items_read + 1}: {content_str[:200]}...")
                            items_read += 1
                            
                    elif file_format == 'txt':
                        # Handle text files line by line
                        lines = content_str.split('\n')
                        rows_to_read = min(max_items - items_read, len(lines))
                        
                        for i in range(rows_to_read):
                            if lines[i].strip():  # Skip empty lines
                                print(f"Item {items_read + 1}: {lines[i]}")
                                items_read += 1
                                if items_read >= max_items:
                                    break
                else:
                    # For other file types, just show basic info
                    print(f"Item {items_read + 1}: Binary file - {blob.name} (size: {len(content)} bytes)")
                    items_read += 1
                    
            except Exception as e:
                print(f"Error reading file {blob.name}: {str(e)}")
                continue
        
        print(f"\n✓ Successfully read {items_read} sample items from Google Cloud Storage")
        
    except Exception as e:
        print(f"✗ Connection test failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_google_storage_connection()