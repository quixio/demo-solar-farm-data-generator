# DEPENDENCIES:
# pip install google-cloud-storage
# pip install pandas
# END_DEPENDENCIES

import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
import io
import sys

def get_gcp_credentials():
    """Get GCP credentials from environment variable containing JSON"""
    try:
        # Get the secret name from environment variable
        secret_key = os.getenv('GS_SECRET_KEY')
        if not secret_key:
            raise ValueError("GS_SECRET_KEY environment variable not found")
        
        # Get the actual credentials JSON from the secret
        credentials_json = os.getenv(secret_key)
        if not credentials_json:
            raise ValueError(f"Credentials not found in environment variable: {secret_key}")
        
        # Parse the JSON credentials
        credentials_dict = json.loads(credentials_json)
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        return credentials
    
    except Exception as e:
        print(f"Error loading GCP credentials: {e}")
        raise

def list_and_read_files():
    """Connect to Google Storage and read sample data"""
    try:
        # Get environment variables
        bucket_name = os.getenv('GS_BUCKET')
        project_id = os.getenv('GS_PROJECT_ID')
        folder_path = os.getenv('GS_FOLDER_PATH', '/')
        file_format = os.getenv('GS_FILE_FORMAT', 'csv')
        file_compression = os.getenv('GS_FILE_COMPRESSION', 'none')
        
        if not bucket_name:
            raise ValueError("GS_BUCKET environment variable not found")
        if not project_id:
            raise ValueError("GS_PROJECT_ID environment variable not found")
        
        print(f"Connecting to Google Storage bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"File compression: {file_compression}")
        
        # Get credentials and create client
        credentials = get_gcp_credentials()
        client = storage.Client(project=project_id, credentials=credentials)
        
        # Get bucket
        bucket = client.bucket(bucket_name)
        
        # Clean up folder path
        prefix = folder_path.strip('/')
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        elif prefix == '':
            prefix = None
        
        print(f"\nListing files with prefix: {prefix}")
        
        # List blobs with the specified prefix
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            print("No files found in the specified path")
            return
        
        # Filter files by format if specified
        filtered_blobs = []
        for blob in blobs:
            if not blob.name.endswith('/'):  # Skip directories
                if file_format.lower() == 'csv' and blob.name.lower().endswith('.csv'):
                    filtered_blobs.append(blob)
                elif file_format.lower() == 'json' and blob.name.lower().endswith('.json'):
                    filtered_blobs.append(blob)
                elif file_format.lower() == 'parquet' and blob.name.lower().endswith('.parquet'):
                    filtered_blobs.append(blob)
                elif file_format == '*':  # Any file format
                    filtered_blobs.append(blob)
        
        if not filtered_blobs:
            print(f"No {file_format} files found in the specified path")
            return
        
        print(f"\nFound {len(filtered_blobs)} {file_format} files")
        
        # Read from the first file
        blob = filtered_blobs[0]
        print(f"\nReading from file: {blob.name}")
        print(f"File size: {blob.size} bytes")
        
        # Download file content
        file_content = blob.download_as_text()
        
        # Read and display sample records based on file format
        items_read = 0
        
        if file_format.lower() == 'csv':
            # Read CSV data
            df = pd.read_csv(io.StringIO(file_content))
            print(f"\nCSV file has {len(df)} rows and {len(df.columns)} columns")
            print("Column names:", list(df.columns))
            
            print("\n=== Sample Records (up to 10) ===")
            for idx, row in df.head(10).iterrows():
                print(f"\nRecord {items_read + 1}:")
                for col in df.columns:
                    print(f"  {col}: {row[col]}")
                items_read += 1
                
        elif file_format.lower() == 'json':
            # Read JSON data
            try:
                # Try to read as JSON Lines format first
                lines = file_content.strip().split('\n')
                json_objects = []
                for line in lines:
                    if line.strip():
                        try:
                            json_objects.append(json.loads(line))
                        except json.JSONDecodeError:
                            # If not JSON Lines, try as single JSON array
                            json_objects = json.loads(file_content)
                            break
                
                if not isinstance(json_objects, list):
                    json_objects = [json_objects]
                
                print(f"\nJSON file contains {len(json_objects)} objects")
                
                print("\n=== Sample Records (up to 10) ===")
                for i, obj in enumerate(json_objects[:10]):
                    print(f"\nRecord {i + 1}:")
                    print(f"  {json.dumps(obj, indent=2)}")
                    items_read += 1
                    
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
                print("Raw content preview:")
                print(file_content[:500] + "..." if len(file_content) > 500 else file_content)
                
        else:
            # For other file formats, show raw content
            lines = file_content.split('\n')
            print(f"\nFile contains {len(lines)} lines")
            
            print("\n=== Sample Lines (up to 10) ===")
            for i, line in enumerate(lines[:10]):
                if line.strip():  # Skip empty lines
                    print(f"Line {items_read + 1}: {line}")
                    items_read += 1
                    if items_read >= 10:
                        break
        
        print(f"\n✅ Successfully read {items_read} sample records from Google Storage")
        
    except Exception as e:
        print(f"❌ Error reading from Google Storage: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    try:
        list_and_read_files()
    except KeyboardInterrupt:
        print("\n⚠️ Connection test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        sys.exit(1)