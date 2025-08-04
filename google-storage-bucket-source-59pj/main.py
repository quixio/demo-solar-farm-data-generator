# DEPENDENCIES:
# pip install google-cloud-storage
# pip install pandas
# END_DEPENDENCIES

import os
import json
import tempfile
from google.cloud import storage
import pandas as pd

def test_google_storage_connection():
    """Test connection to Google Storage Bucket and read sample data."""
    
    try:
        # Get environment variables
        bucket_name = os.getenv('GS_BUCKET')
        project_id = os.getenv('GS_PROJECT_ID')
        folder_path = os.getenv('GS_FOLDER_PATH', '/')
        file_format = os.getenv('GS_FILE_FORMAT', 'csv')
        credentials_json = os.getenv('GCLOUD_PK_JSON')
        
        # Validate required environment variables
        if not bucket_name:
            raise ValueError("GS_BUCKET environment variable is required")
        if not project_id:
            raise ValueError("GS_PROJECT_ID environment variable is required")
        if not credentials_json:
            raise ValueError("GCLOUD_PK_JSON environment variable is required")
        
        print(f"Connecting to Google Storage Bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder Path: {folder_path}")
        print(f"File Format: {file_format}")
        
        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in credentials")
        
        # Create temporary credentials file for Google Cloud client
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(credentials_dict, temp_file)
            temp_credentials_path = temp_file.name
        
        try:
            # Initialize Google Cloud Storage client
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_credentials_path
            client = storage.Client(project=project_id)
            
            # Get bucket
            bucket = client.bucket(bucket_name)
            
            print(f"Successfully connected to bucket: {bucket_name}")
            
            # Clean folder path
            folder_path = folder_path.strip('/')
            if folder_path:
                folder_path += '/'
            
            # List files in the bucket
            blobs = list(client.list_blobs(bucket, prefix=folder_path))
            
            # Filter files by format
            target_files = []
            for blob in blobs:
                if blob.name.endswith(f'.{file_format}') and not blob.name.endswith('/'):
                    target_files.append(blob)
            
            if not target_files:
                print(f"No {file_format} files found in bucket {bucket_name}")
                if folder_path:
                    print(f"Searched in folder: {folder_path}")
                return
            
            print(f"Found {len(target_files)} {file_format} files")
            
            # Read from the first file found
            first_file = target_files[0]
            print(f"Reading from file: {first_file.name}")
            
            # Download file content
            file_content = first_file.download_as_text()
            
            # Parse based on file format
            if file_format.lower() == 'csv':
                # Create a temporary file to read CSV
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_csv:
                    temp_csv.write(file_content)
                    temp_csv_path = temp_csv.name
                
                try:
                    # Read CSV file
                    df = pd.read_csv(temp_csv_path)
                    
                    # Get first 10 records
                    sample_records = df.head(10)
                    
                    print(f"\nSample data from {first_file.name}:")
                    print("=" * 50)
                    
                    for idx, row in sample_records.iterrows():
                        print(f"Record {idx + 1}:")
                        print(row.to_dict())
                        print("-" * 30)
                        
                finally:
                    # Clean up temporary CSV file
                    if os.path.exists(temp_csv_path):
                        os.unlink(temp_csv_path)
            
            elif file_format.lower() == 'json':
                # Parse JSON content
                try:
                    json_data = json.loads(file_content)
                    
                    # Handle different JSON structures
                    if isinstance(json_data, list):
                        records = json_data[:10]
                    elif isinstance(json_data, dict):
                        # If it's a single object, treat as one record
                        records = [json_data]
                    else:
                        records = [json_data]
                    
                    print(f"\nSample data from {first_file.name}:")
                    print("=" * 50)
                    
                    for idx, record in enumerate(records):
                        print(f"Record {idx + 1}:")
                        print(json.dumps(record, indent=2))
                        print("-" * 30)
                        
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON file: {e}")
                    return
            
            else:
                # For other formats, just show raw content lines
                lines = file_content.split('\n')
                sample_lines = lines[:10]
                
                print(f"\nSample data from {first_file.name}:")
                print("=" * 50)
                
                for idx, line in enumerate(sample_lines):
                    if line.strip():  # Skip empty lines
                        print(f"Record {idx + 1}:")
                        print(line)
                        print("-" * 30)
            
            print("\nConnection test completed successfully!")
            
        finally:
            # Clean up temporary credentials file
            if os.path.exists(temp_credentials_path):
                os.unlink(temp_credentials_path)
            
            # Remove credentials from environment
            if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
                del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
                
    except Exception as e:
        print(f"Error testing Google Storage connection: {e}")
        raise

if __name__ == "__main__":
    test_google_storage_connection()