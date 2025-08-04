# DEPENDENCIES:
# pip install google-cloud-storage
# pip install pandas
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import tempfile
from google.cloud import storage
import pandas as pd
from dotenv import load_dotenv

def test_google_storage_connection():
    """Test connection to Google Storage Bucket and read 10 sample records"""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    bucket_name = os.getenv('GS_BUCKET')
    project_id = os.getenv('GS_PROJECT_ID')
    credentials_json = os.getenv('GS_SECRET_KEY')
    folder_path = os.getenv('GS_FOLDER_PATH', '/')
    file_format = os.getenv('GS_FILE_FORMAT', 'csv')
    file_compression = os.getenv('GS_FILE_COMPRESSION', 'none')
    
    if not bucket_name:
        raise ValueError("GS_BUCKET environment variable is required")
    
    if not project_id:
        raise ValueError("GS_PROJECT_ID environment variable is required")
    
    if not credentials_json:
        raise ValueError("GS_SECRET_KEY environment variable is required")
    
    try:
        # Parse credentials JSON
        credentials_dict = json.loads(credentials_json)
        
        # Create a temporary file for credentials
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(credentials_dict, temp_file)
            temp_credentials_path = temp_file.name
        
        try:
            # Initialize Google Cloud Storage client
            client = storage.Client.from_service_account_json(
                temp_credentials_path,
                project=project_id
            )
            
            print(f"Successfully connected to Google Cloud Storage")
            print(f"Project ID: {project_id}")
            print(f"Bucket: {bucket_name}")
            print(f"Folder path: {folder_path}")
            print(f"File format: {file_format}")
            print(f"File compression: {file_compression}")
            print("-" * 50)
            
            # Get bucket
            bucket = client.bucket(bucket_name)
            
            # Clean folder path
            if folder_path.startswith('/'):
                folder_path = folder_path[1:]
            if folder_path and not folder_path.endswith('/'):
                folder_path += '/'
            
            # List files in the bucket with the specified folder path and format
            blobs = list(bucket.list_blobs(prefix=folder_path))
            
            # Filter files by format
            target_files = []
            for blob in blobs:
                if blob.name.endswith(f'.{file_format}'):
                    target_files.append(blob)
            
            if not target_files:
                print(f"No {file_format} files found in bucket {bucket_name} with prefix {folder_path}")
                return
            
            print(f"Found {len(target_files)} {file_format} files in the bucket")
            
            # Read from the first file found
            first_file = target_files[0]
            print(f"Reading from file: {first_file.name}")
            
            # Download file content
            file_content = first_file.download_as_text()
            
            # Handle different file formats
            if file_format.lower() == 'csv':
                # Create DataFrame from CSV content
                from io import StringIO
                df = pd.read_csv(StringIO(file_content))
                
                # Get up to 10 sample records
                sample_count = min(10, len(df))
                sample_data = df.head(sample_count)
                
                print(f"\nSample records from {first_file.name} ({sample_count} records):")
                print("-" * 50)
                
                for idx, row in sample_data.iterrows():
                    print(f"Record {idx + 1}:")
                    for col, value in row.items():
                        print(f"  {col}: {value}")
                    print()
                    
            elif file_format.lower() == 'json':
                # Parse JSON content
                import json
                try:
                    json_data = json.loads(file_content)
                    
                    # Handle different JSON structures
                    if isinstance(json_data, list):
                        # Array of objects
                        sample_count = min(10, len(json_data))
                        sample_data = json_data[:sample_count]
                    elif isinstance(json_data, dict):
                        # Single object - treat as one record
                        sample_data = [json_data]
                        sample_count = 1
                    else:
                        print(f"Unexpected JSON structure in {first_file.name}")
                        return
                        
                    print(f"\nSample records from {first_file.name} ({sample_count} records):")
                    print("-" * 50)
                    
                    for idx, record in enumerate(sample_data):
                        print(f"Record {idx + 1}:")
                        print(json.dumps(record, indent=2))
                        print()
                        
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON file {first_file.name}: {str(e)}")
                    return
                    
            else:
                # For other formats, just show raw content (first 10 lines)
                lines = file_content.split('\n')
                sample_count = min(10, len(lines))
                
                print(f"\nSample lines from {first_file.name} ({sample_count} lines):")
                print("-" * 50)
                
                for idx, line in enumerate(lines[:sample_count]):
                    if line.strip():  # Skip empty lines
                        print(f"Line {idx + 1}: {line}")
                        
        finally:
            # Clean up temporary credentials file
            os.unlink(temp_credentials_path)
            
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON format in credentials")
    except Exception as e:
        print(f"Error connecting to Google Storage Bucket: {str(e)}")
        raise

if __name__ == "__main__":
    test_google_storage_connection()