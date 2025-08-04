# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import tempfile
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

def test_google_storage_connection():
    """Test connection to Google Cloud Storage and read sample data."""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    bucket_name = os.getenv('GOOGLE_STORAGE_OUTPUT_BUCKET')
    folder_path = os.getenv('GOOGLE_STORAGE_FOLDER_PATH', 'data/')
    file_format = os.getenv('GOOGLE_STORAGE_FILE_FORMAT', 'csv')
    credentials_secret = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    if not bucket_name:
        raise ValueError("GOOGLE_STORAGE_OUTPUT_BUCKET environment variable is required")
    
    if not credentials_secret:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable is required")
    
    client = None
    temp_creds_file = None
    
    try:
        print(f"Connecting to Google Cloud Storage...")
        print(f"Bucket: {bucket_name}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print("-" * 50)
        
        # Get credentials from secret (assuming it contains the JSON content)
        credentials_json = os.getenv(credentials_secret)
        if not credentials_json:
            raise ValueError(f"Credentials not found in secret: {credentials_secret}")
        
        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in credentials: {e}")
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # Check if bucket exists
        if not bucket.exists():
            raise ValueError(f"Bucket '{bucket_name}' does not exist or is not accessible")
        
        print(f"Successfully connected to bucket: {bucket_name}")
        
        # List blobs in the specified folder with the specified format
        prefix = folder_path if folder_path and not folder_path.endswith('/') else folder_path
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        # Filter blobs by file format if specified
        if file_format and file_format.lower() != 'none':
            filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format.lower()}')]
        else:
            filtered_blobs = blobs
        
        if not filtered_blobs:
            print(f"No files found in bucket '{bucket_name}' with prefix '{prefix}' and format '{file_format}'")
            return
        
        print(f"Found {len(filtered_blobs)} files. Reading sample data from first file...")
        print("-" * 50)
        
        # Read sample data from the first file
        first_blob = filtered_blobs[0]
        print(f"Reading from file: {first_blob.name}")
        print(f"File size: {first_blob.size} bytes")
        print(f"Content type: {first_blob.content_type}")
        print("-" * 30)
        
        # Download and read file content
        file_content = first_blob.download_as_text()
        
        # Split content into lines for sampling
        lines = file_content.strip().split('\n')
        
        # Print up to 10 sample lines
        sample_count = min(10, len(lines))
        print(f"Sample data ({sample_count} lines):")
        print("-" * 30)
        
        for i in range(sample_count):
            print(f"Line {i+1}: {lines[i]}")
        
        if len(lines) > 10:
            print(f"... and {len(lines) - 10} more lines")
        
        print("-" * 50)
        print(f"Connection test completed successfully!")
        print(f"Total files available: {len(filtered_blobs)}")
        print(f"Total lines in first file: {len(lines)}")
        
    except Exception as e:
        print(f"Error during connection test: {str(e)}")
        raise
    
    finally:
        # Cleanup
        if temp_creds_file and os.path.exists(temp_creds_file):
            os.unlink(temp_creds_file)
        
        # Note: Google Cloud Storage client doesn't require explicit connection closing

if __name__ == "__main__":
    test_google_storage_connection()