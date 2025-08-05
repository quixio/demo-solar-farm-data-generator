# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# pip install pandas
# END_DEPENDENCIES

import os
import json
import csv
import io
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_gcs_client():
    """Create and return a Google Cloud Storage client using service account credentials."""
    try:
        # Get credentials JSON from environment variable
        credentials_json = os.environ.get('GCP_CREDENTIALS_KEY')
        if not credentials_json:
            raise ValueError("GCP credentials not found in environment variables")
        
        # Parse the JSON credentials
        credentials_dict = json.loads(credentials_json)
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Create and return the client
        client = storage.Client(
            credentials=credentials,
            project=os.environ.get('GCP_PROJECT_ID')
        )
        
        return client
    
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON format in GCP credentials")
    except Exception as e:
        raise ValueError(f"Failed to create GCS client: {str(e)}")

def read_csv_file(blob):
    """Read CSV file content and return rows as list of dictionaries."""
    try:
        content = blob.download_as_text()
        csv_reader = csv.DictReader(io.StringIO(content))
        return list(csv_reader)
    except Exception as e:
        print(f"Error reading CSV file {blob.name}: {str(e)}")
        return []

def read_text_file(blob):
    """Read text file content and return lines."""
    try:
        content = blob.download_as_text()
        lines = content.strip().split('\n')
        return [{'line_number': i+1, 'content': line} for i, line in enumerate(lines)]
    except Exception as e:
        print(f"Error reading text file {blob.name}: {str(e)}")
        return []

def read_json_file(blob):
    """Read JSON file content."""
    try:
        content = blob.download_as_text()
        data = json.loads(content)
        if isinstance(data, list):
            return data
        else:
            return [data]  # Wrap single objects in a list
    except Exception as e:
        print(f"Error reading JSON file {blob.name}: {str(e)}")
        return []

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read sample data."""
    try:
        print("Testing Google Cloud Storage connection...")
        
        # Get configuration from environment variables
        bucket_name = os.environ.get('GCS_BUCKET')
        folder_path = os.environ.get('GCS_FOLDER_PATH', '/')
        file_format = os.environ.get('GCS_FILE_FORMAT', 'csv').lower()
        
        if not bucket_name:
            raise ValueError("GCS_BUCKET environment variable is required")
        
        print(f"Bucket: {bucket_name}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print("-" * 50)
        
        # Create GCS client
        client = create_gcs_client()
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # Check if bucket exists and is accessible
        if not bucket.exists():
            raise ValueError(f"Bucket '{bucket_name}' does not exist or is not accessible")
        
        print(f"Successfully connected to bucket: {bucket_name}")
        
        # Clean up folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        # List files in the specified folder
        blobs = list(bucket.list_blobs(prefix=folder_path))
        
        if not blobs:
            print(f"No files found in folder: {folder_path}")
            return
        
        print(f"Found {len(blobs)} files in the bucket")
        
        # Filter files by format if specified
        if file_format != 'all':
            filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format}')]
            if not filtered_blobs:
                print(f"No {file_format} files found")
                return
            blobs = filtered_blobs
        
        items_read = 0
        target_items = 10
        
        print(f"\nReading up to {target_items} sample items:")
        print("=" * 60)
        
        for blob in blobs:
            if items_read >= target_items:
                break
                
            # Skip directories
            if blob.name.endswith('/'):
                continue
                
            print(f"\nProcessing file: {blob.name}")
            print(f"File size: {blob.size} bytes")
            print(f"Content type: {blob.content_type}")
            print("-" * 40)
            
            try:
                # Determine file type and read accordingly
                file_extension = blob.name.lower().split('.')[-1] if '.' in blob.name else ''
                
                if file_extension == 'csv' or file_format == 'csv':
                    rows = read_csv_file(blob)
                elif file_extension == 'json' or file_format == 'json':
                    rows = read_json_file(blob)
                else:
                    rows = read_text_file(blob)
                
                # Print sample items from this file
                for i, item in enumerate(rows):
                    if items_read >= target_items:
                        break
                    
                    items_read += 1
                    print(f"Item {items_read}:")
                    print(f"  File: {blob.name}")
                    print(f"  Content: {item}")
                    print()
                
                if items_read >= target_items:
                    break
                    
            except Exception as e:
                print(f"Error processing file {blob.name}: {str(e)}")
                continue
        
        if items_read == 0:
            print("No readable items found in the specified files")
        else:
            print(f"\nSuccessfully read {items_read} items from Google Cloud Storage")
            
    except Exception as e:
        print(f"Connection test failed: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_gcs_connection()
    if success:
        print("\n✅ Google Cloud Storage connection test completed successfully!")
    else:
        print("\n❌ Google Cloud Storage connection test failed!")