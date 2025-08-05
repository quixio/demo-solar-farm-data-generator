# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# pip install pandas
# END_DEPENDENCIES

import os
import json
import tempfile
from google.cloud import storage
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

def create_credentials_file():
    """Create a temporary credentials file from environment variable"""
    try:
        # Get the JSON credentials from environment variable
        credentials_json = os.environ.get('GS_SECRET_KEY')
        if not credentials_json:
            raise ValueError("GS_SECRET_KEY environment variable not found")
        
        # Parse the JSON to validate it
        credentials_dict = json.loads(credentials_json)
        
        # Create a temporary file for credentials
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(credentials_dict, temp_file)
        temp_file.close()
        
        return temp_file.name
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON format in credentials")
    except Exception as e:
        raise ValueError(f"Error processing credentials: {str(e)}")

def test_google_storage_connection():
    """Test connection to Google Storage Bucket and read sample data"""
    
    # Get environment variables
    bucket_name = os.environ.get('GS_BUCKET')
    project_id = os.environ.get('GS_PROJECT_ID')
    folder_path = os.environ.get('GS_FOLDER_PATH', '/')
    file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
    file_compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
    
    if not bucket_name:
        raise ValueError("GS_BUCKET environment variable is required")
    if not project_id:
        raise ValueError("GS_PROJECT_ID environment variable is required")
    
    credentials_file = None
    
    try:
        print(f"Connecting to Google Storage Bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {file_compression}")
        print("-" * 50)
        
        # Create temporary credentials file
        credentials_file = create_credentials_file()
        
        # Set the credentials environment variable for Google Cloud client
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_file
        
        # Initialize the Google Cloud Storage client
        client = storage.Client(project=project_id)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # Clean up folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        # List files in the specified folder
        print(f"Listing files in folder: {folder_path or 'root'}")
        
        # Get blobs (files) from the bucket
        blobs = list(bucket.list_blobs(prefix=folder_path))
        
        if not blobs:
            print("No files found in the specified folder")
            return
        
        # Filter files by format if specified
        if file_format and file_format.lower() != 'none':
            extension = f".{file_format.lower()}"
            if file_compression and file_compression.lower() != 'none':
                extension += f".{file_compression.lower()}"
            
            filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith(extension)]
            if filtered_blobs:
                blobs = filtered_blobs
        
        # Remove directory entries (keep only files)
        file_blobs = [blob for blob in blobs if not blob.name.endswith('/')]
        
        if not file_blobs:
            print(f"No files with format '{file_format}' found in the specified folder")
            return
        
        print(f"Found {len(file_blobs)} files")
        print("-" * 50)
        
        # Read sample data from files
        items_read = 0
        max_items = 10
        
        for blob in file_blobs:
            if items_read >= max_items:
                break
                
            print(f"\nReading from file: {blob.name}")
            print(f"File size: {blob.size} bytes")
            
            try:
                # Download the file content
                content = blob.download_as_text()
                
                if file_format.lower() == 'csv':
                    # Parse CSV content
                    from io import StringIO
                    csv_data = pd.read_csv(StringIO(content))
                    
                    # Print available rows (up to remaining items needed)
                    rows_to_show = min(len(csv_data), max_items - items_read)
                    
                    for i in range(rows_to_show):
                        print(f"\nItem {items_read + 1}:")
                        print(csv_data.iloc[i].to_dict())
                        items_read += 1
                        
                elif file_format.lower() == 'json':
                    # Parse JSON content
                    try:
                        json_data = json.loads(content)
                        if isinstance(json_data, list):
                            # JSON array
                            rows_to_show = min(len(json_data), max_items - items_read)
                            for i in range(rows_to_show):
                                print(f"\nItem {items_read + 1}:")
                                print(json_data[i])
                                items_read += 1
                        else:
                            # Single JSON object
                            print(f"\nItem {items_read + 1}:")
                            print(json_data)
                            items_read += 1
                    except json.JSONDecodeError:
                        print("Invalid JSON format in file")
                        
                else:
                    # For other formats, just show raw content (truncated)
                    lines = content.split('\n')
                    rows_to_show = min(len(lines), max_items - items_read)
                    
                    for i in range(rows_to_show):
                        if lines[i].strip():  # Skip empty lines
                            print(f"\nItem {items_read + 1}:")
                            print(lines[i][:200] + "..." if len(lines[i]) > 200 else lines[i])
                            items_read += 1
                            
            except Exception as e:
                print(f"Error reading file {blob.name}: {str(e)}")
                continue
        
        print(f"\n" + "=" * 50)
        print(f"Successfully read {items_read} items from Google Storage Bucket")
        
    except Exception as e:
        print(f"Error connecting to Google Storage: {str(e)}")
        raise
    
    finally:
        # Clean up temporary credentials file
        if credentials_file and os.path.exists(credentials_file):
            try:
                os.unlink(credentials_file)
            except:
                pass  # Ignore cleanup errors

if __name__ == "__main__":
    test_google_storage_connection()