import os
import json
import tempfile
from google.cloud import storage
from dotenv import load_dotenv

def test_google_storage_connection():
    """Test connection to Google Storage Bucket and read 10 sample items."""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    bucket_name = os.environ.get('GS_BUCKET')
    project_id = os.environ.get('GS_PROJECT_ID')
    credentials_json = os.environ.get('GS_SECRET_KEY')
    folder_path = os.environ.get('GS_FOLDER_PATH', '/')
    file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
    
    if not bucket_name:
        raise ValueError("GS_BUCKET environment variable is required")
    
    if not project_id:
        raise ValueError("GS_PROJECT_ID environment variable is required")
    
    if not credentials_json:
        raise ValueError("GS_SECRET_KEY environment variable is required")
    
    client = None
    temp_file = None
    
    try:
        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in credentials")
        
        # Create temporary credentials file
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(credentials_dict, temp_file)
        temp_file.close()
        
        # Set credentials environment variable for Google Cloud client
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_file.name
        
        # Initialize Google Cloud Storage client
        client = storage.Client(project=project_id)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        print(f"Successfully connected to Google Storage bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Looking for files in folder: {folder_path}")
        print(f"Target file format: {file_format}")
        print("-" * 50)
        
        # Clean up folder path
        prefix = folder_path.strip('/')
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        
        # List blobs in the bucket with the specified prefix
        blobs = list(bucket.list_blobs(prefix=prefix if prefix != '/' else None, max_results=50))
        
        if not blobs:
            print("No files found in the specified folder")
            return
        
        # Filter blobs by file format if specified
        if file_format and file_format.lower() != 'none':
            filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format.lower()}')]
            if filtered_blobs:
                blobs = filtered_blobs
        
        # Remove directory entries (blobs ending with '/')
        file_blobs = [blob for blob in blobs if not blob.name.endswith('/')]
        
        if not file_blobs:
            print(f"No {file_format} files found in the specified folder")
            return
        
        print(f"Found {len(file_blobs)} files in the bucket")
        print("Reading up to 10 sample files:")
        print("-" * 50)
        
        # Read and display up to 10 files
        for i, blob in enumerate(file_blobs[:10]):
            try:
                print(f"\nFile {i+1}: {blob.name}")
                print(f"Size: {blob.size} bytes")
                print(f"Content Type: {blob.content_type}")
                print(f"Created: {blob.time_created}")
                
                # Download and display file content (first 500 characters)
                content = blob.download_as_text(encoding='utf-8')
                
                if content:
                    print("Content preview:")
                    preview = content[:500]
                    if len(content) > 500:
                        preview += "... (truncated)"
                    print(preview)
                else:
                    print("File is empty")
                    
                print("-" * 30)
                
            except Exception as e:
                print(f"Error reading file {blob.name}: {str(e)}")
                continue
        
        print(f"\nSuccessfully processed {min(10, len(file_blobs))} files from Google Storage bucket")
        
    except Exception as e:
        print(f"Error connecting to Google Storage: {str(e)}")
        raise
    
    finally:
        # Clean up temporary credentials file
        if temp_file and os.path.exists(temp_file.name):
            try:
                os.unlink(temp_file.name)
            except:
                pass
        
        # Remove credentials from environment
        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
            del os.environ['GOOGLE_APPLICATION_CREDENTIALS']

if __name__ == "__main__":
    test_google_storage_connection()