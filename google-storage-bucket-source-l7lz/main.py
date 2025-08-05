# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import tempfile
from google.cloud import storage
from dotenv import load_dotenv

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read sample files."""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    bucket_name = os.environ.get('GCS_BUCKET')
    folder_path = os.environ.get('GCS_FOLDER_PATH', '/')
    file_format = os.environ.get('GCS_FILE_FORMAT', 'csv')
    project_id = os.environ.get('GCP_PROJECT_ID')
    credentials_json = os.environ.get('GCP_CREDENTIALS_KEY')
    
    # Validate required environment variables
    if not bucket_name:
        raise ValueError("GCS_BUCKET environment variable is required")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID environment variable is required")
    if not credentials_json:
        raise ValueError("GCP_CREDENTIALS_KEY environment variable is required")
    
    print(f"Connecting to GCS bucket: {bucket_name}")
    print(f"Project ID: {project_id}")
    print(f"Folder path: {folder_path}")
    print(f"File format: {file_format}")
    
    client = None
    
    try:
        # Parse credentials JSON and create temporary credentials file
        credentials_dict = json.loads(credentials_json)
        
        # Create a temporary file for credentials
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(credentials_dict, temp_file)
            temp_credentials_path = temp_file.name
        
        try:
            # Initialize GCS client with credentials
            client = storage.Client.from_service_account_json(
                temp_credentials_path,
                project=project_id
            )
            
            # Get the bucket
            bucket = client.bucket(bucket_name)
            
            # Normalize folder path
            prefix = folder_path.strip('/')
            if prefix and not prefix.endswith('/'):
                prefix += '/'
            
            print(f"\nListing files in bucket with prefix: '{prefix}'")
            
            # List blobs with the specified prefix
            blobs = bucket.list_blobs(prefix=prefix)
            
            # Filter files by format if specified
            matching_files = []
            for blob in blobs:
                # Skip directories (blobs ending with '/')
                if blob.name.endswith('/'):
                    continue
                
                # Check file format
                if file_format.lower() != 'none':
                    if not blob.name.lower().endswith(f'.{file_format.lower()}'):
                        continue
                
                matching_files.append(blob)
                
                # Stop at 10 files
                if len(matching_files) >= 10:
                    break
            
            if not matching_files:
                print("No matching files found in the specified bucket and path.")
                return
            
            print(f"\nFound {len(matching_files)} matching files. Reading sample content:")
            print("-" * 60)
            
            # Read and print content from each file
            for i, blob in enumerate(matching_files, 1):
                try:
                    print(f"\n[File {i}] {blob.name}")
                    print(f"Size: {blob.size} bytes")
                    print(f"Content Type: {blob.content_type}")
                    print(f"Last Modified: {blob.time_created}")
                    
                    # Download file content
                    content = blob.download_as_text()
                    
                    # Print first few lines or characters
                    lines = content.split('\n')
                    preview_lines = lines[:5] if len(lines) > 5 else lines
                    
                    print("Content preview:")
                    for line_num, line in enumerate(preview_lines, 1):
                        # Truncate very long lines
                        display_line = line[:100] + "..." if len(line) > 100 else line
                        print(f"  {line_num}: {display_line}")
                    
                    if len(lines) > 5:
                        print(f"  ... ({len(lines) - 5} more lines)")
                    
                    print("-" * 40)
                    
                except Exception as e:
                    print(f"Error reading file {blob.name}: {str(e)}")
                    continue
            
            print(f"\nSuccessfully processed {len(matching_files)} files from GCS bucket '{bucket_name}'")
            
        finally:
            # Clean up temporary credentials file
            try:
                os.unlink(temp_credentials_path)
            except:
                pass
                
    except json.JSONDecodeError:
        print("Error: Invalid JSON in GCP_CREDENTIALS_KEY environment variable")
        raise
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {str(e)}")
        raise
    finally:
        # Client cleanup is handled automatically
        pass

if __name__ == "__main__":
    test_gcs_connection()