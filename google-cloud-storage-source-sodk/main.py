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
from typing import List, Dict, Any

def create_credentials_file(key_file_path: str) -> str:
    """Create a temporary credentials file from the environment variable."""
    try:
        # The key file path might contain the actual JSON content
        if key_file_path.startswith('{'):
            # It's JSON content, create a temporary file
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
                temp_file.write(key_file_path)
                return temp_file.name
        else:
            # It's a file path
            return key_file_path
    except Exception as e:
        print(f"Error creating credentials file: {e}")
        raise

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read sample files."""
    
    # Get environment variables
    bucket_name = os.getenv('GCS_BUCKET_NAME', 'quix-workflow')
    folder_path = os.getenv('GCS_FOLDER_PATH', '/')
    file_format = os.getenv('GCS_FILE_FORMAT', 'csv')
    file_compression = os.getenv('GCS_FILE_COMPRESSION', 'gzip')
    project_id = os.getenv('GCS_PROJECT_ID', 'quix-testing-365012')
    key_file_path = os.getenv('GCS_KEY_FILE_PATH', 'GCLOUD_PK_JSON')
    
    print(f"Testing GCS connection...")
    print(f"Bucket: {bucket_name}")
    print(f"Project ID: {project_id}")
    print(f"Folder path: {folder_path}")
    print(f"File format: {file_format}")
    print(f"File compression: {file_compression}")
    print("-" * 50)
    
    client = None
    temp_creds_path = None
    
    try:
        # Create credentials file
        temp_creds_path = create_credentials_file(key_file_path)
        
        # Initialize GCS client
        client = storage.Client.from_service_account_json(temp_creds_path, project=project_id)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        print(f"Successfully connected to GCS bucket: {bucket_name}")
        
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
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if blob.name.endswith(f'.{file_format}') or (file_compression == 'gzip' and blob.name.endswith(f'.{file_format}.gz')):
                target_files.append(blob)
        
        if not target_files:
            print(f"No {file_format} files found in folder: {folder_path}")
            print("Available files:")
            for blob in blobs[:10]:  # Show first 10 files
                print(f"  - {blob.name}")
            return
        
        print(f"Found {len(target_files)} {file_format} files")
        
        # Read sample data from the first few files
        samples_collected = 0
        target_samples = 10
        
        for blob in target_files:
            if samples_collected >= target_samples:
                break
                
            print(f"\nReading from file: {blob.name}")
            print(f"File size: {blob.size} bytes")
            print(f"Last modified: {blob.time_created}")
            
            try:
                # Download file content
                file_content = blob.download_as_bytes()
                
                # Handle different file formats and compression
                if file_format.lower() == 'csv':
                    # Create a temporary file to handle compression
                    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                        temp_file.write(file_content)
                        temp_file_path = temp_file.name
                    
                    try:
                        # Read CSV with appropriate compression
                        compression_param = 'gzip' if file_compression == 'gzip' else None
                        df = pd.read_csv(temp_file_path, compression=compression_param, nrows=min(10, target_samples - samples_collected))
                        
                        print(f"CSV columns: {list(df.columns)}")
                        print(f"Shape: {df.shape}")
                        
                        # Print each row
                        for idx, row in df.iterrows():
                            if samples_collected >= target_samples:
                                break
                            samples_collected += 1
                            print(f"\nSample {samples_collected}:")
                            for col in df.columns:
                                print(f"  {col}: {row[col]}")
                                
                    finally:
                        # Clean up temp file
                        if os.path.exists(temp_file_path):
                            os.unlink(temp_file_path)
                
                elif file_format.lower() == 'json':
                    # Handle JSON files
                    import gzip
                    if file_compression == 'gzip':
                        file_content = gzip.decompress(file_content)
                    
                    content_str = file_content.decode('utf-8')
                    
                    # Try to parse as JSON lines or regular JSON
                    try:
                        # Try JSON lines first
                        lines = content_str.strip().split('\n')
                        for line_num, line in enumerate(lines):
                            if samples_collected >= target_samples:
                                break
                            if line.strip():
                                try:
                                    json_obj = json.loads(line)
                                    samples_collected += 1
                                    print(f"\nSample {samples_collected} (JSON line {line_num + 1}):")
                                    print(json.dumps(json_obj, indent=2))
                                except json.JSONDecodeError:
                                    continue
                    except:
                        # Try regular JSON
                        try:
                            json_data = json.loads(content_str)
                            if isinstance(json_data, list):
                                for item in json_data[:min(10, target_samples - samples_collected)]:
                                    samples_collected += 1
                                    print(f"\nSample {samples_collected}:")
                                    print(json.dumps(item, indent=2))
                            else:
                                samples_collected += 1
                                print(f"\nSample {samples_collected}:")
                                print(json.dumps(json_data, indent=2))
                        except json.JSONDecodeError as e:
                            print(f"Error parsing JSON: {e}")
                
                else:
                    # Handle other file formats as text
                    if file_compression == 'gzip':
                        import gzip
                        file_content = gzip.decompress(file_content)
                    
                    content_str = file_content.decode('utf-8')
                    lines = content_str.split('\n')
                    
                    for line_num, line in enumerate(lines):
                        if samples_collected >= target_samples:
                            break
                        if line.strip():
                            samples_collected += 1
                            print(f"\nSample {samples_collected} (line {line_num + 1}):")
                            print(line)
                            
            except Exception as file_error:
                print(f"Error reading file {blob.name}: {file_error}")
                continue
        
        print(f"\n" + "="*50)
        print(f"Successfully collected {samples_collected} samples from GCS")
        print(f"Connection test completed successfully!")
        
    except Exception as e:
        print(f"Error connecting to GCS: {e}")
        raise
    
    finally:
        # Clean up temporary credentials file
        if temp_creds_path and temp_creds_path != key_file_path and os.path.exists(temp_creds_path):
            try:
                os.unlink(temp_creds_path)
            except:
                pass

if __name__ == "__main__":
    try:
        test_gcs_connection()
    except KeyboardInterrupt:
        print("\nConnection test interrupted by user")
    except Exception as e:
        print(f"Connection test failed: {e}")
        exit(1)