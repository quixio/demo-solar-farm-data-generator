# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# pip install pandas
# END_DEPENDENCIES

import os
import json
import tempfile
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
import pandas as pd

def main():
    # Load environment variables
    load_dotenv()
    
    try:
        # Get configuration from environment variables
        bucket_name = os.environ['GCS_BUCKET']
        folder_path = os.environ['GCS_FOLDER_PATH'].strip('/')
        file_format = os.environ['GCS_FILE_FORMAT'].lower()
        file_compression = os.environ['GCS_FILE_COMPRESSION']
        project_id = os.environ['GCP_PROJECT_ID']
        credentials_json_str = os.environ['GCP_CREDENTIALS_KEY']
        
        print(f"Connecting to GCS bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {file_compression}")
        
        # Parse credentials JSON
        credentials_dict = json.loads(credentials_json_str)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize GCS client
        client = storage.Client(credentials=credentials, project=project_id)
        bucket = client.bucket(bucket_name)
        
        print(f"\nSuccessfully connected to bucket: {bucket_name}")
        
        # List files in the specified folder
        prefix = folder_path + "/" if folder_path else ""
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            print(f"No files found in folder: {folder_path}")
            return
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if blob.name.endswith(f'.{file_format}'):
                target_files.append(blob)
        
        if not target_files:
            print(f"No {file_format} files found in folder: {folder_path}")
            return
        
        print(f"\nFound {len(target_files)} {file_format} files")
        
        # Read sample data from files
        items_read = 0
        target_items = 10
        
        for blob in target_files:
            if items_read >= target_items:
                break
                
            print(f"\nReading from file: {blob.name}")
            
            try:
                # Download blob content
                content = blob.download_as_text()
                
                if file_format == 'csv':
                    # Create a temporary file to read CSV with pandas
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                        temp_file.write(content)
                        temp_file_path = temp_file.name
                    
                    try:
                        # Read CSV with pandas
                        df = pd.read_csv(temp_file_path)
                        
                        # Read remaining items needed
                        remaining_items = target_items - items_read
                        rows_to_read = min(len(df), remaining_items)
                        
                        for i in range(rows_to_read):
                            print(f"\nItem {items_read + 1}:")
                            print(df.iloc[i].to_dict())
                            items_read += 1
                            
                    finally:
                        # Clean up temporary file
                        os.unlink(temp_file_path)
                        
                elif file_format == 'json':
                    # Handle JSON files
                    lines = content.strip().split('\n')
                    for line in lines:
                        if items_read >= target_items:
                            break
                        if line.strip():
                            try:
                                item = json.loads(line)
                                print(f"\nItem {items_read + 1}:")
                                print(item)
                                items_read += 1
                            except json.JSONDecodeError:
                                continue
                                
                else:
                    # Handle text files or other formats
                    lines = content.strip().split('\n')
                    for line in lines:
                        if items_read >= target_items:
                            break
                        if line.strip():
                            print(f"\nItem {items_read + 1}:")
                            print(line.strip())
                            items_read += 1
                            
            except Exception as e:
                print(f"Error reading file {blob.name}: {str(e)}")
                continue
        
        print(f"\nSuccessfully read {items_read} items from GCS bucket")
        
    except KeyError as e:
        print(f"Missing required environment variable: {e}")
    except json.JSONDecodeError:
        print("Invalid JSON format in credentials")
    except Exception as e:
        print(f"Connection test failed: {str(e)}")

if __name__ == "__main__":
    main()