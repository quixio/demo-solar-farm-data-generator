# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# pip install pandas
# END_DEPENDENCIES

import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
from io import StringIO

def main():
    # Load environment variables
    load_dotenv()
    
    try:
        # Get configuration from environment variables
        bucket_name = os.environ.get('GCS_BUCKET', 'quix-workflow')
        folder_path = os.environ.get('GCS_FOLDER_PATH', '/')
        file_format = os.environ.get('GCS_FILE_FORMAT', 'csv')
        credentials_json = os.environ.get('GCP_CREDENTIALS_KEY')
        project_id = os.environ.get('GCP_PROJECT_ID', 'quix-testing-365012')
        
        if not credentials_json:
            raise ValueError("GCP credentials not found in environment variables")
        
        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in GCP credentials")
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        print(f"Successfully connected to Google Cloud Storage bucket: {bucket_name}")
        print(f"Looking for {file_format} files in folder: {folder_path}")
        
        # List files in the specified folder with the specified format
        folder_prefix = folder_path.lstrip('/')
        if folder_prefix and not folder_prefix.endswith('/'):
            folder_prefix += '/'
        
        blobs = list(bucket.list_blobs(prefix=folder_prefix))
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if blob.name.endswith(f'.{file_format}') and not blob.name.endswith('/'):
                target_files.append(blob)
        
        if not target_files:
            print(f"No {file_format} files found in the specified folder")
            return
        
        print(f"Found {len(target_files)} {file_format} file(s)")
        
        items_read = 0
        target_items = 10
        
        # Read data from files
        for blob in target_files:
            if items_read >= target_items:
                break
                
            print(f"\nReading from file: {blob.name}")
            
            try:
                # Download file content
                content = blob.download_as_text()
                
                if file_format.lower() == 'csv':
                    # Parse CSV content
                    df = pd.read_csv(StringIO(content))
                    
                    # Read rows from this file
                    for index, row in df.iterrows():
                        if items_read >= target_items:
                            break
                        
                        items_read += 1
                        print(f"Item {items_read}:")
                        print(f"  File: {blob.name}")
                        print(f"  Row {index + 1}: {row.to_dict()}")
                        print()
                
                elif file_format.lower() == 'json':
                    # Parse JSON content
                    try:
                        json_data = json.loads(content)
                        
                        # Handle different JSON structures
                        if isinstance(json_data, list):
                            # Array of objects
                            for item in json_data:
                                if items_read >= target_items:
                                    break
                                
                                items_read += 1
                                print(f"Item {items_read}:")
                                print(f"  File: {blob.name}")
                                print(f"  Data: {item}")
                                print()
                        
                        elif isinstance(json_data, dict):
                            # Single object
                            items_read += 1
                            print(f"Item {items_read}:")
                            print(f"  File: {blob.name}")
                            print(f"  Data: {json_data}")
                            print()
                    
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON file {blob.name}: Invalid JSON format")
                        continue
                
                else:
                    # For other formats, treat as text
                    lines = content.split('\n')
                    for line in lines:
                        if items_read >= target_items:
                            break
                        if line.strip():  # Skip empty lines
                            items_read += 1
                            print(f"Item {items_read}:")
                            print(f"  File: {blob.name}")
                            print(f"  Content: {line.strip()}")
                            print()
            
            except Exception as e:
                print(f"Error reading file {blob.name}: {str(e)}")
                continue
        
        print(f"\nSuccessfully read {items_read} items from Google Cloud Storage")
    
    except Exception as e:
        print(f"Connection test failed: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    main()