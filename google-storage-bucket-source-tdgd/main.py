# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# pip install pandas
# END_DEPENDENCIES

import os
import json
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
import pandas as pd
from io import StringIO

def test_gcs_connection():
    """Test connection to Google Cloud Storage and read sample data."""
    
    # Load environment variables
    load_dotenv()
    
    try:
        # Get environment variables
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
        
        # Parse credentials JSON
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize GCS client
        client = storage.Client(credentials=credentials, project=project_id)
        bucket = client.bucket(bucket_name)
        
        print(f"\nSuccessfully connected to bucket: {bucket_name}")
        
        # Clean folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        # List files in the bucket/folder
        blobs = list(client.list_blobs(bucket_name, prefix=folder_path))
        
        if not blobs:
            print(f"No files found in bucket {bucket_name} with prefix {folder_path}")
            return
        
        # Filter files by format if specified
        if file_format and file_format != 'none':
            blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format.lower()}')]
        
        if not blobs:
            print(f"No {file_format} files found in the specified location")
            return
        
        print(f"\nFound {len(blobs)} files. Processing first file...")
        
        # Get the first file
        first_blob = blobs[0]
        print(f"Reading from file: {first_blob.name}")
        
        # Download file content
        file_content = first_blob.download_as_text()
        
        # Process based on file format
        items_read = 0
        max_items = 10
        
        if file_format.lower() == 'csv':
            # Read CSV data
            df = pd.read_csv(StringIO(file_content))
            print(f"\nFound {len(df)} rows in CSV file")
            print("Reading first 10 records:\n")
            
            for index, row in df.head(max_items).iterrows():
                items_read += 1
                print(f"Record {items_read}:")
                print(row.to_dict())
                print("-" * 50)
                
        elif file_format.lower() == 'json':
            # Read JSON data
            try:
                # Try to parse as JSON lines
                lines = file_content.strip().split('\n')
                print(f"\nFound {len(lines)} JSON lines")
                print("Reading first 10 records:\n")
                
                for i, line in enumerate(lines[:max_items]):
                    if line.strip():
                        items_read += 1
                        try:
                            json_obj = json.loads(line)
                            print(f"Record {items_read}:")
                            print(json_obj)
                            print("-" * 50)
                        except json.JSONDecodeError:
                            print(f"Skipping invalid JSON line {i+1}")
                            
            except Exception:
                # Try to parse as single JSON object/array
                try:
                    data = json.loads(file_content)
                    if isinstance(data, list):
                        print(f"\nFound JSON array with {len(data)} items")
                        print("Reading first 10 records:\n")
                        
                        for i, item in enumerate(data[:max_items]):
                            items_read += 1
                            print(f"Record {items_read}:")
                            print(item)
                            print("-" * 50)
                    else:
                        items_read = 1
                        print("\nFound single JSON object:")
                        print("Record 1:")
                        print(data)
                        print("-" * 50)
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON: {e}")
                    
        else:
            # Read as plain text
            lines = file_content.split('\n')
            print(f"\nFound {len(lines)} lines in text file")
            print("Reading first 10 lines:\n")
            
            for i, line in enumerate(lines[:max_items]):
                if line.strip():  # Skip empty lines
                    items_read += 1
                    print(f"Record {items_read}:")
                    print(line.strip())
                    print("-" * 50)
        
        print(f"\nSuccessfully read {items_read} records from Google Cloud Storage")
        
    except json.JSONDecodeError as e:
        print(f"Error parsing credentials JSON: {e}")
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {e}")
    
    finally:
        print("\nConnection test completed.")

if __name__ == "__main__":
    test_gcs_connection()