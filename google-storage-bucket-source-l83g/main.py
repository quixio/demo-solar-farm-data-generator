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

def test_google_storage_connection():
    """Test connection to Google Cloud Storage and read 10 sample records."""
    
    # Load environment variables
    load_dotenv()
    
    try:
        # Get configuration from environment variables
        bucket_name = os.environ['GOOGLE_STORAGE_BUCKET']
        folder_path = os.environ['GOOGLE_STORAGE_FOLDER_PATH'].strip('/')
        file_format = os.environ['GOOGLE_STORAGE_FILE_FORMAT'].lower()
        credentials_json = os.environ['GOOGLE_STORAGE_SECRET_KEY']
        
        print(f"Connecting to Google Cloud Storage bucket: {bucket_name}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        
        # Parse credentials JSON
        if not credentials_json:
            raise ValueError("Google Cloud Storage credentials not found")
        
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize the Google Cloud Storage client
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)
        
        print("Successfully connected to Google Cloud Storage")
        
        # List files in the specified folder
        blobs = list(bucket.list_blobs(prefix=folder_path + '/' if folder_path else ''))
        
        if not blobs:
            print("No files found in the specified folder")
            return
        
        # Filter files by format
        target_files = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format}')]
        
        if not target_files:
            print(f"No {file_format} files found in the specified folder")
            return
        
        print(f"Found {len(target_files)} {file_format} files")
        
        # Read from the first file
        first_file = target_files[0]
        print(f"Reading from file: {first_file.name}")
        
        # Download file content
        file_content = first_file.download_as_text()
        
        # Process based on file format
        records_read = 0
        
        if file_format == 'csv':
            # Read CSV file
            df = pd.read_csv(StringIO(file_content))
            
            print(f"CSV file has {len(df)} rows and {len(df.columns)} columns")
            print(f"Columns: {list(df.columns)}")
            print("\n--- Sample Records (up to 10) ---")
            
            for index, row in df.head(10).iterrows():
                records_read += 1
                print(f"Record {records_read}:")
                for col in df.columns:
                    print(f"  {col}: {row[col]}")
                print("-" * 40)
                
        elif file_format == 'json':
            # Read JSON file
            try:
                # Try to parse as JSON Lines format first
                lines = file_content.strip().split('\n')
                json_records = []
                
                for line in lines:
                    if line.strip():
                        json_records.append(json.loads(line))
                
                if not json_records:
                    # Try to parse as single JSON array
                    json_data = json.loads(file_content)
                    if isinstance(json_data, list):
                        json_records = json_data
                    else:
                        json_records = [json_data]
                
                print(f"JSON file contains {len(json_records)} records")
                print("\n--- Sample Records (up to 10) ---")
                
                for i, record in enumerate(json_records[:10]):
                    records_read += 1
                    print(f"Record {records_read}:")
                    if isinstance(record, dict):
                        for key, value in record.items():
                            print(f"  {key}: {value}")
                    else:
                        print(f"  Value: {record}")
                    print("-" * 40)
                    
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON file: {e}")
                return
                
        elif file_format == 'txt':
            # Read text file line by line
            lines = file_content.strip().split('\n')
            print(f"Text file contains {len(lines)} lines")
            print("\n--- Sample Records (up to 10 lines) ---")
            
            for i, line in enumerate(lines[:10]):
                records_read += 1
                print(f"Line {records_read}: {line}")
                
        else:
            print(f"Unsupported file format: {file_format}")
            return
        
        print(f"\nSuccessfully read {records_read} sample records from Google Cloud Storage")
        
    except KeyError as e:
        print(f"Missing required environment variable: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing credentials JSON: {e}")
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {e}")
    finally:
        print("Connection test completed")

if __name__ == "__main__":
    test_google_storage_connection()