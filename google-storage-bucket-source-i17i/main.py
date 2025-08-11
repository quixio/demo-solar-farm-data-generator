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
        bucket_name = os.environ['GOOGLE_STORAGE_BUCKET']
        folder_path = os.environ['GOOGLE_STORAGE_FOLDER_PATH']
        file_format = os.environ['GOOGLE_STORAGE_FILE_FORMAT']
        credentials_json_str = os.environ['GOOGLE_STORAGE_SECRET_KEY']
        
        print(f"Connecting to Google Cloud Storage bucket: {bucket_name}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        
        # Parse credentials JSON
        if not credentials_json_str:
            raise ValueError("Google Cloud Storage credentials not found in environment variable")
        
        try:
            credentials_dict = json.loads(credentials_json_str)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in Google Cloud Storage credentials")
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize the Google Cloud Storage client
        client = storage.Client(credentials=credentials)
        
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        print(f"Successfully connected to bucket: {bucket_name}")
        
        # Clean folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        
        # List files in the specified folder
        blobs = list(bucket.list_blobs(prefix=folder_path))
        
        if not blobs:
            print(f"No files found in folder: {folder_path}")
            return
        
        # Filter files by format if specified
        if file_format and file_format != 'none':
            blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{file_format.lower()}')]
        
        if not blobs:
            print(f"No {file_format} files found in folder: {folder_path}")
            return
        
        print(f"Found {len(blobs)} files")
        
        # Read and process files to get exactly 10 records
        records_read = 0
        target_records = 10
        
        for blob in blobs:
            if records_read >= target_records:
                break
                
            # Skip directories
            if blob.name.endswith('/'):
                continue
                
            print(f"\nReading file: {blob.name}")
            
            try:
                # Download file content
                content = blob.download_as_text()
                
                if file_format.lower() == 'csv':
                    # Parse CSV content
                    df = pd.read_csv(StringIO(content))
                    
                    # Read records from this file
                    for index, row in df.iterrows():
                        if records_read >= target_records:
                            break
                        
                        records_read += 1
                        print(f"Record {records_read}:")
                        print(f"  File: {blob.name}")
                        print(f"  Data: {row.to_dict()}")
                        print()
                
                elif file_format.lower() == 'json':
                    # Parse JSON content
                    try:
                        # Try to parse as JSON Lines (one JSON object per line)
                        lines = content.strip().split('\n')
                        for line in lines:
                            if records_read >= target_records:
                                break
                            if line.strip():
                                try:
                                    record = json.loads(line)
                                    records_read += 1
                                    print(f"Record {records_read}:")
                                    print(f"  File: {blob.name}")
                                    print(f"  Data: {record}")
                                    print()
                                except json.JSONDecodeError:
                                    continue
                    except:
                        # Try to parse as single JSON object or array
                        try:
                            data = json.loads(content)
                            if isinstance(data, list):
                                for item in data:
                                    if records_read >= target_records:
                                        break
                                    records_read += 1
                                    print(f"Record {records_read}:")
                                    print(f"  File: {blob.name}")
                                    print(f"  Data: {item}")
                                    print()
                            else:
                                records_read += 1
                                print(f"Record {records_read}:")
                                print(f"  File: {blob.name}")
                                print(f"  Data: {data}")
                                print()
                        except json.JSONDecodeError as e:
                            print(f"Error parsing JSON in file {blob.name}: {e}")
                            continue
                
                else:
                    # For other formats, treat as text and split by lines
                    lines = content.strip().split('\n')
                    for line in lines:
                        if records_read >= target_records:
                            break
                        if line.strip():
                            records_read += 1
                            print(f"Record {records_read}:")
                            print(f"  File: {blob.name}")
                            print(f"  Data: {line}")
                            print()
                
            except Exception as e:
                print(f"Error reading file {blob.name}: {e}")
                continue
        
        print(f"\nConnection test completed. Read {records_read} records from Google Cloud Storage.")
        
    except KeyError as e:
        print(f"Missing required environment variable: {e}")
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {e}")

if __name__ == "__main__":
    main()