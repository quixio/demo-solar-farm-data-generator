# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# pip install pandas
# END_DEPENDENCIES

import os
import json
import io
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd

def main():
    # Load environment variables
    load_dotenv()
    
    try:
        # Get environment variables
        bucket_name = os.environ['GS_BUCKET']
        project_id = os.environ['GS_PROJECT_ID']
        folder_path = os.environ['GS_FOLDER_PATH'].strip('/')
        file_format = os.environ['GS_FILE_FORMAT'].lower()
        compression = os.environ['GS_FILE_COMPRESSION']
        credentials_json = os.environ['GS_SECRET_KEY']
        
        print(f"Connecting to Google Storage bucket: {bucket_name}")
        print(f"Project ID: {project_id}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {compression}")
        
        # Parse credentials JSON
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials, project=project_id)
        bucket = client.bucket(bucket_name)
        
        print(f"\nSuccessfully connected to bucket: {bucket_name}")
        
        # List files in the specified folder
        blobs = list(bucket.list_blobs(prefix=folder_path))
        
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
        
        # Read from the first file and extract 10 records
        target_blob = target_files[0]
        print(f"\nReading from file: {target_blob.name}")
        
        # Download file content
        file_content = target_blob.download_as_text()
        
        # Handle compression if needed
        if compression and compression.lower() != 'none':
            print(f"Note: Compression '{compression}' specified but not implemented in this test")
        
        # Parse based on file format
        records_printed = 0
        
        if file_format == 'csv':
            # Use pandas to read CSV
            df = pd.read_csv(io.StringIO(file_content))
            print(f"\nCSV file has {len(df)} rows and {len(df.columns)} columns")
            print(f"Columns: {list(df.columns)}")
            print("\n--- Reading 10 sample records ---")
            
            for index, row in df.head(10).iterrows():
                print(f"Record {records_printed + 1}:")
                print(row.to_dict())
                print("-" * 40)
                records_printed += 1
                
        elif file_format == 'json':
            # Handle JSON files
            try:
                # Try to parse as JSON Lines (one JSON object per line)
                lines = file_content.strip().split('\n')
                print(f"\nJSON file has {len(lines)} lines")
                print("\n--- Reading 10 sample records ---")
                
                for i, line in enumerate(lines[:10]):
                    if line.strip():
                        try:
                            record = json.loads(line.strip())
                            print(f"Record {records_printed + 1}:")
                            print(record)
                            print("-" * 40)
                            records_printed += 1
                        except json.JSONDecodeError:
                            continue
                            
                # If JSON Lines didn't work, try as single JSON array
                if records_printed == 0:
                    data = json.loads(file_content)
                    if isinstance(data, list):
                        print(f"JSON array has {len(data)} items")
                        for i, item in enumerate(data[:10]):
                            print(f"Record {records_printed + 1}:")
                            print(item)
                            print("-" * 40)
                            records_printed += 1
                    else:
                        print("Record 1:")
                        print(data)
                        records_printed = 1
                        
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
                return
                
        elif file_format == 'txt':
            # Handle text files
            lines = file_content.strip().split('\n')
            print(f"\nText file has {len(lines)} lines")
            print("\n--- Reading 10 sample records ---")
            
            for i, line in enumerate(lines[:10]):
                if line.strip():
                    print(f"Record {records_printed + 1}:")
                    print(line.strip())
                    print("-" * 40)
                    records_printed += 1
                    
        else:
            # Handle other formats as raw text
            lines = file_content.strip().split('\n')
            print(f"\nFile has {len(lines)} lines")
            print("\n--- Reading 10 sample records ---")
            
            for i, line in enumerate(lines[:10]):
                if line.strip():
                    print(f"Record {records_printed + 1}:")
                    print(line.strip())
                    print("-" * 40)
                    records_printed += 1
        
        print(f"\nSuccessfully read {records_printed} records from Google Storage bucket")
        
    except KeyError as e:
        print(f"Missing required environment variable: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing credentials JSON: {e}")
    except Exception as e:
        print(f"Error connecting to Google Storage: {e}")

if __name__ == "__main__":
    main()