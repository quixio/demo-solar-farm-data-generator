# DEPENDENCIES:
# pip install google-cloud-storage
# pip install python-dotenv
# pip install pandas
# END_DEPENDENCIES

import os
import json
import io
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
import pandas as pd

def main():
    # Load environment variables
    load_dotenv()
    
    try:
        # Get configuration from environment variables
        bucket_name = os.environ['GOOGLE_STORAGE_BUCKET']
        region = os.environ['GOOGLE_STORAGE_REGION']
        credentials_json = os.environ['GOOGLE_STORAGE_SECRET_KEY']
        folder_path = os.environ['GOOGLE_STORAGE_FOLDER_PATH']
        file_format = os.environ['GOOGLE_STORAGE_FILE_FORMAT']
        compression = os.environ['GOOGLE_STORAGE_FILE_COMPRESSION']
        
        print(f"Connecting to Google Cloud Storage bucket: {bucket_name}")
        print(f"Region: {region}")
        print(f"Folder path: {folder_path}")
        print(f"File format: {file_format}")
        print(f"Compression: {compression}")
        
        # Parse credentials JSON
        if not credentials_json:
            raise ValueError("Google Cloud Storage credentials not found")
            
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Initialize Google Cloud Storage client
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)
        
        print(f"\nSuccessfully connected to bucket: {bucket_name}")
        
        # Clean folder path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
            
        # List files in the specified folder
        blobs = list(bucket.list_blobs(prefix=folder_path))
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if blob.name.endswith(f'.{file_format}'):
                target_files.append(blob)
        
        if not target_files:
            print(f"No {file_format} files found in folder: {folder_path}")
            return
            
        print(f"\nFound {len(target_files)} {file_format} files")
        
        # Read and process files to get 10 sample records
        records_read = 0
        target_records = 10
        
        for blob in target_files:
            if records_read >= target_records:
                break
                
            print(f"\nReading file: {blob.name}")
            
            try:
                # Download file content
                file_content = blob.download_as_bytes()
                
                # Handle compression
                if compression != 'none':
                    if compression == 'gzip':
                        import gzip
                        file_content = gzip.decompress(file_content)
                    elif compression == 'zip':
                        import zipfile
                        with zipfile.ZipFile(io.BytesIO(file_content)) as zip_file:
                            # Get first file from zip
                            file_names = zip_file.namelist()
                            if file_names:
                                file_content = zip_file.read(file_names[0])
                
                # Process based on file format
                if file_format.lower() == 'csv':
                    # Read CSV data
                    df = pd.read_csv(io.BytesIO(file_content))
                    
                    # Print records
                    for index, row in df.iterrows():
                        if records_read >= target_records:
                            break
                        records_read += 1
                        print(f"\nRecord {records_read}:")
                        print(row.to_dict())
                        
                elif file_format.lower() == 'json':
                    # Read JSON data
                    json_data = json.loads(file_content.decode('utf-8'))
                    
                    # Handle different JSON structures
                    if isinstance(json_data, list):
                        # Array of objects
                        for item in json_data:
                            if records_read >= target_records:
                                break
                            records_read += 1
                            print(f"\nRecord {records_read}:")
                            print(item)
                    else:
                        # Single object
                        records_read += 1
                        print(f"\nRecord {records_read}:")
                        print(json_data)
                        
                elif file_format.lower() == 'txt':
                    # Read text file line by line
                    lines = file_content.decode('utf-8').split('\n')
                    for line in lines:
                        if records_read >= target_records:
                            break
                        if line.strip():  # Skip empty lines
                            records_read += 1
                            print(f"\nRecord {records_read}:")
                            print(line.strip())
                            
                else:
                    print(f"Unsupported file format: {file_format}")
                    
            except Exception as e:
                print(f"Error reading file {blob.name}: {str(e)}")
                continue
        
        print(f"\nSuccessfully read {records_read} records from Google Cloud Storage")
        
    except KeyError as e:
        print(f"Missing required environment variable: {e}")
    except json.JSONDecodeError as e:
        print(f"Invalid JSON in credentials: {str(e)}")
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {str(e)}")

if __name__ == "__main__":
    main()