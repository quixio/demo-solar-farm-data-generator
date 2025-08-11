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
import io

def test_google_storage_connection():
    """Test connection to Google Cloud Storage and read 10 sample records."""
    
    # Load environment variables
    load_dotenv()
    
    try:
        # Get configuration from environment variables
        bucket_name = os.environ['GOOGLE_STORAGE_BUCKET']
        folder_path = os.environ['GOOGLE_STORAGE_FOLDER_PATH']
        file_format = os.environ['GOOGLE_STORAGE_FILE_FORMAT']
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
        
        print("Successfully connected to Google Cloud Storage!")
        
        # List blobs in the specified folder path
        folder_prefix = folder_path.strip('/')
        if folder_prefix and not folder_prefix.endswith('/'):
            folder_prefix += '/'
        
        blobs = list(bucket.list_blobs(prefix=folder_prefix if folder_prefix != '/' else None))
        
        if not blobs:
            print("No files found in the specified folder path")
            return
            
        # Filter blobs by file format
        matching_blobs = []
        for blob in blobs:
            if blob.name.lower().endswith(f'.{file_format.lower()}'):
                matching_blobs.append(blob)
        
        if not matching_blobs:
            print(f"No {file_format} files found in the specified folder path")
            return
            
        print(f"Found {len(matching_blobs)} {file_format} file(s)")
        
        # Read from the first matching file
        first_blob = matching_blobs[0]
        print(f"Reading from file: {first_blob.name}")
        
        # Download the file content
        file_content = first_blob.download_as_text()
        
        # Process based on file format
        records_read = 0
        max_records = 10
        
        if file_format.lower() == 'csv':
            # Use pandas to read CSV
            df = pd.read_csv(io.StringIO(file_content))
            
            print(f"\nCSV file has {len(df)} rows and {len(df.columns)} columns")
            print(f"Columns: {list(df.columns)}")
            print("\n--- Sample Records (first 10) ---")
            
            for index, row in df.head(max_records).iterrows():
                records_read += 1
                print(f"Record {records_read}:")
                for col in df.columns:
                    print(f"  {col}: {row[col]}")
                print("-" * 40)
                
        elif file_format.lower() == 'json':
            # Process JSON file
            try:
                # Try to parse as JSON Lines format first
                lines = file_content.strip().split('\n')
                json_records = []
                
                for line in lines:
                    if line.strip():
                        try:
                            json_records.append(json.loads(line))
                        except json.JSONDecodeError:
                            # If JSON Lines fails, try parsing as single JSON array
                            json_records = json.loads(file_content)
                            break
                
                print(f"\nJSON file has {len(json_records)} records")
                print("\n--- Sample Records (first 10) ---")
                
                for i, record in enumerate(json_records[:max_records]):
                    records_read += 1
                    print(f"Record {records_read}:")
                    print(f"  {json.dumps(record, indent=2)}")
                    print("-" * 40)
                    
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON file: {e}")
                return
                
        else:
            # For other formats, read as text lines
            lines = file_content.strip().split('\n')
            print(f"\nText file has {len(lines)} lines")
            print("\n--- Sample Records (first 10 lines) ---")
            
            for i, line in enumerate(lines[:max_records]):
                records_read += 1
                print(f"Record {records_read}: {line}")
                print("-" * 40)
        
        print(f"\nSuccessfully read {records_read} sample records from Google Cloud Storage!")
        
    except KeyError as e:
        print(f"Missing required environment variable: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing credentials JSON: {e}")
    except Exception as e:
        print(f"Error connecting to Google Cloud Storage: {e}")
    finally:
        print("Connection test completed.")

if __name__ == "__main__":
    test_google_storage_connection()