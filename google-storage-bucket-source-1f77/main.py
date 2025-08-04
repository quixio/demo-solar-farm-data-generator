import os
import json
import tempfile
from google.cloud import storage
import pandas as pd
from dotenv import load_dotenv

def main():
    load_dotenv()
    
    # Get environment variables
    bucket_name = os.getenv('GS_BUCKET')
    project_id = os.getenv('GS_PROJECT_ID')
    credentials_json = os.getenv('GS_SECRET_KEY')
    folder_path = os.getenv('GS_FOLDER_PATH', '/')
    file_format = os.getenv('GS_FILE_FORMAT', 'csv')
    file_compression = os.getenv('GS_FILE_COMPRESSION', 'none')
    
    if not bucket_name or not project_id or not credentials_json:
        print("Error: Missing required environment variables")
        return
    
    client = None
    temp_file = None
    
    try:
        # Create temporary credentials file
        credentials_dict = json.loads(credentials_json)
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(credentials_dict, temp_file)
        temp_file.close()
        
        # Initialize Google Cloud Storage client
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_file.name
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        
        print(f"Connected to Google Storage bucket: {bucket_name}")
        
        # List files in the specified folder
        prefix = folder_path.lstrip('/') if folder_path != '/' else ''
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        # Filter files by format
        target_files = []
        for blob in blobs:
            if not blob.name.endswith('/'):  # Skip directories
                if file_format.lower() == 'csv' and blob.name.lower().endswith('.csv'):
                    target_files.append(blob)
                elif file_format.lower() == 'json' and blob.name.lower().endswith('.json'):
                    target_files.append(blob)
                elif file_format.lower() == 'parquet' and blob.name.lower().endswith('.parquet'):
                    target_files.append(blob)
        
        if not target_files:
            print(f"No {file_format} files found in the bucket")
            return
        
        print(f"Found {len(target_files)} {file_format} files")
        
        # Read data from the first file
        first_file = target_files[0]
        print(f"Reading from file: {first_file.name}")
        
        # Download file content
        file_content = first_file.download_as_text()
        
        records_read = 0
        
        if file_format.lower() == 'csv':
            # Create a temporary file to read CSV
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_csv:
                temp_csv.write(file_content)
                temp_csv_path = temp_csv.name
            
            try:
                df = pd.read_csv(temp_csv_path)
                print(f"CSV file has {len(df)} rows and {len(df.columns)} columns")
                print("Column names:", list(df.columns))
                
                # Print first 10 records
                for idx, row in df.head(10).iterrows():
                    print(f"Record {records_read + 1}:")
                    print(row.to_dict())
                    print("-" * 50)
                    records_read += 1
                    
            finally:
                os.unlink(temp_csv_path)
                
        elif file_format.lower() == 'json':
            lines = file_content.strip().split('\n')
            for i, line in enumerate(lines[:10]):
                if line.strip():
                    try:
                        record = json.loads(line)
                        print(f"Record {records_read + 1}:")
                        print(record)
                        print("-" * 50)
                        records_read += 1
                    except json.JSONDecodeError:
                        continue
        
        print(f"Successfully read {records_read} records from Google Storage")
        
    except Exception as e:
        print(f"Error connecting to Google Storage: {str(e)}")
    finally:
        # Cleanup
        if temp_file and os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
            del os.environ['GOOGLE_APPLICATION_CREDENTIALS']

if __name__ == "__main__":
    main()