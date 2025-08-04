# DEPENDENCIES:
# pip install google-cloud-storage
# pip install pandas
# END_DEPENDENCIES

import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from io import StringIO
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_gcp_credentials():
    """Get GCP credentials from secret."""
    try:
        # Get the secret name from environment variable
        secret_key = os.getenv('GS_SECRET_KEY')
        if not secret_key:
            raise ValueError("GS_SECRET_KEY environment variable not found")
        
        # Read credentials from the secret (assuming it's already loaded into environment)
        credentials_json = os.getenv(secret_key)
        if not credentials_json:
            raise ValueError(f"Secret {secret_key} not found in environment")
        
        # Parse JSON credentials
        credentials_dict = json.loads(credentials_json)
        
        # Create service account credentials
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        return credentials
    except Exception as e:
        logger.error(f"Error getting GCP credentials: {e}")
        raise

def list_bucket_files(client, bucket_name, folder_path, file_format):
    """List files in the bucket with specified format."""
    try:
        bucket = client.bucket(bucket_name)
        
        # Remove leading slash from folder path if present
        prefix = folder_path.lstrip('/')
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        
        # List blobs with prefix
        blobs = bucket.list_blobs(prefix=prefix)
        
        # Filter by file format
        matching_files = []
        for blob in blobs:
            if blob.name.lower().endswith(f'.{file_format.lower()}'):
                matching_files.append(blob)
        
        return matching_files
    except Exception as e:
        logger.error(f"Error listing bucket files: {e}")
        raise

def read_csv_sample(client, bucket_name, blob_name, max_records=10):
    """Read sample records from a CSV file in GCS."""
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        # Download the blob content as text
        content = blob.download_as_text()
        
        # Read CSV content into pandas DataFrame
        df = pd.read_csv(StringIO(content))
        
        # Return first max_records rows
        return df.head(max_records)
    except Exception as e:
        logger.error(f"Error reading CSV file {blob_name}: {e}")
        raise

def main():
    """Main function to test Google Storage Bucket connection."""
    try:
        # Get environment variables
        bucket_name = os.getenv('GS_BUCKET')
        region = os.getenv('GS_REGION')
        folder_path = os.getenv('GS_FOLDER_PATH', '/')
        file_format = os.getenv('GS_FILE_FORMAT', 'csv')
        project_id = os.getenv('GS_PROJECT_ID')
        
        logger.info(f"Connecting to Google Storage Bucket: {bucket_name}")
        logger.info(f"Region: {region}")
        logger.info(f"Project ID: {project_id}")
        logger.info(f"Folder Path: {folder_path}")
        logger.info(f"File Format: {file_format}")
        
        # Get credentials
        credentials = get_gcp_credentials()
        
        # Create storage client
        client = storage.Client(credentials=credentials, project=project_id)
        
        # Test bucket access
        bucket = client.bucket(bucket_name)
        if not bucket.exists():
            raise ValueError(f"Bucket {bucket_name} does not exist or is not accessible")
        
        logger.info(f"Successfully connected to bucket: {bucket_name}")
        
        # List files in the specified folder with the specified format
        files = list_bucket_files(client, bucket_name, folder_path, file_format)
        
        if not files:
            logger.warning(f"No {file_format} files found in {folder_path}")
            return
        
        logger.info(f"Found {len(files)} {file_format} files")
        
        # Read sample data from the first file
        first_file = files[0]
        logger.info(f"Reading sample data from: {first_file.name}")
        
        if file_format.lower() == 'csv':
            sample_data = read_csv_sample(client, bucket_name, first_file.name, max_records=10)
            
            logger.info("Sample data (first 10 records):")
            logger.info("-" * 50)
            
            # Print each record
            for index, row in sample_data.iterrows():
                print(f"Record {index + 1}:")
                for column, value in row.items():
                    print(f"  {column}: {value}")
                print("-" * 30)
        else:
            # For non-CSV files, just show file info
            logger.info(f"File: {first_file.name}")
            logger.info(f"Size: {first_file.size} bytes")
            logger.info(f"Updated: {first_file.updated}")
            logger.info("Note: Sample data reading is currently only supported for CSV files")
        
        logger.info("Connection test completed successfully!")
        
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        raise
    finally:
        logger.info("Connection test finished")

if __name__ == "__main__":
    main()