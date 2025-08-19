"""
GCP Cloud Storage CSV Connection Test

This script tests the connection to a CSV file stored in Google Cloud Storage.
It reads the first 10 records and displays them for verification.

This is a CONNECTION TEST ONLY - no Kafka/Quix Streams integration yet.
"""

import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from io import StringIO

def test_gcp_storage_connection():
    """
    Test connection to GCP Storage and read sample CSV data.
    """
    print("=== GCP Cloud Storage CSV Connection Test ===")
    
    try:
        # Get environment variables
        service_account_key_json = os.environ.get('GCP_SERVICE_ACCOUNT_KEY')
        bucket_name = os.environ.get('GCP_BUCKET_NAME')
        csv_file_path = os.environ.get('CSV_FILE_PATH')
        
        if not service_account_key_json:
            raise ValueError("GCP_SERVICE_ACCOUNT_KEY environment variable is required")
        if not bucket_name:
            raise ValueError("GCP_BUCKET_NAME environment variable is required")
        if not csv_file_path:
            raise ValueError("CSV_FILE_PATH environment variable is required")
        
        print(f"✓ Environment variables loaded:")
        print(f"  - Bucket: {bucket_name}")
        print(f"  - CSV file: {csv_file_path}")
        
        # Parse service account credentials from JSON string
        try:
            credentials_info = json.loads(service_account_key_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            print("✓ Service account credentials parsed successfully")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in GCP_SERVICE_ACCOUNT_KEY: {e}")
        except Exception as e:
            raise ValueError(f"Failed to create credentials: {e}")
        
        # Initialize the Storage client
        client = storage.Client(credentials=credentials, project=credentials_info.get('project_id'))
        print("✓ GCP Storage client initialized")
        
        # Get the bucket
        try:
            bucket = client.bucket(bucket_name)
            # Test if bucket exists by trying to get its metadata
            bucket.reload()
            print(f"✓ Successfully connected to bucket '{bucket_name}'")
        except Exception as e:
            raise ConnectionError(f"Failed to access bucket '{bucket_name}': {e}")
        
        # Get the CSV file blob
        try:
            blob = bucket.blob(csv_file_path)
            if not blob.exists():
                raise FileNotFoundError(f"CSV file '{csv_file_path}' not found in bucket '{bucket_name}'")
            print(f"✓ CSV file '{csv_file_path}' found in bucket")
        except Exception as e:
            if "not found" not in str(e).lower():
                raise ConnectionError(f"Failed to check CSV file existence: {e}")
            raise
        
        # Download and read the CSV file
        try:
            print(f"📥 Downloading CSV file...")
            csv_content = blob.download_as_text()
            print(f"✓ CSV file downloaded successfully ({len(csv_content)} characters)")
            
            # Parse CSV content with pandas
            csv_data = pd.read_csv(StringIO(csv_content))
            total_records = len(csv_data)
            print(f"✓ CSV parsed successfully - {total_records} total records found")
            
            # Display file metadata
            print(f"\n📊 File Metadata:")
            print(f"  - File size: {blob.size} bytes")
            print(f"  - Content type: {blob.content_type}")
            print(f"  - Last modified: {blob.updated}")
            print(f"  - Total rows: {total_records}")
            print(f"  - Columns: {list(csv_data.columns)}")
            
        except Exception as e:
            raise RuntimeError(f"Failed to download or parse CSV file: {e}")
        
        # Display first 10 records (or all if less than 10)
        sample_size = min(10, total_records)
        print(f"\n📋 Sample Data (first {sample_size} records):")
        print("=" * 60)
        
        for i in range(sample_size):
            record = csv_data.iloc[i]
            print(f"\n🔢 Record {i + 1}:")
            for column, value in record.items():
                # Handle different data types for display
                if pd.isna(value):
                    display_value = "NULL"
                elif isinstance(value, (int, float)):
                    display_value = str(value)
                else:
                    display_value = f'"{str(value)}"'
                print(f"  {column}: {display_value}")
        
        print(f"\n✅ CONNECTION TEST SUCCESSFUL!")
        print(f"Successfully read {sample_size} sample records from GCP Storage CSV file.")
        print(f"The data structure has been verified and is ready for Kafka integration.")
        
        return True
        
    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
        return False
    except ConnectionError as e:
        print(f"❌ Connection Error: {e}")
        return False
    except FileNotFoundError as e:
        print(f"❌ File Not Found: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        return False

def main():
    """
    Main function to run the connection test.
    """
    success = test_gcp_storage_connection()
    
    if not success:
        print(f"\n💡 Troubleshooting Tips:")
        print(f"  1. Verify your GCP service account key is valid JSON")
        print(f"  2. Ensure the service account has 'Storage Object Viewer' permission")
        print(f"  3. Check that the bucket name is correct and accessible")
        print(f"  4. Verify the CSV file path exists in the bucket")
        print(f"  5. Make sure the CSV file is not empty and properly formatted")
        
        exit(1)

if __name__ == "__main__":
    main()