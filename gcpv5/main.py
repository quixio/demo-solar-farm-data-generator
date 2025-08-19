# Cached connection test code for read a csv file from a gcp storage bucket
# Generated on 2025-08-19 16:15:07
# Template: Unknown
# This is cached code - delete this file to force regeneration

import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from io import StringIO

def test_gcp_storage_connection():
    """
    Test connection to Google Cloud Storage and read CSV file.
    This is a connection test only - no Kafka integration yet.
    """
    
    print("=" * 60)
    print("GCP Storage CSV Connection Test")
    print("=" * 60)
    
    try:
        # Get environment variables
        project_id = os.environ.get('GCP_PROJECT_ID')
        bucket_name = os.environ.get('GCS_BUCKET_NAME')
        csv_file_path = os.environ.get('GCS_CSV_FILE_PATH')
        service_account_json = os.environ.get('GCP_SERVICE_ACCOUNT_KEY')
        
        # Validate required environment variables
        if not project_id:
            raise ValueError("GCP_PROJECT_ID environment variable is required")
        if not bucket_name:
            raise ValueError("GCS_BUCKET_NAME environment variable is required")
        if not csv_file_path:
            raise ValueError("GCS_CSV_FILE_PATH environment variable is required")
        if not service_account_json:
            raise ValueError("GCP_SERVICE_ACCOUNT_KEY environment variable is required")
        
        print(f"✓ Project ID: {project_id}")
        print(f"✓ Bucket Name: {bucket_name}")
        print(f"✓ CSV File Path: {csv_file_path}")
        print("✓ Service Account credentials loaded")
        print()
        
        # Parse service account credentials
        try:
            credentials_dict = json.loads(service_account_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            print("✓ Service Account credentials parsed successfully")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in GCP_SERVICE_ACCOUNT_KEY: {e}")
        except Exception as e:
            raise ValueError(f"Failed to create credentials: {e}")
        
        # Initialize GCS client
        print("Connecting to Google Cloud Storage...")
        try:
            client = storage.Client(project=project_id, credentials=credentials)
            print("✓ GCS Client initialized successfully")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize GCS client: {e}")
        
        # Get bucket
        print(f"Accessing bucket '{bucket_name}'...")
        try:
            bucket = client.bucket(bucket_name)
            # Test bucket access by checking if it exists
            bucket.reload()
            print(f"✓ Successfully accessed bucket '{bucket_name}'")
        except Exception as e:
            raise ConnectionError(f"Failed to access bucket '{bucket_name}': {e}")
        
        # Get CSV file blob
        print(f"Accessing CSV file '{csv_file_path}'...")
        try:
            blob = bucket.blob(csv_file_path)
            if not blob.exists():
                raise FileNotFoundError(f"CSV file '{csv_file_path}' not found in bucket '{bucket_name}'")
            print(f"✓ Successfully found CSV file '{csv_file_path}'")
            
            # Get file metadata
            blob.reload()
            file_size = blob.size
            created_time = blob.time_created
            updated_time = blob.updated
            content_type = blob.content_type
            
            print(f"  • File size: {file_size:,} bytes")
            print(f"  • Content type: {content_type}")
            print(f"  • Created: {created_time}")
            print(f"  • Last updated: {updated_time}")
            
        except Exception as e:
            raise ConnectionError(f"Failed to access CSV file '{csv_file_path}': {e}")
        
        # Download and read CSV content
        print("\nDownloading and parsing CSV content...")
        try:
            # Download the file content as string
            csv_content = blob.download_as_text()
            print(f"✓ Successfully downloaded CSV content ({len(csv_content)} characters)")
            
            # Parse CSV using pandas
            df = pd.read_csv(StringIO(csv_content))
            total_rows = len(df)
            total_columns = len(df.columns)
            
            print(f"✓ Successfully parsed CSV: {total_rows} rows, {total_columns} columns")
            print(f"✓ Column names: {list(df.columns)}")
            
        except Exception as e:
            raise ValueError(f"Failed to download or parse CSV content: {e}")
        
        # Display sample data (first 10 rows)
        print("\n" + "=" * 60)
        print("SAMPLE DATA (First 10 rows)")
        print("=" * 60)
        
        sample_size = min(10, total_rows)
        sample_data = df.head(sample_size)
        
        # Display data in a formatted way
        for idx, (_, row) in enumerate(sample_data.iterrows(), 1):
            print(f"\nRecord {idx}:")
            print("-" * 20)
            for col_name, value in row.items():
                # Handle different data types for clean display
                if pd.isna(value):
                    display_value = "NULL"
                elif isinstance(value, float):
                    display_value = f"{value:.6g}"
                else:
                    display_value = str(value)
                print(f"  {col_name}: {display_value}")
        
        # Summary
        print("\n" + "=" * 60)
        print("CONNECTION TEST SUMMARY")
        print("=" * 60)
        print(f"✓ Successfully connected to GCP Storage")
        print(f"✓ Successfully accessed bucket: {bucket_name}")
        print(f"✓ Successfully read CSV file: {csv_file_path}")
        print(f"✓ Total rows available: {total_rows:,}")
        print(f"✓ Total columns: {total_columns}")
        print(f"✓ Sample records displayed: {sample_size}")
        print(f"✓ Data types detected: {dict(df.dtypes)}")
        print("✓ Connection test completed successfully!")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Connection test failed: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        return False

def main():
    """
    Main function to run the connection test.
    This is a connection test only - no Kafka integration yet.
    """
    
    # Run connection test
    success = test_gcp_storage_connection()
    
    if success:
        print(f"\n🎉 Connection test passed! Ready for Quix Streams integration.")
        exit(0)
    else:
        print(f"\n💥 Connection test failed. Please check your configuration.")
        exit(1)

if __name__ == "__main__":
    main()