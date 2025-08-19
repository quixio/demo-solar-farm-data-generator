"""
GCP Storage CSV Connection Test
===============================
This script tests the connection to Google Cloud Storage and reads sample data from a CSV file.
This is a connection test only - no Kafka/Quix Streams integration yet.
"""

import os
import json
import io
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

def load_gcp_credentials():
    """
    Load GCP service account credentials from environment variable.
    Returns the credentials object for authenticating with Google Cloud Storage.
    """
    try:
        # Get the JSON credentials from environment variable
        credentials_json = os.environ.get('GCP_SERVICE_ACCOUNT_KEY')
        if not credentials_json:
            raise ValueError("GCP_SERVICE_ACCOUNT_KEY environment variable is not set")
        
        if not credentials_json.strip():
            raise ValueError("GCP_SERVICE_ACCOUNT_KEY environment variable is empty")
        
        # Parse JSON credentials
        try:
            credentials_info = json.loads(credentials_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in GCP_SERVICE_ACCOUNT_KEY: {e}. Please ensure the JSON is properly formatted.")
        
        # Validate required fields in service account JSON
        required_fields = ['type', 'project_id', 'private_key_id', 'private_key', 'client_email']
        missing_fields = [field for field in required_fields if field not in credentials_info]
        if missing_fields:
            raise ValueError(f"Service account JSON missing required fields: {', '.join(missing_fields)}")
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        
        print("✓ GCP credentials loaded successfully")
        return credentials
        
    except ValueError:
        # Re-raise ValueError as is
        raise
    except Exception as e:
        raise ValueError(f"Failed to load GCP credentials: {e}")

def test_gcp_storage_connection():
    """
    Test connection to Google Cloud Storage and read sample CSV data.
    """
    try:
        print("=" * 50)
        print("GCP Storage CSV Connection Test")
        print("=" * 50)
        
        # Load environment variables
        bucket_name = os.environ.get('GCP_BUCKET_NAME')
        csv_file_path = os.environ.get('CSV_FILE_PATH')
        
        if not bucket_name:
            raise ValueError("GCP_BUCKET_NAME environment variable is not set")
        if not csv_file_path:
            raise ValueError("CSV_FILE_PATH environment variable is not set")
            
        print(f"Bucket: {bucket_name}")
        print(f"CSV File: {csv_file_path}")
        print()
        
        # Load credentials and create client
        credentials = load_gcp_credentials()
        client = storage.Client(credentials=credentials)
        
        # Test bucket access
        print("Testing bucket access...")
        try:
            bucket = client.bucket(bucket_name)
            bucket.reload()  # This will raise an exception if bucket doesn't exist or no access
            print("✓ Successfully connected to GCP Storage bucket")
        except Exception as e:
            raise Exception(f"Failed to access bucket '{bucket_name}': {e}")
        
        # Test file access
        print(f"Testing file access: {csv_file_path}")
        try:
            blob = bucket.blob(csv_file_path)
            if not blob.exists():
                raise FileNotFoundError(f"CSV file '{csv_file_path}' not found in bucket '{bucket_name}'")
            print("✓ CSV file found in bucket")
        except Exception as e:
            raise Exception(f"Failed to access file '{csv_file_path}': {e}")
        
        # Get file metadata
        print("\nFile Metadata:")
        try:
            # Reload blob to ensure we have all metadata
            blob.reload()
            
            # Safe formatting with None checks
            size = blob.size if blob.size is not None else 0
            content_type = blob.content_type if blob.content_type is not None else "Unknown"
            updated = blob.updated if blob.updated is not None else "Unknown"
            
            print(f"  Size: {size:,} bytes")
            print(f"  Content Type: {content_type}")
            print(f"  Last Modified: {updated}")
        except Exception as e:
            print(f"  Warning: Could not retrieve metadata: {e}")
            print(f"  Size: Unknown")
            print(f"  Content Type: Unknown")
            print(f"  Last Modified: Unknown")
        
        # Read and parse CSV data
        print("\nReading CSV data...")
        try:
            # Download file content as bytes
            csv_content = blob.download_as_bytes()
            
            # Convert bytes to string and read with pandas
            csv_string = csv_content.decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_string))
            
            print("✓ CSV data loaded successfully")
            print(f"  Total rows: {len(df):,}")
            print(f"  Total columns: {len(df.columns)}")
            print(f"  Column names: {list(df.columns)}")
            
        except Exception as e:
            raise Exception(f"Failed to read CSV data: {e}")
        
        # Display sample data (first 10 records)
        print("\n" + "=" * 50)
        print("SAMPLE DATA (First 10 records)")
        print("=" * 50)
        
        sample_size = min(10, len(df))
        for i in range(sample_size):
            try:
                row = df.iloc[i]
                print(f"\nRecord {i + 1}:")
                for col in df.columns:
                    try:
                        value = row[col]
                        # Format the output nicely - handle None/NaN values safely
                        if pd.isna(value) or value is None:
                            formatted_value = "null"
                        elif isinstance(value, float):
                            # Check if the float can be converted to int without losing precision
                            try:
                                formatted_value = f"{value:.6f}" if value != int(value) else str(int(value))
                            except (ValueError, OverflowError):
                                formatted_value = str(value)
                        else:
                            formatted_value = str(value) if value is not None else "null"
                        print(f"  {col}: {formatted_value}")
                    except Exception as e:
                        print(f"  {col}: <Error formatting value: {e}>")
            except Exception as e:
                print(f"Error processing record {i + 1}: {e}")
        
        print("\n" + "=" * 50)
        print("CONNECTION TEST SUMMARY")
        print("=" * 50)
        print("✓ GCP credentials authentication: SUCCESS")
        print("✓ Bucket access: SUCCESS")
        print("✓ File access: SUCCESS")
        print("✓ CSV data parsing: SUCCESS")
        print(f"✓ Sample data retrieved: {sample_size} records")
        print()
        print("Connection test completed successfully!")
        print("Ready for Quix Streams integration.")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Connection test failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Verify GCP_SERVICE_ACCOUNT_KEY contains valid JSON credentials")
        print("2. Ensure the service account has Storage Object Viewer permissions")
        print("3. Check that GCP_BUCKET_NAME is correct and accessible")
        print("4. Verify CSV_FILE_PATH points to an existing file in the bucket")
        print("5. Confirm the CSV file is properly formatted")
        return False

def main():
    """
    Main function to run the GCP Storage connection test.
    """
    try:
        # Test connection
        success = test_gcp_storage_connection()
        
        if success:
            print("\n🎉 All tests passed! GCP Storage connection is working properly.")
        else:
            print("\n💥 Tests failed. Please check your configuration.")
            
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user.")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")

if __name__ == "__main__":
    main()