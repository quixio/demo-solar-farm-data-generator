import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
import sys
from io import StringIO

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


def create_gcp_client():
    """
    Create a GCP Storage client using service account credentials from environment variable.
    """
    try:
        # Get credentials JSON from environment variable
        credentials_json_str = os.environ.get('GCP_CREDENTIALS_JSON')
        if not credentials_json_str:
            raise ValueError("GCP_CREDENTIALS_JSON environment variable is not set")
        
        # Parse the JSON credentials
        credentials_info = json.loads(credentials_json_str)
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        
        # Create storage client
        client = storage.Client(credentials=credentials, project=credentials_info.get('project_id'))
        
        print("✅ Successfully created GCP Storage client")
        return client
        
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing GCP credentials JSON: {e}")
        return None
    except Exception as e:
        print(f"❌ Error creating GCP Storage client: {e}")
        return None


def test_bucket_access(client, bucket_name):
    """
    Test access to the specified bucket.
    """
    try:
        bucket = client.bucket(bucket_name)
        
        # Check if bucket exists and we have access
        if not bucket.exists():
            print(f"❌ Bucket '{bucket_name}' does not exist or is not accessible")
            return None
        
        print(f"✅ Successfully accessed bucket: {bucket_name}")
        return bucket
        
    except Exception as e:
        print(f"❌ Error accessing bucket '{bucket_name}': {e}")
        return None


def read_csv_from_bucket(bucket, file_path, max_records=10):
    """
    Read CSV file from GCP Storage bucket and return sample records.
    """
    try:
        # Get the blob (file) from the bucket
        blob = bucket.blob(file_path)
        
        # Check if file exists
        if not blob.exists():
            print(f"❌ File '{file_path}' does not exist in the bucket")
            return None
        
        print(f"✅ Found file: {file_path}")
        
        # Get file metadata
        blob.reload()
        file_size = blob.size
        content_type = blob.content_type
        created = blob.time_created
        updated = blob.updated
        
        print(f"📊 File metadata:")
        print(f"   - Size: {file_size:,} bytes")
        print(f"   - Content Type: {content_type}")
        print(f"   - Created: {created}")
        print(f"   - Updated: {updated}")
        
        # Download and read the CSV content
        print("📥 Downloading file content...")
        csv_content = blob.download_as_text()
        
        # Parse CSV using pandas
        df = pd.read_csv(StringIO(csv_content))
        
        print(f"✅ Successfully parsed CSV file")
        print(f"📈 Total records in file: {len(df)}")
        print(f"📝 Columns: {list(df.columns)}")
        
        # Get sample records (first 10 or max_records)
        sample_records = df.head(max_records)
        
        return sample_records
        
    except pd.errors.EmptyDataError:
        print(f"❌ CSV file '{file_path}' is empty")
        return None
    except pd.errors.ParserError as e:
        print(f"❌ Error parsing CSV file '{file_path}': {e}")
        return None
    except Exception as e:
        print(f"❌ Error reading file '{file_path}' from bucket: {e}")
        return None


def print_sample_data(sample_data):
    """
    Print sample data in a readable format.
    """
    if sample_data is None or len(sample_data) == 0:
        print("❌ No sample data to display")
        return
    
    print(f"\n📋 Sample data ({len(sample_data)} records):")
    print("=" * 80)
    
    # Print each record
    for index, row in sample_data.iterrows():
        print(f"\n🔸 Record #{index + 1}:")
        for column, value in row.items():
            # Handle different data types for better display
            if pd.isna(value):
                value_str = "NULL"
            elif isinstance(value, float):
                value_str = f"{value:.2f}"
            else:
                value_str = str(value)
            
            print(f"   {column}: {value_str}")
        print("-" * 40)


def main():
    """
    Main function to test GCP Storage bucket CSV connection.
    """
    print("🚀 Starting GCP Storage CSV Connection Test")
    print("=" * 50)
    
    # Get required environment variables
    project_id = os.environ.get('GCP_PROJECT_ID')
    bucket_name = os.environ.get('GCP_BUCKET_NAME')
    csv_file_path = os.environ.get('GCP_CSV_FILE_PATH')
    
    # Validate required environment variables
    missing_vars = []
    if not project_id:
        missing_vars.append('GCP_PROJECT_ID')
    if not bucket_name:
        missing_vars.append('GCP_BUCKET_NAME')
    if not csv_file_path:
        missing_vars.append('GCP_CSV_FILE_PATH')
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)
    
    print(f"🔧 Configuration:")
    print(f"   - Project ID: {project_id}")
    print(f"   - Bucket Name: {bucket_name}")
    print(f"   - CSV File Path: {csv_file_path}")
    print()
    
    # Step 1: Create GCP Storage client
    print("1️⃣ Creating GCP Storage client...")
    client = create_gcp_client()
    if client is None:
        print("❌ Failed to create GCP Storage client. Exiting.")
        sys.exit(1)
    
    # Step 2: Test bucket access
    print("\n2️⃣ Testing bucket access...")
    bucket = test_bucket_access(client, bucket_name)
    if bucket is None:
        print("❌ Failed to access bucket. Exiting.")
        sys.exit(1)
    
    # Step 3: Read CSV file and get sample data
    print("\n3️⃣ Reading CSV file...")
    sample_data = read_csv_from_bucket(bucket, csv_file_path, max_records=10)
    if sample_data is None:
        print("❌ Failed to read CSV file. Exiting.")
        sys.exit(1)
    
    # Step 4: Display sample data
    print_sample_data(sample_data)
    
    print("\n🎉 Connection test completed successfully!")
    print("✅ All checks passed - ready for Quix integration")


if __name__ == "__main__":
    main()