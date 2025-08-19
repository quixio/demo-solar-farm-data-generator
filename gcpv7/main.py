"""
GCP Storage CSV Source
=====================
Quix Streams source that reads CSV data from Google Cloud Storage and publishes to a Kafka topic.
"""

import os
import json
import io
import time
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source
from typing import Optional


class GCPStorageCSVSource(Source):
    """
    A Quix Streams source that reads CSV data from Google Cloud Storage.
    """

    def __init__(
        self,
        bucket_name: str,
        csv_file_path: str,
        credentials_json: str,
        name: str = "gcp-csv-source",
        shutdown_timeout: float = 10.0,
        row_delay: float = 0.1,
    ):
        """
        Initialize the GCP Storage CSV Source.

        Args:
            bucket_name: Name of the GCP Storage bucket
            csv_file_path: Path to the CSV file within the bucket
            credentials_json: GCP service account JSON credentials as string
            name: Source name
            shutdown_timeout: Timeout for source shutdown
            row_delay: Delay between rows (to prevent overwhelming downstream systems)
        """
        super().__init__(name=name, shutdown_timeout=shutdown_timeout)
        self.bucket_name = bucket_name
        self.csv_file_path = csv_file_path
        self.credentials_json = credentials_json
        self.row_delay = row_delay
        self.client: Optional[storage.Client] = None
        self.bucket: Optional[storage.Bucket] = None

    def _load_gcp_credentials(self):
        """Load and validate GCP credentials."""
        try:
            if not self.credentials_json or not self.credentials_json.strip():
                raise ValueError("GCP credentials are empty")

            # Parse JSON credentials
            try:
                credentials_info = json.loads(self.credentials_json)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in GCP credentials: {e}")

            # Validate required fields
            required_fields = ['type', 'project_id', 'private_key_id', 'private_key', 'client_email']
            missing_fields = [field for field in required_fields if field not in credentials_info]
            if missing_fields:
                raise ValueError(f"Service account JSON missing required fields: {', '.join(missing_fields)}")

            # Create credentials object
            credentials = service_account.Credentials.from_service_account_info(
                credentials_info,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )

            self._logger.info("GCP credentials loaded successfully")
            return credentials

        except Exception as e:
            self._logger.error(f"Failed to load GCP credentials: {e}")
            raise

    def setup(self):
        """Set up the GCP Storage client and validate access."""
        try:
            # Load credentials and create client
            credentials = self._load_gcp_credentials()
            self.client = storage.Client(credentials=credentials)

            # Test bucket access
            self._logger.info(f"Testing access to bucket: {self.bucket_name}")
            self.bucket = self.client.bucket(self.bucket_name)
            self.bucket.reload()  # This will raise an exception if bucket doesn't exist or no access
            self._logger.info("Successfully connected to GCP Storage bucket")

            # Test file access
            self._logger.info(f"Testing file access: {self.csv_file_path}")
            blob = self.bucket.blob(self.csv_file_path)
            if not blob.exists():
                raise FileNotFoundError(f"CSV file '{self.csv_file_path}' not found in bucket '{self.bucket_name}'")
            self._logger.info("CSV file found in bucket")

            # Log file metadata
            blob.reload()
            size = blob.size if blob.size is not None else 0
            self._logger.info(f"File size: {size:,} bytes")

        except Exception as e:
            self._logger.error(f"Setup failed: {e}")
            raise

    def run(self):
        """Read CSV data from GCP Storage and produce to Kafka topic."""
        try:
            self._logger.info("Starting GCP CSV source...")
            
            # Get the blob and download content
            blob = self.bucket.blob(self.csv_file_path)
            self._logger.info(f"Downloading CSV file: {self.csv_file_path}")
            
            csv_content = blob.download_as_bytes()
            csv_string = csv_content.decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_string))
            
            self._logger.info(f"CSV loaded: {len(df)} rows, {len(df.columns)} columns")
            self._logger.info(f"Columns: {list(df.columns)}")

            # Process each row
            for index, row in df.iterrows():
                if not self.running:
                    self._logger.info("Source stopping...")
                    break

                try:
                    # Convert row to dictionary and handle NaN values
                    row_dict = row.to_dict()
                    
                    # Convert pandas NaN values to None (which will serialize as null in JSON)
                    for key, value in row_dict.items():
                        if pd.isna(value):
                            row_dict[key] = None

                    # Create a unique key for each row (using index)
                    message_key = f"row_{index}"
                    
                    # Add metadata
                    message = {
                        "data": row_dict,
                        "source_file": self.csv_file_path,
                        "row_number": int(index),
                        "timestamp": time.time()
                    }

                    # Serialize and produce message
                    serialized = self.serialize(key=message_key, value=message)
                    self.produce(key=serialized.key, value=serialized.value)

                    if index % 100 == 0:  # Log every 100 records
                        self._logger.info(f"Produced {index + 1} records...")

                    # Add delay to prevent overwhelming downstream systems
                    if self.row_delay > 0:
                        time.sleep(self.row_delay)

                except Exception as e:
                    self._logger.error(f"Error processing row {index}: {e}")
                    continue

            self._logger.info(f"Finished processing {len(df)} records from GCP Storage CSV")

        except Exception as e:
            self._logger.error(f"Error in run method: {e}")
            raise


def load_env_vars():
    """Load and validate environment variables."""
    bucket_name = os.environ.get('GCP_BUCKET_NAME')
    csv_file_path = os.environ.get('CSV_FILE_PATH')
    credentials_json = os.environ.get('GCP_SERVICE_ACCOUNT_KEY')
    output_topic = os.environ.get('output', 'gcp-output')

    if not bucket_name:
        raise ValueError("GCP_BUCKET_NAME environment variable is not set")
    if not csv_file_path:
        raise ValueError("CSV_FILE_PATH environment variable is not set")
    if not credentials_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY environment variable is not set")

    return bucket_name, csv_file_path, credentials_json, output_topic


def main():
    """Main function that creates and runs the Quix Streams application."""
    try:
        print("=" * 50)
        print("GCP Storage CSV Source")
        print("=" * 50)

        # Load environment variables
        bucket_name, csv_file_path, credentials_json, output_topic = load_env_vars()

        print(f"Bucket: {bucket_name}")
        print(f"CSV File: {csv_file_path}")
        print(f"Output Topic: {output_topic}")
        print()

        # Create Quix application
        app = Application()

        # Create the GCP CSV source
        source = GCPStorageCSVSource(
            bucket_name=bucket_name,
            csv_file_path=csv_file_path,
            credentials_json=credentials_json,
            name="gcp-csv-source",
            row_delay=0.01  # Small delay between rows
        )

        # Create output topic
        topic = app.topic(output_topic, value_serializer="json")

        # Create streaming dataframe with the source
        sdf = app.dataframe(topic=topic, source=source)
        
        # Log the messages being produced (optional, for debugging)
        sdf.print(metadata=True)

        # Run with limits for testing (remove these for production)
        print("Starting Quix application...")
        app.run(count=100, timeout=60)  # Process up to 100 messages or timeout after 60 seconds

    except KeyboardInterrupt:
        print("\nApplication interrupted by user")
    except Exception as e:
        print(f"Application error: {e}")
        raise


if __name__ == "__main__":
    main()