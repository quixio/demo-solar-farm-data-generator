"""
Google Cloud Storage Bucket Source for Quix Streams

This application reads CSV files from a Google Cloud Storage bucket
and streams the data to a Kafka topic using Quix Streams.
"""

import os
import json
import base64
import csv
import logging
import time
from io import StringIO
from typing import Dict, Optional, Iterator, List

from google.cloud import storage
from google.oauth2 import service_account
from google.api_core import exceptions as gcs_exceptions
from quixstreams import Application
from quixstreams.sources.base import Source

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GoogleStorageBucketSource(Source):
    """
    A Quix Streams source that reads CSV files from Google Cloud Storage
    and produces messages to a Kafka topic.
    """
    
    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        service_account_key: str,
        folder_path: str = "/",
        file_format: str = "csv",
        file_compression: str = "none",
        region: str = "us-central1",
        name: str = "google-storage-bucket-source",
        shutdown_timeout: int = 10,
        batch_size: int = 100,
        max_messages: Optional[int] = None,
    ):
        """
        Initialize the Google Storage Bucket Source.
        
        Args:
            bucket_name: Name of the GCS bucket
            project_id: Google Cloud project ID
            service_account_key: Service account JSON key (raw JSON, base64, or env var name)
            folder_path: Path within the bucket to read from
            file_format: Format of files to read (currently supports 'csv')
            file_compression: Compression type (currently supports 'none')
            region: GCS region
            name: Name of the source
            shutdown_timeout: Timeout for graceful shutdown
            batch_size: Number of records to process in each batch
            max_messages: Maximum number of messages to process (for testing)
        """
        super().__init__(name, shutdown_timeout)
        
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.service_account_key = service_account_key
        self.folder_path = folder_path.strip("/")
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.region = region
        self.batch_size = batch_size
        self.max_messages = max_messages
        
        self._client: Optional[storage.Client] = None
        self._bucket: Optional[storage.Bucket] = None
        self._processed_files: set = set()
        self._message_count = 0
        
        # Validate parameters
        if self.file_format != "csv":
            raise ValueError(f"Unsupported file format: {self.file_format}")
        
        if self.file_compression != "none":
            raise ValueError(f"Unsupported compression: {self.file_compression}")

    def _load_service_account_json(self) -> Dict:
        """Load and parse the service account JSON from various formats."""
        raw_val = self.service_account_key
        if not raw_val:
            raise RuntimeError("Service account key is not provided")

        # Check if it's already JSON
        if raw_val.lstrip().startswith("{"):
            return json.loads(raw_val)

        # Try base64 decoding
        try:
            decoded = base64.b64decode(raw_val).decode()
            if decoded.lstrip().startswith("{"):
                return json.loads(decoded)
        except Exception:
            pass

        # Try as environment variable reference
        env_val = os.getenv(raw_val)
        if env_val is None:
            raise RuntimeError(f"Environment variable '{raw_val}' is not set")

        try:
            return json.loads(env_val)
        except json.JSONDecodeError:
            # Handle escaped newlines
            return json.loads(env_val.replace("\\n", "\n"))

    def _initialize_gcs_client(self) -> None:
        """Initialize the Google Cloud Storage client."""
        try:
            sa_info = self._load_service_account_json()
            credentials = service_account.Credentials.from_service_account_info(sa_info)
            project = self.project_id or sa_info.get("project_id")
            
            self._client = storage.Client(project=project, credentials=credentials)
            self._bucket = self._client.bucket(self.bucket_name)
            
            logger.info(f"✓ GCS client initialized for project {project}, bucket {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {e}")
            raise

    def _get_file_prefix(self) -> Optional[str]:
        """Get the folder prefix for listing files."""
        if not self.folder_path or self.folder_path == "/":
            return None
        return f"{self.folder_path}/"

    def _list_csv_files(self) -> List[storage.Blob]:
        """List all CSV files in the specified bucket path."""
        try:
            prefix = self._get_file_prefix()
            blobs = list(self._bucket.list_blobs(prefix=prefix))
            
            csv_files = [
                blob for blob in blobs 
                if blob.name.lower().endswith('.csv') and blob.name not in self._processed_files
            ]
            
            logger.info(f"Found {len(csv_files)} unprocessed CSV files")
            return csv_files
            
        except gcs_exceptions.NotFound:
            logger.error(f"Bucket '{self.bucket_name}' not found")
            raise
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            raise

    def _read_csv_file(self, blob: storage.Blob) -> Iterator[Dict]:
        """Read and parse a CSV file from GCS."""
        try:
            logger.info(f"Processing file: {blob.name} ({blob.size} bytes)")
            
            # Download file content as text
            content = blob.download_as_text(encoding='utf-8')
            
            # Parse CSV
            csv_reader = csv.DictReader(StringIO(content))
            
            row_count = 0
            for row in csv_reader:
                if not self.running:
                    break
                    
                row_count += 1
                
                # Add metadata to each row
                enriched_row = {
                    **row,
                    '_metadata': {
                        'source_file': blob.name,
                        'file_size': blob.size,
                        'bucket': self.bucket_name,
                        'row_number': row_count,
                        'processed_at': time.time()
                    }
                }
                
                yield enriched_row
                
                # Check message limit
                if self.max_messages and self._message_count >= self.max_messages:
                    logger.info(f"Reached maximum message count: {self.max_messages}")
                    return
            
            logger.info(f"Completed processing file: {blob.name} ({row_count} rows)")
            
        except Exception as e:
            logger.error(f"Error reading CSV file {blob.name}: {e}")
            raise

    def setup(self) -> None:
        """Set up the GCS connection and test connectivity."""
        try:
            self._initialize_gcs_client()
            
            # Test bucket access
            try:
                self._bucket.reload()
                logger.info("✓ Successfully connected to GCS bucket")
                
                # Call success callback if available
                if hasattr(self, '_on_client_connect_success') and self._on_client_connect_success:
                    self._on_client_connect_success()
                    
            except gcs_exceptions.NotFound:
                error_msg = f"Bucket '{self.bucket_name}' not found or access denied"
                logger.error(error_msg)
                
                # Call failure callback if available
                if hasattr(self, '_on_client_connect_failure') and self._on_client_connect_failure:
                    self._on_client_connect_failure(Exception(error_msg))
                raise
                
        except Exception as e:
            logger.error(f"Failed to setup GCS connection: {e}")
            # Call failure callback if available
            if hasattr(self, '_on_client_connect_failure') and self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def run(self) -> None:
        """Main execution loop that processes CSV files from GCS."""
        logger.info("Starting Google Storage Bucket source")
        
        try:
            # Setup GCS connection
            self.setup()
            
            while self.running:
                # Get list of CSV files to process
                csv_files = self._list_csv_files()
                
                if not csv_files:
                    logger.info("No new CSV files found, waiting...")
                    time.sleep(5)
                    continue
                
                # Process each file
                for blob in csv_files:
                    if not self.running:
                        break
                        
                    try:
                        # Process file in batches
                        batch = []
                        
                        for row_data in self._read_csv_file(blob):
                            if not self.running:
                                break
                                
                            batch.append(row_data)
                            
                            # Process batch when it reaches the specified size
                            if len(batch) >= self.batch_size:
                                self._process_batch(batch, blob.name)
                                batch = []
                                
                            self._message_count += 1
                            
                            # Check if we've reached the message limit
                            if self.max_messages and self._message_count >= self.max_messages:
                                logger.info(f"Reached maximum message count: {self.max_messages}")
                                self.stop()
                                break
                        
                        # Process remaining messages in batch
                        if batch and self.running:
                            self._process_batch(batch, blob.name)
                        
                        # Mark file as processed
                        self._processed_files.add(blob.name)
                        
                    except Exception as e:
                        logger.error(f"Error processing file {blob.name}: {e}")
                        continue
                
                # If no files to process and we have a message limit, stop
                if self.max_messages and self._message_count >= self.max_messages:
                    logger.info("Completed processing all files within message limit")
                    break
                    
                # Brief pause before checking for new files
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in main processing loop: {e}")
            raise
        finally:
            logger.info(f"Google Storage Bucket source stopped. Processed {self._message_count} messages")

    def _process_batch(self, batch: List[Dict], filename: str) -> None:
        """Process a batch of messages by sending them to Kafka."""
        try:
            for row_data in batch:
                # Create a meaningful key based on file and row
                key = f"{filename}_{row_data['_metadata']['row_number']}"
                
                # Serialize the message
                message = self.serialize(key=key, value=row_data)
                
                # Send to Kafka
                self.produce(
                    key=message.key,
                    value=message.value,
                    headers={"source": "google-storage-bucket"}
                )
            
            logger.debug(f"Processed batch of {len(batch)} messages from {filename}")
            
        except Exception as e:
            logger.error(f"Error processing batch from {filename}: {e}")
            raise

    def default_topic(self) -> str:
        """Return the default topic name for this source."""
        return f"source__{self.name}"


def get_env_var(key: str, default: str = None) -> str:
    """Get environment variable with error handling."""
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"Environment variable '{key}' is required but not set")
    return value


def main():
    """Main application entry point."""
    logger.info("Starting Google Storage Bucket Source Application")
    
    try:
        # Load configuration from environment variables
        config = {
            'bucket_name': get_env_var('GS_BUCKET'),
            'project_id': get_env_var('GS_PROJECT_ID'),
            'service_account_key': get_env_var('GS_SECRET_KEY'),
            'folder_path': get_env_var('GS_FOLDER_PATH', '/'),
            'file_format': get_env_var('GS_FILE_FORMAT', 'csv'),
            'file_compression': get_env_var('GS_FILE_COMPRESSION', 'none'),
            'region': get_env_var('GS_REGION', 'us-central1'),
        }
        
        output_topic_name = get_env_var('output', 'output')
        
        # Create Quix Streams application
        app = Application(
            broker_address=os.getenv('Quix__Kafka__Broker__Address', 'localhost:9092'),
            consumer_group='google-storage-bucket-source-draft-84cc',
            auto_offset_reset='earliest',
        )
        
        # Create the source
        source = GoogleStorageBucketSource(
            name="google-storage-bucket-source",
            max_messages=100,  # Limit for testing - remove for production
            **config
        )
        
        # Create output topic
        output_topic = app.topic(
            name=output_topic_name,
            value_serializer='json',
            key_serializer='string'
        )
        
        # Set up connection callbacks
        def on_connect_success():
            logger.info("✓ Successfully connected to Google Cloud Storage")
        
        def on_connect_failure(exception):
            logger.error(f"✗ Failed to connect to Google Cloud Storage: {exception}")
        
        source.on_client_connect_success = on_connect_success
        source.on_client_connect_failure = on_connect_failure
        
        # Create streaming dataframe
        sdf = app.dataframe(source=source, topic=output_topic)
        
        # Add some basic processing and logging
        sdf = sdf.update(lambda row: logger.info(f"Processing message from file: {row.get('_metadata', {}).get('source_file', 'unknown')}") or row)
        
        # Print messages for debugging (remove for production)
        sdf.print(metadata=True)
        
        logger.info(f"Starting application - will read from GCS bucket '{config['bucket_name']}' and write to topic '{output_topic_name}'")
        
        # Run the application
        app.run()
        
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise
    finally:
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    main()