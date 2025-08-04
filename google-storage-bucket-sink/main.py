"""
Google Cloud Storage Source Application for Quix Streams

This application reads CSV files from a Google Cloud Storage bucket and streams
the data to a Kafka topic using Quix Streams.
"""

import os
import json
import base64
import csv
import logging
import time
from io import StringIO
from typing import Dict, Optional, List, Iterator

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


class GoogleCloudStorageSource(Source):
    """
    A Quix Streams source that reads CSV files from Google Cloud Storage.
    """
    
    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        folder_path: str = "/",
        file_format: str = "csv",
        file_compression: str = "none",
        poll_interval: int = 30,
        max_messages: Optional[int] = None,
        **kwargs
    ):
        """
        Initialize the Google Cloud Storage source.
        
        Args:
            bucket_name: Name of the GCS bucket
            project_id: Google Cloud project ID
            folder_path: Path within the bucket to read files from
            file_format: Format of files to read (currently supports 'csv')
            file_compression: Compression format (currently supports 'none')
            poll_interval: How often to check for new files (seconds)
            max_messages: Maximum number of messages to process (for testing)
            **kwargs: Additional arguments passed to Source
        """
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.folder_path = folder_path.strip("/")
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.poll_interval = poll_interval
        self.max_messages = max_messages
        
        self._client = None
        self._bucket = None
        self._processed_files = set()
        self._message_count = 0
        
        logger.info(
            f"Initialized GCS Source for bucket: {bucket_name}, "
            f"folder: {folder_path if folder_path else '/'}, "
            f"format: {file_format}"
        )
    
    def _load_service_account_json(self) -> Dict:
        """Load service account JSON from environment variables."""
        raw_val = os.getenv("GS_API_KEY")
        if raw_val is None:
            raise RuntimeError("GS_API_KEY environment variable is not set")
        
        # Try direct JSON first
        if raw_val.lstrip().startswith("{"):
            try:
                return json.loads(raw_val)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Failed to parse GS_API_KEY as JSON: {e}")
        
        # Try base64 encoded JSON
        try:
            decoded = base64.b64decode(raw_val).decode()
            if decoded.lstrip().startswith("{"):
                return json.loads(decoded)
        except Exception:
            pass
        
        # Try as pointer to another environment variable
        pointed = os.getenv(raw_val)
        if pointed is None:
            raise RuntimeError(f"Pointer environment variable '{raw_val}' is not set")
        
        try:
            return json.loads(pointed)
        except json.JSONDecodeError:
            # Handle escaped newlines
            return json.loads(pointed.replace("\\n", "\n"))
    
    def setup(self) -> None:
        """Set up the GCS client and authenticate."""
        try:
            sa_info = self._load_service_account_json()
            credentials = service_account.Credentials.from_service_account_info(sa_info)
            
            # Use project_id from service account if not explicitly provided
            project_id = self.project_id or sa_info.get("project_id")
            if not project_id:
                raise RuntimeError("Project ID must be provided either explicitly or in service account JSON")
            
            self._client = storage.Client(project=project_id, credentials=credentials)
            self._bucket = self._client.bucket(self.bucket_name)
            
            # Test the connection
            try:
                self._bucket.reload()
                logger.info(f"Successfully connected to GCS bucket: {self.bucket_name}")
                if hasattr(self, 'on_client_connect_success') and callable(self.on_client_connect_success):
                    self.on_client_connect_success()
            except gcs_exceptions.NotFound:
                error_msg = f"Bucket '{self.bucket_name}' not found"
                logger.error(error_msg)
                if hasattr(self, 'on_client_connect_failure') and callable(self.on_client_connect_failure):
                    self.on_client_connect_failure(Exception(error_msg))
                raise RuntimeError(error_msg)
            except Exception as e:
                error_msg = f"Failed to access bucket '{self.bucket_name}': {e}"
                logger.error(error_msg)
                if hasattr(self, 'on_client_connect_failure') and callable(self.on_client_connect_failure):
                    self.on_client_connect_failure(e)
                raise RuntimeError(error_msg)
                
        except Exception as e:
            logger.error(f"Failed to set up GCS client: {e}")
            raise
    
    def _get_prefix(self) -> Optional[str]:
        """Get the folder prefix for listing files."""
        if not self.folder_path or self.folder_path == "/":
            return None
        return f"{self.folder_path}/"
    
    def _list_csv_files(self) -> List[storage.Blob]:
        """List all CSV files in the specified bucket/folder."""
        try:
            prefix = self._get_prefix()
            blobs = list(self._bucket.list_blobs(prefix=prefix))
            
            # Filter for CSV files that haven't been processed
            csv_files = [
                blob for blob in blobs
                if (blob.name.lower().endswith('.csv') and 
                    blob.name not in self._processed_files and
                    not blob.name.endswith('/'))  # Exclude folders
            ]
            
            logger.info(f"Found {len(csv_files)} unprocessed CSV files")
            return csv_files
            
        except Exception as e:
            logger.error(f"Failed to list files from bucket: {e}")
            raise
    
    def _read_csv_file(self, blob: storage.Blob) -> Iterator[Dict]:
        """Read and parse a CSV file from GCS."""
        try:
            logger.info(f"Reading file: {blob.name} ({blob.size} bytes)")
            
            # Download file content
            content = blob.download_as_text(encoding='utf-8')
            
            # Parse CSV
            csv_reader = csv.DictReader(StringIO(content))
            
            row_count = 0
            for row in csv_reader:
                row_count += 1
                yield {
                    'file_name': blob.name,
                    'file_size': blob.size,
                    'row_number': row_count,
                    'data': row,
                    'timestamp': int(time.time() * 1000)  # Unix timestamp in milliseconds
                }
            
            logger.info(f"Successfully processed {row_count} rows from {blob.name}")
            
        except Exception as e:
            logger.error(f"Failed to read file {blob.name}: {e}")
            raise
    
    def run(self) -> None:
        """Main execution loop for the source."""
        logger.info("Starting Google Cloud Storage source")
        
        try:
            while self.running:
                # Check if we've reached the message limit (for testing)
                if self.max_messages and self._message_count >= self.max_messages:
                    logger.info(f"Reached maximum message limit: {self.max_messages}")
                    break
                
                # List available CSV files
                csv_files = self._list_csv_files()
                
                if not csv_files:
                    logger.info("No new CSV files found, waiting...")
                    time.sleep(self.poll_interval)
                    continue
                
                # Process each file
                for blob in csv_files:
                    if not self.running:
                        break
                    
                    try:
                        # Read and stream file content
                        for row_data in self._read_csv_file(blob):
                            if not self.running:
                                break
                            
                            # Check message limit
                            if self.max_messages and self._message_count >= self.max_messages:
                                logger.info(f"Reached maximum message limit: {self.max_messages}")
                                return
                            
                            # Serialize and produce message
                            message = self.serialize(
                                key=blob.name,  # Use filename as key
                                value=row_data
                            )
                            
                            self.produce(
                                key=message.key,
                                value=message.value
                            )
                            
                            self._message_count += 1
                            
                            if self._message_count % 100 == 0:
                                logger.info(f"Processed {self._message_count} messages")
                        
                        # Mark file as processed
                        self._processed_files.add(blob.name)
                        logger.info(f"Completed processing file: {blob.name}")
                        
                    except Exception as e:
                        logger.error(f"Error processing file {blob.name}: {e}")
                        continue
                
                # Wait before next polling cycle
                if self.running:
                    logger.info(f"Waiting {self.poll_interval} seconds before next check...")
                    time.sleep(self.poll_interval)
                
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Unexpected error in source execution: {e}")
            raise
        finally:
            logger.info("Google Cloud Storage source stopped")


def main():
    """Main application entry point."""
    
    # Load configuration from environment variables
    bucket_name = os.getenv("GS_BUCKET", "quix-workflow")
    project_id = os.getenv("GS_PROJECT_ID", "quix-testing-365012")
    folder_path = os.getenv("GS_FOLDER_PATH", "/")
    file_format = os.getenv("GS_FILE_FORMAT", "csv")
    file_compression = os.getenv("GS_FILE_COMPRESSION", "none")
    output_topic = os.getenv("output", "solar-data")
    
    logger.info("Starting Google Cloud Storage to Kafka streaming application")
    logger.info(f"Configuration: bucket={bucket_name}, topic={output_topic}, folder={folder_path}")
    
    try:
        # Initialize Quix Streams application
        app = Application(
            broker_address=os.getenv("Quix__Broker__Address", "localhost:9092"),
            consumer_group="gcs-source-consumer",
            auto_offset_reset="latest"
        )
        
        # Create the source
        source = GoogleCloudStorageSource(
            name="gcs-source",
            bucket_name=bucket_name,
            project_id=project_id,
            folder_path=folder_path,
            file_format=file_format,
            file_compression=file_compression,
            poll_interval=30,
            max_messages=100  # Stop after 100 messages for testing
        )
        
        # Set up connection callbacks
        def on_connect_success():
            logger.info("✅ Successfully connected to Google Cloud Storage")
        
        def on_connect_failure(exception):
            logger.error(f"❌ Failed to connect to Google Cloud Storage: {exception}")
        
        source.on_client_connect_success = on_connect_success
        source.on_client_connect_failure = on_connect_failure
        
        # Create topic for the source
        topic = app.topic(
            name=output_topic,
            value_serializer="json",
            key_serializer="string"
        )
        
        # Create streaming dataframe
        sdf = app.dataframe(source=source, topic=topic)
        
        # Optional: Add some basic processing/logging
        sdf = sdf.update(lambda value: logger.info(f"Streaming message from file: {value.get('file_name')}") or value)
        
        # Print messages for debugging (remove in production)
        sdf.print(metadata=True)
        
        logger.info("Application configured successfully, starting processing...")
        
        # Run the application
        app.run()
        
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application failed with error: {e}")
        raise
    finally:
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    main()