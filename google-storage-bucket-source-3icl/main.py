"""
Google Storage Bucket Source for Quix Streams

This application reads CSV files from a Google Storage Bucket and streams them to a Kafka topic.
It includes proper error handling, connection management, and graceful shutdown capabilities.
"""

import os
import json
import base64
import csv
import logging
import time
from io import StringIO
from typing import Dict, Optional, List, Any
from dataclasses import dataclass

from google.cloud import storage
from google.oauth2 import service_account
from google.api_core import exceptions as gcs_exceptions

from quixstreams import Application
from quixstreams.sources.base import Source

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class GCSConfig:
    """Configuration for Google Cloud Storage connection."""
    bucket: str
    project_id: str
    folder_path: str = "/"
    file_format: str = "csv"
    file_compression: str = "none"
    region: str = "us-central1"
    api_key_env: str = "GS_API_KEY"


class GCSSource(Source):
    """
    A Quix Streams source that reads CSV files from Google Cloud Storage.
    
    This source:
    1. Connects to GCS using service account credentials
    2. Lists and processes CSV files from the specified bucket/folder
    3. Streams row data to Kafka with proper error handling
    4. Supports graceful shutdown and connection management
    """
    
    def __init__(self, config: GCSConfig, name: str = "gcs-source"):
        super().__init__(name=name)
        self.config = config
        self.client: Optional[storage.Client] = None
        self.bucket: Optional[storage.Bucket] = None
        self.processed_files: set = set()
        self.message_count = 0
        self.max_messages = 100  # Stop after 100 messages for testing
        
    def setup(self) -> bool:
        """
        Set up the GCS client and test the connection.
        
        Returns:
            bool: True if setup successful, False otherwise
        """
        try:
            logger.info("Setting up GCS client...")
            
            # Load service account credentials
            sa_info = self._load_service_account_json()
            credentials = service_account.Credentials.from_service_account_info(sa_info)
            
            # Initialize GCS client
            project_id = self.config.project_id or sa_info.get("project_id")
            self.client = storage.Client(project=project_id, credentials=credentials)
            
            # Test connection by accessing the bucket
            self.bucket = self.client.bucket(self.config.bucket)
            
            # Verify bucket exists and we have access
            if not self.bucket.exists():
                logger.error(f"Bucket '{self.config.bucket}' does not exist or is not accessible")
                return False
                
            logger.info(f"✓ Successfully connected to GCS bucket: {self.config.bucket}")
            logger.info(f"✓ Project ID: {project_id}")
            logger.info(f"✓ Folder path: {self.config.folder_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup GCS client: {e}")
            return False
    
    def _load_service_account_json(self) -> Dict:
        """
        Load service account JSON from environment variable.
        Supports direct JSON, base64-encoded JSON, or env var pointer.
        
        Returns:
            Dict: Service account information
            
        Raises:
            RuntimeError: If credentials cannot be loaded
        """
        raw_val = os.getenv(self.config.api_key_env)
        if raw_val is None:
            raise RuntimeError(f"{self.config.api_key_env} environment variable is not set")
        
        # Try direct JSON first
        if raw_val.lstrip().startswith("{"):
            try:
                return json.loads(raw_val)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Invalid JSON in {self.config.api_key_env}: {e}")
        
        # Try base64-encoded JSON
        try:
            decoded = base64.b64decode(raw_val).decode()
            if decoded.lstrip().startswith("{"):
                return json.loads(decoded)
        except Exception:
            pass
        
        # Try as pointer to another environment variable
        pointed_val = os.getenv(raw_val)
        if pointed_val is None:
            raise RuntimeError(f"Pointer environment variable '{raw_val}' is not set")
        
        try:
            # Handle escaped newlines in JSON
            cleaned_json = pointed_val.replace("\\n", "\n")
            return json.loads(cleaned_json)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid JSON in pointed environment variable: {e}")
    
    def _get_folder_prefix(self) -> Optional[str]:
        """Get the folder prefix for blob listing."""
        path = self.config.folder_path.strip("/")
        return None if path == "" else f"{path}/"
    
    def _list_csv_files(self) -> List[storage.Blob]:
        """
        List all CSV files in the configured bucket/folder.
        
        Returns:
            List[storage.Blob]: List of CSV blob objects
        """
        try:
            prefix = self._get_folder_prefix()
            blobs = list(self.bucket.list_blobs(prefix=prefix))
            
            csv_blobs = [
                blob for blob in blobs
                if blob.name.lower().endswith('.csv') and blob.size > 0
            ]
            
            logger.info(f"Found {len(csv_blobs)} CSV files in bucket")
            return csv_blobs
            
        except gcs_exceptions.GoogleCloudError as e:
            logger.error(f"Error listing files from GCS: {e}")
            return []
    
    def _process_csv_file(self, blob: storage.Blob) -> int:
        """
        Process a single CSV file and send rows to Kafka.
        
        Args:
            blob: GCS blob object representing the CSV file
            
        Returns:
            int: Number of rows processed
        """
        rows_processed = 0
        
        try:
            logger.info(f"Processing file: {blob.name} ({blob.size} bytes)")
            
            # Download file content
            content = blob.download_as_text(encoding='utf-8')
            
            # Parse CSV content
            csv_reader = csv.DictReader(StringIO(content))
            
            # Get headers for logging
            headers = csv_reader.fieldnames
            if headers:
                logger.info(f"CSV columns: {headers}")
            
            # Process each row
            for row_num, row in enumerate(csv_reader, 1):
                if not self.running:
                    logger.info("Shutdown requested, stopping file processing")
                    break
                
                # Create message
                message_key = f"{blob.name}#{row_num}"
                message_value = {
                    "file_name": blob.name,
                    "file_size": blob.size,
                    "row_number": row_num,
                    "data": row,
                    "processed_at": time.time()
                }
                
                # Serialize and send message
                try:
                    serialized = self.serialize(
                        key=message_key,
                        value=message_value
                    )
                    
                    self.produce(
                        key=serialized.key,
                        value=serialized.value
                    )
                    
                    rows_processed += 1
                    self.message_count += 1
                    
                    # Log progress every 10 rows
                    if row_num % 10 == 0:
                        logger.info(f"Processed {row_num} rows from {blob.name}")
                    
                    # Stop after max messages for testing
                    if self.message_count >= self.max_messages:
                        logger.info(f"Reached maximum message limit ({self.max_messages}), stopping")
                        return rows_processed
                        
                except Exception as e:
                    logger.error(f"Error processing row {row_num} from {blob.name}: {e}")
                    continue
            
            logger.info(f"Completed processing {blob.name}: {rows_processed} rows")
            
        except gcs_exceptions.GoogleCloudError as e:
            logger.error(f"GCS error processing {blob.name}: {e}")
        except UnicodeDecodeError as e:
            logger.error(f"Encoding error processing {blob.name}: {e}")
        except csv.Error as e:
            logger.error(f"CSV parsing error in {blob.name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error processing {blob.name}: {e}")
        
        return rows_processed
    
    def run(self):
        """
        Main processing loop that reads CSV files from GCS and streams to Kafka.
        """
        logger.info("Starting GCS Source processing...")
        
        if not self.setup():
            logger.error("Failed to setup GCS connection, stopping source")
            return
        
        try:
            # Get list of CSV files
            csv_files = self._list_csv_files()
            
            if not csv_files:
                logger.warning("No CSV files found in the specified location")
                return
            
            total_rows_processed = 0
            
            # Process each CSV file
            for blob in csv_files:
                if not self.running:
                    logger.info("Shutdown requested, stopping processing")
                    break
                
                if blob.name in self.processed_files:
                    logger.debug(f"Skipping already processed file: {blob.name}")
                    continue
                
                # Process the file
                rows_processed = self._process_csv_file(blob)
                total_rows_processed += rows_processed
                
                # Mark file as processed
                self.processed_files.add(blob.name)
                
                # Check if we've hit the message limit
                if self.message_count >= self.max_messages:
                    break
            
            logger.info(f"Processing completed. Total rows processed: {total_rows_processed}")
            logger.info(f"Total messages sent: {self.message_count}")
            
        except Exception as e:
            logger.error(f"Error in main processing loop: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Clean up resources."""
        logger.info("Cleaning up GCS Source resources...")
        self.client = None
        self.bucket = None
    
    def stop(self):
        """Stop the source gracefully."""
        logger.info("Stopping GCS Source...")
        super().stop()
        self._cleanup()


def create_gcs_config() -> GCSConfig:
    """
    Create GCS configuration from environment variables.
    
    Returns:
        GCSConfig: Configuration object
    """
    return GCSConfig(
        bucket=os.getenv("GS_BUCKET", "quix-workflow"),
        project_id=os.getenv("GS_PROJECT_ID", "quix-testing-365012"),
        folder_path=os.getenv("GS_FOLDER_PATH", "/"),
        file_format=os.getenv("GS_FILE_FORMAT", "csv"),
        file_compression=os.getenv("GS_FILE_COMPRESSION", "none"),
        region=os.getenv("GS_REGION", "us-central1"),
        api_key_env="GS_API_KEY"
    )


def main():
    """
    Main application entry point.
    """
    logger.info("Starting Google Storage Bucket Source Application")
    
    try:
        # Create application
        app = Application(
            broker_address=os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092"),
            consumer_group="gcs-source-group",
            auto_offset_reset="latest"
        )
        
        # Create output topic
        output_topic_name = os.getenv("output", "output")
        topic = app.topic(
            name=output_topic_name,
            value_serializer="json",
            key_serializer="string"
        )
        
        # Create and configure GCS source
        config = create_gcs_config()
        source = GCSSource(config=config, name="google-storage-bucket-source-draft-3icl")
        
        # Set up connection callbacks
        def on_success():
            logger.info("✓ GCS connection successful")
        
        def on_failure(exc: Exception):
            logger.error(f"✗ GCS connection failed: {exc}")
        
        source.on_client_connect_success = on_success
        source.on_client_connect_failure = on_failure
        
        # Create streaming dataframe
        sdf = app.dataframe(source=source, topic=topic)
        
        # Log messages being processed
        sdf = sdf.update(lambda value: logger.info(f"Processing message from file: {value.get('file_name', 'unknown')}") or value)
        
        logger.info(f"Application configured:")
        logger.info(f"  - Output topic: {output_topic_name}")
        logger.info(f"  - GCS bucket: {config.bucket}")
        logger.info(f"  - GCS folder: {config.folder_path}")
        logger.info(f"  - File format: {config.file_format}")
        
        # Start the application
        logger.info("Starting Quix Streams application...")
        app.run()
        
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    main()