"""
Google Storage Bucket Source Application for Quix Streams

Reads CSV files from a Google Storage Bucket and streams them to a Kafka topic.
Supports authentication via service account JSON key and handles fault tolerance.
"""

import os
import json
import base64
import csv
import logging
import time
from io import StringIO
from typing import Dict, Optional, Any

from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GoogleStorageBucketSource(Source):
    """
    A Quix Streams source that reads CSV files from Google Storage Bucket
    and produces messages to a Kafka topic.
    """
    
    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        folder_path: str = "/",
        file_format: str = "csv",
        file_compression: str = "none",
        max_messages: int = 100,
        **kwargs
    ):
        """
        Initialize the Google Storage Bucket source.
        
        Args:
            bucket_name: Name of the GCS bucket
            project_id: Google Cloud project ID
            folder_path: Path within bucket to read files from
            file_format: File format to process (currently supports 'csv')
            file_compression: Compression type (currently supports 'none')
            max_messages: Maximum number of messages to process (for testing)
        """
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.folder_path = folder_path.strip("/")
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.max_messages = max_messages
        self.messages_processed = 0
        
        # Client will be initialized in setup()
        self._client = None
        self._bucket = None
        
        # Validate configuration
        if self.file_format != "csv":
            raise ValueError(f"Unsupported file format: {self.file_format}")
        if self.file_compression != "none":
            raise ValueError(f"Unsupported compression: {self.file_compression}")

    def _load_service_account_json(self) -> Dict:
        """
        Load service account JSON from environment variables.
        Supports multiple formats: direct JSON, base64-encoded, or env var pointer.
        """
        # Try GS_API_KEY first (for compatibility)
        raw_val = os.getenv("GS_API_KEY")
        if not raw_val:
            # Fallback to the configured secret key
            secret_key = os.getenv("GS_SECRET_KEY", "GCLOUD_PK_JSON")
            raw_val = os.getenv(secret_key)
        
        if not raw_val:
            raise RuntimeError("No service account JSON found in environment variables")

        # Direct JSON format
        if raw_val.lstrip().startswith("{"):
            return json.loads(raw_val)

        # Base64-encoded JSON
        try:
            decoded = base64.b64decode(raw_val).decode()
            if decoded.lstrip().startswith("{"):
                return json.loads(decoded)
        except Exception:
            pass

        # Pointer to another env var
        pointed = os.getenv(raw_val)
        if pointed is None:
            raise RuntimeError(f"Pointer env-var '{raw_val}' is unset or empty")

        try:
            return json.loads(pointed)
        except json.JSONDecodeError:
            # Handle escaped newlines
            return json.loads(pointed.replace("\\n", "\n"))

    def setup(self) -> None:
        """
        Set up the Google Cloud Storage client and test connection.
        This method is called before run() starts.
        """
        try:
            logger.info("Setting up Google Cloud Storage client...")
            
            # Load service account credentials
            sa_info = self._load_service_account_json()
            credentials = service_account.Credentials.from_service_account_info(sa_info)
            
            # Use project_id from credentials if not explicitly set
            project_id = self.project_id or sa_info.get("project_id")
            if not project_id:
                raise RuntimeError("No project_id found in configuration or credentials")
            
            # Initialize GCS client
            self._client = storage.Client(project=project_id, credentials=credentials)
            self._bucket = self._client.bucket(self.bucket_name)
            
            # Test connection by listing a few blobs
            prefix = f"{self.folder_path}/" if self.folder_path else None
            blobs = list(self._client.list_blobs(
                self._bucket, 
                prefix=prefix,
                max_results=1
            ))
            
            logger.info(f"✓ Successfully connected to GCS bucket: {self.bucket_name}")
            logger.info(f"✓ Project: {project_id}")
            logger.info(f"✓ Folder path: {self.folder_path or '/'}")
            
            # Trigger success callback if available
            if hasattr(self, '_on_client_connect_success') and self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to setup GCS client: {e}")
            # Trigger failure callback if available
            if hasattr(self, '_on_client_connect_failure') and self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def _get_prefix(self) -> Optional[str]:
        """Get the folder prefix for blob listing."""
        return f"{self.folder_path}/" if self.folder_path else None

    def _list_csv_files(self):
        """List all CSV files in the specified bucket and folder."""
        prefix = self._get_prefix()
        blobs = self._client.list_blobs(self._bucket, prefix=prefix)
        
        csv_files = [
            blob for blob in blobs
            if blob.name.lower().endswith('.csv') and blob.size > 0
        ]
        
        logger.info(f"Found {len(csv_files)} CSV files in gs://{self.bucket_name}/{prefix or ''}")
        return csv_files

    def _process_csv_file(self, blob) -> int:
        """
        Process a single CSV file and produce messages to Kafka.
        
        Args:
            blob: GCS blob object representing the CSV file
            
        Returns:
            Number of messages produced
        """
        logger.info(f"Processing file: {blob.name} ({blob.size} bytes)")
        
        try:
            # Download file content as text
            content = blob.download_as_text(encoding='utf-8')
            
            # Parse CSV content
            csv_reader = csv.DictReader(StringIO(content))
            messages_from_file = 0
            
            for row_num, row in enumerate(csv_reader, 1):
                if not self.running:
                    logger.info("Source stopped, breaking from file processing")
                    break
                    
                if self.messages_processed >= self.max_messages:
                    logger.info(f"Reached maximum messages limit: {self.max_messages}")
                    break
                
                # Create message with metadata
                message_data = {
                    "data": row,
                    "metadata": {
                        "source_file": blob.name,
                        "row_number": row_num,
                        "file_size": blob.size,
                        "timestamp": time.time()
                    }
                }
                
                # Serialize the message
                serialized = self.serialize(
                    key=f"{blob.name}:{row_num}",
                    value=message_data
                )
                
                # Produce to Kafka
                self.produce(
                    key=serialized.key,
                    value=serialized.value
                )
                
                self.messages_processed += 1
                messages_from_file += 1
                
                # Log progress every 100 messages
                if messages_from_file % 100 == 0:
                    logger.info(f"Processed {messages_from_file} messages from {blob.name}")
            
            logger.info(f"Completed processing {blob.name}: {messages_from_file} messages")
            return messages_from_file
            
        except Exception as e:
            logger.error(f"Error processing file {blob.name}: {e}")
            raise

    def run(self) -> None:
        """
        Main execution loop that reads CSV files from GCS and produces messages.
        """
        logger.info("Starting Google Storage Bucket source")
        logger.info(f"Bucket: {self.bucket_name}")
        logger.info(f"Folder: {self.folder_path or '/'}")
        logger.info(f"Max messages: {self.max_messages}")
        
        try:
            # List all CSV files
            csv_files = self._list_csv_files()
            
            if not csv_files:
                logger.warning("No CSV files found to process")
                return
            
            # Process each file
            total_files_processed = 0
            for blob in csv_files:
                if not self.running:
                    logger.info("Source stopped, breaking from main loop")
                    break
                    
                if self.messages_processed >= self.max_messages:
                    logger.info("Reached maximum messages limit")
                    break
                
                try:
                    messages_from_file = self._process_csv_file(blob)
                    total_files_processed += 1
                    
                    logger.info(
                        f"Progress: {total_files_processed}/{len(csv_files)} files, "
                        f"{self.messages_processed} total messages"
                    )
                    
                except Exception as e:
                    logger.error(f"Failed to process file {blob.name}: {e}")
                    # Continue with next file instead of stopping
                    continue
            
            logger.info(
                f"Source completed: {total_files_processed} files processed, "
                f"{self.messages_processed} total messages produced"
            )
            
        except Exception as e:
            logger.error(f"Error in source run loop: {e}")
            raise
        finally:
            logger.info("Google Storage Bucket source finished")


def main():
    """Main application entry point."""
    
    # Load configuration from environment variables
    config = {
        "bucket_name": os.getenv("GS_BUCKET", "quix-workflow"),
        "project_id": os.getenv("GS_PROJECT_ID", "quix-testing-365012"),
        "folder_path": os.getenv("GS_FOLDER_PATH", "/"),
        "file_format": os.getenv("GS_FILE_FORMAT", "csv"),
        "file_compression": os.getenv("GS_FILE_COMPRESSION", "none"),
        "output_topic": os.getenv("output", "solar-data"),
    }
    
    logger.info("Starting Google Storage Bucket Source Application")
    logger.info(f"Configuration: {config}")
    
    try:
        # Initialize Quix Streams application
        app = Application(
            consumer_group="google-storage-bucket-source-draft-6a4g",
            auto_offset_reset="earliest",
        )
        
        # Create the source instance
        source = GoogleStorageBucketSource(
            name="google-storage-bucket-source",
            bucket_name=config["bucket_name"],
            project_id=config["project_id"],
            folder_path=config["folder_path"],
            file_format=config["file_format"],
            file_compression=config["file_compression"],
            max_messages=100  # Limit for testing
        )
        
        # Define the topic for output
        topic = app.topic(
            name=config["output_topic"],
            value_serializer="json",
            key_serializer="string"
        )
        
        # Create StreamingDataFrame from source
        sdf = app.dataframe(source=source, topic=topic)
        
        # Add processing pipeline (optional - just for monitoring)
        sdf = sdf.update(lambda value: logger.info(f"Produced message: {value['metadata']['source_file']}:{value['metadata']['row_number']}") or value)
        
        # Print messages for debugging (optional)
        sdf.print(metadata=True)
        
        logger.info("Starting application...")
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