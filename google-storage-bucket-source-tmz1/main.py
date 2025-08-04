import os
import json
import base64
import csv
import logging
from io import StringIO
from typing import Dict, Optional, Iterator, Any
import time

from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source
from quixstreams.models.topics import TopicConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GoogleStorageBucketSource(Source):
    """
    A Quix Streams source that reads CSV files from Google Cloud Storage bucket
    and produces messages to a Kafka topic.
    """
    
    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        folder_path: str = "/",
        file_format: str = "csv",
        file_compression: str = "none",
        name: str = "google-storage-source",
        shutdown_timeout: float = 10.0,
        **kwargs
    ):
        """
        Initialize the Google Storage Bucket Source.
        
        Args:
            bucket_name: Name of the GCS bucket
            project_id: Google Cloud project ID
            folder_path: Path within the bucket to read files from
            file_format: Format of files to read (currently supports 'csv')
            file_compression: Compression type (not implemented yet)
            name: Name of the source
            shutdown_timeout: Timeout for graceful shutdown
        """
        super().__init__(name=name, shutdown_timeout=shutdown_timeout, **kwargs)
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.folder_path = folder_path.strip("/")
        self.file_format = file_format
        self.file_compression = file_compression
        self.client = None
        self.bucket = None
        self.processed_files = set()
        self.message_count = 0
        self.max_messages = 100  # For testing purposes
        
    def _load_service_account_json(self) -> Dict:
        """Load service account JSON from environment variables."""
        raw_val: Optional[str] = os.getenv("GS_SECRET_KEY")
        if raw_val is None:
            raise RuntimeError("GS_SECRET_KEY environment variable is not set")

        # Check if it's already raw JSON
        if raw_val.lstrip().startswith("{"):
            return json.loads(raw_val)

        # Try base64 decoding
        try:
            decoded = base64.b64decode(raw_val).decode()
            if decoded.lstrip().startswith("{"):
                return json.loads(decoded)
        except Exception:
            pass

        # Try as pointer to another environment variable
        pointed = os.getenv(raw_val)
        if pointed is None:
            raise RuntimeError(f"Pointer environment variable '{raw_val}' is unset or empty")

        try:
            return json.loads(pointed)
        except json.JSONDecodeError:
            # Handle escaped newlines
            return json.loads(pointed.replace("\\n", "\n"))

    def _get_prefix(self) -> Optional[str]:
        """Get the folder prefix for listing blobs."""
        path = self.folder_path.strip("/")
        return None if path == "" else f"{path}/"

    def setup(self) -> None:
        """Set up the Google Cloud Storage client and test connection."""
        try:
            # Load service account credentials
            sa_info = self._load_service_account_json()
            credentials = service_account.Credentials.from_service_account_info(sa_info)
            
            # Initialize GCS client
            project = self.project_id or sa_info.get("project_id")
            self.client = storage.Client(project=project, credentials=credentials)
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test connection by trying to access bucket metadata
            _ = self.bucket.exists()
            logger.info(f"✓ Successfully connected to GCS bucket: {self.bucket_name}")
            
        except Exception as e:
            error_msg = f"Failed to setup Google Storage connection: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def _list_csv_files(self) -> Iterator[storage.Blob]:
        """List all CSV files in the specified bucket and folder."""
        try:
            prefix = self._get_prefix()
            blobs = self.bucket.list_blobs(prefix=prefix)
            
            for blob in blobs:
                if (blob.name.lower().endswith('.csv') and 
                    blob.name not in self.processed_files):
                    yield blob
                    
        except Exception as e:
            logger.error(f"Error listing files from bucket: {str(e)}")
            raise

    def _process_csv_file(self, blob: storage.Blob) -> Iterator[Dict[str, Any]]:
        """Process a single CSV file and yield records."""
        try:
            logger.info(f"Processing file: {blob.name} ({blob.size} bytes)")
            
            # Download file content
            content = blob.download_as_text(encoding='utf-8')
            
            # Parse CSV
            csv_reader = csv.DictReader(StringIO(content))
            
            for row_num, row in enumerate(csv_reader, 1):
                if not self.running:
                    break
                    
                # Add metadata to each record
                record = {
                    "data": row,
                    "metadata": {
                        "file_name": blob.name,
                        "bucket_name": self.bucket_name,
                        "row_number": row_num,
                        "file_size": blob.size,
                        "processed_at": time.time()
                    }
                }
                
                yield record
                
        except Exception as e:
            logger.error(f"Error processing file {blob.name}: {str(e)}")
            raise

    def run(self) -> None:
        """Main execution loop that processes files and produces messages."""
        logger.info(f"Starting Google Storage Bucket Source for bucket: {self.bucket_name}")
        
        try:
            while self.running and self.message_count < self.max_messages:
                files_processed = 0
                
                # Get list of CSV files to process
                csv_files = list(self._list_csv_files())
                
                if not csv_files:
                    logger.info("No new CSV files found to process")
                    time.sleep(10)  # Wait before checking again
                    continue
                
                logger.info(f"Found {len(csv_files)} CSV files to process")
                
                # Process each file
                for blob in csv_files:
                    if not self.running or self.message_count >= self.max_messages:
                        break
                        
                    try:
                        # Process records from the file
                        for record in self._process_csv_file(blob):
                            if not self.running or self.message_count >= self.max_messages:
                                break
                            
                            # Create message key from file name and row number
                            message_key = f"{blob.name}:{record['metadata']['row_number']}"
                            
                            # Serialize and produce message
                            serialized = self.serialize(
                                key=message_key,
                                value=record
                            )
                            
                            self.produce(
                                key=serialized.key,
                                value=serialized.value,
                                headers={"source": "google-storage-bucket"}
                            )
                            
                            self.message_count += 1
                            
                            if self.message_count % 10 == 0:
                                logger.info(f"Processed {self.message_count} messages")
                        
                        # Mark file as processed
                        self.processed_files.add(blob.name)
                        files_processed += 1
                        logger.info(f"Completed processing file: {blob.name}")
                        
                    except Exception as e:
                        logger.error(f"Error processing file {blob.name}: {str(e)}")
                        continue
                
                if files_processed == 0:
                    logger.info("No files were processed in this iteration")
                    time.sleep(10)
                else:
                    logger.info(f"Processed {files_processed} files in this iteration")
                    
                # For testing: stop after processing available files once
                if self.message_count >= self.max_messages:
                    logger.info(f"Reached maximum message limit ({self.max_messages})")
                    break
                    
        except Exception as e:
            logger.error(f"Error in source run loop: {str(e)}")
            raise
        finally:
            logger.info(f"Google Storage Bucket Source stopped. Total messages processed: {self.message_count}")

    def default_topic(self):
        """Return the default topic configuration for this source."""
        return self._producer_topic


def main():
    """Main function to run the Google Storage Bucket source application."""
    
    # Get configuration from environment variables
    bucket_name = os.getenv("GS_BUCKET", "quix-workflow")
    project_id = os.getenv("GS_PROJECT_ID", "quix-testing-365012")
    folder_path = os.getenv("GS_FOLDER_PATH", "/")
    file_format = os.getenv("GS_FILE_FORMAT", "csv")
    file_compression = os.getenv("GS_FILE_COMPRESSION", "none")
    output_topic_name = os.getenv("output", "output")
    
    logger.info("Starting Google Storage Bucket Source Application")
    logger.info(f"Configuration: bucket={bucket_name}, project={project_id}, folder={folder_path}")
    
    try:
        # Initialize Quix Streams Application
        app = Application(
            broker_address=os.getenv("Quix__Kafka__Brokers", "localhost:9092"),
            consumer_group="google-storage-bucket-source-draft-tmz1",
            auto_offset_reset="earliest",
        )
        
        # Create the source
        source = GoogleStorageBucketSource(
            bucket_name=bucket_name,
            project_id=project_id,
            folder_path=folder_path,
            file_format=file_format,
            file_compression=file_compression,
            name="google-storage-bucket-source"
        )
        
        # Create output topic with JSON serialization
        output_topic = app.topic(
            name=output_topic_name,
            value_serializer="json",
            key_serializer="string",
            config=TopicConfig(
                num_partitions=1,
                replication_factor=1
            )
        )
        
        # Create streaming dataframe from source
        sdf = app.dataframe(source=source, topic=output_topic)
        
        # Add some basic processing to log messages
        sdf = sdf.update(lambda value: logger.info(f"Processed message from file: {value['metadata']['file_name']}") or value)
        
        # Print messages for debugging (can be removed in production)
        sdf.print(metadata=True)
        
        logger.info("Starting application...")
        app.run()
        
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        raise
    finally:
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    main()