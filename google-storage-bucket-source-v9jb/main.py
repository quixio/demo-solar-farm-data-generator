import os
import json
import logging
import time
import io
from typing import Optional, Dict, Any
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd

from quixstreams import Application
from quixstreams.sources.base import Source

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoogleStorageBucketSource(Source):
    """
    A Quix Streams source that reads data from Google Storage Bucket.
    
    Supports reading CSV files and other text-based formats from a specified
    bucket and folder path, then streams the data to a Kafka topic.
    """
    
    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        credentials_json: str,
        folder_path: str = "/",
        file_format: str = "csv",
        file_compression: str = "none",
        **kwargs
    ):
        """
        Initialize the Google Storage Bucket source.
        
        Args:
            bucket_name: Name of the Google Storage bucket
            project_id: Google Cloud project ID
            credentials_json: JSON string containing service account credentials
            folder_path: Path within the bucket to read files from
            file_format: Format of files to read (csv, json, txt, etc.)
            file_compression: Compression format (not implemented in this version)
        """
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path.strip('/')
        self.file_format = file_format.lower()
        self.file_compression = file_compression
        
        # Internal state
        self._client: Optional[storage.Client] = None
        self._bucket: Optional[storage.Bucket] = None
        self._processed_files = set()
        self._message_count = 0
        
    def setup(self) -> None:
        """
        Set up the Google Storage client and test connection.
        Called once before starting the source.
        """
        try:
            logger.info(f"Setting up Google Storage Bucket source for bucket: {self.bucket_name}")
            
            # Parse credentials JSON
            try:
                credentials_dict = json.loads(self.credentials_json)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON format in credentials: {e}")
                if hasattr(self, 'on_client_connect_failure'):
                    self.on_client_connect_failure(e)
                raise
            
            # Create credentials object
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            
            # Initialize Google Cloud Storage client
            self._client = storage.Client(credentials=credentials, project=self.project_id)
            self._bucket = self._client.bucket(self.bucket_name)
            
            # Test connection by listing objects
            test_blobs = list(self._bucket.list_blobs(max_results=1))
            logger.info("Successfully connected to Google Storage Bucket")
            
            if hasattr(self, 'on_client_connect_success'):
                self.on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to setup Google Storage connection: {e}")
            if hasattr(self, 'on_client_connect_failure'):
                self.on_client_connect_failure(e)
            raise
    
    def run(self) -> None:
        """
        Main execution loop that reads data from Google Storage and produces to Kafka.
        """
        logger.info("Starting Google Storage Bucket source")
        
        try:
            while self.running:
                files_processed = self._process_bucket_files()
                
                if files_processed == 0:
                    logger.info("No new files to process, waiting...")
                    time.sleep(10)  # Wait before checking for new files
                else:
                    logger.info(f"Processed {files_processed} files")
                
                # Stop condition for testing (remove for production)
                if self._message_count >= 100:
                    logger.info("Reached message limit of 100, stopping...")
                    break
                    
        except Exception as e:
            logger.error(f"Error in run loop: {e}")
            raise
        finally:
            logger.info("Google Storage Bucket source stopped")
    
    def _process_bucket_files(self) -> int:
        """
        Process files in the bucket that haven't been processed yet.
        
        Returns:
            Number of files processed in this iteration
        """
        files_processed = 0
        
        try:
            # Clean folder path
            prefix = self.folder_path
            if prefix and not prefix.endswith('/'):
                prefix += '/'
            
            # List files in the bucket with the specified prefix
            blobs = list(self._bucket.list_blobs(prefix=prefix))
            
            # Filter files by format and exclude already processed
            target_files = [
                blob for blob in blobs 
                if (not self.file_format or blob.name.lower().endswith(f'.{self.file_format}'))
                and blob.name not in self._processed_files
                and not blob.name.endswith('/')  # Exclude directories
            ]
            
            logger.info(f"Found {len(target_files)} new {self.file_format} files to process")
            
            for blob in target_files:
                if not self.running:
                    break
                    
                try:
                    self._process_file(blob)
                    self._processed_files.add(blob.name)
                    files_processed += 1
                    
                    # Stop condition for testing
                    if self._message_count >= 100:
                        break
                        
                except Exception as e:
                    logger.error(f"Error processing file {blob.name}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error listing bucket files: {e}")
            raise
            
        return files_processed
    
    def _process_file(self, blob: storage.Blob) -> None:
        """
        Process a single file from the bucket.
        
        Args:
            blob: Google Storage blob object to process
        """
        logger.info(f"Processing file: {blob.name} (size: {blob.size} bytes)")
        
        try:
            # Download file content
            file_content = blob.download_as_text()
            
            if self.file_format == 'csv':
                self._process_csv_content(file_content, blob.name)
            elif self.file_format == 'json':
                self._process_json_content(file_content, blob.name)
            else:
                self._process_text_content(file_content, blob.name)
                
        except Exception as e:
            logger.error(f"Error processing file content for {blob.name}: {e}")
            raise
    
    def _process_csv_content(self, content: str, filename: str) -> None:
        """Process CSV file content and produce messages."""
        try:
            df = pd.read_csv(io.StringIO(content))
            
            for idx, row in df.iterrows():
                if not self.running or self._message_count >= 100:
                    break
                    
                # Convert row to dictionary
                record = row.to_dict()
                
                # Add metadata
                message_data = {
                    "data": record,
                    "metadata": {
                        "source_file": filename,
                        "row_number": idx + 1,
                        "file_format": self.file_format,
                        "bucket_name": self.bucket_name,
                        "processed_at": time.time()
                    }
                }
                
                # Serialize and produce message
                msg = self.serialize(
                    key=f"{filename}_{idx}",
                    value=message_data
                )
                
                self.produce(
                    key=msg.key,
                    value=msg.value
                )
                
                self._message_count += 1
                
                if self._message_count % 10 == 0:
                    logger.info(f"Produced {self._message_count} messages")
                    
        except Exception as e:
            logger.error(f"Error processing CSV content: {e}")
            raise
    
    def _process_json_content(self, content: str, filename: str) -> None:
        """Process JSON file content and produce messages."""
        try:
            # Try to parse as JSON array first, then as single object
            try:
                data = json.loads(content)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON content in {filename}: {e}")
                return
            
            # Handle different JSON structures
            if isinstance(data, list):
                records = data
            else:
                records = [data]
            
            for idx, record in enumerate(records):
                if not self.running or self._message_count >= 100:
                    break
                
                message_data = {
                    "data": record,
                    "metadata": {
                        "source_file": filename,
                        "record_number": idx + 1,
                        "file_format": self.file_format,
                        "bucket_name": self.bucket_name,
                        "processed_at": time.time()
                    }
                }
                
                msg = self.serialize(
                    key=f"{filename}_{idx}",
                    value=message_data
                )
                
                self.produce(
                    key=msg.key,
                    value=msg.value
                )
                
                self._message_count += 1
                
                if self._message_count % 10 == 0:
                    logger.info(f"Produced {self._message_count} messages")
                    
        except Exception as e:
            logger.error(f"Error processing JSON content: {e}")
            raise
    
    def _process_text_content(self, content: str, filename: str) -> None:
        """Process plain text file content and produce messages."""
        try:
            lines = content.split('\n')
            
            for idx, line in enumerate(lines):
                if not self.running or self._message_count >= 100:
                    break
                
                # Skip empty lines
                if not line.strip():
                    continue
                
                message_data = {
                    "data": {"line": line.strip()},
                    "metadata": {
                        "source_file": filename,
                        "line_number": idx + 1,
                        "file_format": self.file_format,
                        "bucket_name": self.bucket_name,
                        "processed_at": time.time()
                    }
                }
                
                msg = self.serialize(
                    key=f"{filename}_{idx}",
                    value=message_data
                )
                
                self.produce(
                    key=msg.key,
                    value=msg.value
                )
                
                self._message_count += 1
                
                if self._message_count % 10 == 0:
                    logger.info(f"Produced {self._message_count} messages")
                    
        except Exception as e:
            logger.error(f"Error processing text content: {e}")
            raise


def main():
    """
    Main function to set up and run the Google Storage Bucket source application.
    """
    # Get configuration from environment variables
    bucket_name = os.getenv('GS_BUCKET')
    project_id = os.getenv('GS_PROJECT_ID')
    folder_path = os.getenv('GS_FOLDER_PATH', '/')
    file_format = os.getenv('GS_FILE_FORMAT', 'csv')
    file_compression = os.getenv('GS_FILE_COMPRESSION', 'none')
    secret_key_name = os.getenv('GS_SECRET_KEY')
    output_topic = os.getenv('output', 'output')
    
    # Validate required environment variables
    if not bucket_name:
        raise ValueError("GS_BUCKET environment variable is required")
    if not project_id:
        raise ValueError("GS_PROJECT_ID environment variable is required")
    if not secret_key_name:
        raise ValueError("GS_SECRET_KEY environment variable is required")
    
    # Get credentials from environment variable
    credentials_json = os.getenv(secret_key_name)
    if not credentials_json:
        raise ValueError(f"Credentials not found in environment variable: {secret_key_name}")
    
    logger.info(f"Starting Google Storage Bucket source application")
    logger.info(f"Bucket: {bucket_name}")
    logger.info(f"Project ID: {project_id}")
    logger.info(f"Folder path: {folder_path}")
    logger.info(f"File format: {file_format}")
    logger.info(f"Output topic: {output_topic}")
    
    # Create the Quix Streams application
    app = Application(
        broker_address=os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092"),
        consumer_group="google-storage-bucket-source-draft-v9jb",
        auto_offset_reset="earliest",
    )
    
    # Create the Google Storage source
    source = GoogleStorageBucketSource(
        name="google-storage-bucket-source",
        bucket_name=bucket_name,
        project_id=project_id,
        credentials_json=credentials_json,
        folder_path=folder_path,
        file_format=file_format,
        file_compression=file_compression,
    )
    
    # Create output topic with JSON serialization
    topic = app.topic(
        output_topic,
        value_serializer="json",
        key_serializer="string"
    )
    
    # Create StreamingDataFrame from the source
    sdf = app.dataframe(source=source, topic=topic)
    
    # Add some basic processing and logging
    sdf = sdf.update(lambda value: logger.info(f"Processing message from {value.get('metadata', {}).get('source_file', 'unknown')}") or value)
    
    # Print messages for debugging (can be removed in production)
    sdf.print(metadata=True)
    
    try:
        logger.info("Starting Quix Streams application...")
        app.run()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    main()