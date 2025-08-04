Looking at the requirements and the connection test code, I'll create a complete Google Cloud Storage source application using Quix Streams. The application will read files from a GCS bucket and stream them to a Kafka topic.

```python
import os
import json
import logging
import time
from typing import Optional, Dict, Any, List
import pandas as pd
import io
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoogleStorageBucketSource(Source):
    """
    A Quix Streams source that reads data from Google Cloud Storage bucket
    and streams it to a Kafka topic.
    """
    
    def __init__(self, name: str = "google-storage-bucket-source"):
        super().__init__(name)
        
        # Get configuration from environment variables
        self.bucket_name = os.getenv('GS_BUCKET')
        self.project_id = os.getenv('GS_PROJECT_ID')
        self.credentials_json = os.getenv('GS_SECRET_KEY')
        self.folder_path = os.getenv('GS_FOLDER_PATH', '/').strip('/')
        self.file_format = os.getenv('GS_FILE_FORMAT', 'csv').lower()
        self.compression = os.getenv('GS_FILE_COMPRESSION', 'none').lower()
        
        # Validate required environment variables
        if not self.bucket_name:
            raise ValueError("GS_BUCKET environment variable is required")
        if not self.project_id:
            raise ValueError("GS_PROJECT_ID environment variable is required")
        if not self.credentials_json:
            raise ValueError("GS_SECRET_KEY environment variable is required")
        
        # Initialize client placeholders
        self.client: Optional[storage.Client] = None
        self.bucket: Optional[storage.Bucket] = None
        
        # Processing state
        self.processed_files = set()
        self.messages_sent = 0
        self.max_messages_for_testing = 100
        
    def setup(self) -> None:
        """
        Set up the Google Cloud Storage client and authenticate.
        This method is called before the source starts producing messages.
        """
        try:
            logger.info(f"Setting up Google Cloud Storage connection...")
            logger.info(f"Project ID: {self.project_id}")
            logger.info(f"Bucket: {self.bucket_name}")
            logger.info(f"Folder path: {self.folder_path if self.folder_path else 'root'}")
            logger.info(f"File format: {self.file_format}")
            logger.info(f"Compression: {self.compression}")
            
            # Parse credentials JSON
            try:
                credentials_dict = json.loads(self.credentials_json)
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_dict,
                    scopes=['https://www.googleapis.com/auth/cloud-platform']
                )
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON format in credentials")
            except Exception as e:
                raise ValueError(f"Failed to create credentials: {str(e)}")
            
            # Initialize the client
            self.client = storage.Client(project=self.project_id, credentials=credentials)
            
            # Get the bucket and verify access
            try:
                self.bucket = self.client.bucket(self.bucket_name)
                self.bucket.reload()
                logger.info(f"✓ Successfully connected to bucket '{self.bucket_name}'")
                
                # Call success callback if available
                if hasattr(self, 'on_client_connect_success') and callable(self.on_client_connect_success):
                    self.on_client_connect_success()
                    
            except Exception as e:
                error_msg = f"Failed to access bucket '{self.bucket_name}': {str(e)}"
                logger.error(error_msg)
                
                # Call failure callback if available
                if hasattr(self, 'on_client_connect_failure') and callable(self.on_client_connect_failure):
                    self.on_client_connect_failure(error_msg)
                
                raise ValueError(error_msg)
                
        except Exception as e:
            logger.error(f"Failed to setup Google Cloud Storage connection: {str(e)}")
            raise
    
    def _get_file_list(self) -> List[storage.Blob]:
        """Get list of files from the bucket based on configuration."""
        prefix = self.folder_path + '/' if self.folder_path and not self.folder_path.endswith('/') else self.folder_path
        if prefix == '/':
            prefix = ''
        
        # List all blobs with the specified prefix
        blobs = list(self.client.list_blobs(self.bucket_name, prefix=prefix))
        
        # Filter files by format if specified
        if self.file_format != 'all':
            filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{self.file_format}')]
            blobs = filtered_blobs
        
        # Filter out already processed files
        new_blobs = [blob for blob in blobs if blob.name not in self.processed_files]
        
        logger.info(f"Found {len(new_blobs)} new files to process")
        return new_blobs
    
    def _decompress_content(self, content: bytes) -> bytes:
        """Decompress content based on compression setting."""
        if self.compression in ['gzip', 'gz']:
            import gzip
            return gzip.decompress(content)
        elif self.compression in ['bz2', 'bzip2']:
            import bz2
            return bz2.decompress(content)
        else:
            return content
    
    def _process_csv_file(self, blob: storage.Blob, content_str: str) -> None:
        """Process CSV file and send messages."""
        try:
            df = pd.read_csv(io.StringIO(content_str))
            logger.info(f"Processing CSV file {blob.name} with {len(df)} rows")
            
            for index, row in df.iterrows():
                if not self.running or self.messages_sent >= self.max_messages_for_testing:
                    return
                
                # Create message with metadata
                message_data = {
                    "file_name": blob.name,
                    "file_size": blob.size,
                    "row_index": index,
                    "data": row.to_dict(),
                    "timestamp": time.time(),
                    "bucket": self.bucket_name,
                    "file_format": self.file_format
                }
                
                # Serialize the message
                serialized = self.serialize(
                    key=f"{blob.name}:{index}",
                    value=message_data
                )
                
                # Send to Kafka
                self.produce(
                    key=serialized.key,
                    value=serialized.value
                )
                
                self.messages_sent += 1
                
                if self.messages_sent % 10 == 0:
                    logger.info(f"Processed {self.messages_sent} messages")
                    
        except Exception as e:
            logger.error(f"Error processing CSV file {blob.name}: {str(e)}")
    
    def _process_json_file(self, blob: storage.Blob, content_str: str) -> None:
        """Process JSON file and send messages."""
        try:
            data = json.loads(content_str)
            logger.info(f"Processing JSON file {blob.name}")
            
            if isinstance(data, list):
                for index, item in enumerate(data):
                    if not self.running or self.messages_sent >= self.max_messages_for_testing:
                        return
                    
                    message_data = {
                        "file_name": blob.name,
                        "file_size": blob.size,
                        "item_index": index,
                        "data": item,
                        "timestamp": time.time(),
                        "bucket": self.bucket_name,
                        "file_format": self.file_format
                    }
                    
                    serialized = self.serialize(
                        key=f"{blob.name}:{index}",
                        value=message_data
                    )
                    
                    self.produce(
                        key=serialized.key,
                        value=serialized.value
                    )
                    
                    self.messages_sent += 1
                    
                    if self.messages_sent % 10 == 0:
                        logger.info(f"Processed {self.messages_sent} messages")
            else:
                # Single JSON object
                message_data = {
                    "file_name": blob.name,
                    "file_size": blob.size,
                    "data": data,
                    "timestamp": time.time(),
                    "bucket": self.bucket_name,
                    "file_format": self.file_format
                }
                
                serialized = self.serialize(
                    key=blob.name,
                    value=message_data
                )
                
                self.produce(
                    key=serialized.key,
                    value=serialized.value
                )
                
                self.messages_sent += 1
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file {blob.name}: {str(e)}")
        except Exception as e:
            logger.error(f"Error processing JSON file {blob.name}: {str(e)}")
    
    def _process_text_file(self, blob: storage.Blob, content_str: str) -> None:
        """Process text file and send messages."""
        try:
            lines = content_str.split('\n')
            logger.info(f"Processing text file {blob.name} with {len(lines)} lines")
            
            for index, line in enumerate(lines):
                if not self.running or self.messages_sent >= self.max_messages_for_testing:
                    return
                
                if line.strip():  # Skip empty lines
                    message_data = {
                        "file_name": blob.name,
                        "file_size": blob.size,
                        "line_number": index + 1,
                        "data": line.strip(),
                        "timestamp": time.time(),
                        "bucket": self.bucket_name,
                        "file_format": self.file_format
                    }
                    
                    serialized = self.serialize(
                        key=f"{blob.name}:{index}",
                        value=message_data
                    )
                    
                    self.produce(
                        key=serialized.key,
                        value=serialized.value
                    )
                    
                    self.messages_sent += 1
                    
                    if self.messages_sent % 10 == 0:
                        logger.info(f"Processed {self.messages_sent} messages")
                        
        except Exception as e:
            logger.error(f"Error processing text file {blob.name}: {str(e)}")
    
    def _process_binary_file(self, blob: storage.Blob, content: bytes) -> None:
        """Process binary file and send message with metadata."""
        try:
            message_data = {
                "file_name": blob.name,
                "file_size": blob.size,
                "content_type": blob.content_type or "application/octet-stream",
                "data_size": len(content),
                "timestamp": time.time(),
                "bucket": self.bucket_name,
                "file_format": self.file_format,
                "note": "Binary file - data not included in message"
            }
            
            serialized = self.serialize(
                key=blob.name,
                value=message_data
            )
            
            self.produce(
                key=serialized.key,
                value=serialized.value
            )
            
            self.messages_sent += 1
            logger.info(f"Processed binary file {blob.name} ({len(content)} bytes)")
            
        except Exception as e:
            logger.error(f"Error processing binary file {blob.name}: {str(e)}")
    
    def _process_file(self, blob: storage.Blob) -> None:
        """Process a single file from the bucket."""
        try:
            logger.info(f"Processing file: {blob.name}")
            
            # Download file content
            content = blob.download_as_bytes()
            
            # Handle compression
            content = self._decompress_content(content)
            
            # Process based on file format
            if self.file_format in ['csv', 'txt', 'json']:
                try:
                    content_str = content.decode('utf-8')
                    
                    if self.file_format == 'csv':
                        self._process_csv_file(blob, content_str)
                    elif self.file_format == 'json':
                        self._process_json_file(blob, content_str)
                    elif self.file_format == 'txt':
                        self._process_text_file(blob, content_str)
                        
                except UnicodeDecodeError:
                    logger.warning(f"Could not decode file {blob.name} as UTF-8, treating as binary")
                    self._process_binary_file(blob, content)
            else:
                # Handle other file types as binary
                self._process_binary_file(blob, content)
            
            # Mark file as processed
            self.processed_files.add(blob.name)
            
        except Exception as e:
            logger.error(f"Error processing file {blob.name}: {str(e)}")
    
    def run(self) -> None:
        """
        Main loop that reads files from Google Cloud Storage and sends messages to Kafka.
        """
        logger.info("Starting Google Cloud Storage source...")
        
        try:
            # Setup connection if not already done
            if self.client is None:
                self.setup()
            
            while self.running and self.messages_sent < self.max_messages_for_testing:
                try:
                    # Get list of files to process
                    files_to_process = self._get_file_list()
                    
                    if not files_to_process:
                        logger.info("No new files to process, waiting...")
                        time.sleep(5)
                        continue
                    
                    # Process each file
                    for blob in files_to_process:
                        if not self.running or self.messages_sent >= self.max_messages_for_testing:
                            break
                        
                        self._process_file(blob)
                    
                    # If we've processed all available files, wait before checking again
                    if self.messages_sent < self.max_messages_for_testing:
                        logger.info("Finished processing current batch, waiting for new files...")
                        time.sleep(10)
                    
                except Exception as e:
                    logger.error(f"Error in main processing loop: {str(e)}")
                    time.sleep(5)
                    continue
            
            logger.info(f"Source completed. Processed {self.messages_sent} messages from {len(self.processed_files)} files")
            
        except Exception as e:
            logger.error(f"Fatal error in Google Cloud Storage source: {str(e)}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self) -> None:
        """Clean up resources."""
        logger.info("Cleaning up Google Cloud Storage source...")
        self.client = None
        self.bucket = None


def main():
    """
    Main function to run the Google Cloud Storage source application.
    """
    # Initialize the Quix Streams application
    app = Application(
        application_id="google-storage-bucket-source-draft-zvuo",
        auto_offset_reset="latest"
    )
    
    # Get output topic from environment variable
    output_topic_name = os.getenv('output', 'output')
    
    # Create the output topic
    output_topic = app.topic(
        name=output_topic_name,