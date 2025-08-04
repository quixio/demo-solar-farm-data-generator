import os
import logging
import json
import csv
import io
import gzip
import zipfile
from typing import Any, Dict, Optional, List
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoogleStorageSource(Source):
    """
    A Quix Streams source that reads files from Google Cloud Storage bucket
    and produces messages to a Kafka topic.
    """
    
    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        credentials_json: str,
        folder_path: str = "/",
        file_format: str = "csv",
        file_compression: str = "none",
        region: str = "us-central1",
        name: str = "google-storage-source",
        **kwargs
    ):
        super().__init__(name=name, **kwargs)
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path.strip("/")
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.region = region
        self.client = None
        self.bucket = None
        self.processed_files = set()
        self.message_count = 0
        self.max_messages = 100  # Stop condition for testing
        
    def setup(self) -> bool:
        """
        Setup Google Cloud Storage client and test connection.
        Returns True if connection is successful, False otherwise.
        """
        try:
            # Parse credentials JSON
            if self.credentials_json.startswith('{'):
                # Direct JSON string
                credentials_info = json.loads(self.credentials_json)
            else:
                # File path or environment variable
                if os.path.isfile(self.credentials_json):
                    with open(self.credentials_json, 'r') as f:
                        credentials_info = json.load(f)
                else:
                    # Treat as environment variable
                    credentials_info = json.loads(os.getenv(self.credentials_json, '{}'))
            
            # Create credentials
            credentials = service_account.Credentials.from_service_account_info(
                credentials_info,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
            
            # Initialize client
            self.client = storage.Client(
                project=self.project_id,
                credentials=credentials
            )
            
            # Test connection by accessing the bucket
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test bucket access
            if self.bucket.exists():
                logger.info(f"Successfully connected to bucket: {self.bucket_name}")
                return True
            else:
                logger.error(f"Bucket {self.bucket_name} does not exist")
                return False
                
        except Exception as e:
            logger.error(f"Failed to setup Google Cloud Storage client: {e}")
            return False
    
    def _decompress_content(self, content: bytes, filename: str) -> str:
        """
        Decompress file content based on compression type.
        """
        try:
            if self.file_compression == "gzip" or filename.endswith('.gz'):
                return gzip.decompress(content).decode('utf-8')
            elif self.file_compression == "zip" or filename.endswith('.zip'):
                with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                    # Read the first file in the zip
                    names = zip_file.namelist()
                    if names:
                        return zip_file.read(names[0]).decode('utf-8')
                    else:
                        raise ValueError("Empty zip file")
            else:
                # No compression or unsupported
                return content.decode('utf-8')
        except Exception as e:
            logger.error(f"Failed to decompress {filename}: {e}")
            raise
    
    def _parse_csv_content(self, content: str, filename: str) -> List[Dict[str, Any]]:
        """
        Parse CSV content and return list of dictionaries.
        """
        try:
            csv_reader = csv.DictReader(io.StringIO(content))
            records = []
            for row in csv_reader:
                # Convert empty strings to None
                cleaned_row = {k: (v if v != '' else None) for k, v in row.items()}
                records.append(cleaned_row)
            return records
        except Exception as e:
            logger.error(f"Failed to parse CSV content from {filename}: {e}")
            return []
    
    def _parse_json_content(self, content: str, filename: str) -> List[Dict[str, Any]]:
        """
        Parse JSON content. Handles both single objects and arrays.
        """
        try:
            data = json.loads(content)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            else:
                logger.warning(f"Unexpected JSON format in {filename}: {type(data)}")
                return [{"data": data}]
        except Exception as e:
            logger.error(f"Failed to parse JSON content from {filename}: {e}")
            return []
    
    def _parse_text_content(self, content: str, filename: str) -> List[Dict[str, Any]]:
        """
        Parse text content line by line.
        """
        lines = content.strip().split('\n')
        records = []
        for i, line in enumerate(lines):
            if line.strip():  # Skip empty lines
                records.append({
                    "line_number": i + 1,
                    "content": line.strip(),
                    "filename": filename
                })
        return records
    
    def _process_file(self, blob) -> int:
        """
        Process a single file from the bucket.
        Returns the number of messages produced.
        """
        try:
            filename = blob.name
            logger.info(f"Processing file: {filename}")
            
            # Download file content
            content_bytes = blob.download_as_bytes()
            
            # Decompress if needed
            content_str = self._decompress_content(content_bytes, filename)
            
            # Parse content based on file format
            records = []
            if self.file_format == "csv":
                records = self._parse_csv_content(content_str, filename)
            elif self.file_format == "json":
                records = self._parse_json_content(content_str, filename)
            elif self.file_format == "txt" or self.file_format == "text":
                records = self._parse_text_content(content_str, filename)
            else:
                logger.warning(f"Unsupported file format: {self.file_format}")
                return 0
            
            # Produce messages to Kafka
            messages_produced = 0
            for record in records:
                if not self.running or self.message_count >= self.max_messages:
                    break
                
                # Add metadata to each record
                enriched_record = {
                    **record,
                    "_metadata": {
                        "filename": filename,
                        "bucket": self.bucket_name,
                        "file_size": blob.size,
                        "last_modified": blob.time_created.isoformat() if blob.time_created else None,
                        "processing_time": self._get_current_timestamp()
                    }
                }
                
                # Serialize and produce message
                try:
                    serialized = self.serialize(
                        key=filename,  # Use filename as message key
                        value=enriched_record
                    )
                    
                    self.produce(
                        key=serialized.key,
                        value=serialized.value
                    )
                    
                    messages_produced += 1
                    self.message_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to produce message from {filename}: {e}")
                    continue
            
            logger.info(f"Produced {messages_produced} messages from {filename}")
            return messages_produced
            
        except Exception as e:
            logger.error(f"Failed to process file {blob.name}: {e}")
            return 0
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime
        return datetime.utcnow().isoformat() + "Z"
    
    def _list_files(self) -> List:
        """
        List files in the bucket based on folder path and file format.
        """
        try:
            # Set prefix for folder path
            prefix = self.folder_path if self.folder_path else None
            
            # List blobs
            blobs = list(self.bucket.list_blobs(prefix=prefix))
            
            # Filter by file format if specified
            if self.file_format in ["csv", "json", "txt", "text"]:
                file_extensions = {
                    "csv": [".csv"],
                    "json": [".json", ".jsonl"],
                    "txt": [".txt"],
                    "text": [".txt", ".text"]
                }
                
                valid_extensions = file_extensions.get(self.file_format, [])
                blobs = [
                    blob for blob in blobs 
                    if any(blob.name.lower().endswith(ext) for ext in valid_extensions)
                    and not blob.name.endswith('/')  # Exclude directories
                ]
            
            logger.info(f"Found {len(blobs)} files to process")
            return blobs
            
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            return []
    
    def run(self):
        """
        Main execution loop for the source.
        """
        logger.info(f"Starting Google Storage Source for bucket: {self.bucket_name}")
        
        try:
            # List all files to process
            files = self._list_files()
            
            if not files:
                logger.warning("No files found to process")
                return
            
            # Process each file
            total_messages = 0
            for blob in files:
                if not self.running or self.message_count >= self.max_messages:
                    break
                
                if blob.name not in self.processed_files:
                    messages_produced = self._process_file(blob)
                    total_messages += messages_produced
                    self.processed_files.add(blob.name)
                    
                    # Log progress
                    if total_messages > 0 and total_messages % 50 == 0:
                        logger.info(f"Processed {total_messages} messages so far...")
            
            logger.info(f"Completed processing. Total messages produced: {total_messages}")
            
        except Exception as e:
            logger.error(f"Error in run method: {e}")
            raise


def main():
    """
    Main function to initialize and run the Google Storage source application.
    """
    # Get configuration from environment variables
    bucket_name = os.getenv("GS_BUCKET", "quix-workflow")
    project_id = os.getenv("GS_PROJECT_ID", "quix-testing-365012")
    credentials_key = os.getenv("GS_SECRET_KEY", "GCLOUD_PK_JSON")
    credentials_json = os.getenv(credentials_key, "{}")
    folder_path = os.getenv("GS_FOLDER_PATH", "/")
    file_format = os.getenv("GS_FILE_FORMAT", "csv")
    file_compression = os.getenv("GS_FILE_COMPRESSION", "none")
    region = os.getenv("GS_REGION", "us-central1")
    output_topic = os.getenv("output", "output")
    
    # Validate required configuration
    if not bucket_name:
        raise ValueError("GS_BUCKET environment variable is required")
    if not project_id:
        raise ValueError("GS_PROJECT_ID environment variable is required")
    if not credentials_json or credentials_json == "{}":
        raise ValueError(f"Google Cloud credentials not found in {credentials_key}")
    
    logger.info(f"Initializing Google Storage source with bucket: {bucket_name}")
    logger.info(f"Project ID: {project_id}")
    logger.info(f"Folder path: {folder_path}")
    logger.info(f"File format: {file_format}")
    logger.info(f"File compression: {file_compression}")
    logger.info(f"Output topic: {output_topic}")
    
    # Initialize Quix Streams application
    app = Application(
        broker_address=os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092"),
        consumer_group="google-storage-bucket-source-draft",
        auto_offset_reset="earliest"
    )
    
    # Create the source
    source = GoogleStorageSource(
        bucket_name=bucket_name,
        project_id=project_id,
        credentials_json=credentials_json,
        folder_path=folder_path,
        file_format=file_format,
        file_compression=file_compression,
        region=region,
        name="google-storage-source"
    )
    
    # Test connection
    if not source.setup():
        logger.error("Failed to connect to Google Cloud Storage")
        return
    
    # Create output topic
    topic = app.topic(output_topic)
    
    # Create streaming dataframe
    sdf = app.dataframe(source=source, topic=topic)
    
    # Print messages for monitoring (optional)
    sdf = sdf.print(metadata=True)
    
    try:
        logger.info("Starting the application...")
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