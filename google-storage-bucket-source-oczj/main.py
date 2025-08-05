import os
import json
import csv
import gzip
import bz2
import logging
from io import StringIO, BytesIO
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources import Source
from typing import Iterator, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageSource(Source):
    def __init__(self, bucket_name: str, project_id: str, credentials_json: str, 
                 folder_path: str = "/", file_format: str = "csv", 
                 file_compression: str = "none"):
        super().__init__()
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path.lstrip('/')
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.client = None
        self.bucket = None
        self._processed_files = set()
        self._message_count = 0
        self._max_messages = 100
        
    def configure(self):
        """Configure the Google Storage client"""
        try:
            # Parse credentials JSON
            credentials_dict = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            
            # Initialize Google Cloud Storage client
            self.client = storage.Client(credentials=credentials, project=self.project_id)
            self.bucket = self.client.bucket(self.bucket_name)
            
            logger.info(f"Successfully connected to Google Storage bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to configure Google Storage client: {str(e)}")
            raise
            
    def read(self) -> Iterator[Dict[str, Any]]:
        """Read data from Google Storage bucket"""
        if self.client is None:
            self.configure()
            
        try:
            # List files in the specified folder
            blobs = list(self.bucket.list_blobs(prefix=self.folder_path))
            
            if not blobs:
                logger.warning("No files found in the specified folder")
                return
                
            # Filter files by format if specified
            if self.file_format != 'none':
                filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{self.file_format}')]
                if filtered_blobs:
                    blobs = filtered_blobs
                else:
                    logger.warning(f"No {self.file_format} files found, using all available files")
            
            logger.info(f"Found {len(blobs)} files to process")
            
            for blob in blobs:
                if self._message_count >= self._max_messages:
                    logger.info(f"Reached maximum message limit of {self._max_messages}")
                    break
                    
                # Skip directories and already processed files
                if blob.name.endswith('/') or blob.name in self._processed_files:
                    continue
                    
                logger.info(f"Processing file: {blob.name}")
                
                try:
                    # Download file content
                    file_content = blob.download_as_bytes()
                    
                    # Handle compression
                    if self.file_compression == 'gzip':
                        file_content = gzip.decompress(file_content)
                    elif self.file_compression == 'bz2':
                        file_content = bz2.decompress(file_content)
                    
                    # Process based on file format
                    for message in self._process_file_content(file_content, blob.name):
                        if self._message_count >= self._max_messages:
                            break
                            
                        # Add metadata to the message
                        enriched_message = {
                            "data": message,
                            "metadata": {
                                "source_file": blob.name,
                                "bucket": self.bucket_name,
                                "processed_at": message.get("timestamp", ""),
                                "file_format": self.file_format
                            }
                        }
                        
                        self._message_count += 1
                        yield enriched_message
                        
                    self._processed_files.add(blob.name)
                    
                except Exception as e:
                    logger.error(f"Error processing file {blob.name}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error reading from Google Storage: {str(e)}")
            raise
            
    def _process_file_content(self, file_content: bytes, file_name: str) -> Iterator[Dict[str, Any]]:
        """Process file content based on format"""
        try:
            if self.file_format == 'csv':
                content_str = file_content.decode('utf-8')
                csv_reader = csv.DictReader(StringIO(content_str))
                
                for row in csv_reader:
                    # Convert temperature fields to float
                    processed_row = {}
                    for key, value in row.items():
                        if key == 'timestamp':
                            processed_row[key] = value
                        elif 'temperature' in key.lower():
                            try:
                                processed_row[key] = float(value)
                            except (ValueError, TypeError):
                                processed_row[key] = value
                        else:
                            processed_row[key] = value
                    
                    yield processed_row
                    
            elif self.file_format == 'json':
                content_str = file_content.decode('utf-8')
                try:
                    # Try parsing as JSON array
                    json_data = json.loads(content_str)
                    if isinstance(json_data, list):
                        for item in json_data:
                            yield item
                    else:
                        # Single JSON object
                        yield json_data
                except json.JSONDecodeError:
                    # Try reading as JSONL (newline-delimited JSON)
                    lines = content_str.strip().split('\n')
                    for line in lines:
                        if line.strip():
                            try:
                                item = json.loads(line)
                                yield item
                            except json.JSONDecodeError:
                                continue
                                
            elif self.file_format == 'txt':
                content_str = file_content.decode('utf-8')
                lines = content_str.strip().split('\n')
                for line_num, line in enumerate(lines, 1):
                    if line.strip():
                        yield {
                            "line_number": line_num,
                            "content": line.strip(),
                            "file": file_name
                        }
            else:
                # For other formats, yield as binary content info
                yield {
                    "file": file_name,
                    "size": len(file_content),
                    "content_type": "binary"
                }
                
        except Exception as e:
            logger.error(f"Error processing file content: {str(e)}")
            raise

def main():
    # Get environment variables
    bucket_name = os.environ.get('GS_BUCKET')
    project_id = os.environ.get('GS_PROJECT_ID')
    folder_path = os.environ.get('GS_FOLDER_PATH', '/')
    file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
    file_compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
    credentials_json = os.environ.get('GS_SECRET_KEY')
    output_topic_name = os.environ.get('output', 'output')
    
    # Validate required environment variables
    if not bucket_name:
        raise ValueError("GS_BUCKET environment variable is required")
    if not project_id:
        raise ValueError("GS_PROJECT_ID environment variable is required")
    if not credentials_json:
        raise ValueError("GS_SECRET_KEY environment variable is required")
    
    logger.info(f"Starting Google Storage source application")
    logger.info(f"Bucket: {bucket_name}")
    logger.info(f"Project ID: {project_id}")
    logger.info(f"Folder path: {folder_path}")
    logger.info(f"File format: {file_format}")
    logger.info(f"Compression: {file_compression}")
    logger.info(f"Output topic: {output_topic_name}")
    
    # Create Quix Application
    app = Application()
    
    # Create output topic
    output_topic = app.topic(output_topic_name)
    
    # Create Google Storage source
    source = GoogleStorageSource(
        bucket_name=bucket_name,
        project_id=project_id,
        credentials_json=credentials_json,
        folder_path=folder_path,
        file_format=file_format,
        file_compression=file_compression
    )
    
    # Create streaming dataframe
    sdf = app.dataframe(topic=output_topic, source=source)
    
    # Print messages for debugging
    sdf.print(metadata=True)
    
    # Add source to application
    app.add_source(source)
    
    logger.info("Starting application...")
    app.run()

if __name__ == "__main__":
    main()