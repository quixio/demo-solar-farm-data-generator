import os
import json
import logging
import tempfile
from typing import Iterator, Optional, Any, Dict
from google.cloud import storage
from quixstreams import Application
from quixstreams.sources.base import Source
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GCSSource(Source):
    def __init__(self, bucket_name: str, folder_path: str, file_format: str, 
                 file_compression: str, project_id: str, credentials_json: str,
                 name: str = "gcs-source"):
        super().__init__(name=name)
        self.bucket_name = bucket_name
        self.folder_path = folder_path.strip('/')
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.client = None
        self.bucket = None
        self.processed_count = 0
        self.max_messages = 100
        
    def setup(self) -> bool:
        """Setup GCS connection and test connectivity"""
        try:
            # Parse credentials JSON
            credentials_dict = json.loads(self.credentials_json)
            
            # Create temporary credentials file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
                json.dump(credentials_dict, temp_file)
                self.temp_credentials_path = temp_file.name
            
            # Initialize GCS client
            self.client = storage.Client.from_service_account_json(
                self.temp_credentials_path,
                project=self.project_id
            )
            
            # Get bucket reference
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test bucket access
            if not self.bucket.exists():
                logger.error(f"Bucket {self.bucket_name} does not exist or is not accessible")
                return False
                
            logger.info(f"Successfully connected to GCS bucket: {self.bucket_name}")
            return True
            
        except json.JSONDecodeError:
            logger.error("Invalid JSON in credentials")
            return False
        except Exception as e:
            logger.error(f"Failed to setup GCS connection: {e}")
            return False
    
    def _get_files(self) -> Iterator[storage.Blob]:
        """Get files from GCS bucket matching the criteria"""
        try:
            prefix = self.folder_path
            if prefix and not prefix.endswith('/'):
                prefix += '/'
            
            blobs = self.bucket.list_blobs(prefix=prefix)
            
            for blob in blobs:
                # Skip directories
                if blob.name.endswith('/'):
                    continue
                
                # Check file format if specified
                if self.file_format != 'none' and not blob.name.lower().endswith(f'.{self.file_format}'):
                    continue
                
                yield blob
                
        except Exception as e:
            logger.error(f"Error listing files from bucket: {e}")
            raise
    
    def _parse_csv_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a CSV line into a dictionary"""
        try:
            # Remove newline and split by comma
            fields = line.strip().split(',')
            
            if len(fields) != 5:
                return None
            
            # Based on schema analysis: timestamp, hotend_temperature, bed_temperature, ambient_temperature, fluctuated_ambient_temperature
            return {
                "timestamp": fields[0],
                "hotend_temperature": float(fields[1]),
                "bed_temperature": float(fields[2]),
                "ambient_temperature": float(fields[3]),
                "fluctuated_ambient_temperature": float(fields[4])
            }
        except (ValueError, IndexError) as e:
            logger.warning(f"Failed to parse CSV line: {line}, error: {e}")
            return None
    
    def _process_file(self, blob: storage.Blob) -> Iterator[Dict[str, Any]]:
        """Process a single file and yield records"""
        try:
            logger.info(f"Processing file: {blob.name}")
            content = blob.download_as_text()
            
            lines = content.split('\n')
            
            # Skip header if it exists (check if first line contains 'timestamp')
            start_idx = 1 if lines and 'timestamp' in lines[0].lower() else 0
            
            for line in lines[start_idx:]:
                if not line.strip():
                    continue
                
                if self.file_format == 'csv':
                    record = self._parse_csv_line(line)
                    if record:
                        yield record
                else:
                    # For other formats, treat as raw text
                    yield {"content": line.strip(), "filename": blob.name}
                    
        except Exception as e:
            logger.error(f"Error processing file {blob.name}: {e}")
            raise
    
    def run(self):
        """Main execution loop"""
        try:
            logger.info("Starting GCS source")
            
            for blob in self._get_files():
                if not self.running:
                    break
                
                if self.processed_count >= self.max_messages:
                    logger.info(f"Reached maximum message limit: {self.max_messages}")
                    break
                
                for record in self._process_file(blob):
                    if not self.running:
                        return
                    
                    if self.processed_count >= self.max_messages:
                        logger.info(f"Reached maximum message limit: {self.max_messages}")
                        return
                    
                    # Create message key from filename and timestamp
                    key = f"{blob.name}_{record.get('timestamp', str(self.processed_count))}"
                    
                    # Serialize the message
                    msg = self.serialize(
                        key=key,
                        value=record,
                        headers={"source_file": blob.name, "processed_at": datetime.utcnow().isoformat()}
                    )
                    
                    # Produce to Kafka
                    self.produce(
                        key=msg.key,
                        value=msg.value,
                        headers=msg.headers
                    )
                    
                    self.processed_count += 1
                    
                    if self.processed_count % 10 == 0:
                        logger.info(f"Processed {self.processed_count} messages")
            
            logger.info(f"Finished processing. Total messages: {self.processed_count}")
            
        except Exception as e:
            logger.error(f"Error in GCS source run loop: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'temp_credentials_path'):
                os.unlink(self.temp_credentials_path)
        except Exception as e:
            logger.warning(f"Error cleaning up temporary file: {e}")

def main():
    # Get configuration from environment variables
    bucket_name = os.environ.get('GCS_BUCKET')
    folder_path = os.environ.get('GCS_FOLDER_PATH', '/')
    file_format = os.environ.get('GCS_FILE_FORMAT', 'csv')
    file_compression = os.environ.get('GCS_FILE_COMPRESSION', 'none')
    project_id = os.environ.get('GCP_PROJECT_ID')
    credentials_json = os.environ.get('GCP_CREDENTIALS_KEY')
    output_topic_name = os.environ.get('output', 'output')
    
    # Validate required environment variables
    if not bucket_name:
        raise ValueError("GCS_BUCKET environment variable is required")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID environment variable is required")
    if not credentials_json:
        raise ValueError("GCP_CREDENTIALS_KEY environment variable is required")
    
    logger.info(f"Initializing GCS source with bucket: {bucket_name}")
    
    # Create Quix Streams application
    app = Application()
    
    # Create output topic
    topic = app.topic(output_topic_name)
    
    # Create GCS source
    source = GCSSource(
        bucket_name=bucket_name,
        folder_path=folder_path,
        file_format=file_format,
        file_compression=file_compression,
        project_id=project_id,
        credentials_json=credentials_json,
        name="gcs-source"
    )
    
    # Set up connection callbacks
    def on_success():
        logger.info("GCS connection established successfully")
    
    def on_failure(error):
        logger.error(f"GCS connection failed: {error}")
    
    source.on_client_connect_success = on_success
    source.on_client_connect_failure = on_failure
    
    # Create streaming dataframe
    sdf = app.dataframe(topic=topic, source=source)
    
    # Add data transformation and validation
    def validate_and_transform(value):
        try:
            # Ensure timestamp is properly formatted
            if 'timestamp' in value:
                # Convert timestamp to ISO format if needed
                timestamp_str = value['timestamp']
                # Add basic validation
                if not timestamp_str:
                    value['timestamp'] = datetime.utcnow().isoformat()
                    
            # Add processing metadata
            value['processed_at'] = datetime.utcnow().isoformat()
            value['source'] = 'gcs-bucket'
            
            return value
        except Exception as e:
            logger.warning(f"Error transforming message: {e}")
            return value
    
    # Apply transformation
    sdf = sdf.apply(validate_and_transform)
    
    # Print messages for monitoring
    sdf.print(metadata=True)
    
    # Run the application
    logger.info("Starting Quix Streams application")
    app.run()

if __name__ == "__main__":
    main()