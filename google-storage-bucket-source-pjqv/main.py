import os
import json
import csv
import io
import logging
from typing import Dict, Any, Optional
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageBucketSource(Source):
    def __init__(self, bucket_name: str, folder_path: str, file_format: str, 
                 file_compression: str, project_id: str, credentials_json: str):
        super().__init__(name="google-storage-bucket-source")
        self.bucket_name = bucket_name
        self.folder_path = folder_path.lstrip('/')
        if self.folder_path and not self.folder_path.endswith('/'):
            self.folder_path += '/'
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.client = None
        self.bucket = None
        self.processed_files = set()
        self.message_count = 0
        
    def setup(self):
        """Setup the Google Cloud Storage client and test connection."""
        try:
            credentials_dict = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            
            self.client = storage.Client(
                credentials=credentials,
                project=self.project_id
            )
            
            self.bucket = self.client.bucket(self.bucket_name)
            
            if not self.bucket.exists():
                raise ValueError(f"Bucket '{self.bucket_name}' does not exist or is not accessible")
                
            logger.info(f"Successfully connected to Google Storage bucket: {self.bucket_name}")
            return True
            
        except json.JSONDecodeError:
            logger.error("Invalid JSON format in GCP credentials")
            return False
        except Exception as e:
            logger.error(f"Failed to setup Google Storage client: {str(e)}")
            return False
    
    def _read_csv_file(self, blob) -> list:
        """Read CSV file and return rows as list of dictionaries."""
        try:
            content = blob.download_as_text()
            csv_reader = csv.DictReader(io.StringIO(content))
            return list(csv_reader)
        except Exception as e:
            logger.error(f"Error reading CSV file {blob.name}: {str(e)}")
            return []
    
    def _read_json_file(self, blob) -> list:
        """Read JSON file and return data as list."""
        try:
            content = blob.download_as_text()
            data = json.loads(content)
            if isinstance(data, list):
                return data
            else:
                return [data]
        except Exception as e:
            logger.error(f"Error reading JSON file {blob.name}: {str(e)}")
            return []
    
    def _read_text_file(self, blob) -> list:
        """Read text file and return lines as list of dictionaries."""
        try:
            content = blob.download_as_text()
            lines = content.strip().split('\n')
            return [{'line_number': i+1, 'content': line} for i, line in enumerate(lines)]
        except Exception as e:
            logger.error(f"Error reading text file {blob.name}: {str(e)}")
            return []
    
    def _transform_message(self, data: Dict[str, Any], source_file: str) -> Dict[str, Any]:
        """Transform data into appropriate Kafka message format."""
        try:
            # Handle CSV data with temperature readings
            if all(key in data for key in ['timestamp', 'hotend_temperature', 'bed_temperature']):
                return {
                    'timestamp': data.get('timestamp'),
                    'hotend_temperature': float(data.get('hotend_temperature', 0)),
                    'bed_temperature': float(data.get('bed_temperature', 0)),
                    'ambient_temperature': float(data.get('ambient_temperature', 0)),
                    'fluctuated_ambient_temperature': float(data.get('fluctuated_ambient_temperature', 0)),
                    'source_file': source_file,
                    'message_type': '3d_printer_data'
                }
            else:
                # Generic data transformation
                return {
                    'data': data,
                    'source_file': source_file,
                    'message_type': 'generic_data'
                }
        except Exception as e:
            logger.error(f"Error transforming message: {str(e)}")
            return {
                'raw_data': str(data),
                'source_file': source_file,
                'message_type': 'raw_data',
                'error': str(e)
            }
    
    def run(self):
        """Main processing loop to read data from Google Storage and produce to Kafka."""
        if not self.setup():
            logger.error("Failed to setup Google Storage connection")
            return
            
        logger.info(f"Starting to process files from bucket: {self.bucket_name}")
        
        try:
            # List all blobs in the specified folder
            blobs = list(self.bucket.list_blobs(prefix=self.folder_path))
            
            if not blobs:
                logger.warning(f"No files found in folder: {self.folder_path}")
                return
                
            # Filter files by format if specified
            if self.file_format != 'all':
                filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{self.file_format}')]
                if not filtered_blobs:
                    logger.warning(f"No {self.file_format} files found")
                    return
                blobs = filtered_blobs
            
            logger.info(f"Found {len(blobs)} files to process")
            
            for blob in blobs:
                if not self.running:
                    break
                    
                # Skip directories
                if blob.name.endswith('/'):
                    continue
                    
                # Skip already processed files
                if blob.name in self.processed_files:
                    continue
                
                logger.info(f"Processing file: {blob.name}")
                
                try:
                    # Determine file type and read accordingly
                    file_extension = blob.name.lower().split('.')[-1] if '.' in blob.name else ''
                    
                    if file_extension == 'csv' or self.file_format == 'csv':
                        rows = self._read_csv_file(blob)
                    elif file_extension == 'json' or self.file_format == 'json':
                        rows = self._read_json_file(blob)
                    else:
                        rows = self._read_text_file(blob)
                    
                    # Process each row
                    for row in rows:
                        if not self.running or self.message_count >= 100:
                            break
                            
                        # Transform the data
                        transformed_data = self._transform_message(row, blob.name)
                        
                        # Serialize the message
                        msg = self.serialize(
                            key=blob.name,
                            value=transformed_data
                        )
                        
                        # Produce to Kafka
                        self.produce(
                            key=msg.key,
                            value=msg.value
                        )
                        
                        self.message_count += 1
                        
                        if self.message_count >= 100:
                            logger.info("Reached 100 messages limit, stopping...")
                            break
                    
                    self.processed_files.add(blob.name)
                    
                    if self.message_count >= 100:
                        break
                        
                except Exception as e:
                    logger.error(f"Error processing file {blob.name}: {str(e)}")
                    continue
            
            logger.info(f"Finished processing. Total messages produced: {self.message_count}")
            
        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
        finally:
            if self.client:
                logger.info("Closing Google Storage client connection")

def main():
    """Main application entry point."""
    app = Application()
    
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
    
    # Create output topic
    topic = app.topic(output_topic_name)
    
    # Create the source
    source = GoogleStorageBucketSource(
        bucket_name=bucket_name,
        folder_path=folder_path,
        file_format=file_format,
        file_compression=file_compression,
        project_id=project_id,
        credentials_json=credentials_json
    )
    
    # Create streaming dataframe
    sdf = app.dataframe(topic=topic, source=source)
    sdf.print(metadata=True)
    
    logger.info("Starting Google Storage Bucket source application...")
    app.run()

if __name__ == "__main__":
    main()