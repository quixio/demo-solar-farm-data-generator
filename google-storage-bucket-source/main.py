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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageBucketSource(Source):
    def __init__(self, bucket_name: str, folder_path: str, file_format: str, 
                 file_compression: str, service_account_key: str, **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.folder_path = folder_path.strip('/')
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.service_account_key = service_account_key
        self.client = None
        self.bucket = None
        self.processed_count = 0
        self.max_messages = 100

    def setup(self):
        """Setup Google Cloud Storage client and test connection"""
        try:
            if not self.service_account_key:
                raise ValueError("Service account credentials not found")
            
            credentials_info = json.loads(self.service_account_key)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            
            self.client = storage.Client(credentials=credentials)
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test connection by checking if bucket exists
            if not self.bucket.exists():
                raise ValueError(f"Bucket {self.bucket_name} does not exist or is not accessible")
            
            logger.info(f"Successfully connected to Google Cloud Storage bucket: {self.bucket_name}")
            
            if hasattr(self, 'on_client_connect_success') and callable(self.on_client_connect_success):
                self.on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to connect to Google Cloud Storage: {str(e)}")
            if hasattr(self, 'on_client_connect_failure') and callable(self.on_client_connect_failure):
                self.on_client_connect_failure(e)
            raise

    def run(self):
        """Main processing loop"""
        try:
            self.setup()
            
            # List blobs in the specified folder with the specified format
            prefix = f"{self.folder_path}/" if self.folder_path else ""
            blobs = self.bucket.list_blobs(prefix=prefix)
            
            # Filter blobs by file format
            matching_blobs = []
            for blob in blobs:
                if blob.name.endswith(f'.{self.file_format}') and not blob.name.endswith('/'):
                    matching_blobs.append(blob)
            
            if not matching_blobs:
                logger.warning(f"No {self.file_format} files found in folder: {self.folder_path}")
                return
            
            logger.info(f"Found {len(matching_blobs)} {self.file_format} file(s)")
            
            # Process each file
            for blob in matching_blobs:
                if not self.running or self.processed_count >= self.max_messages:
                    break
                
                try:
                    self._process_file(blob)
                except Exception as e:
                    logger.error(f"Error processing file {blob.name}: {str(e)}")
                    continue
            
            logger.info(f"Processing completed. Total messages processed: {self.processed_count}")
            
        except Exception as e:
            logger.error(f"Error in source run method: {str(e)}")
            raise

    def _process_file(self, blob):
        """Process a single file from Google Cloud Storage"""
        logger.info(f"Processing file: {blob.name} (size: {blob.size} bytes)")
        
        try:
            # Download blob content
            content = blob.download_as_text()
            
            if self.file_format == 'csv':
                self._process_csv_content(content, blob.name)
            elif self.file_format == 'json':
                self._process_json_content(content, blob.name)
            elif self.file_format == 'txt':
                self._process_text_content(content, blob.name)
            else:
                logger.warning(f"Unsupported file format: {self.file_format}")
                
        except Exception as e:
            logger.error(f"Error processing file {blob.name}: {str(e)}")
            raise

    def _process_csv_content(self, content: str, filename: str):
        """Process CSV file content"""
        csv_reader = csv.DictReader(io.StringIO(content))
        
        for row in csv_reader:
            if not self.running or self.processed_count >= self.max_messages:
                break
            
            try:
                # Transform CSV row to match the expected schema
                message_value = self._transform_csv_row(row)
                
                # Create message key from filename and row number
                message_key = f"{filename}_{self.processed_count}"
                
                # Serialize and produce message
                msg = self.serialize(key=message_key, value=message_value)
                self.produce(key=msg.key, value=msg.value)
                
                self.processed_count += 1
                
                if self.processed_count % 10 == 0:
                    logger.info(f"Processed {self.processed_count} messages")
                
            except Exception as e:
                logger.error(f"Error processing CSV row: {str(e)}")
                continue

    def _transform_csv_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Transform CSV row to match the expected schema"""
        try:
            # Based on the schema analysis, transform the CSV row
            transformed = {
                "timestamp": row.get("timestamp", ""),
                "hotend_temperature": float(row.get("hotend_temperature", 0.0)),
                "bed_temperature": float(row.get("bed_temperature", 0.0)),
                "ambient_temperature": float(row.get("ambient_temperature", 0.0)),
                "fluctuated_ambient_temperature": float(row.get("fluctuated_ambient_temperature", 0.0))
            }
            return transformed
        except (ValueError, TypeError) as e:
            logger.error(f"Error transforming CSV row: {str(e)}")
            # Return raw row if transformation fails
            return dict(row)

    def _process_json_content(self, content: str, filename: str):
        """Process JSON file content"""
        try:
            # Try parsing as JSON array
            data = json.loads(content)
            if isinstance(data, list):
                for item in data:
                    if not self.running or self.processed_count >= self.max_messages:
                        break
                    self._produce_json_message(item, filename)
            else:
                # Single JSON object
                self._produce_json_message(data, filename)
                
        except json.JSONDecodeError:
            # Try parsing as JSONL (newline-delimited JSON)
            lines = content.strip().split('\n')
            for line in lines:
                if not self.running or self.processed_count >= self.max_messages:
                    break
                try:
                    item = json.loads(line)
                    self._produce_json_message(item, filename)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON line: {line}")
                    continue

    def _produce_json_message(self, data: Any, filename: str):
        """Produce a JSON message to Kafka"""
        try:
            message_key = f"{filename}_{self.processed_count}"
            msg = self.serialize(key=message_key, value=data)
            self.produce(key=msg.key, value=msg.value)
            
            self.processed_count += 1
            
            if self.processed_count % 10 == 0:
                logger.info(f"Processed {self.processed_count} messages")
                
        except Exception as e:
            logger.error(f"Error producing JSON message: {str(e)}")

    def _process_text_content(self, content: str, filename: str):
        """Process text file content"""
        lines = content.strip().split('\n')
        
        for line in lines:
            if not self.running or self.processed_count >= self.max_messages:
                break
            
            try:
                message_key = f"{filename}_{self.processed_count}"
                message_value = {"text": line.strip(), "source_file": filename}
                
                msg = self.serialize(key=message_key, value=message_value)
                self.produce(key=msg.key, value=msg.value)
                
                self.processed_count += 1
                
                if self.processed_count % 10 == 0:
                    logger.info(f"Processed {self.processed_count} messages")
                    
            except Exception as e:
                logger.error(f"Error processing text line: {str(e)}")
                continue

def main():
    # Create Quix Streams application
    app = Application()
    
    # Get configuration from environment variables
    output_topic_name = os.environ.get('output', 'badboy')
    bucket_uri = os.environ.get('GOOGLE_BUCKET_URI', 'gs://quix-workflow')
    bucket_name = bucket_uri.replace('gs://', '')
    folder_path = os.environ.get('GOOGLE_FOLDER_PATH', '/')
    file_format = os.environ.get('GOOGLE_FILE_FORMAT', 'csv')
    file_compression = os.environ.get('GOOGLE_FILE_COMPRESSION', 'none')
    service_account_key = os.environ.get('GOOGLE_SERVICE_ACCOUNT_KEY_KEY', '')
    
    # Create output topic
    topic = app.topic(output_topic_name)
    
    # Create Google Storage Bucket source
    source = GoogleStorageBucketSource(
        name="google-storage-bucket-source",
        bucket_name=bucket_name,
        folder_path=folder_path,
        file_format=file_format,
        file_compression=file_compression,
        service_account_key=service_account_key
    )
    
    # Create streaming dataframe
    sdf = app.dataframe(topic=topic, source=source)
    
    # Print messages for monitoring
    sdf.print(metadata=True)
    
    # Run the application
    logger.info("Starting Google Storage Bucket source application")
    app.run()

if __name__ == "__main__":
    main()