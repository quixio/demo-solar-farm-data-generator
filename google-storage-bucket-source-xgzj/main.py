import os
import json
import logging
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageSource(Source):
    def __init__(self, bucket_name, credentials_json, project_id, folder_path="/", file_format="csv", compression="none", **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.credentials_json = credentials_json
        self.project_id = project_id
        self.folder_path = folder_path.lstrip('/')
        self.file_format = file_format
        self.compression = compression
        self.client = None
        self.bucket = None
        self.processed_count = 0
        
    def setup(self):
        """Setup Google Cloud Storage client and test connection."""
        try:
            if not self.credentials_json:
                raise ValueError("Credentials not found in environment variable")
            
            credentials_dict = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            
            self.client = storage.Client(
                credentials=credentials,
                project=self.project_id
            )
            
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test connection
            self.bucket.reload()
            logger.info(f"Successfully connected to Google Storage bucket: {self.bucket_name}")
            
            if hasattr(self, 'on_client_connect_success') and callable(self.on_client_connect_success):
                self.on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to connect to Google Storage: {str(e)}")
            if hasattr(self, 'on_client_connect_failure') and callable(self.on_client_connect_failure):
                self.on_client_connect_failure(e)
            raise
    
    def run(self):
        """Main processing loop to read files from Google Storage."""
        self.setup()
        
        try:
            blobs = self.bucket.list_blobs(prefix=self.folder_path)
            
            for blob in blobs:
                if not self.running:
                    break
                
                # Skip directories
                if blob.name.endswith('/'):
                    continue
                
                # Check if we've processed enough messages for testing
                if self.processed_count >= 100:
                    logger.info("Processed 100 messages, stopping for testing")
                    break
                
                try:
                    self._process_file(blob)
                    self.processed_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing file {blob.name}: {str(e)}")
                    continue
            
            logger.info(f"Finished processing. Total files processed: {self.processed_count}")
            
        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
            raise
    
    def _process_file(self, blob):
        """Process a single file from Google Storage."""
        try:
            # Download file content
            content = blob.download_as_text()
            
            # Create message data
            message_data = {
                "file_name": blob.name,
                "file_size": blob.size,
                "content_type": blob.content_type,
                "created_time": blob.time_created.isoformat() if blob.time_created else None,
                "updated_time": blob.updated.isoformat() if blob.updated else None,
                "content": content,
                "bucket_name": self.bucket_name,
                "file_format": self.file_format,
                "compression": self.compression
            }
            
            # Handle different file formats
            if self.file_format.lower() == "csv":
                message_data["data_type"] = "csv"
                # For CSV, we could parse it, but for now keep as text
                message_data["raw_content"] = content
            elif self.file_format.lower() == "json":
                message_data["data_type"] = "json"
                try:
                    parsed_json = json.loads(content)
                    message_data["parsed_content"] = parsed_json
                except json.JSONDecodeError:
                    message_data["parse_error"] = "Invalid JSON format"
            else:
                message_data["data_type"] = "text"
                message_data["raw_content"] = content
            
            # Serialize the message
            serialized = self.serialize(
                key=blob.name,
                value=message_data
            )
            
            # Produce the message to Kafka
            self.produce(
                key=serialized.key,
                value=serialized.value
            )
            
            logger.info(f"Successfully processed file: {blob.name}")
            
        except Exception as e:
            logger.error(f"Error processing file {blob.name}: {str(e)}")
            raise

def main():
    # Initialize the Quix Streams application
    app = Application()
    
    # Get configuration from environment variables
    bucket_name = os.environ.get('GS_BUCKET', 'quix-workflow')
    credentials_json = os.environ.get('GS_SECRET_KEY')
    project_id = os.environ.get('GS_PROJECT_ID', 'quix-testing-365012')
    folder_path = os.environ.get('GS_FOLDER_PATH', '/')
    file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
    compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
    output_topic_name = os.environ.get('output', 'output')
    
    # Create the output topic
    output_topic = app.topic(output_topic_name)
    
    # Create the Google Storage source
    source = GoogleStorageSource(
        name="google-storage-source",
        bucket_name=bucket_name,
        credentials_json=credentials_json,
        project_id=project_id,
        folder_path=folder_path,
        file_format=file_format,
        compression=compression
    )
    
    # Add the source to the application
    app.add_source(source, topic=output_topic)
    
    # Run the application
    logger.info("Starting Google Storage source application...")
    app.run()

if __name__ == "__main__":
    main()