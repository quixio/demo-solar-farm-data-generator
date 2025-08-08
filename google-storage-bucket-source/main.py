import os
import json
import csv
import io
import logging
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageBucketSource(Source):
    def __init__(self, bucket_name, project_id, credentials_json, folder_path="/", file_format="csv", file_compression="none", **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path.strip('/')
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.client = None
        self.bucket = None
        self.messages_processed = 0
        self.max_messages = 100

    def setup(self):
        try:
            credentials_info = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            self.client = storage.Client(credentials=credentials, project=self.project_id)
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test connection
            self.bucket.exists()
            logger.info(f"Successfully connected to Google Cloud Storage bucket: {self.bucket_name}")
            
            if hasattr(self, 'on_client_connect_success') and callable(self.on_client_connect_success):
                self.on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to connect to Google Cloud Storage: {str(e)}")
            if hasattr(self, 'on_client_connect_failure') and callable(self.on_client_connect_failure):
                self.on_client_connect_failure(e)
            raise

    def run(self):
        try:
            prefix = self.folder_path + '/' if self.folder_path else ''
            blobs = list(self.bucket.list_blobs(prefix=prefix))
            
            if not blobs:
                logger.warning(f"No files found in folder: {self.folder_path}")
                return
            
            # Filter files by format
            target_files = []
            if self.file_format and self.file_format != 'none':
                target_files = [blob for blob in blobs if blob.name.lower().endswith(f'.{self.file_format}')]
            else:
                target_files = blobs
            
            if not target_files:
                logger.warning(f"No files with format '{self.file_format}' found")
                return
            
            logger.info(f"Found {len(target_files)} files to process")
            
            for blob in target_files:
                if not self.running or self.messages_processed >= self.max_messages:
                    break
                    
                self._process_file(blob)
                
        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
            raise

    def _process_file(self, blob):
        try:
            logger.info(f"Processing file: {blob.name}")
            content = blob.download_as_bytes()
            
            if self.file_format == 'csv':
                self._process_csv_content(content, blob.name)
            elif self.file_format == 'json':
                self._process_json_content(content, blob.name)
            else:
                self._process_text_content(content, blob.name)
                
        except Exception as e:
            logger.error(f"Error processing file {blob.name}: {str(e)}")

    def _process_csv_content(self, content, filename):
        try:
            content_str = content.decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(content_str))
            
            for row in csv_reader:
                if not self.running or self.messages_processed >= self.max_messages:
                    break
                
                # Transform CSV row to match the schema
                message_data = {
                    "timestamp": row.get("timestamp", ""),
                    "hotend_temperature": float(row.get("hotend_temperature", 0.0)),
                    "bed_temperature": float(row.get("bed_temperature", 0.0)),
                    "ambient_temperature": float(row.get("ambient_temperature", 0.0)),
                    "fluctuated_ambient_temperature": float(row.get("fluctuated_ambient_temperature", 0.0))
                }
                
                msg = self.serialize(
                    key=f"{filename}_{self.messages_processed}",
                    value=message_data
                )
                
                self.produce(
                    key=msg.key,
                    value=msg.value
                )
                
                self.messages_processed += 1
                
        except Exception as e:
            logger.error(f"Error processing CSV content from {filename}: {str(e)}")

    def _process_json_content(self, content, filename):
        try:
            content_str = content.decode('utf-8')
            
            try:
                json_data = json.loads(content_str)
                if isinstance(json_data, list):
                    for item in json_data:
                        if not self.running or self.messages_processed >= self.max_messages:
                            break
                        self._produce_json_message(item, filename)
                else:
                    self._produce_json_message(json_data, filename)
            except json.JSONDecodeError:
                # Try JSONL format
                lines = content_str.strip().split('\n')
                for line in lines:
                    if not self.running or self.messages_processed >= self.max_messages:
                        break
                    if line.strip():
                        try:
                            json_obj = json.loads(line)
                            self._produce_json_message(json_obj, filename)
                        except json.JSONDecodeError:
                            logger.warning(f"Skipping invalid JSON line in {filename}")
                            
        except Exception as e:
            logger.error(f"Error processing JSON content from {filename}: {str(e)}")

    def _produce_json_message(self, data, filename):
        msg = self.serialize(
            key=f"{filename}_{self.messages_processed}",
            value=data
        )
        
        self.produce(
            key=msg.key,
            value=msg.value
        )
        
        self.messages_processed += 1

    def _process_text_content(self, content, filename):
        try:
            content_str = content.decode('utf-8')
            lines = content_str.split('\n')
            
            for line in lines:
                if not self.running or self.messages_processed >= self.max_messages:
                    break
                if line.strip():
                    msg = self.serialize(
                        key=f"{filename}_{self.messages_processed}",
                        value={"content": line.strip(), "filename": filename}
                    )
                    
                    self.produce(
                        key=msg.key,
                        value=msg.value
                    )
                    
                    self.messages_processed += 1
                    
        except UnicodeDecodeError:
            logger.warning(f"Binary content detected in {filename}, skipping")
        except Exception as e:
            logger.error(f"Error processing text content from {filename}: {str(e)}")

def main():
    app = Application()
    
    # Get configuration from environment variables
    bucket_name = os.environ.get('GS_BUCKET', 'quix-workflow')
    project_id = os.environ.get('GS_PROJECT_ID', 'quix-testing-365012')
    folder_path = os.environ.get('GS_FOLDER_PATH', '/')
    file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
    file_compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
    credentials_json = os.environ.get('GS_SECRET_KEY')
    
    if not credentials_json:
        raise ValueError("GS_SECRET_KEY environment variable is required")
    
    # Create output topic
    output_topic = app.topic(os.environ.get('output', 'output'))
    
    # Create the Google Storage source
    source = GoogleStorageBucketSource(
        name="google-storage-bucket-source",
        bucket_name=bucket_name,
        project_id=project_id,
        credentials_json=credentials_json,
        folder_path=folder_path,
        file_format=file_format,
        file_compression=file_compression
    )
    
    # Create streaming dataframe
    sdf = app.dataframe(topic=output_topic, source=source)
    sdf.print(metadata=True)
    
    logger.info("Starting Google Storage Bucket source application")
    app.run()

if __name__ == "__main__":
    main()