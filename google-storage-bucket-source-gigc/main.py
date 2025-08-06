import os
import json
import csv
import logging
from io import StringIO
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageSource(Source):
    def __init__(self, bucket_name, folder_path, project_id, credentials_json, file_format="csv", file_compression="none", **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.folder_path = folder_path.strip('/')
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.client = None
        self.bucket = None
        self.messages_processed = 0
        self.max_messages = 100

    def setup(self):
        """Setup Google Cloud Storage client and test connection"""
        try:
            if not self.credentials_json:
                raise ValueError("GCP credentials not found in environment variable")
            
            credentials_dict = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            
            self.client = storage.Client(
                credentials=credentials,
                project=self.project_id
            )
            
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test connection by checking if bucket exists
            if not self.bucket.exists():
                raise ValueError(f"Bucket {self.bucket_name} does not exist or is not accessible")
            
            logger.info(f"Successfully connected to GCS bucket: {self.bucket_name}")
            
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in GCP credentials")
            raise ValueError("Invalid JSON in GCP credentials") from e
        except Exception as e:
            logger.error(f"Failed to setup GCS connection: {str(e)}")
            raise

    def run(self):
        """Main processing loop to read files from GCS and produce messages"""
        try:
            self.setup()
            
            # List files in the specified folder
            prefix = self.folder_path + '/' if self.folder_path else ''
            blobs = list(self.bucket.list_blobs(prefix=prefix))
            
            # Filter files by format
            target_files = [blob for blob in blobs if blob.name.lower().endswith(f'.{self.file_format}')]
            
            if not target_files:
                logger.warning(f"No {self.file_format} files found in folder: {self.folder_path}")
                return
            
            logger.info(f"Found {len(target_files)} {self.file_format} files to process")
            
            # Process each file
            for blob in target_files:
                if not self.running or self.messages_processed >= self.max_messages:
                    break
                
                logger.info(f"Processing file: {blob.name}")
                self._process_file(blob)
            
            logger.info(f"Completed processing. Total messages: {self.messages_processed}")
            
        except Exception as e:
            logger.error(f"Error in run method: {str(e)}")
            raise

    def _process_file(self, blob):
        """Process a single file from GCS"""
        try:
            # Download file content
            content = blob.download_as_text()
            
            if self.file_format == 'csv':
                self._process_csv_content(content, blob.name)
            elif self.file_format == 'json':
                self._process_json_content(content, blob.name)
            else:
                self._process_text_content(content, blob.name)
                
        except Exception as e:
            logger.error(f"Error processing file {blob.name}: {str(e)}")
            raise

    def _process_csv_content(self, content, filename):
        """Process CSV content and produce messages"""
        try:
            csv_reader = csv.DictReader(StringIO(content))
            
            for row in csv_reader:
                if not self.running or self.messages_processed >= self.max_messages:
                    break
                
                # Transform CSV row to match the expected schema
                message_data = self._transform_csv_row(row)
                
                # Serialize the message
                msg = self.serialize(
                    key=filename,
                    value=message_data
                )
                
                # Produce the message
                self.produce(
                    key=msg.key,
                    value=msg.value
                )
                
                self.messages_processed += 1
                
                if self.messages_processed % 10 == 0:
                    logger.info(f"Processed {self.messages_processed} messages")
                    
        except Exception as e:
            logger.error(f"Error processing CSV content: {str(e)}")
            raise

    def _process_json_content(self, content, filename):
        """Process JSON content and produce messages"""
        try:
            # Try JSON Lines format first
            lines = content.strip().split('\n')
            json_objects = []
            
            for line in lines:
                if line.strip():
                    try:
                        json_objects.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
            
            # If no objects found, try parsing as single JSON array
            if not json_objects:
                data = json.loads(content)
                if isinstance(data, list):
                    json_objects = data
                else:
                    json_objects = [data]
            
            for obj in json_objects:
                if not self.running or self.messages_processed >= self.max_messages:
                    break
                
                msg = self.serialize(
                    key=filename,
                    value=obj
                )
                
                self.produce(
                    key=msg.key,
                    value=msg.value
                )
                
                self.messages_processed += 1
                
                if self.messages_processed % 10 == 0:
                    logger.info(f"Processed {self.messages_processed} messages")
                    
        except Exception as e:
            logger.error(f"Error processing JSON content: {str(e)}")
            raise

    def _process_text_content(self, content, filename):
        """Process text content line by line"""
        try:
            lines = content.strip().split('\n')
            
            for line in lines:
                if not self.running or self.messages_processed >= self.max_messages:
                    break
                
                if line.strip():
                    msg = self.serialize(
                        key=filename,
                        value={"line": line.strip()}
                    )
                    
                    self.produce(
                        key=msg.key,
                        value=msg.value
                    )
                    
                    self.messages_processed += 1
                    
                    if self.messages_processed % 10 == 0:
                        logger.info(f"Processed {self.messages_processed} messages")
                        
        except Exception as e:
            logger.error(f"Error processing text content: {str(e)}")
            raise

    def _transform_csv_row(self, row):
        """Transform CSV row to match the expected 3D printer data schema"""
        try:
            transformed_data = {}
            
            # Map CSV columns to expected schema
            for key, value in row.items():
                if key == 'timestamp':
                    transformed_data['timestamp'] = value
                elif key in ['hotend_temperature', 'bed_temperature', 'ambient_temperature', 'fluctuated_ambient_temperature']:
                    try:
                        transformed_data[key] = float(value) if value else 0.0
                    except (ValueError, TypeError):
                        transformed_data[key] = 0.0
                else:
                    transformed_data[key] = value
            
            return transformed_data
            
        except Exception as e:
            logger.error(f"Error transforming CSV row: {str(e)}")
            return row

    def stop(self):
        """Clean up resources"""
        logger.info("Stopping Google Storage Source")
        super().stop()

def main():
    # Initialize the Quix Streams application
    app = Application()
    
    # Get configuration from environment variables
    bucket_name = os.environ.get('GCS_BUCKET', 'quix-workflow')
    folder_path = os.environ.get('GCS_FOLDER_PATH', '/')
    project_id = os.environ.get('GCS_PROJECT_ID', 'quix-testing-365012')
    file_format = os.environ.get('GCS_FILE_FORMAT', 'csv')
    file_compression = os.environ.get('GCS_FILE_COMPRESSION', 'none')
    credentials_json = os.environ.get('GCP_CREDENTIALS_KEY')
    
    if not credentials_json:
        logger.error("GCP_CREDENTIALS_KEY environment variable is required")
        raise ValueError("GCP_CREDENTIALS_KEY environment variable is required")
    
    # Create the output topic
    output_topic = app.topic(os.environ.get('output', 'output'))
    
    # Create the Google Storage source
    source = GoogleStorageSource(
        bucket_name=bucket_name,
        folder_path=folder_path,
        project_id=project_id,
        credentials_json=credentials_json,
        file_format=file_format,
        file_compression=file_compression,
        name="google-storage-source"
    )
    
    # Create a streaming dataframe from the source
    sdf = app.dataframe(topic=output_topic, source=source)
    
    # Print messages for monitoring
    sdf.print(metadata=True)
    
    # Run the application
    logger.info("Starting Google Storage Bucket source application")
    app.run()

if __name__ == "__main__":
    main()