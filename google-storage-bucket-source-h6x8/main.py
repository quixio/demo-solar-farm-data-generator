import os
import json
import pandas as pd
import logging
from io import StringIO
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageSource(Source):
    def __init__(self, bucket_name, folder_path, file_format, credentials_json, project_id, **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.folder_path = folder_path
        self.file_format = file_format
        self.credentials_json = credentials_json
        self.project_id = project_id
        self.client = None
        self.bucket = None
        self.processed_count = 0
        self.max_messages = 100
        
    def setup(self):
        try:
            # Parse credentials JSON
            credentials_dict = json.loads(self.credentials_json)
            
            # Create credentials object
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            
            # Initialize Google Cloud Storage client
            self.client = storage.Client(credentials=credentials, project=self.project_id)
            
            # Get the bucket
            self.bucket = self.client.bucket(self.bucket_name)
            
            logger.info(f"Successfully connected to Google Cloud Storage bucket: {self.bucket_name}")
            
            # Test connection by attempting to list blobs
            list(self.bucket.list_blobs(max_results=1))
            
            if hasattr(self, 'on_client_connect_success') and callable(self.on_client_connect_success):
                self.on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to connect to Google Cloud Storage: {str(e)}")
            if hasattr(self, 'on_client_connect_failure') and callable(self.on_client_connect_failure):
                self.on_client_connect_failure(e)
            raise
    
    def run(self):
        try:
            self.setup()
            
            # List files in the specified folder with the specified format
            folder_prefix = self.folder_path.lstrip('/')
            if folder_prefix and not folder_prefix.endswith('/'):
                folder_prefix += '/'
            
            blobs = list(self.bucket.list_blobs(prefix=folder_prefix))
            
            # Filter files by format
            target_files = []
            for blob in blobs:
                if blob.name.endswith(f'.{self.file_format}') and not blob.name.endswith('/'):
                    target_files.append(blob)
            
            if not target_files:
                logger.warning(f"No {self.file_format} files found in the specified folder")
                return
            
            logger.info(f"Found {len(target_files)} {self.file_format} file(s)")
            
            # Read data from files
            for blob in target_files:
                if not self.running or self.processed_count >= self.max_messages:
                    break
                    
                logger.info(f"Reading from file: {blob.name}")
                
                try:
                    # Download file content
                    content = blob.download_as_text()
                    
                    if self.file_format.lower() == 'csv':
                        # Parse CSV content
                        df = pd.read_csv(StringIO(content))
                        
                        # Process each row
                        for index, row in df.iterrows():
                            if not self.running or self.processed_count >= self.max_messages:
                                break
                            
                            # Convert row to dictionary and ensure proper data types
                            row_dict = row.to_dict()
                            
                            # Transform data based on schema analysis
                            message_data = {
                                "timestamp": str(row_dict.get("timestamp", "")),
                                "hotend_temperature": float(row_dict.get("hotend_temperature", 0.0)),
                                "bed_temperature": float(row_dict.get("bed_temperature", 0.0)),
                                "ambient_temperature": float(row_dict.get("ambient_temperature", 0.0)),
                                "fluctuated_ambient_temperature": float(row_dict.get("fluctuated_ambient_temperature", 0.0))
                            }
                            
                            # Serialize the message
                            msg = self.serialize(
                                key=f"{blob.name}_{index}",
                                value=message_data
                            )
                            
                            # Produce the message
                            self.produce(
                                key=msg.key,
                                value=msg.value
                            )
                            
                            self.processed_count += 1
                            
                            if self.processed_count % 10 == 0:
                                logger.info(f"Processed {self.processed_count} messages")
                    
                    elif self.file_format.lower() == 'json':
                        # Parse JSON content
                        try:
                            json_data = json.loads(content)
                            
                            # Handle different JSON structures
                            if isinstance(json_data, list):
                                # Array of objects
                                for item in json_data:
                                    if not self.running or self.processed_count >= self.max_messages:
                                        break
                                    
                                    # Serialize the message
                                    msg = self.serialize(
                                        key=f"{blob.name}_{self.processed_count}",
                                        value=item
                                    )
                                    
                                    # Produce the message
                                    self.produce(
                                        key=msg.key,
                                        value=msg.value
                                    )
                                    
                                    self.processed_count += 1
                            
                            elif isinstance(json_data, dict):
                                # Single object
                                if self.running and self.processed_count < self.max_messages:
                                    # Serialize the message
                                    msg = self.serialize(
                                        key=blob.name,
                                        value=json_data
                                    )
                                    
                                    # Produce the message
                                    self.produce(
                                        key=msg.key,
                                        value=msg.value
                                    )
                                    
                                    self.processed_count += 1
                        
                        except json.JSONDecodeError as e:
                            logger.error(f"Error parsing JSON file {blob.name}: Invalid JSON format")
                            continue
                    
                    else:
                        # For other formats, treat as text
                        lines = content.split('\n')
                        for line_num, line in enumerate(lines):
                            if not self.running or self.processed_count >= self.max_messages:
                                break
                                
                            if line.strip():  # Skip empty lines
                                # Serialize the message
                                msg = self.serialize(
                                    key=f"{blob.name}_{line_num}",
                                    value={"content": line.strip()}
                                )
                                
                                # Produce the message
                                self.produce(
                                    key=msg.key,
                                    value=msg.value
                                )
                                
                                self.processed_count += 1
                
                except Exception as e:
                    logger.error(f"Error reading file {blob.name}: {str(e)}")
                    continue
            
            logger.info(f"Successfully processed {self.processed_count} messages from Google Cloud Storage")
            
        except Exception as e:
            logger.error(f"Error in Google Storage Source: {str(e)}")
            raise

def main():
    # Get configuration from environment variables
    bucket_name = os.environ.get('GCS_BUCKET', 'quix-workflow')
    folder_path = os.environ.get('GCS_FOLDER_PATH', '/')
    file_format = os.environ.get('GCS_FILE_FORMAT', 'csv')
    credentials_json = os.environ.get('GCP_CREDENTIALS_KEY')
    project_id = os.environ.get('GCP_PROJECT_ID', 'quix-testing-365012')
    output_topic_name = os.environ.get('output', 'output')
    
    if not credentials_json:
        raise ValueError("GCP credentials not found in environment variables")
    
    # Create application
    app = Application()
    
    # Create output topic
    output_topic = app.topic(output_topic_name)
    
    # Create Google Storage source
    source = GoogleStorageSource(
        bucket_name=bucket_name,
        folder_path=folder_path,
        file_format=file_format,
        credentials_json=credentials_json,
        project_id=project_id,
        name="google-storage-source"
    )
    
    # Create streaming dataframe
    sdf = app.dataframe(topic=output_topic, source=source)
    
    # Print messages for debugging
    sdf.print(metadata=True)
    
    # Run the application
    logger.info("Starting Google Storage Bucket source application")
    app.run()

if __name__ == "__main__":
    main()