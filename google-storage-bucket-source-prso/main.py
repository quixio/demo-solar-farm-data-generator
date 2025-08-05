import os
import json
import logging
import time
from typing import Dict, Any, Iterator
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import io
from quixstreams import Application
from quixstreams.sources import Source
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class GoogleStorageSource(Source):
    def __init__(
        self,
        bucket_name: str,
        folder_path: str,
        file_format: str,
        file_compression: str,
        credentials: Dict[str, Any],
        project_id: str
    ):
        super().__init__()
        self.bucket_name = bucket_name
        self.folder_path = folder_path.lstrip('/').rstrip('/') + '/' if folder_path.strip('/') else ''
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.project_id = project_id
        self.credentials = service_account.Credentials.from_service_account_info(credentials)
        self.client = None
        self.bucket = None
        self.processed_files = set()
        self.message_count = 0
        self.max_messages = 100

    def configure(self, topic, **kwargs):
        super().configure(topic, **kwargs)
        try:
            self.client = storage.Client(credentials=self.credentials, project=self.project_id)
            self.bucket = self.client.bucket(self.bucket_name)
            logger.info(f"Successfully connected to Google Storage bucket: {self.bucket_name}")
        except Exception as e:
            logger.error(f"Failed to connect to Google Storage: {e}")
            raise

    def run(self) -> Iterator[Dict[str, Any]]:
        logger.info(f"Starting to process files from bucket: {self.bucket_name}")
        
        try:
            # List all files in the bucket with the specified folder path
            blobs = list(self.bucket.list_blobs(prefix=self.folder_path))
            
            # Filter files by format
            matching_files = [blob for blob in blobs if blob.name.endswith(f'.{self.file_format}')]
            
            if not matching_files:
                logger.warning(f"No {self.file_format} files found in folder: {self.folder_path}")
                return

            logger.info(f"Found {len(matching_files)} {self.file_format} files to process")

            for blob in matching_files:
                if self.message_count >= self.max_messages:
                    logger.info(f"Reached maximum message limit of {self.max_messages}")
                    break
                    
                if blob.name in self.processed_files:
                    continue

                logger.info(f"Processing file: {blob.name}")
                
                try:
                    # Download file content
                    file_content = blob.download_as_bytes()
                    
                    # Handle compression
                    if self.file_compression == 'gzip':
                        import gzip
                        file_content = gzip.decompress(file_content)
                    elif self.file_compression == 'bz2':
                        import bz2
                        file_content = bz2.decompress(file_content)
                    
                    # Process based on file format
                    if self.file_format == 'csv':
                        yield from self._process_csv(file_content, blob.name)
                    elif self.file_format == 'json':
                        yield from self._process_json(file_content, blob.name)
                    else:
                        logger.warning(f"Unsupported file format: {self.file_format}")
                        continue
                    
                    self.processed_files.add(blob.name)
                    
                except Exception as e:
                    logger.error(f"Error processing file {blob.name}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error during file processing: {e}")
            raise

    def _process_csv(self, file_content: bytes, filename: str) -> Iterator[Dict[str, Any]]:
        try:
            df = pd.read_csv(io.BytesIO(file_content))
            
            for index, row in df.iterrows():
                if self.message_count >= self.max_messages:
                    break
                    
                # Transform the row to the expected format
                message = {
                    "timestamp": str(row.get("timestamp", "")),
                    "hotend_temperature": float(row.get("hotend_temperature", 0.0)),
                    "bed_temperature": float(row.get("bed_temperature", 0.0)),
                    "ambient_temperature": float(row.get("ambient_temperature", 0.0)),
                    "fluctuated_ambient_temperature": float(row.get("fluctuated_ambient_temperature", 0.0)),
                    "_source_file": filename,
                    "_record_index": int(index)
                }
                
                self.message_count += 1
                yield message
                
        except Exception as e:
            logger.error(f"Error processing CSV file {filename}: {e}")
            raise

    def _process_json(self, file_content: bytes, filename: str) -> Iterator[Dict[str, Any]]:
        try:
            content_str = file_content.decode('utf-8')
            
            # Try JSON Lines format first
            try:
                lines = content_str.strip().split('\n')
                for index, line in enumerate(lines):
                    if self.message_count >= self.max_messages:
                        break
                        
                    if line.strip():
                        record = json.loads(line)
                        message = self._transform_json_record(record, filename, index)
                        self.message_count += 1
                        yield message
                        
            except json.JSONDecodeError:
                # Try as single JSON array or object
                data = json.loads(content_str)
                if isinstance(data, list):
                    for index, record in enumerate(data):
                        if self.message_count >= self.max_messages:
                            break
                        message = self._transform_json_record(record, filename, index)
                        self.message_count += 1
                        yield message
                else:
                    # Single JSON object
                    message = self._transform_json_record(data, filename, 0)
                    self.message_count += 1
                    yield message
                    
        except Exception as e:
            logger.error(f"Error processing JSON file {filename}: {e}")
            raise

    def _transform_json_record(self, record: Dict[str, Any], filename: str, index: int) -> Dict[str, Any]:
        return {
            "timestamp": str(record.get("timestamp", "")),
            "hotend_temperature": float(record.get("hotend_temperature", 0.0)),
            "bed_temperature": float(record.get("bed_temperature", 0.0)),
            "ambient_temperature": float(record.get("ambient_temperature", 0.0)),
            "fluctuated_ambient_temperature": float(record.get("fluctuated_ambient_temperature", 0.0)),
            "_source_file": filename,
            "_record_index": int(index)
        }

def main():
    try:
        # Get environment variables
        bucket_name = os.environ.get('GCS_BUCKET')
        folder_path = os.environ.get('GCS_FOLDER_PATH', '/')
        file_format = os.environ.get('GCS_FILE_FORMAT', 'csv')
        file_compression = os.environ.get('GCS_FILE_COMPRESSION', 'none')
        project_id = os.environ.get('GCP_PROJECT_ID')
        credentials_json = os.environ.get('GCP_CREDENTIALS_KEY')
        output_topic_name = os.environ.get('output', 'output')

        # Validate required environment variables
        required_vars = {
            'GCS_BUCKET': bucket_name,
            'GCP_PROJECT_ID': project_id,
            'GCP_CREDENTIALS_KEY': credentials_json
        }
        
        for var_name, var_value in required_vars.items():
            if not var_value:
                raise ValueError(f"Required environment variable {var_name} is not set")

        # Parse credentials JSON
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in GCP_CREDENTIALS_KEY")

        logger.info(f"Initializing Google Storage source for bucket: {bucket_name}")
        logger.info(f"Folder path: {folder_path}")
        logger.info(f"File format: {file_format}")
        logger.info(f"File compression: {file_compression}")

        # Initialize Quix Streams application
        app = Application()

        # Create output topic
        output_topic = app.topic(output_topic_name)

        # Create Google Storage source
        source = GoogleStorageSource(
            bucket_name=bucket_name,
            folder_path=folder_path,
            file_format=file_format,
            file_compression=file_compression,
            credentials=credentials_dict,
            project_id=project_id
        )

        # Create streaming dataframe
        sdf = app.dataframe(topic=output_topic, source=source)
        
        # Add message processing and logging
        sdf = sdf.apply(lambda row: {
            **row,
            "_processed_at": time.time()
        })
        
        # Print messages for debugging
        sdf.print(metadata=True)

        logger.info("Starting Quix Streams application...")
        app.run()

    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise

if __name__ == "__main__":
    main()