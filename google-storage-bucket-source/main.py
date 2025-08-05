import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from io import BytesIO
from quixstreams import Application
from quixstreams.sources import Source
import logging
import time
from typing import Optional, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageBucketSource(Source):
    def __init__(self, bucket_name: str, project_id: str, credentials_json: str, 
                 folder_path: str = "/", file_format: str = "csv", 
                 compression: str = "none", region: str = "us-central1"):
        super().__init__()
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path
        self.file_format = file_format.lower()
        self.compression = compression.lower() if compression else "none"
        self.region = region
        self._client = None
        self._bucket = None
        self._processed_files = set()
        self._messages_produced = 0
        self._max_messages = 100

    def _create_client(self) -> storage.Client:
        """Create and return a Google Cloud Storage client."""
        try:
            credentials_info = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            return storage.Client(credentials=credentials, project=self.project_id)
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON format in credentials")
            raise
        except Exception as e:
            logger.error(f"Failed to create GCS client: {str(e)}")
            raise

    def _get_bucket(self) -> storage.Bucket:
        """Get the storage bucket."""
        if not self._client:
            self._client = self._create_client()
        
        if not self._bucket:
            self._bucket = self._client.bucket(self.bucket_name)
            if not self._bucket.exists():
                raise ValueError(f"Bucket '{self.bucket_name}' does not exist or is not accessible")
        
        return self._bucket

    def _clean_folder_path(self, path: str) -> str:
        """Clean and normalize folder path."""
        if path.startswith('/'):
            path = path[1:]
        if path and not path.endswith('/'):
            path += '/'
        return path

    def _read_csv_from_blob(self, blob) -> Optional[pd.DataFrame]:
        """Read CSV data from a blob and return as DataFrame."""
        try:
            content = blob.download_as_bytes()
            
            if self.compression and self.compression != 'none':
                df = pd.read_csv(BytesIO(content), compression=self.compression)
            else:
                df = pd.read_csv(BytesIO(content))
            
            return df
        except Exception as e:
            logger.error(f"Error reading CSV from blob {blob.name}: {str(e)}")
            return None

    def _transform_record(self, record: Dict[str, Any], file_name: str) -> Dict[str, Any]:
        """Transform a record into the expected Kafka message format."""
        try:
            # Convert pandas Series to dict if needed
            if hasattr(record, 'to_dict'):
                record = record.to_dict()
            
            # Create the message with metadata
            message = {
                "timestamp": str(record.get("timestamp", "")),
                "hotend_temperature": float(record.get("hotend_temperature", 0.0)),
                "bed_temperature": float(record.get("bed_temperature", 0.0)),
                "ambient_temperature": float(record.get("ambient_temperature", 0.0)),
                "fluctuated_ambient_temperature": float(record.get("fluctuated_ambient_temperature", 0.0)),
                "source_file": file_name,
                "processed_at": time.time()
            }
            
            return message
        except Exception as e:
            logger.error(f"Error transforming record: {str(e)}")
            return None

    def run(self):
        """Main run method that yields messages from Google Storage Bucket."""
        try:
            bucket = self._get_bucket()
            folder_path = self._clean_folder_path(self.folder_path)
            
            logger.info(f"Starting to read from bucket: {self.bucket_name}, folder: {folder_path}")
            
            # List blobs with the specified prefix
            blobs = list(bucket.list_blobs(prefix=folder_path))
            
            # Filter blobs by file format
            filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{self.file_format}')]
            
            if not filtered_blobs:
                logger.warning(f"No {self.file_format} files found in folder path: {folder_path}")
                return
            
            logger.info(f"Found {len(filtered_blobs)} {self.file_format} files")
            
            # Process each file
            for blob in filtered_blobs:
                if self._messages_produced >= self._max_messages:
                    logger.info(f"Reached maximum messages limit ({self._max_messages})")
                    break
                
                if blob.name in self._processed_files:
                    continue
                
                logger.info(f"Processing file: {blob.name}")
                
                if self.file_format == 'csv':
                    df = self._read_csv_from_blob(blob)
                    if df is not None and not df.empty:
                        for _, row in df.iterrows():
                            if self._messages_produced >= self._max_messages:
                                break
                            
                            message = self._transform_record(row, blob.name)
                            if message:
                                yield message
                                self._messages_produced += 1
                                
                                # Add small delay to prevent overwhelming
                                time.sleep(0.1)
                else:
                    # Handle other file formats as raw text
                    try:
                        content = blob.download_as_text()
                        lines = content.split('\n')
                        
                        for line in lines:
                            if self._messages_produced >= self._max_messages:
                                break
                            
                            if line.strip():
                                message = {
                                    "content": line.strip(),
                                    "source_file": blob.name,
                                    "processed_at": time.time()
                                }
                                yield message
                                self._messages_produced += 1
                                time.sleep(0.1)
                    except Exception as e:
                        logger.error(f"Error reading text from blob {blob.name}: {str(e)}")
                
                self._processed_files.add(blob.name)
                logger.info(f"Completed processing file: {blob.name}")
            
            logger.info(f"Finished processing. Total messages produced: {self._messages_produced}")
            
        except Exception as e:
            logger.error(f"Error in GoogleStorageBucketSource run method: {str(e)}")
            raise

def main():
    """Main function to create and run the Quix Streams application."""
    try:
        # Initialize Quix Streams application
        app = Application()
        
        # Create output topic
        output_topic = app.topic(os.environ['output'])
        
        # Get configuration from environment variables
        bucket_name = os.environ['GS_BUCKET']
        project_id = os.environ['GS_PROJECT_ID']
        credentials_json = os.environ['GS_SECRET_KEY']
        folder_path = os.environ.get('GS_FOLDER_PATH', '/')
        file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
        compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
        region = os.environ.get('GS_REGION', 'us-central1')
        
        logger.info(f"Initializing Google Storage Bucket source for bucket: {bucket_name}")
        
        # Create the Google Storage Bucket source
        source = GoogleStorageBucketSource(
            bucket_name=bucket_name,
            project_id=project_id,
            credentials_json=credentials_json,
            folder_path=folder_path,
            file_format=file_format,
            compression=compression,
            region=region
        )
        
        # Create streaming dataframe
        sdf = app.dataframe(topic=output_topic, source=source)
        
        # Add processing and print for debugging
        sdf = sdf.update(lambda row: logger.info(f"Processing message from file: {row.get('source_file', 'unknown')}") or row)
        sdf.print(metadata=True)
        
        logger.info("Starting Quix Streams application...")
        app.run()
        
    except KeyError as e:
        logger.error(f"Missing required environment variable: {e}")
        raise
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()