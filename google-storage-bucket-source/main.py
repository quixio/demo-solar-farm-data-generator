import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources import Source
from quixstreams.models import TopicConfig, MessageContext
from io import BytesIO
import logging
from typing import Optional, Dict, Any, Iterator
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageSource(Source):
    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        credentials_json: str,
        folder_path: str = "/",
        file_format: str = "csv",
        compression: str = "none",
        region: str = "us-central1"
    ):
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path
        self.file_format = file_format.lower()
        self.compression = compression
        self.region = region
        self.client = None
        self.bucket = None
        self.processed_files = set()
        self.message_count = 0
        
        super().__init__(name="google-storage-source")

    def configure(self, topic: str, **kwargs) -> None:
        """Configure the source with topic information"""
        super().configure(topic=topic, **kwargs)
        self._setup_client()

    def _setup_client(self) -> None:
        """Initialize Google Cloud Storage client"""
        try:
            logger.info("Setting up Google Cloud Storage client")
            
            # Parse credentials JSON
            credentials_info = json.loads(self.credentials_json)
            
            # Create credentials object
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            
            # Create client
            self.client = storage.Client(credentials=credentials, project=self.project_id)
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Verify bucket exists
            if not self.bucket.exists():
                raise ValueError(f"Bucket '{self.bucket_name}' does not exist or is not accessible")
            
            logger.info(f"Successfully connected to bucket: {self.bucket_name}")
            
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON format in credentials")
            raise ValueError("Invalid JSON format in credentials") from e
        except Exception as e:
            logger.error("Failed to create GCS client")
            raise ValueError("Failed to create GCS client") from e

    def _clean_folder_path(self, path: str) -> str:
        """Clean and normalize folder path"""
        if path.startswith('/'):
            path = path[1:]
        if path and not path.endswith('/'):
            path += '/'
        return path

    def _get_file_blobs(self) -> Iterator[storage.Blob]:
        """Get all matching file blobs from the bucket"""
        folder_path = self._clean_folder_path(self.folder_path)
        
        logger.info(f"Looking for {self.file_format} files in folder: {folder_path}")
        
        # List all blobs with the specified prefix
        blobs = self.bucket.list_blobs(prefix=folder_path)
        
        # Filter by file format and exclude already processed files
        for blob in blobs:
            if (blob.name.lower().endswith(f'.{self.file_format}') and 
                blob.name not in self.processed_files):
                yield blob

    def _read_csv_data(self, blob: storage.Blob) -> Optional[pd.DataFrame]:
        """Read CSV data from a blob"""
        try:
            logger.info(f"Reading CSV data from: {blob.name}")
            content = blob.download_as_bytes()
            
            # Handle compression
            if self.compression and self.compression.lower() != 'none':
                df = pd.read_csv(BytesIO(content), compression=self.compression)
            else:
                df = pd.read_csv(BytesIO(content))
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading CSV from blob {blob.name}: {str(e)}")
            return None

    def _transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single record into the expected format"""
        transformed = {}
        
        for key, value in record.items():
            # Handle different data types appropriately
            if pd.isna(value):
                transformed[key] = None
            elif isinstance(value, (int, float)):
                transformed[key] = float(value) if key.endswith('_temperature') else value
            else:
                transformed[key] = str(value)
        
        return transformed

    def run(self) -> Iterator[Dict[str, Any]]:
        """Main run method that yields messages"""
        try:
            logger.info("Starting Google Storage Source")
            
            for blob in self._get_file_blobs():
                if self.message_count >= 100:  # Stop condition for testing
                    logger.info("Reached message limit of 100, stopping")
                    break
                
                logger.info(f"Processing file: {blob.name}")
                
                if self.file_format == 'csv':
                    df = self._read_csv_data(blob)
                    if df is not None and not df.empty:
                        for _, row in df.iterrows():
                            if self.message_count >= 100:
                                break
                            
                            record_dict = row.to_dict()
                            transformed_record = self._transform_record(record_dict)
                            
                            # Add metadata
                            message = {
                                "data": transformed_record,
                                "metadata": {
                                    "source_file": blob.name,
                                    "file_size": blob.size,
                                    "last_modified": blob.time_created.isoformat() if blob.time_created else None,
                                    "record_number": self.message_count + 1
                                }
                            }
                            
                            yield message
                            self.message_count += 1
                            
                            # Add small delay to prevent overwhelming the system
                            time.sleep(0.01)
                
                # Mark file as processed
                self.processed_files.add(blob.name)
                logger.info(f"Completed processing file: {blob.name}")
            
            logger.info(f"Source completed. Total messages produced: {self.message_count}")
            
        except Exception as e:
            logger.error(f"Error in source run method: {str(e)}")
            raise

def main():
    """Main application function"""
    try:
        # Initialize Quix Streams application
        app = Application(application_id="google-storage-bucket-source-draft")
        
        # Create output topic
        output_topic = app.topic(
            name=os.environ['output'],
            config=TopicConfig(num_partitions=1, replication_factor=1)
        )
        
        # Get environment variables
        bucket_name = os.environ['GS_BUCKET']
        project_id = os.environ['GS_PROJECT_ID']
        credentials_json = os.environ['GS_SECRET_KEY']
        folder_path = os.environ.get('GS_FOLDER_PATH', '/')
        file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
        compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
        region = os.environ.get('GS_REGION', 'us-central1')
        
        logger.info(f"Configuration - Bucket: {bucket_name}, Project: {project_id}, Format: {file_format}")
        
        # Create Google Storage source
        source = GoogleStorageSource(
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
        
        # Transform messages to extract data payload
        sdf = sdf.apply(lambda message: message["data"])
        
        # Add debug printing
        sdf.print(metadata=True)
        
        logger.info("Starting Quix Streams application")
        app.run()
        
    except KeyError as e:
        logger.error(f"Missing required environment variable: {e}")
        raise
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        raise

if __name__ == "__main__":
    main()