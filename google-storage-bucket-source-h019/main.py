import os
import json
import io
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd

from quixstreams import Application
from quixstreams.sources import Source

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
        super().__init__()
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path.strip("/")
        self.file_format = file_format.lower()
        self.compression = compression
        self.region = region
        self.client = None
        self.bucket = None
        self.processed_files = set()
        self.message_count = 0
        self.max_messages = 100  # Stop condition for testing
        
    def configure(self):
        try:
            # Parse credentials JSON
            credentials_dict = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            
            # Initialize Google Cloud Storage client
            self.client = storage.Client(credentials=credentials, project=self.project_id)
            self.bucket = self.client.bucket(self.bucket_name)
            
            logger.info(f"Successfully configured Google Storage client for bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to configure Google Storage client: {e}")
            raise
    
    def cleanup(self):
        if self.client:
            self.client.close()
            logger.info("Google Storage client connection closed")
    
    def run(self):
        try:
            # List files in the specified folder
            blobs = list(self.bucket.list_blobs(prefix=self.folder_path))
            
            if not blobs:
                logger.warning(f"No files found in folder: {self.folder_path}")
                return
            
            # Filter files by format
            target_files = []
            for blob in blobs:
                if blob.name.endswith(f'.{self.file_format}') and blob.name not in self.processed_files:
                    target_files.append(blob)
            
            if not target_files:
                logger.warning(f"No unprocessed {self.file_format} files found in folder: {self.folder_path}")
                return
            
            logger.info(f"Found {len(target_files)} unprocessed {self.file_format} files")
            
            # Process each file
            for blob in target_files:
                if self.message_count >= self.max_messages:
                    logger.info(f"Reached maximum message limit ({self.max_messages}). Stopping.")
                    break
                    
                logger.info(f"Processing file: {blob.name}")
                
                try:
                    # Download file content
                    file_content = blob.download_as_text()
                    
                    # Process file based on format
                    if self.file_format == 'csv':
                        yield from self._process_csv(file_content, blob.name)
                    elif self.file_format == 'json':
                        yield from self._process_json(file_content, blob.name)
                    else:
                        yield from self._process_text(file_content, blob.name)
                    
                    self.processed_files.add(blob.name)
                    
                except Exception as e:
                    logger.error(f"Error processing file {blob.name}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error in run method: {e}")
            raise
    
    def _process_csv(self, file_content: str, filename: str):
        try:
            df = pd.read_csv(io.StringIO(file_content))
            logger.info(f"CSV file {filename} has {len(df)} rows and {len(df.columns)} columns")
            
            for index, row in df.iterrows():
                if self.message_count >= self.max_messages:
                    break
                    
                # Transform row to message format
                message = self._transform_to_message(row.to_dict(), filename, index)
                yield message
                self.message_count += 1
                
        except Exception as e:
            logger.error(f"Error processing CSV file {filename}: {e}")
            raise
    
    def _process_json(self, file_content: str, filename: str):
        try:
            # Try JSON Lines format first
            lines = file_content.strip().split('\n')
            records_processed = 0
            
            for i, line in enumerate(lines):
                if self.message_count >= self.max_messages:
                    break
                    
                if line.strip():
                    try:
                        record = json.loads(line.strip())
                        message = self._transform_to_message(record, filename, i)
                        yield message
                        self.message_count += 1
                        records_processed += 1
                    except json.JSONDecodeError:
                        continue
            
            # If no records processed, try as single JSON array
            if records_processed == 0:
                data = json.loads(file_content)
                if isinstance(data, list):
                    for i, item in enumerate(data):
                        if self.message_count >= self.max_messages:
                            break
                        message = self._transform_to_message(item, filename, i)
                        yield message
                        self.message_count += 1
                else:
                    message = self._transform_to_message(data, filename, 0)
                    yield message
                    self.message_count += 1
                    
        except Exception as e:
            logger.error(f"Error processing JSON file {filename}: {e}")
            raise
    
    def _process_text(self, file_content: str, filename: str):
        try:
            lines = file_content.strip().split('\n')
            
            for i, line in enumerate(lines):
                if self.message_count >= self.max_messages:
                    break
                    
                if line.strip():
                    # Create a simple record for text files
                    record = {
                        "line_number": i + 1,
                        "content": line.strip()
                    }
                    message = self._transform_to_message(record, filename, i)
                    yield message
                    self.message_count += 1
                    
        except Exception as e:
            logger.error(f"Error processing text file {filename}: {e}")
            raise
    
    def _transform_to_message(self, data: Dict[str, Any], filename: str, record_index: int) -> Dict[str, Any]:
        """Transform data into Kafka message format"""
        try:
            # Add metadata
            message = {
                "source_file": filename,
                "record_index": record_index,
                "processed_at": datetime.utcnow().isoformat(),
                "data": data
            }
            
            # For CSV files with expected schema, flatten the structure
            if self.file_format == 'csv' and isinstance(data, dict):
                # Check if this looks like the 3D printer data schema
                if all(key in data for key in ['timestamp', 'hotend_temperature', 'bed_temperature']):
                    message.update({
                        "timestamp": str(data.get('timestamp', '')),
                        "hotend_temperature": float(data.get('hotend_temperature', 0.0)),
                        "bed_temperature": float(data.get('bed_temperature', 0.0)),
                        "ambient_temperature": float(data.get('ambient_temperature', 0.0)),
                        "fluctuated_ambient_temperature": float(data.get('fluctuated_ambient_temperature', 0.0))
                    })
            
            return message
            
        except Exception as e:
            logger.error(f"Error transforming message: {e}")
            # Return basic message structure on error
            return {
                "source_file": filename,
                "record_index": record_index,
                "processed_at": datetime.utcnow().isoformat(),
                "data": data,
                "error": str(e)
            }

def main():
    try:
        # Get environment variables
        bucket_name = os.environ['GS_BUCKET']
        project_id = os.environ['GS_PROJECT_ID']
        folder_path = os.environ['GS_FOLDER_PATH']
        file_format = os.environ['GS_FILE_FORMAT']
        compression = os.environ['GS_FILE_COMPRESSION']
        credentials_json = os.environ['GS_SECRET_KEY']
        region = os.environ['GS_REGION']
        output_topic_name = os.environ['output']
        
        logger.info(f"Starting Google Storage source application")
        logger.info(f"Bucket: {bucket_name}, Project: {project_id}")
        logger.info(f"Folder: {folder_path}, Format: {file_format}")
        
        # Create Quix Streams application
        app = Application()
        
        # Create output topic
        output_topic = app.topic(output_topic_name)
        
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
        
        # Configure source
        source.configure()
        
        # Create streaming dataframe
        sdf = app.dataframe(topic=output_topic, source=source)
        
        # Add print statement to see messages being produced
        sdf.print(metadata=True)
        
        logger.info("Starting Quix Streams application...")
        
        # Run the application
        try:
            app.run()
        finally:
            # Cleanup
            source.cleanup()
            logger.info("Application stopped and resources cleaned up")
            
    except KeyError as e:
        logger.error(f"Missing required environment variable: {e}")
        raise
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise

if __name__ == "__main__":
    main()