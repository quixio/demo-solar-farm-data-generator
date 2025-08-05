import os
import json
import logging
from google.cloud import storage
from google.oauth2 import service_account
from io import StringIO
import pandas as pd
from datetime import datetime
from quixstreams import Application
from quixstreams.sources import Source
from typing import Optional, Any, Dict, List
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
        max_messages: int = 100
    ):
        super().__init__()
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path
        self.file_format = file_format
        self.compression = compression
        self.max_messages = max_messages
        self.messages_sent = 0
        self._client = None
        self._bucket = None
        
    def configure(self):
        """Configure the Google Cloud Storage client"""
        try:
            # Parse credentials JSON
            credentials_dict = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            
            # Initialize GCS client
            self._client = storage.Client(credentials=credentials, project=self.project_id)
            self._bucket = self._client.bucket(self.bucket_name)
            
            logger.info(f"Successfully configured Google Cloud Storage client for bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to configure Google Cloud Storage client: {e}")
            raise
    
    def _clean_folder_path(self, path: str) -> str:
        """Clean and normalize folder path"""
        if path.startswith('/'):
            path = path[1:]
        if path and not path.endswith('/'):
            path += '/'
        return path
    
    def _parse_csv_data(self, content: str) -> List[Dict[str, Any]]:
        """Parse CSV content and return list of records"""
        try:
            df = pd.read_csv(StringIO(content))
            records = []
            
            for _, row in df.iterrows():
                record = {
                    "timestamp": str(row.get("timestamp", "")),
                    "hotend_temperature": float(row.get("hotend_temperature", 0.0)),
                    "bed_temperature": float(row.get("bed_temperature", 0.0)),
                    "ambient_temperature": float(row.get("ambient_temperature", 0.0)),
                    "fluctuated_ambient_temperature": float(row.get("fluctuated_ambient_temperature", 0.0))
                }
                records.append(record)
            
            return records
            
        except Exception as e:
            logger.error(f"Error parsing CSV data: {e}")
            return []
    
    def _parse_json_data(self, content: str) -> List[Dict[str, Any]]:
        """Parse JSON content and return list of records"""
        try:
            # Try JSON lines format first
            lines = content.strip().split('\n')
            records = []
            
            for line in lines:
                if line.strip():
                    try:
                        json_obj = json.loads(line)
                        records.append(json_obj)
                    except json.JSONDecodeError:
                        continue
            
            if records:
                return records
            
            # Try single JSON object/array
            data = json.loads(content)
            if isinstance(data, list):
                return data
            else:
                return [data]
                
        except Exception as e:
            logger.error(f"Error parsing JSON data: {e}")
            return []
    
    def _process_file_content(self, content: str) -> List[Dict[str, Any]]:
        """Process file content based on format"""
        if self.file_format.lower() == "csv":
            return self._parse_csv_data(content)
        elif self.file_format.lower() == "json":
            return self._parse_json_data(content)
        else:
            # Treat as text, each line as a record
            lines = content.split('\n')
            records = []
            for i, line in enumerate(lines):
                if line.strip():
                    records.append({
                        "line_number": i + 1,
                        "content": line.strip(),
                        "timestamp": datetime.now().isoformat()
                    })
            return records
    
    def run(self):
        """Main run method to read data from Google Storage and yield messages"""
        if not self._client or not self._bucket:
            self.configure()
        
        try:
            # Clean folder path
            folder_path = self._clean_folder_path(self.folder_path)
            
            # List files in the bucket/folder
            blobs = list(self._client.list_blobs(self.bucket_name, prefix=folder_path))
            
            if not blobs:
                logger.warning(f"No files found in bucket {self.bucket_name} with prefix {folder_path}")
                return
            
            # Filter files by format if specified
            if self.file_format and self.file_format.lower() != 'none':
                blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{self.file_format.lower()}')]
            
            if not blobs:
                logger.warning(f"No {self.file_format} files found in the specified location")
                return
            
            logger.info(f"Found {len(blobs)} files to process")
            
            # Process each file
            for blob in blobs:
                if self.messages_sent >= self.max_messages:
                    logger.info(f"Reached maximum messages limit: {self.max_messages}")
                    break
                
                try:
                    logger.info(f"Processing file: {blob.name}")
                    
                    # Download file content
                    file_content = blob.download_as_text()
                    
                    # Process file content
                    records = self._process_file_content(file_content)
                    
                    logger.info(f"Found {len(records)} records in file: {blob.name}")
                    
                    # Yield each record as a message
                    for record in records:
                        if self.messages_sent >= self.max_messages:
                            break
                        
                        # Create message with metadata
                        message = {
                            "key": f"{blob.name}_{self.messages_sent}",
                            "value": record,
                            "headers": {
                                "source_file": blob.name,
                                "file_format": self.file_format,
                                "processed_at": datetime.now().isoformat()
                            }
                        }
                        
                        yield message
                        self.messages_sent += 1
                        
                        # Small delay to prevent overwhelming
                        time.sleep(0.01)
                
                except Exception as e:
                    logger.error(f"Error processing file {blob.name}: {e}")
                    continue
            
            logger.info(f"Completed processing. Total messages sent: {self.messages_sent}")
            
        except Exception as e:
            logger.error(f"Error in run method: {e}")
            raise

def main():
    """Main application function"""
    
    # Get environment variables
    bucket_name = os.environ.get('GCS_BUCKET')
    project_id = os.environ.get('GCP_PROJECT_ID')
    credentials_json = os.environ.get('GCP_CREDENTIALS_KEY')
    folder_path = os.environ.get('GCS_FOLDER_PATH', '/')
    file_format = os.environ.get('GCS_FILE_FORMAT', 'csv')
    compression = os.environ.get('GCS_FILE_COMPRESSION', 'none')
    output_topic_name = os.environ.get('output', 'output')
    
    # Validate required environment variables
    if not bucket_name:
        raise ValueError("GCS_BUCKET environment variable is required")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID environment variable is required")
    if not credentials_json:
        raise ValueError("GCP_CREDENTIALS_KEY environment variable is required")
    
    logger.info(f"Starting Google Storage Source application")
    logger.info(f"Bucket: {bucket_name}")
    logger.info(f"Project ID: {project_id}")
    logger.info(f"Folder path: {folder_path}")
    logger.info(f"File format: {file_format}")
    logger.info(f"Output topic: {output_topic_name}")
    
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
        max_messages=100  # Stop after 100 messages for testing
    )
    
    # Create streaming dataframe
    sdf = app.dataframe(topic=output_topic, source=source)
    
    # Print messages for debugging
    sdf.print(metadata=True)
    
    # Run the application
    try:
        logger.info("Starting Quix Streams application...")
        app.run()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        logger.info("Application shutdown complete")

if __name__ == "__main__":
    main()