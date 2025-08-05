import os
import json
import logging
import tempfile
from datetime import datetime
from typing import Dict, Any, Optional
from google.cloud import storage
import pandas as pd
from io import StringIO

from quixstreams import Application
from quixstreams.sources import Source
from quixstreams.sources.base import BaseSource

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageSource(BaseSource):
    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        credentials_json: str,
        folder_path: str = "/",
        file_format: str = "csv",
        file_compression: str = "none",
        max_messages: int = 100
    ):
        super().__init__()
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.max_messages = max_messages
        self.messages_sent = 0
        self.credentials_file = None
        self._client = None
        self._bucket = None
        
    def _create_credentials_file(self) -> str:
        """Create a temporary credentials file from environment variable"""
        try:
            credentials_dict = json.loads(self.credentials_json)
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
            json.dump(credentials_dict, temp_file)
            temp_file.close()
            return temp_file.name
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in credentials")
        except Exception as e:
            raise ValueError(f"Error processing credentials: {str(e)}")
    
    def _initialize_client(self):
        """Initialize Google Cloud Storage client"""
        if self._client is None:
            try:
                self.credentials_file = self._create_credentials_file()
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials_file
                self._client = storage.Client(project=self.project_id)
                self._bucket = self._client.bucket(self.bucket_name)
                logger.info(f"Successfully connected to Google Storage bucket: {self.bucket_name}")
            except Exception as e:
                logger.error(f"Failed to initialize Google Storage client: {str(e)}")
                raise
    
    def _cleanup(self):
        """Clean up temporary credentials file"""
        if self.credentials_file and os.path.exists(self.credentials_file):
            try:
                os.unlink(self.credentials_file)
                logger.info("Cleaned up temporary credentials file")
            except Exception as e:
                logger.warning(f"Failed to cleanup credentials file: {str(e)}")
    
    def _get_file_blobs(self):
        """Get list of file blobs from the bucket"""
        try:
            folder_path = self.folder_path
            if folder_path.startswith('/'):
                folder_path = folder_path[1:]
            if folder_path and not folder_path.endswith('/'):
                folder_path += '/'
            
            blobs = list(self._bucket.list_blobs(prefix=folder_path))
            
            # Filter files by format if specified
            if self.file_format and self.file_format != 'none':
                extension = f".{self.file_format}"
                if self.file_compression and self.file_compression != 'none':
                    extension += f".{self.file_compression}"
                
                filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith(extension)]
                if filtered_blobs:
                    blobs = filtered_blobs
            
            # Remove directory entries (keep only files)
            file_blobs = [blob for blob in blobs if not blob.name.endswith('/')]
            
            logger.info(f"Found {len(file_blobs)} files in bucket")
            return file_blobs
            
        except Exception as e:
            logger.error(f"Error listing files from bucket: {str(e)}")
            raise
    
    def _parse_csv_content(self, content: str) -> list:
        """Parse CSV content and return list of dictionaries"""
        try:
            csv_data = pd.read_csv(StringIO(content))
            return csv_data.to_dict('records')
        except Exception as e:
            logger.error(f"Error parsing CSV content: {str(e)}")
            return []
    
    def _parse_json_content(self, content: str) -> list:
        """Parse JSON content and return list of dictionaries"""
        try:
            json_data = json.loads(content)
            if isinstance(json_data, list):
                return json_data
            else:
                return [json_data]
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON content: {str(e)}")
            return []
    
    def _transform_message(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into proper message format"""
        try:
            # Convert string fields to appropriate types based on schema
            transformed = {}
            
            for key, value in data.items():
                if key == 'timestamp':
                    # Keep timestamp as string but ensure it's properly formatted
                    transformed[key] = str(value)
                elif key in ['hotend_temperature', 'bed_temperature', 'ambient_temperature', 'fluctuated_ambient_temperature']:
                    # Convert temperature fields to float
                    try:
                        transformed[key] = float(value)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert {key} value '{value}' to float, using 0.0")
                        transformed[key] = 0.0
                else:
                    # Keep other fields as-is
                    transformed[key] = value
            
            # Add metadata
            transformed['_source'] = 'google-storage-bucket'
            transformed['_processed_at'] = datetime.utcnow().isoformat()
            
            return transformed
            
        except Exception as e:
            logger.error(f"Error transforming message: {str(e)}")
            return data
    
    def run(self):
        """Main run method to read data from Google Storage and yield messages"""
        try:
            self._initialize_client()
            file_blobs = self._get_file_blobs()
            
            if not file_blobs:
                logger.warning("No files found in the specified bucket/folder")
                return
            
            for blob in file_blobs:
                if self.messages_sent >= self.max_messages:
                    logger.info(f"Reached maximum message limit of {self.max_messages}")
                    break
                
                try:
                    logger.info(f"Processing file: {blob.name}")
                    content = blob.download_as_text()
                    
                    # Parse content based on file format
                    if self.file_format == 'csv':
                        records = self._parse_csv_content(content)
                    elif self.file_format == 'json':
                        records = self._parse_json_content(content)
                    else:
                        # For other formats, treat each line as a separate record
                        lines = [line.strip() for line in content.split('\n') if line.strip()]
                        records = [{'raw_data': line, 'file_name': blob.name} for line in lines]
                    
                    # Process each record
                    for record in records:
                        if self.messages_sent >= self.max_messages:
                            break
                        
                        try:
                            # Transform the message
                            transformed_record = self._transform_message(record)
                            
                            # Create message with key and value
                            message_key = transformed_record.get('timestamp', str(self.messages_sent))
                            
                            # Yield the message
                            yield message_key, transformed_record, int(datetime.utcnow().timestamp() * 1000)
                            
                            self.messages_sent += 1
                            
                            if self.messages_sent % 10 == 0:
                                logger.info(f"Processed {self.messages_sent} messages")
                                
                        except Exception as e:
                            logger.error(f"Error processing record: {str(e)}")
                            continue
                
                except Exception as e:
                    logger.error(f"Error processing file {blob.name}: {str(e)}")
                    continue
            
            logger.info(f"Finished processing. Total messages sent: {self.messages_sent}")
            
        except Exception as e:
            logger.error(f"Error in run method: {str(e)}")
            raise
        finally:
            self._cleanup()


def main():
    """Main application function"""
    try:
        # Get environment variables
        bucket_name = os.environ.get('GS_BUCKET')
        project_id = os.environ.get('GS_PROJECT_ID')
        credentials_json = os.environ.get('GS_SECRET_KEY')
        folder_path = os.environ.get('GS_FOLDER_PATH', '/')
        file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
        file_compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
        output_topic = os.environ.get('output', 'output')
        
        # Validate required environment variables
        if not bucket_name:
            raise ValueError("GS_BUCKET environment variable is required")
        if not project_id:
            raise ValueError("GS_PROJECT_ID environment variable is required")
        if not credentials_json:
            raise ValueError("GS_SECRET_KEY environment variable is required")
        
        logger.info(f"Starting Google Storage Bucket source application")
        logger.info(f"Bucket: {bucket_name}")
        logger.info(f"Project ID: {project_id}")
        logger.info(f"Folder path: {folder_path}")
        logger.info(f"File format: {file_format}")
        logger.info(f"Output topic: {output_topic}")
        
        # Create Quix Streams application
        app = Application()
        
        # Create output topic
        topic = app.topic(output_topic)
        
        # Create Google Storage source
        source = GoogleStorageSource(
            bucket_name=bucket_name,
            project_id=project_id,
            credentials_json=credentials_json,
            folder_path=folder_path,
            file_format=file_format,
            file_compression=file_compression,
            max_messages=100  # Stop after 100 messages for testing
        )
        
        # Create streaming dataframe
        sdf = app.dataframe(topic=topic, source=source)
        
        # Print messages for debugging
        sdf.print(metadata=True)
        
        # Run the application
        logger.info("Starting Quix Streams application...")
        app.run()
        
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        raise


if __name__ == "__main__":
    main()