import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources import Source
from quixstreams.sources.base import BaseSource
import pandas as pd
from io import BytesIO
import gzip
import bz2

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
        self.folder_path = folder_path.lstrip('/')
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.max_messages = max_messages
        self.messages_processed = 0
        self._client = None
        self._bucket = None
        
    def configure(self):
        """Configure the Google Storage client"""
        try:
            # Parse credentials JSON
            credentials_dict = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            
            # Initialize Google Cloud Storage client
            self._client = storage.Client(credentials=credentials, project=self.project_id)
            self._bucket = self._client.bucket(self.bucket_name)
            
            logger.info(f"Connected to Google Storage bucket: {self.bucket_name}")
            
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in credentials")
        except Exception as e:
            logger.error(f"Failed to configure Google Storage client: {e}")
            raise
    
    def read(self):
        """Read data from Google Storage Bucket and yield messages"""
        if not self._client or not self._bucket:
            self.configure()
        
        try:
            # List files in the specified folder
            blobs = list(self._bucket.list_blobs(prefix=self.folder_path))
            
            if not blobs:
                logger.warning("No files found in the specified folder")
                return
                
            logger.info(f"Found {len(blobs)} files in bucket")
            
            # Filter files by format if specified
            if self.file_format != 'none':
                filtered_blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{self.file_format}')]
                if filtered_blobs:
                    blobs = filtered_blobs
                else:
                    logger.warning(f"No {self.file_format} files found, using all available files")
            
            # Process files and yield messages
            for blob in blobs:
                if self.messages_processed >= self.max_messages:
                    logger.info(f"Reached maximum message limit: {self.max_messages}")
                    break
                
                # Skip directories
                if blob.name.endswith('/'):
                    continue
                
                try:
                    yield from self._process_file(blob)
                except Exception as e:
                    logger.error(f"Error processing file {blob.name}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error reading from Google Storage: {e}")
            raise
    
    def _process_file(self, blob):
        """Process a single file and yield messages"""
        logger.info(f"Processing file: {blob.name}")
        
        try:
            # Download file content
            file_content = blob.download_as_bytes()
            
            # Handle compression
            if self.file_compression == 'gzip':
                file_content = gzip.decompress(file_content)
            elif self.file_compression == 'bz2':
                file_content = bz2.decompress(file_content)
            
            # Process based on file format
            if self.file_format == 'csv':
                yield from self._process_csv(file_content, blob.name)
            elif self.file_format == 'json':
                yield from self._process_json(file_content, blob.name)
            elif self.file_format == 'txt':
                yield from self._process_txt(file_content, blob.name)
            else:
                yield from self._process_binary(file_content, blob.name)
                
        except Exception as e:
            logger.error(f"Error downloading file {blob.name}: {e}")
            raise
    
    def _process_csv(self, file_content: bytes, filename: str):
        """Process CSV file content"""
        try:
            df = pd.read_csv(BytesIO(file_content))
            
            for _, row in df.iterrows():
                if self.messages_processed >= self.max_messages:
                    break
                
                # Convert row to dictionary and create message
                message_data = row.to_dict()
                
                # Convert timestamp string to datetime if present
                if 'timestamp' in message_data:
                    try:
                        timestamp_str = str(message_data['timestamp'])
                        parsed_timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        message_data['timestamp'] = parsed_timestamp.isoformat()
                    except (ValueError, TypeError):
                        # Keep original timestamp if parsing fails
                        pass
                
                # Ensure numeric fields are properly typed
                for field in ['hotend_temperature', 'bed_temperature', 'ambient_temperature', 'fluctuated_ambient_temperature']:
                    if field in message_data:
                        try:
                            message_data[field] = float(message_data[field])
                        except (ValueError, TypeError):
                            logger.warning(f"Could not convert {field} to float: {message_data[field]}")
                
                # Add metadata
                enriched_message = {
                    **message_data,
                    'source_file': filename,
                    'processed_at': datetime.utcnow().isoformat(),
                    'message_id': self.messages_processed + 1
                }
                
                yield self._create_message(enriched_message)
                self.messages_processed += 1
                
        except Exception as e:
            logger.error(f"Error processing CSV file {filename}: {e}")
            raise
    
    def _process_json(self, file_content: bytes, filename: str):
        """Process JSON file content"""
        try:
            content_str = file_content.decode('utf-8')
            
            try:
                # Try parsing as JSON array
                json_data = json.loads(content_str)
                if isinstance(json_data, list):
                    for item in json_data:
                        if self.messages_processed >= self.max_messages:
                            break
                        enriched_message = {
                            **item,
                            'source_file': filename,
                            'processed_at': datetime.utcnow().isoformat(),
                            'message_id': self.messages_processed + 1
                        }
                        yield self._create_message(enriched_message)
                        self.messages_processed += 1
                else:
                    # Single JSON object
                    enriched_message = {
                        **json_data,
                        'source_file': filename,
                        'processed_at': datetime.utcnow().isoformat(),
                        'message_id': self.messages_processed + 1
                    }
                    yield self._create_message(enriched_message)
                    self.messages_processed += 1
                    
            except json.JSONDecodeError:
                # Try reading as JSONL (newline-delimited JSON)
                lines = content_str.strip().split('\n')
                for line in lines:
                    if self.messages_processed >= self.max_messages:
                        break
                    if line.strip():
                        try:
                            item = json.loads(line)
                            enriched_message = {
                                **item,
                                'source_file': filename,
                                'processed_at': datetime.utcnow().isoformat(),
                                'message_id': self.messages_processed + 1
                            }
                            yield self._create_message(enriched_message)
                            self.messages_processed += 1
                        except json.JSONDecodeError:
                            continue
                            
        except Exception as e:
            logger.error(f"Error processing JSON file {filename}: {e}")
            raise
    
    def _process_txt(self, file_content: bytes, filename: str):
        """Process text file content"""
        try:
            content_str = file_content.decode('utf-8')
            lines = content_str.strip().split('\n')
            
            for line_num, line in enumerate(lines):
                if self.messages_processed >= self.max_messages:
                    break
                if line.strip():
                    enriched_message = {
                        'content': line.strip(),
                        'line_number': line_num + 1,
                        'source_file': filename,
                        'processed_at': datetime.utcnow().isoformat(),
                        'message_id': self.messages_processed + 1
                    }
                    yield self._create_message(enriched_message)
                    self.messages_processed += 1
                    
        except Exception as e:
            logger.error(f"Error processing text file {filename}: {e}")
            raise
    
    def _process_binary(self, file_content: bytes, filename: str):
        """Process binary file content"""
        try:
            enriched_message = {
                'content_preview': file_content[:100].hex(),
                'file_size': len(file_content),
                'source_file': filename,
                'processed_at': datetime.utcnow().isoformat(),
                'message_id': self.messages_processed + 1
            }
            yield self._create_message(enriched_message)
            self.messages_processed += 1
            
        except Exception as e:
            logger.error(f"Error processing binary file {filename}: {e}")
            raise
    
    def _create_message(self, data: Dict[str, Any]):
        """Create a message with proper key and timestamp"""
        # Use timestamp from data or current time as message key
        message_key = None
        if 'timestamp' in data:
            message_key = str(data['timestamp'])
        elif 'message_id' in data:
            message_key = str(data['message_id'])
        
        return self._serialize_key(message_key), json.dumps(data), datetime.utcnow().timestamp()

def main():
    # Initialize Quix Streams application
    app = Application()
    
    # Get environment variables
    bucket_name = os.environ.get('GS_BUCKET')
    project_id = os.environ.get('GS_PROJECT_ID')
    folder_path = os.environ.get('GS_FOLDER_PATH', '/')
    file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
    file_compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
    credentials_json = os.environ.get('GS_SECRET_KEY')
    output_topic_name = os.environ.get('output', 'output')
    
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
    logger.info(f"Compression: {file_compression}")
    logger.info(f"Output topic: {output_topic_name}")
    
    # Create output topic
    output_topic = app.topic(output_topic_name)
    
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
    sdf = app.dataframe(topic=output_topic, source=source)
    
    # Print messages for debugging
    sdf.print(metadata=True)
    
    # Start the application
    logger.info("Starting Quix Streams application...")
    app.run()

if __name__ == "__main__":
    main()