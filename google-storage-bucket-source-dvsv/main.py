import os
import json
import io
import csv
import logging
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageBucketSource:
    def __init__(self):
        self.bucket_name = os.environ.get('GS_BUCKET')
        self.project_id = os.environ.get('GS_PROJECT_ID')
        self.credentials_json = os.environ.get('GS_SECRET_KEY')
        self.folder_path = os.environ.get('GS_FOLDER_PATH', '/')
        self.file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
        self.file_compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
        
        # Validate required environment variables
        if not self.bucket_name:
            raise ValueError("GS_BUCKET environment variable is required")
        if not self.project_id:
            raise ValueError("GS_PROJECT_ID environment variable is required")
        if not self.credentials_json:
            raise ValueError("GS_SECRET_KEY environment variable is required")
        
        # Initialize client
        self._init_client()
        
    def _init_client(self):
        try:
            credentials_dict = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            self.client = storage.Client(credentials=credentials, project=self.project_id)
            self.bucket = self.client.bucket(self.bucket_name)
            logger.info(f"Successfully connected to Google Storage Bucket: {self.bucket_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Google Storage client: {str(e)}")
            raise
    
    def _clean_folder_path(self):
        folder_path = self.folder_path
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        return folder_path
    
    def _get_target_files(self):
        folder_path = self._clean_folder_path()
        blobs = list(self.client.list_blobs(self.bucket, prefix=folder_path))
        
        target_files = []
        for blob in blobs:
            if not blob.name.endswith('/'):
                if self.file_format.lower() == 'csv' and blob.name.lower().endswith('.csv'):
                    target_files.append(blob)
                elif self.file_format.lower() == 'json' and blob.name.lower().endswith('.json'):
                    target_files.append(blob)
                elif self.file_format.lower() == 'txt' and blob.name.lower().endswith('.txt'):
                    target_files.append(blob)
        
        return target_files
    
    def _process_csv_content(self, content):
        try:
            content_str = content.decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(content_str))
            
            for row in csv_reader:
                message = {
                    'timestamp': row.get('timestamp'),
                    'hotend_temperature': float(row.get('hotend_temperature', 0)),
                    'bed_temperature': float(row.get('bed_temperature', 0)),
                    'ambient_temperature': float(row.get('ambient_temperature', 0)),
                    'fluctuated_ambient_temperature': float(row.get('fluctuated_ambient_temperature', 0))
                }
                yield message
        except Exception as e:
            logger.error(f"Error processing CSV content: {str(e)}")
            raise
    
    def _process_json_content(self, content):
        try:
            content_str = content.decode('utf-8')
            try:
                data = json.loads(content_str)
                if isinstance(data, list):
                    for record in data:
                        yield record
                else:
                    yield data
            except json.JSONDecodeError:
                lines = content_str.strip().split('\n')
                for line in lines:
                    try:
                        record = json.loads(line)
                        yield record
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.error(f"Error processing JSON content: {str(e)}")
            raise
    
    def _process_txt_content(self, content):
        try:
            content_str = content.decode('utf-8')
            lines = content_str.strip().split('\n')
            for line in lines:
                yield {'data': line.strip()}
        except Exception as e:
            logger.error(f"Error processing TXT content: {str(e)}")
            raise
    
    def read_data(self):
        try:
            target_files = self._get_target_files()
            
            if not target_files:
                logger.warning(f"No {self.file_format} files found in the specified path")
                return
            
            logger.info(f"Found {len(target_files)} {self.file_format} files")
            
            for blob in target_files:
                logger.info(f"Processing file: {blob.name}")
                
                file_content = blob.download_as_bytes()
                
                # Handle compression
                if self.file_compression.lower() == 'gzip':
                    import gzip
                    file_content = gzip.decompress(file_content)
                
                # Process content based on format
                if self.file_format.lower() == 'csv':
                    for record in self._process_csv_content(file_content):
                        yield record
                elif self.file_format.lower() == 'json':
                    for record in self._process_json_content(file_content):
                        yield record
                elif self.file_format.lower() == 'txt':
                    for record in self._process_txt_content(file_content):
                        yield record
                        
        except Exception as e:
            logger.error(f"Error reading data from Google Storage: {str(e)}")
            raise

def main():
    app = Application()
    
    # Create output topic
    output_topic = app.topic(os.environ.get('output', 'output'))
    
    # Initialize Google Storage source
    gs_source = GoogleStorageBucketSource()
    
    # Create source function
    def source_func():
        message_count = 0
        max_messages = 100  # Stop condition for testing
        
        for record in gs_source.read_data():
            if message_count >= max_messages:
                logger.info(f"Reached maximum message limit ({max_messages}). Stopping...")
                break
            
            # Add metadata
            enriched_record = {
                'data': record,
                'source': 'google-storage-bucket',
                'processed_at': datetime.utcnow().isoformat(),
                'message_id': message_count + 1
            }
            
            yield enriched_record
            message_count += 1
            
            if message_count % 10 == 0:
                logger.info(f"Processed {message_count} messages")
    
    # Create streaming dataframe
    sdf = app.dataframe(topic=output_topic, source=source_func)
    
    # Print messages for debugging
    sdf.print(metadata=True)
    
    logger.info("Starting Google Storage Bucket source application...")
    app.run()

if __name__ == "__main__":
    main()