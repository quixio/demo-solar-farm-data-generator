import os
import json
import csv
import io
import logging
from typing import Dict, Any
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageBucketSource(Source):
    def __init__(self, bucket_name: str, folder_path: str, file_format: str, 
                 file_compression: str, credentials_json: str, **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.folder_path = folder_path.strip('/')
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.credentials_json = credentials_json
        self.client = None
        self.bucket = None
        self.processed_count = 0
        self.max_messages = 100

    def setup(self):
        try:
            credentials_info = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            self.client = storage.Client(credentials=credentials)
            self.bucket = self.client.bucket(self.bucket_name)
            logger.info(f"Successfully connected to Google Cloud Storage bucket: {self.bucket_name}")
        except Exception as e:
            logger.error(f"Failed to connect to Google Cloud Storage: {str(e)}")
            raise

    def run(self):
        try:
            prefix = f"{self.folder_path}/" if self.folder_path else ""
            blobs = list(self.bucket.list_blobs(prefix=prefix))
            
            if not blobs:
                logger.warning(f"No files found in folder: {self.folder_path}")
                return

            if self.file_format and self.file_format != 'none':
                blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{self.file_format}')]

            if not blobs:
                logger.warning(f"No {self.file_format} files found in folder: {self.folder_path}")
                return

            logger.info(f"Found {len(blobs)} file(s) matching criteria")

            for blob in blobs:
                if not self.running or self.processed_count >= self.max_messages:
                    break
                
                logger.info(f"Processing file: {blob.name}")
                self._process_file(blob)

            logger.info(f"Finished processing. Total messages: {self.processed_count}")

        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
            raise

    def _process_file(self, blob):
        try:
            file_content = blob.download_as_text()
            
            if self.file_format == 'csv':
                self._process_csv(file_content, blob.name)
            elif self.file_format == 'json':
                self._process_json(file_content, blob.name)
            else:
                self._process_text(file_content, blob.name)
                
        except Exception as e:
            logger.error(f"Error processing file {blob.name}: {str(e)}")

    def _process_csv(self, content: str, filename: str):
        csv_reader = csv.DictReader(io.StringIO(content))
        
        for row in csv_reader:
            if not self.running or self.processed_count >= self.max_messages:
                break
                
            message_data = self._transform_csv_row(row)
            self._send_message(filename, message_data)

    def _process_json(self, content: str, filename: str):
        try:
            data = json.loads(content)
            if isinstance(data, list):
                for item in data:
                    if not self.running or self.processed_count >= self.max_messages:
                        break
                    self._send_message(filename, item)
            else:
                self._send_message(filename, data)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON in file {filename}: {str(e)}")

    def _process_text(self, content: str, filename: str):
        lines = content.strip().split('\n')
        for line in lines:
            if not self.running or self.processed_count >= self.max_messages:
                break
            if line.strip():
                self._send_message(filename, {"text": line.strip()})

    def _transform_csv_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        transformed = {}
        
        for key, value in row.items():
            if key == 'timestamp':
                transformed[key] = value
            elif key in ['hotend_temperature', 'bed_temperature', 'ambient_temperature', 'fluctuated_ambient_temperature']:
                try:
                    transformed[key] = float(value)
                except (ValueError, TypeError):
                    transformed[key] = value
            else:
                transformed[key] = value
                
        return transformed

    def _send_message(self, filename: str, data: Dict[str, Any]):
        try:
            message_key = filename
            
            serialized = self.serialize(
                key=message_key,
                value=data
            )
            
            self.produce(
                key=serialized.key,
                value=serialized.value
            )
            
            self.processed_count += 1
            
            if self.processed_count % 10 == 0:
                logger.info(f"Processed {self.processed_count} messages")
                
        except Exception as e:
            logger.error(f"Error sending message: {str(e)}")

def main():
    app = Application()
    
    output_topic = app.topic(os.environ['output'])
    
    source = GoogleStorageBucketSource(
        name="google-storage-bucket-source",
        bucket_name=os.environ['GOOGLE_BUCKET_NAME'],
        folder_path=os.environ['GOOGLE_FOLDER_PATH'],
        file_format=os.environ['GOOGLE_FILE_FORMAT'],
        file_compression=os.environ['GOOGLE_FILE_COMPRESSION'],
        credentials_json=os.environ['GOOGLE_SECRET_KEY']
    )
    
    sdf = app.dataframe(topic=output_topic, source=source)
    sdf.print(metadata=True)
    
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
