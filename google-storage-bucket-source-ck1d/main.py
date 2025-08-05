import os
import json
import io
import csv
import logging
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageSource(Source):
    def __init__(self, bucket_name, folder_path, file_format, project_id, credentials_json_str, **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.folder_path = folder_path.strip('/')
        self.file_format = file_format
        self.project_id = project_id
        self.credentials_json_str = credentials_json_str
        self.client = None
        self.bucket = None
        self.messages_processed = 0
        self.max_messages = 100

    def setup(self):
        try:
            credentials_info = json.loads(self.credentials_json_str)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            self.client = storage.Client(credentials=credentials, project=self.project_id)
            self.bucket = self.client.bucket(self.bucket_name)
            logger.info(f"Successfully connected to Google Cloud Storage bucket: {self.bucket_name}")
        except Exception as e:
            logger.error(f"Failed to connect to Google Cloud Storage: {e}")
            raise

    def run(self):
        if not self.client or not self.bucket:
            self.setup()

        try:
            prefix = self.folder_path + '/' if self.folder_path and not self.folder_path.endswith('/') else self.folder_path
            blobs = list(self.bucket.list_blobs(prefix=prefix))
            
            if not blobs:
                logger.warning("No files found in the specified bucket/folder")
                return

            logger.info(f"Found {len(blobs)} files in bucket")

            for blob in blobs:
                if not self.running or self.messages_processed >= self.max_messages:
                    break

                if blob.name.endswith('/'):
                    continue

                if self.file_format != 'none' and not blob.name.lower().endswith(f'.{self.file_format.lower()}'):
                    continue

                try:
                    self._process_file(blob)
                except Exception as e:
                    logger.error(f"Error processing file {blob.name}: {e}")
                    continue

            logger.info(f"Processing complete. Processed {self.messages_processed} messages")

        except Exception as e:
            logger.error(f"Error during source execution: {e}")
            raise

    def _process_file(self, blob):
        logger.info(f"Processing file: {blob.name}")
        
        try:
            content = blob.download_as_bytes()
            
            if self.file_format.lower() == 'csv':
                self._process_csv_content(content, blob.name)
            elif self.file_format.lower() == 'json':
                self._process_json_content(content, blob.name)
            else:
                self._process_text_content(content, blob.name)
                
        except Exception as e:
            logger.error(f"Error processing file content {blob.name}: {e}")
            raise

    def _process_csv_content(self, content, filename):
        try:
            content_str = content.decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(content_str))
            
            for row in csv_reader:
                if not self.running or self.messages_processed >= self.max_messages:
                    break
                
                message_data = self._transform_csv_row(dict(row))
                
                msg = self.serialize(
                    key=f"{filename}_{self.messages_processed}",
                    value=message_data
                )
                
                self.produce(
                    key=msg.key,
                    value=msg.value
                )
                
                self.messages_processed += 1
                
        except UnicodeDecodeError as e:
            logger.error(f"Could not decode CSV file {filename} as UTF-8: {e}")
            raise

    def _process_json_content(self, content, filename):
        try:
            content_str = content.decode('utf-8')
            json_data = json.loads(content_str)
            
            if isinstance(json_data, list):
                for item in json_data:
                    if not self.running or self.messages_processed >= self.max_messages:
                        break
                    
                    msg = self.serialize(
                        key=f"{filename}_{self.messages_processed}",
                        value=item
                    )
                    
                    self.produce(
                        key=msg.key,
                        value=msg.value
                    )
                    
                    self.messages_processed += 1
            else:
                msg = self.serialize(
                    key=f"{filename}_{self.messages_processed}",
                    value=json_data
                )
                
                self.produce(
                    key=msg.key,
                    value=msg.value
                )
                
                self.messages_processed += 1
                
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            logger.error(f"Error processing JSON file {filename}: {e}")
            raise

    def _process_text_content(self, content, filename):
        try:
            content_str = content.decode('utf-8')
            lines = content_str.split('\n')
            
            for line in lines:
                if not self.running or self.messages_processed >= self.max_messages:
                    break
                
                if line.strip():
                    msg = self.serialize(
                        key=f"{filename}_{self.messages_processed}",
                        value={"line": line.strip(), "filename": filename}
                    )
                    
                    self.produce(
                        key=msg.key,
                        value=msg.value
                    )
                    
                    self.messages_processed += 1
                    
        except UnicodeDecodeError as e:
            logger.error(f"Could not decode text file {filename} as UTF-8: {e}")
            raise

    def _transform_csv_row(self, row):
        transformed_row = {}
        
        for key, value in row.items():
            if key == 'timestamp':
                transformed_row[key] = value
            elif key in ['hotend_temperature', 'bed_temperature', 'ambient_temperature', 'fluctuated_ambient_temperature']:
                try:
                    transformed_row[key] = float(value)
                except (ValueError, TypeError):
                    transformed_row[key] = value
            else:
                transformed_row[key] = value
        
        return transformed_row

def main():
    app = Application()
    
    output_topic = app.topic(os.environ['output'])
    
    source = GoogleStorageSource(
        name="google-storage-source",
        bucket_name=os.environ['GCS_BUCKET'],
        folder_path=os.environ['GCS_FOLDER_PATH'],
        file_format=os.environ['GCS_FILE_FORMAT'],
        project_id=os.environ['GCP_PROJECT_ID'],
        credentials_json_str=os.environ['GCP_CREDENTIALS_KEY']
    )
    
    sdf = app.dataframe(topic=output_topic, source=source)
    sdf.print(metadata=True)
    
    logger.info("Starting Google Cloud Storage source application")
    app.run()

if __name__ == "__main__":
    main()