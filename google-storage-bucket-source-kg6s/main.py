import os
import json
import logging
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageSource(Source):
    def __init__(self, bucket_name, folder_path, file_format, project_id, credentials_json_str, name="google-storage-source"):
        super().__init__(name=name)
        self.bucket_name = bucket_name
        self.folder_path = folder_path.strip('/')
        self.file_format = file_format.lower()
        self.project_id = project_id
        self.credentials_json_str = credentials_json_str
        self.client = None
        self.bucket = None
        self.message_count = 0
        self.max_messages = 100

    def setup(self):
        try:
            credentials_dict = json.loads(self.credentials_json_str)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            self.client = storage.Client(credentials=credentials, project=self.project_id)
            self.bucket = self.client.bucket(self.bucket_name)
            logger.info(f"Successfully connected to GCS bucket: {self.bucket_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to GCS: {str(e)}")
            return False

    def run(self):
        if not self.client or not self.bucket:
            logger.error("GCS client not initialized")
            return

        try:
            prefix = self.folder_path + "/" if self.folder_path else ""
            blobs = list(self.bucket.list_blobs(prefix=prefix))
            
            if not blobs:
                logger.warning(f"No files found in folder: {self.folder_path}")
                return

            target_files = [blob for blob in blobs if blob.name.endswith(f'.{self.file_format}')]
            
            if not target_files:
                logger.warning(f"No {self.file_format} files found in folder: {self.folder_path}")
                return

            logger.info(f"Found {len(target_files)} {self.file_format} files")

            for blob in target_files:
                if not self.running or self.message_count >= self.max_messages:
                    break
                
                self._process_file(blob)

            logger.info(f"Finished processing. Total messages sent: {self.message_count}")

        except Exception as e:
            logger.error(f"Error processing files: {str(e)}")

    def _process_file(self, blob):
        try:
            logger.info(f"Processing file: {blob.name}")
            content = blob.download_as_text()
            
            if self.file_format == 'csv':
                self._process_csv_content(content, blob.name)
            elif self.file_format == 'json':
                self._process_json_content(content, blob.name)
            else:
                self._process_text_content(content, blob.name)
                
        except Exception as e:
            logger.error(f"Error processing file {blob.name}: {str(e)}")

    def _process_csv_content(self, content, filename):
        lines = content.strip().split('\n')
        if not lines:
            return
        
        headers = [header.strip() for header in lines[0].split(',')]
        
        for line in lines[1:]:
            if not self.running or self.message_count >= self.max_messages:
                break
                
            if line.strip():
                try:
                    values = [value.strip() for value in line.split(',')]
                    if len(values) == len(headers):
                        record = dict(zip(headers, values))
                        
                        # Convert numeric fields based on schema analysis
                        if 'hotend_temperature' in record:
                            record['hotend_temperature'] = float(record['hotend_temperature'])
                        if 'bed_temperature' in record:
                            record['bed_temperature'] = float(record['bed_temperature'])
                        if 'ambient_temperature' in record:
                            record['ambient_temperature'] = float(record['ambient_temperature'])
                        if 'fluctuated_ambient_temperature' in record:
                            record['fluctuated_ambient_temperature'] = float(record['fluctuated_ambient_temperature'])
                        
                        # Add metadata
                        record['_source_file'] = filename
                        record['_file_type'] = 'csv'
                        
                        msg = self.serialize(
                            key=filename,
                            value=record
                        )
                        
                        self.produce(
                            key=msg.key,
                            value=msg.value
                        )
                        
                        self.message_count += 1
                        
                except Exception as e:
                    logger.warning(f"Error processing CSV line: {str(e)}")
                    continue

    def _process_json_content(self, content, filename):
        lines = content.strip().split('\n')
        
        for line in lines:
            if not self.running or self.message_count >= self.max_messages:
                break
                
            if line.strip():
                try:
                    record = json.loads(line)
                    record['_source_file'] = filename
                    record['_file_type'] = 'json'
                    
                    msg = self.serialize(
                        key=filename,
                        value=record
                    )
                    
                    self.produce(
                        key=msg.key,
                        value=msg.value
                    )
                    
                    self.message_count += 1
                    
                except json.JSONDecodeError:
                    continue

    def _process_text_content(self, content, filename):
        lines = content.strip().split('\n')
        
        for line in lines:
            if not self.running or self.message_count >= self.max_messages:
                break
                
            if line.strip():
                record = {
                    'content': line.strip(),
                    '_source_file': filename,
                    '_file_type': 'text'
                }
                
                msg = self.serialize(
                    key=filename,
                    value=record
                )
                
                self.produce(
                    key=msg.key,
                    value=msg.value
                )
                
                self.message_count += 1

def main():
    try:
        app = Application()
        
        bucket_name = os.environ['GCS_BUCKET']
        folder_path = os.environ['GCS_FOLDER_PATH']
        file_format = os.environ['GCS_FILE_FORMAT']
        project_id = os.environ['GCP_PROJECT_ID']
        credentials_json_str = os.environ['GCP_CREDENTIALS_KEY']
        output_topic_name = os.environ['output']
        
        logger.info(f"Initializing Google Storage source for bucket: {bucket_name}")
        logger.info(f"Folder path: {folder_path}")
        logger.info(f"File format: {file_format}")
        logger.info(f"Output topic: {output_topic_name}")
        
        source = GoogleStorageSource(
            bucket_name=bucket_name,
            folder_path=folder_path,
            file_format=file_format,
            project_id=project_id,
            credentials_json_str=credentials_json_str,
            name="google-storage-bucket-source-draft-kg6s"
        )
        
        topic = app.topic(
            output_topic_name,
            value_serializer='json',
            key_serializer='string'
        )
        
        sdf = app.dataframe(topic=topic, source=source)
        sdf.print(metadata=True)
        
        logger.info("Starting Google Storage source application")
        app.run()
        
    except KeyError as e:
        logger.error(f"Missing required environment variable: {e}")
        raise
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()