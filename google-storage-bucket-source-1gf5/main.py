import os
import json
import time
import logging
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageSource(Source):
    def __init__(self, bucket_name, project_id, credentials_json, folder_path="/", file_format="csv", compression="none", **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path
        self.file_format = file_format
        self.compression = compression
        self.client = None
        self.bucket = None
        self.processed_count = 0
        self.max_messages = 100

    def setup(self):
        try:
            credentials_info = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            self.client = storage.Client(project=self.project_id, credentials=credentials)
            self.bucket = self.client.bucket(self.bucket_name)
            
            self.bucket.exists()
            logger.info(f"Successfully connected to Google Storage bucket: {self.bucket_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Google Storage")
            return False

    def run(self):
        if not self.setup():
            logger.error("Failed to setup Google Storage connection")
            return

        try:
            prefix = self.folder_path.strip('/')
            if prefix and not prefix.endswith('/'):
                prefix += '/'
            
            blobs = list(self.bucket.list_blobs(prefix=prefix if prefix != '/' else ''))
            
            if not blobs:
                logger.warning("No files found in the specified folder path")
                return

            logger.info(f"Found {len(blobs)} files in bucket")

            for blob in blobs:
                if not self.running or self.processed_count >= self.max_messages:
                    break

                if blob.name.endswith('/'):
                    continue

                try:
                    if blob.size and blob.size > 0:
                        content = blob.download_as_text(encoding='utf-8')
                        
                        if self.file_format.lower() == 'csv':
                            lines = content.strip().split('\n')
                            for i, line in enumerate(lines):
                                if not self.running or self.processed_count >= self.max_messages:
                                    break
                                
                                if not line.strip():
                                    continue
                                
                                message_data = {
                                    'file_name': blob.name,
                                    'file_size': blob.size,
                                    'line_number': i + 1,
                                    'content': line.strip(),
                                    'timestamp': time.time(),
                                    'bucket': self.bucket_name,
                                    'folder_path': self.folder_path
                                }
                                
                                msg = self.serialize(
                                    key=f"{blob.name}_{i+1}",
                                    value=message_data
                                )
                                
                                self.produce(
                                    key=msg.key,
                                    value=msg.value
                                )
                                
                                self.processed_count += 1
                                logger.info(f"Processed message {self.processed_count} from file: {blob.name}")
                                
                                time.sleep(0.1)
                        
                        elif self.file_format.lower() == 'json':
                            try:
                                json_data = json.loads(content)
                                message_data = {
                                    'file_name': blob.name,
                                    'file_size': blob.size,
                                    'content': json_data,
                                    'timestamp': time.time(),
                                    'bucket': self.bucket_name,
                                    'folder_path': self.folder_path
                                }
                                
                                msg = self.serialize(
                                    key=blob.name,
                                    value=message_data
                                )
                                
                                self.produce(
                                    key=msg.key,
                                    value=msg.value
                                )
                                
                                self.processed_count += 1
                                logger.info(f"Processed JSON message from file: {blob.name}")
                            except json.JSONDecodeError as e:
                                logger.error(f"Failed to parse JSON from file {blob.name}")
                        
                        else:
                            message_data = {
                                'file_name': blob.name,
                                'file_size': blob.size,
                                'content': content,
                                'timestamp': time.time(),
                                'bucket': self.bucket_name,
                                'folder_path': self.folder_path,
                                'format': self.file_format
                            }
                            
                            msg = self.serialize(
                                key=blob.name,
                                value=message_data
                            )
                            
                            self.produce(
                                key=msg.key,
                                value=msg.value
                            )
                            
                            self.processed_count += 1
                            logger.info(f"Processed message from file: {blob.name}")
                    
                    else:
                        logger.warning(f"Skipping empty file: {blob.name}")

                except Exception as e:
                    logger.error(f"Error processing file {blob.name}")
                    continue

            logger.info(f"Completed processing. Total messages processed: {self.processed_count}")

        except Exception as e:
            logger.error(f"Error during processing")

    def cleanup(self, failed: bool = False):
        if self.client:
            try:
                self.client.close()
                logger.info("Google Storage client connection closed")
            except Exception as ex:
                logger.warning(f"Error while closing GCS client")

        try:
            super().cleanup(failed=failed)
        except AttributeError:
            pass

    def default_topic(self):
        return f"source__{self.bucket_name}_{self.folder_path.replace('/', '_')}"

def main():
    bucket_name = os.environ.get('GS_BUCKET')
    project_id = os.environ.get('GS_PROJECT_ID')
    credentials_json = os.environ.get('GS_SECRET_KEY')
    folder_path = os.environ.get('GS_FOLDER_PATH', '/')
    file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
    compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
    output_topic_name = os.environ.get('output', 'output')

    if not bucket_name:
        raise ValueError("GS_BUCKET environment variable is required")
    if not project_id:
        raise ValueError("GS_PROJECT_ID environment variable is required")
    if not credentials_json:
        raise ValueError("GS_SECRET_KEY environment variable is required")

    app = Application()

    output_topic = app.topic(output_topic_name)

    source = GoogleStorageSource(
        name="google-storage-source",
        bucket_name=bucket_name,
        project_id=project_id,
        credentials_json=credentials_json,
        folder_path=folder_path,
        file_format=file_format,
        compression=compression
    )

    sdf = app.dataframe(topic=output_topic, source=source)
    
    sdf.print(metadata=True)

    logger.info(f"Starting Google Storage source application")
    logger.info(f"Bucket: {bucket_name}")
    logger.info(f"Project ID: {project_id}")
    logger.info(f"Folder path: {folder_path}")
    logger.info(f"File format: {file_format}")
    logger.info(f"Output topic: {output_topic_name}")

    app.run()

if __name__ == "__main__":
    main()