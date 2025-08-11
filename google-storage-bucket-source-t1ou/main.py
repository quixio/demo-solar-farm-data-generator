import os
import json
import io
import logging
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source
from quixstreams.models.topics import TopicConfig
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageSource(Source):
    def __init__(self, bucket_name, credentials_json, folder_path="/", file_format="csv", compression="none", region="US-CENTRAL1", **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.credentials_json = credentials_json
        self.folder_path = folder_path
        self.file_format = file_format.lower()
        self.compression = compression.lower()
        self.region = region
        self.client = None
        self.bucket = None
        self.messages_processed = 0
        self.max_messages = 100

    def setup(self):
        try:
            if not self.credentials_json:
                raise ValueError("Google Cloud Storage credentials not found")
            
            credentials_dict = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            
            self.client = storage.Client(credentials=credentials)
            self.bucket = self.client.bucket(self.bucket_name)
            
            logger.info(f"Successfully connected to Google Cloud Storage bucket: {self.bucket_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Google Cloud Storage: {str(e)}")
            return False

    def run(self):
        if not self.setup():
            logger.error("Failed to setup Google Cloud Storage connection")
            return

        try:
            folder_path = self.folder_path
            if folder_path.startswith('/'):
                folder_path = folder_path[1:]
            if folder_path and not folder_path.endswith('/'):
                folder_path += '/'

            blobs = list(self.bucket.list_blobs(prefix=folder_path))
            
            if not blobs:
                logger.warning(f"No files found in folder: {self.folder_path}")
                return

            if self.file_format != 'none':
                extension = f".{self.file_format}"
                blobs = [blob for blob in blobs if blob.name.lower().endswith(extension)]

            if not blobs:
                logger.warning(f"No {self.file_format} files found in folder: {self.folder_path}")
                return

            logger.info(f"Found {len(blobs)} file(s) to process")

            for blob in blobs:
                if not self.running or self.messages_processed >= self.max_messages:
                    break

                try:
                    self._process_file(blob)
                except Exception as e:
                    logger.error(f"Error processing file {blob.name}: {str(e)}")
                    continue

            logger.info(f"Finished processing. Total messages: {self.messages_processed}")

        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")

    def _process_file(self, blob):
        logger.info(f"Processing file: {blob.name}")
        
        content = blob.download_as_bytes()
        
        if self.compression != 'none':
            content = self._decompress_content(content)

        if self.file_format == 'csv':
            self._process_csv(content, blob.name)
        elif self.file_format == 'json':
            self._process_json(content, blob.name)
        elif self.file_format == 'txt':
            self._process_txt(content, blob.name)
        else:
            self._process_raw(content, blob.name)

    def _decompress_content(self, content):
        if self.compression == 'gzip':
            import gzip
            return gzip.decompress(content)
        elif self.compression == 'zip':
            import zipfile
            with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                file_names = zip_file.namelist()
                if file_names:
                    return zip_file.read(file_names[0])
        return content

    def _process_csv(self, content, filename):
        try:
            df = pd.read_csv(io.BytesIO(content))
            
            for index, row in df.iterrows():
                if not self.running or self.messages_processed >= self.max_messages:
                    break

                record = row.to_dict()
                
                if 'timestamp' in record:
                    try:
                        timestamp_str = str(record['timestamp'])
                        if not timestamp_str.endswith('Z') and '+' not in timestamp_str:
                            if 'T' not in timestamp_str:
                                timestamp_str = timestamp_str.replace(' ', 'T')
                            if '.' in timestamp_str:
                                timestamp_str = timestamp_str.split('.')[0] + 'Z'
                            else:
                                timestamp_str += 'Z'
                        record['timestamp'] = timestamp_str
                    except Exception:
                        pass

                message_key = f"{filename}_{index}"
                
                serialized = self.serialize(
                    key=message_key,
                    value=record
                )

                self.produce(
                    key=serialized.key,
                    value=serialized.value
                )

                self.messages_processed += 1
                
        except Exception as e:
            logger.error(f"Error processing CSV file {filename}: {str(e)}")

    def _process_json(self, content, filename):
        try:
            content_str = content.decode('utf-8')
            
            try:
                lines = content_str.strip().split('\n')
                for i, line in enumerate(lines):
                    if not self.running or self.messages_processed >= self.max_messages:
                        break
                    if line.strip():
                        record = json.loads(line.strip())
                        message_key = f"{filename}_{i}"
                        
                        serialized = self.serialize(
                            key=message_key,
                            value=record
                        )

                        self.produce(
                            key=serialized.key,
                            value=serialized.value
                        )

                        self.messages_processed += 1
                        
            except json.JSONDecodeError:
                data = json.loads(content_str)
                if isinstance(data, list):
                    for i, record in enumerate(data):
                        if not self.running or self.messages_processed >= self.max_messages:
                            break
                        message_key = f"{filename}_{i}"
                        
                        serialized = self.serialize(
                            key=message_key,
                            value=record
                        )

                        self.produce(
                            key=serialized.key,
                            value=serialized.value
                        )

                        self.messages_processed += 1
                else:
                    message_key = filename
                    
                    serialized = self.serialize(
                        key=message_key,
                        value=data
                    )

                    self.produce(
                        key=serialized.key,
                        value=serialized.value
                    )

                    self.messages_processed += 1
                    
        except Exception as e:
            logger.error(f"Error processing JSON file {filename}: {str(e)}")

    def _process_txt(self, content, filename):
        try:
            content_str = content.decode('utf-8')
            lines = content_str.strip().split('\n')
            
            for i, line in enumerate(lines):
                if not self.running or self.messages_processed >= self.max_messages:
                    break
                if line.strip():
                    record = {"line": line.strip(), "line_number": i + 1, "filename": filename}
                    message_key = f"{filename}_{i}"
                    
                    serialized = self.serialize(
                        key=message_key,
                        value=record
                    )

                    self.produce(
                        key=serialized.key,
                        value=serialized.value
                    )

                    self.messages_processed += 1
                    
        except Exception as e:
            logger.error(f"Error processing TXT file {filename}: {str(e)}")

    def _process_raw(self, content, filename):
        try:
            record = {
                "filename": filename,
                "content": content.decode('utf-8', errors='ignore')[:1000],
                "size": len(content)
            }
            
            serialized = self.serialize(
                key=filename,
                value=record
            )

            self.produce(
                key=serialized.key,
                value=serialized.value
            )

            self.messages_processed += 1
            
        except Exception as e:
            logger.error(f"Error processing raw file {filename}: {str(e)}")

def main():
    app = Application()

    bucket_name = os.environ.get('GOOGLE_STORAGE_BUCKET', 'quix-workflow')
    region = os.environ.get('GOOGLE_STORAGE_REGION', 'US-CENTRAL1')
    credentials_json = os.environ.get('GOOGLE_STORAGE_SECRET_KEY')
    folder_path = os.environ.get('GOOGLE_STORAGE_FOLDER_PATH', '/')
    file_format = os.environ.get('GOOGLE_STORAGE_FILE_FORMAT', 'csv')
    compression = os.environ.get('GOOGLE_STORAGE_FILE_COMPRESSION', 'none')

    topic = app.topic(
        "badboy",
        value_serializer='json',
        key_serializer='string',
        config=TopicConfig(num_partitions=1, replication_factor=1)
    )

    source = GoogleStorageSource(
        name="google-storage-source",
        bucket_name=bucket_name,
        credentials_json=credentials_json,
        folder_path=folder_path,
        file_format=file_format,
        compression=compression,
        region=region
    )

    sdf = app.dataframe(topic=topic, source=source)
    sdf.print(metadata=True)

    logger.info("Starting Google Cloud Storage source application")
    app.run()

if __name__ == "__main__":
    main()