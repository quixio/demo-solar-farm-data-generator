# Cached sandbox code for google storage bucket
# Generated on 2025-08-11 18:37:41
# Template: Unknown
# This is cached code - delete this file to force regeneration

import os
import json
import io
import time
import logging
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
from quixstreams import Application
from quixstreams.sources.base import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageSource(Source):
    def __init__(self, bucket_name, credentials_json, folder_path, file_format, compression, region=None, **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.credentials_json = credentials_json
        self.folder_path = folder_path
        self.file_format = file_format
        self.compression = compression
        self.region = region
        self.client = None
        self.bucket = None
        self.messages_produced = 0
        self.max_messages = 100

    def setup(self):
        try:
            credentials_dict = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            self.client = storage.Client(credentials=credentials)
            self.bucket = self.client.bucket(self.bucket_name)
            logger.info(f"Successfully connected to Google Storage bucket: {self.bucket_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Google Storage: {str(e)}")
            return False

    def run(self):
        if not self.setup():
            logger.error("Failed to setup Google Storage connection")
            return

        try:
            folder_path = self.folder_path
            if folder_path.startswith('/'):
                folder_path = folder_path[1:]
            if folder_path and not folder_path.endswith('/'):
                folder_path += '/'

            blobs = list(self.bucket.list_blobs(prefix=folder_path))
            target_files = [blob for blob in blobs if blob.name.endswith(f'.{self.file_format}')]

            if not target_files:
                logger.warning(f"No {self.file_format} files found in folder: {folder_path}")
                return

            logger.info(f"Found {len(target_files)} {self.file_format} files to process")

            for blob in target_files:
                if not self.running or self.messages_produced >= self.max_messages:
                    break

                logger.info(f"Processing file: {blob.name}")
                
                try:
                    file_content = blob.download_as_bytes()
                    
                    if self.compression != 'none':
                        if self.compression == 'gzip':
                            import gzip
                            file_content = gzip.decompress(file_content)
                        elif self.compression == 'zip':
                            import zipfile
                            with zipfile.ZipFile(io.BytesIO(file_content)) as zip_file:
                                file_names = zip_file.namelist()
                                if file_names:
                                    file_content = zip_file.read(file_names[0])

                    if self.file_format.lower() == 'csv':
                        df = pd.read_csv(io.BytesIO(file_content))
                        
                        for index, row in df.iterrows():
                            if not self.running or self.messages_produced >= self.max_messages:
                                break
                            
                            record = row.to_dict()
                            
                            if 'timestamp' in record:
                                timestamp_str = str(record['timestamp'])
                                if not timestamp_str.endswith('Z') and 'T' not in timestamp_str:
                                    record['timestamp'] = timestamp_str.replace(' ', 'T')
                            
                            message = self.serialize(
                                key=f"{blob.name}_{index}",
                                value=record
                            )
                            
                            self.produce(
                                key=message.key,
                                value=message.value
                            )
                            
                            self.messages_produced += 1
                            
                            if self.messages_produced % 10 == 0:
                                logger.info(f"Produced {self.messages_produced} messages")
                            
                            time.sleep(0.1)

                    elif self.file_format.lower() == 'json':
                        json_data = json.loads(file_content.decode('utf-8'))
                        
                        if isinstance(json_data, list):
                            for i, item in enumerate(json_data):
                                if not self.running or self.messages_produced >= self.max_messages:
                                    break
                                
                                message = self.serialize(
                                    key=f"{blob.name}_{i}",
                                    value=item
                                )
                                
                                self.produce(
                                    key=message.key,
                                    value=message.value
                                )
                                
                                self.messages_produced += 1
                                
                                if self.messages_produced % 10 == 0:
                                    logger.info(f"Produced {self.messages_produced} messages")
                                
                                time.sleep(0.1)
                        else:
                            message = self.serialize(
                                key=blob.name,
                                value=json_data
                            )
                            
                            self.produce(
                                key=message.key,
                                value=message.value
                            )
                            
                            self.messages_produced += 1

                    elif self.file_format.lower() == 'txt':
                        lines = file_content.decode('utf-8').split('\n')
                        for i, line in enumerate(lines):
                            if not self.running or self.messages_produced >= self.max_messages:
                                break
                            
                            if line.strip():
                                message = self.serialize(
                                    key=f"{blob.name}_{i}",
                                    value={"line": line.strip()}
                                )
                                
                                self.produce(
                                    key=message.key,
                                    value=message.value
                                )
                                
                                self.messages_produced += 1
                                
                                if self.messages_produced % 10 == 0:
                                    logger.info(f"Produced {self.messages_produced} messages")
                                
                                time.sleep(0.1)

                except Exception as e:
                    logger.error(f"Error processing file {blob.name}: {str(e)}")
                    continue

            logger.info(f"Completed processing. Total messages produced: {self.messages_produced}")

        except Exception as e:
            logger.error(f"Error during source execution: {str(e)}")
        finally:
            if self.client:
                self.client.close()

def main():
    app = Application()
    
    output_topic = app.topic(os.environ.get('output', 'badboy'))
    
    source = GoogleStorageSource(
        name="google-storage-source",
        bucket_name=os.environ['GOOGLE_STORAGE_BUCKET'],
        credentials_json=os.environ['GOOGLE_STORAGE_SECRET_KEY'],
        folder_path=os.environ['GOOGLE_STORAGE_FOLDER_PATH'],
        file_format=os.environ['GOOGLE_STORAGE_FILE_FORMAT'],
        compression=os.environ['GOOGLE_STORAGE_FILE_COMPRESSION'],
        region=os.environ.get('GOOGLE_STORAGE_REGION')
    )
    
    sdf = app.dataframe(topic=output_topic, source=source)
    sdf.print(metadata=True)
    
    app.run()

if __name__ == "__main__":
    main()