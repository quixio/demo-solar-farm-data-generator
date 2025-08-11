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
    def __init__(self, bucket_name, folder_path, file_format, credentials_json, **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.folder_path = folder_path.lstrip('/') if folder_path.startswith('/') else folder_path
        if self.folder_path and not self.folder_path.endswith('/'):
            self.folder_path += '/'
        self.file_format = file_format.lower()
        self.credentials_json = credentials_json
        self.client = None
        self.bucket = None
        self.processed_files = set()
        self.message_count = 0
        
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
            
        logger.info(f"Starting to read from bucket: {self.bucket_name}, folder: {self.folder_path or 'root'}")
        
        try:
            while self.running and self.message_count < 100:
                blobs = list(self.bucket.list_blobs(prefix=self.folder_path))
                
                target_files = [
                    blob for blob in blobs 
                    if blob.name.lower().endswith(f'.{self.file_format}') 
                    and blob.name not in self.processed_files
                ]
                
                if not target_files:
                    logger.info("No new files found, waiting...")
                    time.sleep(5)
                    continue
                
                for blob in target_files:
                    if not self.running or self.message_count >= 100:
                        break
                        
                    try:
                        logger.info(f"Processing file: {blob.name}")
                        content = blob.download_as_text()
                        
                        if self.file_format == 'csv':
                            self._process_csv_content(content, blob.name)
                        elif self.file_format == 'json':
                            self._process_json_content(content, blob.name)
                        else:
                            self._process_text_content(content, blob.name)
                            
                        self.processed_files.add(blob.name)
                        
                    except Exception as e:
                        logger.error(f"Error processing file {blob.name}: {str(e)}")
                        continue
                
                if self.message_count >= 100:
                    logger.info("Reached 100 messages limit, stopping...")
                    break
                    
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in run loop: {str(e)}")
        finally:
            logger.info(f"Processed {self.message_count} messages total")
    
    def _process_csv_content(self, content, filename):
        lines = content.strip().split('\n')
        if not lines:
            return
            
        headers = None
        for i, line in enumerate(lines):
            if not self.running or self.message_count >= 100:
                break
                
            line = line.strip()
            if not line:
                continue
                
            if i == 0 and not line.replace(',', '').replace('.', '').replace('-', '').replace('_', '').isdigit():
                headers = [h.strip() for h in line.split(',')]
                continue
                
            try:
                values = [v.strip() for v in line.split(',')]
                
                if headers and len(values) == len(headers):
                    record = {}
                    for j, header in enumerate(headers):
                        value = values[j]
                        try:
                            if '.' in value:
                                record[header] = float(value)
                            elif value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
                                record[header] = int(value)
                            else:
                                record[header] = value
                        except (ValueError, IndexError):
                            record[header] = value
                else:
                    record = {f"field_{j}": v for j, v in enumerate(values)}
                
                self._send_message(record, filename)
                
            except Exception as e:
                logger.error(f"Error processing CSV line: {str(e)}")
                continue
    
    def _process_json_content(self, content, filename):
        try:
            data = json.loads(content)
            if isinstance(data, list):
                for item in data:
                    if not self.running or self.message_count >= 100:
                        break
                    self._send_message(item, filename)
            else:
                self._send_message(data, filename)
        except json.JSONDecodeError:
            lines = content.strip().split('\n')
            for line in lines:
                if not self.running or self.message_count >= 100:
                    break
                if line.strip():
                    try:
                        item = json.loads(line)
                        self._send_message(item, filename)
                    except json.JSONDecodeError:
                        continue
    
    def _process_text_content(self, content, filename):
        lines = content.strip().split('\n')
        for line in lines:
            if not self.running or self.message_count >= 100:
                break
            if line.strip():
                record = {"content": line.strip(), "source_file": filename}
                self._send_message(record, filename)
    
    def _send_message(self, record, filename):
        try:
            record["_source_file"] = filename
            record["_timestamp"] = time.time()
            
            msg = self.serialize(
                key=filename,
                value=record
            )
            
            self.produce(
                key=msg.key,
                value=msg.value
            )
            
            self.message_count += 1
            logger.info(f"Sent message {self.message_count} from {filename}")
            
        except Exception as e:
            logger.error(f"Error sending message: {str(e)}")

def main():
    app = Application()
    
    output_topic = app.topic(os.environ.get('output', 'bucket-output'))
    
    source = GoogleStorageSource(
        name="google-storage-source",
        bucket_name=os.environ['GOOGLE_STORAGE_BUCKET'],
        folder_path=os.environ.get('GOOGLE_STORAGE_FOLDER_PATH', '/'),
        file_format=os.environ.get('GOOGLE_STORAGE_FILE_FORMAT', 'csv'),
        credentials_json=os.environ['GOOGLE_STORAGE_SECRET_KEY']
    )
    
    sdf = app.dataframe(topic=output_topic, source=source)
    sdf.print(metadata=True)
    
    logger.info("Starting Google Storage source application...")
    app.run()

if __name__ == "__main__":
    main()