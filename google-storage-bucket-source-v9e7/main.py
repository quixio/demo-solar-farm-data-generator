import os
import json
import csv
from io import StringIO
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

class GoogleStorageSource(Source):
    
    def __init__(self, bucket_name, project_id, credentials_key, folder_path="/", 
                 file_format="csv", file_compression="none", **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_key = credentials_key
        self.folder_path = folder_path
        self.file_format = file_format
        self.file_compression = file_compression
        self.client = None
        self.bucket = None
        
    def setup(self):
        try:
            credentials_json = os.environ.get(self.credentials_key)
            if not credentials_json:
                raise ValueError("Credentials not found in environment variable")
            
            credentials_dict = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            
            self.client = storage.Client(credentials=credentials, project=self.project_id)
            self.bucket = self.client.bucket(self.bucket_name)
            
            if hasattr(self, 'on_client_connect_success'):
                self.on_client_connect_success()
                
        except Exception as e:
            if hasattr(self, 'on_client_connect_failure'):
                self.on_client_connect_failure(e)
            raise
    
    def run(self):
        self.setup()
        
        prefix = self.folder_path.strip('/') + '/' if self.folder_path and self.folder_path != '/' else ''
        blobs = list(self.bucket.list_blobs(prefix=prefix))
        
        if self.file_format and self.file_format != 'none':
            blobs = [blob for blob in blobs if blob.name.lower().endswith(f'.{self.file_format.lower()}')]
        
        message_count = 0
        max_messages = 100
        
        for blob in blobs:
            if not self.running or message_count >= max_messages:
                break
                
            try:
                content = blob.download_as_text()
                
                if self.file_format.lower() == 'csv':
                    csv_reader = csv.DictReader(StringIO(content))
                    
                    for row in csv_reader:
                        if not self.running or message_count >= max_messages:
                            break
                            
                        processed_row = self._process_csv_row(row)
                        
                        msg = self.serialize(
                            key=blob.name,
                            value=processed_row,
                        )
                        
                        self.produce(
                            key=msg.key,
                            value=msg.value,
                        )
                        
                        message_count += 1
                        
                elif self.file_format.lower() == 'json':
                    try:
                        data = json.loads(content)
                        if isinstance(data, list):
                            for item in data:
                                if not self.running or message_count >= max_messages:
                                    break
                                    
                                msg = self.serialize(
                                    key=blob.name,
                                    value=item,
                                )
                                
                                self.produce(
                                    key=msg.key,
                                    value=msg.value,
                                )
                                
                                message_count += 1
                        else:
                            msg = self.serialize(
                                key=blob.name,
                                value=data,
                            )
                            
                            self.produce(
                                key=msg.key,
                                value=msg.value,
                            )
                            
                            message_count += 1
                            
                    except json.JSONDecodeError:
                        lines = content.strip().split('\n')
                        for line in lines:
                            if not self.running or message_count >= max_messages:
                                break
                                
                            try:
                                item = json.loads(line)
                            except json.JSONDecodeError:
                                item = {"raw_line": line}
                                
                            msg = self.serialize(
                                key=blob.name,
                                value=item,
                            )
                            
                            self.produce(
                                key=msg.key,
                                value=msg.value,
                            )
                            
                            message_count += 1
                            
                else:
                    lines = content.strip().split('\n')
                    for line in lines:
                        if not self.running or message_count >= max_messages:
                            break
                            
                        msg = self.serialize(
                            key=blob.name,
                            value={"raw_line": line},
                        )
                        
                        self.produce(
                            key=msg.key,
                            value=msg.value,
                        )
                        
                        message_count += 1
                        
            except Exception as e:
                print(f"Error processing file {blob.name}: {str(e)}")
                continue
    
    def _process_csv_row(self, row):
        processed_row = {}
        
        for key, value in row.items():
            if key == 'timestamp':
                processed_row[key] = value
            elif key in ['hotend_temperature', 'bed_temperature', 'ambient_temperature', 'fluctuated_ambient_temperature']:
                try:
                    processed_row[key] = float(value)
                except (ValueError, TypeError):
                    processed_row[key] = value
            else:
                processed_row[key] = value
                
        return processed_row

def main():
    app = Application()
    
    output_topic = app.topic(os.environ['output'])
    
    source = GoogleStorageSource(
        name="google-storage-source",
        bucket_name=os.environ['GCS_BUCKET'],
        project_id=os.environ['GCP_PROJECT_ID'],
        credentials_key=os.environ['GCP_CREDENTIALS_KEY'],
        folder_path=os.environ.get('GCS_FOLDER_PATH', '/'),
        file_format=os.environ.get('GCS_FILE_FORMAT', 'csv'),
        file_compression=os.environ.get('GCS_FILE_COMPRESSION', 'none')
    )
    
    sdf = app.dataframe(topic=output_topic, source=source)
    sdf.print(metadata=True)
    
    app.run()

if __name__ == "__main__":
    main()