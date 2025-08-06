import os
import json
import time
import logging
from typing import Dict, Any, List
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from quixstreams import Application
from quixstreams.sources.base import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleSheetsSource(Source):
    def __init__(self, sheet_id: str, credentials_json: str, name: str = "google-sheets-source"):
        super().__init__(name=name)
        self.sheet_id = sheet_id
        self.credentials_json = credentials_json
        self.service = None
        self.message_count = 0
        self.max_messages = 100
        
    def setup(self):
        try:
            credentials_info = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(
                credentials_info,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            self.service = build('sheets', 'v4', credentials=credentials)
            
            sheet_metadata = self.service.spreadsheets().get(spreadsheetId=self.sheet_id).execute()
            sheets = sheet_metadata.get('sheets', [])
            
            if not sheets:
                raise ValueError("No sheets found in the spreadsheet")
                
            self.sheet_name = sheets[0]['properties']['title']
            logger.info(f"Connected to Google Sheet: {self.sheet_id}, Sheet: {self.sheet_name}")
            
            if hasattr(self, 'on_client_connect_success') and callable(self.on_client_connect_success):
                self.on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to setup Google Sheets connection: {e}")
            if hasattr(self, 'on_client_connect_failure') and callable(self.on_client_connect_failure):
                self.on_client_connect_failure(e)
            raise
    
    def _read_sheet_data(self) -> List[Dict[str, Any]]:
        try:
            range_name = f"{self.sheet_name}!A:Z"
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.sheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return []
            
            headers = values[0] if len(values) > 0 else []
            data_rows = values[1:] if len(values) > 1 else []
            
            structured_data = []
            for row in data_rows:
                row_dict = {}
                for i, header in enumerate(headers):
                    value = row[i] if i < len(row) else ""
                    if value.isdigit():
                        value = int(value)
                    elif value.replace('.', '', 1).isdigit():
                        value = float(value)
                    row_dict[header] = value
                structured_data.append(row_dict)
            
            return structured_data
            
        except HttpError as error:
            logger.error(f"Google Sheets API error: {error}")
            raise
        except Exception as e:
            logger.error(f"Error reading sheet data: {e}")
            raise
    
    def run(self):
        logger.info("Starting Google Sheets source")
        
        try:
            self.setup()
            
            while self.running and self.message_count < self.max_messages:
                try:
                    sheet_data = self._read_sheet_data()
                    
                    if not sheet_data:
                        logger.info("No data found in sheet")
                        time.sleep(5)
                        continue
                    
                    message_payload = {
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "sheet_id": self.sheet_id,
                        "sheet_name": self.sheet_name,
                        "data": sheet_data
                    }
                    
                    serialized = self.serialize(
                        key=self.sheet_id,
                        value=message_payload
                    )
                    
                    self.produce(
                        key=serialized.key,
                        value=serialized.value
                    )
                    
                    self.message_count += 1
                    logger.info(f"Produced message {self.message_count}/{self.max_messages} with {len(sheet_data)} rows")
                    
                    time.sleep(10)
                    
                except Exception as e:
                    logger.error(f"Error processing sheet data: {e}")
                    time.sleep(5)
                    continue
            
            logger.info(f"Finished producing {self.message_count} messages")
            
        except Exception as e:
            logger.error(f"Fatal error in Google Sheets source: {e}")
            raise

def main():
    logger.info("Initializing Google Sheets source application")
    
    sheet_id = os.environ.get('SHEET_NAME')
    credentials_json = os.environ.get('GCP_SHEETS_CREDENTIALS_KEY')
    
    if not sheet_id:
        raise ValueError("SHEET_NAME environment variable is required")
    
    if not credentials_json:
        raise ValueError("GCP_SHEETS_CREDENTIALS_KEY environment variable is required")
    
    app = Application(
        consumer_group="google-sheets-source-draft-pdck",
        auto_create_topics=True
    )
    
    source = GoogleSheetsSource(
        sheet_id=sheet_id,
        credentials_json=credentials_json,
        name="google-sheets-producer"
    )
    
    output_topic = app.topic(name="output")
    
    sdf = app.dataframe(topic=output_topic, source=source)
    sdf.print(metadata=True)
    
    logger.info("Starting application")
    app.run()

if __name__ == "__main__":
    main()