import os
import json
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from quixstreams import Application
from quixstreams.sources.base import Source


class GoogleSheetsSource(Source):
    def __init__(self, sheet_id, credentials_json, name="google-sheets-source"):
        super().__init__(name=name)
        self.sheet_id = sheet_id
        self.credentials_json = credentials_json
        self.service = None
        self.sheet_name = None
        self.headers = []
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
            print(f"Connected to Google Sheet: {self.sheet_name}")
            
            if hasattr(self, 'on_client_connect_success') and callable(self.on_client_connect_success):
                self.on_client_connect_success()
                
        except Exception as e:
            print(f"Failed to connect to Google Sheets: {e}")
            if hasattr(self, 'on_client_connect_failure') and callable(self.on_client_connect_failure):
                self.on_client_connect_failure(e)
            raise

    def _get_headers(self):
        try:
            range_name = f"{self.sheet_name}!A1:Z1"
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.sheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if values:
                self.headers = values[0]
                print(f"Headers found: {self.headers}")
            else:
                print("No headers found in the sheet")
                
        except HttpError as e:
            print(f"Error reading headers: {e}")
            raise

    def _get_all_data(self):
        try:
            range_name = f"{self.sheet_name}!A2:Z"
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.sheet_id,
                range=range_name
            ).execute()
            
            return result.get('values', [])
            
        except HttpError as e:
            print(f"Error reading data: {e}")
            raise

    def run(self):
        if not self.service:
            self.setup()
            
        self._get_headers()
        
        while self.running and self.message_count < self.max_messages:
            try:
                data_rows = self._get_all_data()
                
                if not data_rows:
                    print("No data found in the sheet")
                    break
                
                for row in data_rows:
                    if not self.running or self.message_count >= self.max_messages:
                        break
                    
                    row_data = {}
                    for i, header in enumerate(self.headers):
                        if i < len(row):
                            value = row[i]
                            if value.isdigit():
                                row_data[header] = int(value)
                            else:
                                try:
                                    row_data[header] = float(value)
                                except ValueError:
                                    row_data[header] = value
                        else:
                            row_data[header] = None
                    
                    message = {
                        "timestamp": time.time(),
                        "sheet_id": self.sheet_id,
                        "sheet_name": self.sheet_name,
                        "data": row_data
                    }
                    
                    serialized = self.serialize(
                        key=f"{self.sheet_id}_{self.message_count}",
                        value=message
                    )
                    
                    self.produce(
                        key=serialized.key,
                        value=serialized.value
                    )
                    
                    self.message_count += 1
                    print(f"Produced message {self.message_count}: {row_data}")
                    
                    time.sleep(0.1)
                
                if self.message_count >= self.max_messages:
                    print(f"Reached maximum message limit of {self.max_messages}")
                    break
                    
                print("Finished processing all data rows, waiting before next poll...")
                time.sleep(10)
                
            except Exception as e:
                print(f"Error during data processing: {e}")
                time.sleep(5)
                continue


def main():
    sheet_id = os.environ.get('SHEET_NAME')
    credentials_json = os.environ.get('GCP_SHEETS_CREDENTIALS_KEY')
    
    if not sheet_id:
        raise ValueError("SHEET_NAME environment variable is required")
    
    if not credentials_json:
        raise ValueError("GCP_SHEETS_CREDENTIALS_KEY environment variable is required")
    
    app = Application(
        consumer_group="google-sheets-source-draft-uyvj",
        auto_create_topics=True
    )
    
    source = GoogleSheetsSource(
        sheet_id=sheet_id,
        credentials_json=credentials_json,
        name="google-sheets-source"
    )
    
    output_topic = app.topic("output")
    
    sdf = app.dataframe(topic=output_topic, source=source)
    sdf.print(metadata=True)
    
    print("Starting Google Sheets source application...")
    app.run()


if __name__ == "__main__":
    main()