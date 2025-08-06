import os
import json
import time
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from quixstreams import Application
from quixstreams.sources.base import Source


class GoogleSheetsSource(Source):
    def __init__(self, sheet_id, credentials_json, name="google-sheets-source"):
        super().__init__(name=name)
        self.sheet_id = sheet_id
        self.credentials_json = credentials_json
        self.service = None
        self.message_count = 0
        self.max_messages = 100
        
    def setup(self):
        """Setup Google Sheets API connection"""
        try:
            credentials_info = json.loads(self.credentials_json)
            credentials = Credentials.from_service_account_info(
                credentials_info,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            self.service = build('sheets', 'v4', credentials=credentials)
            
            # Test connection by getting sheet metadata
            sheet_metadata = self.service.spreadsheets().get(spreadsheetId=self.sheet_id).execute()
            sheets = sheet_metadata.get('sheets', [])
            
            if not sheets:
                raise ValueError("No sheets found in the spreadsheet")
                
            print(f"Successfully connected to Google Sheets. Found {len(sheets)} sheet(s)")
            
        except Exception as e:
            print(f"Failed to setup Google Sheets connection: {str(e)}")
            raise
    
    def run(self):
        """Main source loop to read data from Google Sheets"""
        if not self.service:
            self.setup()
            
        try:
            # Get sheet metadata to find available sheets
            sheet_metadata = self.service.spreadsheets().get(spreadsheetId=self.sheet_id).execute()
            sheets = sheet_metadata.get('sheets', [])
            
            # Use the first sheet
            first_sheet = sheets[0]['properties']['title']
            print(f"Reading from sheet: {first_sheet}")
            
            # Read all data from the sheet
            range_name = f"{first_sheet}"
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.sheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                print("No data found in the sheet")
                return
                
            # Extract headers from first row
            headers = values[0] if values else []
            data_rows = values[1:] if len(values) > 1 else []
            
            print(f"Found {len(data_rows)} data rows with headers: {headers}")
            
            # Process each row
            for row_index, row in enumerate(data_rows):
                if not self.running or self.message_count >= self.max_messages:
                    break
                    
                # Pad row with empty strings if it's shorter than headers
                padded_row = row + [''] * (len(headers) - len(row))
                
                # Create record from row data
                record = {}
                for i, header in enumerate(headers):
                    record[header] = padded_row[i] if i < len(padded_row) else ''
                
                # Create message with metadata
                message = {
                    "timestamp": time.time(),
                    "sheet_id": self.sheet_id,
                    "sheet_name": first_sheet,
                    "row_index": row_index + 2,  # +2 because we skip header and use 1-based indexing
                    "data": record
                }
                
                # Serialize and produce message
                serialized = self.serialize(
                    key=f"{self.sheet_id}_{row_index}",
                    value=message
                )
                
                self.produce(
                    key=serialized.key,
                    value=serialized.value
                )
                
                self.message_count += 1
                print(f"Produced message {self.message_count}: Row {row_index + 2}")
                
                # Small delay to avoid overwhelming the system
                time.sleep(0.1)
                
            print(f"Finished processing {self.message_count} messages from Google Sheets")
            
        except Exception as e:
            print(f"Error reading from Google Sheets: {str(e)}")
            raise


def main():
    # Get configuration from environment variables
    sheet_id = os.environ.get("SHEET_NAME")
    credentials_json = os.environ.get("GCP_SHEETS_CREDENTIALS_KEY")
    
    if not sheet_id:
        raise ValueError("SHEET_NAME environment variable is required")
    if not credentials_json:
        raise ValueError("GCP_SHEETS_CREDENTIALS_KEY environment variable is required")
    
    # Create Quix Streams application
    app = Application(
        consumer_group="google-sheets-source-draft-q3l9",
        auto_create_topics=True
    )
    
    # Create Google Sheets source
    source = GoogleSheetsSource(
        sheet_id=sheet_id,
        credentials_json=credentials_json,
        name="google-sheets-source"
    )
    
    # Create output topic
    output_topic = app.topic(name="output")
    
    # Create streaming dataframe with source
    sdf = app.dataframe(topic=output_topic, source=source)
    
    # Print messages for monitoring
    sdf.print(metadata=True)
    
    # Run the application
    print("Starting Google Sheets source application...")
    app.run()


if __name__ == "__main__":
    main()