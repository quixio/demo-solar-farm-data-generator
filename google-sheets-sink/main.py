# DEPENDENCIES:
# pip install gspread
# pip install oauth2client
# END_DEPENDENCIES

from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch, SinkBackpressureError
import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

class GoogleSheetsSink(BatchingSink):
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._client = None
        self._worksheet = None
        
    def setup(self):
        try:
            # Set up Google Sheets authentication
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            
            # Get credentials from environment variable
            api_key = os.environ.get('GSHEET_API_KEY')
            if not api_key:
                raise ValueError("GSHEET_API_KEY environment variable is required")
            
            # Parse the API key as JSON for service account credentials
            creds_dict = json.loads(api_key)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            
            # Authorize and get the client
            self._client = gspread.authorize(creds)
            
            # Open the spreadsheet
            sheet_id = os.environ.get('GSHEET_ID')
            if not sheet_id:
                raise ValueError("GSHEET_ID environment variable is required")
            
            spreadsheet = self._client.open_by_key(sheet_id)
            
            # Get the worksheet
            sheet_name = os.environ.get('GSHEET_SHEET_NAME', 'Sheet1')
            try:
                self._worksheet = spreadsheet.worksheet(sheet_name)
            except gspread.WorksheetNotFound:
                # Create the worksheet if it doesn't exist
                self._worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
            
            # Set up headers if the sheet is empty
            if not self._worksheet.get_all_values():
                headers = [
                    'panel_id', 'location_id', 'location_name', 'latitude', 'longitude',
                    'timezone', 'power_output', 'unit_power', 'temperature', 'unit_temp',
                    'irradiance', 'unit_irradiance', 'voltage', 'unit_voltage', 'current',
                    'unit_current', 'inverter_status', 'timestamp', 'kafka_timestamp', 'stream_id'
                ]
                self._worksheet.append_row(headers)
                
        except Exception as e:
            if self.on_client_connect_failure:
                self.on_client_connect_failure(e)
            raise e
        
        if self.on_client_connect_success:
            self.on_client_connect_success()

    def write(self, batch: SinkBatch):
        try:
            rows_to_add = []
            
            for item in batch:
                # Parse the JSON value from the message
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Extract solar panel data
                row = [
                    data.get('panel_id', ''),
                    data.get('location_id', ''),
                    data.get('location_name', ''),
                    data.get('latitude', ''),
                    data.get('longitude', ''),
                    data.get('timezone', ''),
                    data.get('power_output', ''),
                    data.get('unit_power', ''),
                    data.get('temperature', ''),
                    data.get('unit_temp', ''),
                    data.get('irradiance', ''),
                    data.get('unit_irradiance', ''),
                    data.get('voltage', ''),
                    data.get('unit_voltage', ''),
                    data.get('current', ''),
                    data.get('unit_current', ''),
                    data.get('inverter_status', ''),
                    data.get('timestamp', ''),
                    item.timestamp,
                    getattr(item, 'stream_id', '')
                ]
                rows_to_add.append(row)
            
            # Batch append all rows
            if rows_to_add:
                self._worksheet.append_rows(rows_to_add)
                
        except Exception as e:
            # Handle rate limiting or other temporary issues
            if "quota" in str(e).lower() or "rate" in str(e).lower():
                raise SinkBackpressureError(
                    retry_after=60.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
            raise e

def main():
    app = Application(
        consumer_group="google_sheets_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Get input topic from environment variable
    input_topic_name = os.environ.get('GSHEET_INPUT')
    if not input_topic_name:
        raise ValueError("GSHEET_INPUT environment variable is required")
    
    input_topic = app.topic(name=input_topic_name)
    sdf = app.dataframe(topic=input_topic)
    
    # Debug: Print raw message structure
    def debug_message(row):
        print(f'Raw message: {row}')
        return row
    
    sdf = sdf.apply(debug_message)
    
    # Initialize the Google Sheets sink
    sheets_sink = GoogleSheetsSink()
    
    # Sink the data
    sdf.sink(sheets_sink)
    
    # Run the application with limited message count
    app.run(count=10, timeout=20)

if __name__ == "__main__":
    main()