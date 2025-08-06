import os
import json
import time
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from quixstreams import Application
from quixstreams.sources.base import Source


class GoogleSheetsSource(Source):
    def __init__(self, name="google-sheets-source", **kwargs):
        super().__init__(name=name, **kwargs)
        self.sheet_id = os.getenv("GOOGLE_SHEET_ID")
        self.access_token_key = os.getenv("GOOGLE_SHEETS_ACCESS_TOKEN_KEY")
        self.credentials = None
        self.service = None
        self.message_count = 0
        self.max_messages = 100

    def setup(self):
        try:
            creds_json = os.getenv(self.access_token_key)
            if not creds_json:
                raise ValueError("Google Sheets credentials not found")

            creds_dict = json.loads(creds_json)
            self.credentials = Credentials.from_service_account_info(
                creds_dict,
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
            )

            self.service = build("sheets", "v4", credentials=self.credentials)
            
            test_result = (
                self.service.spreadsheets()
                .values()
                .get(spreadsheetId=self.sheet_id, range="Sheet1!A1:C1")
                .execute()
            )
            
            print(f"Successfully connected to Google Sheets: {self.sheet_id}")
            
        except Exception as e:
            print(f"Failed to connect to Google Sheets: {e}")
            raise

    def run(self):
        try:
            self.setup()
            
            sheet_range = "Sheet1!A1:Z"
            result = (
                self.service.spreadsheets()
                .values()
                .get(spreadsheetId=self.sheet_id, range=sheet_range)
                .execute()
            )
            
            values = result.get("values", [])
            if not values:
                print("No data found in the Google Sheet")
                return

            print(f"Found {len(values)} rows in Google Sheets")
            
            header_row = values[0] if values else []
            
            for row_index, row in enumerate(values[1:], start=1):
                if not self.running:
                    print("Source stopped by application")
                    break
                    
                if self.message_count >= self.max_messages:
                    print(f"Reached maximum message limit: {self.max_messages}")
                    break

                try:
                    record = {}
                    for col_index, value in enumerate(row):
                        if col_index < len(header_row):
                            field_name = header_row[col_index] or f"field_{col_index + 1}"
                        else:
                            field_name = f"field_{col_index + 1}"
                        
                        if value.isdigit():
                            record[field_name] = int(value)
                        elif self._is_float(value):
                            record[field_name] = float(value)
                        else:
                            record[field_name] = value
                    
                    record["row_number"] = row_index
                    record["timestamp"] = int(time.time() * 1000)
                    
                    message = self.serialize(
                        key=f"row_{row_index}",
                        value=record
                    )
                    
                    self.produce(
                        key=message.key,
                        value=message.value
                    )
                    
                    self.message_count += 1
                    print(f"Produced message {self.message_count}: row_{row_index}")
                    
                    time.sleep(0.1)
                    
                except Exception as e:
                    print(f"Error processing row {row_index}: {e}")
                    continue

            print(f"Google Sheets source finished. Processed {self.message_count} messages")

        except HttpError as error:
            print(f"Google Sheets API error: {error}")
            if error.resp and error.resp.status == 403:
                print("Access denied. Check that the service account has access to the sheet")
            elif error.resp and error.resp.status == 404:
                print("Sheet not found. Verify the sheet ID")
        except json.JSONDecodeError:
            print("Error parsing credentials JSON")
        except Exception as e:
            print(f"Unexpected error in Google Sheets source: {e}")

    def _is_float(self, value):
        try:
            float(value)
            return '.' in value
        except ValueError:
            return False


def main():
    app = Application(
        consumer_group="google-sheets-source-group",
        auto_create_topics=True
    )
    
    source = GoogleSheetsSource(name="google-sheets-producer")
    
    output_topic = app.topic(name="output")
    
    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)
    sdf.to_topic(output_topic)
    
    print("Starting Google Sheets source application...")
    app.run()


if __name__ == "__main__":
    main()