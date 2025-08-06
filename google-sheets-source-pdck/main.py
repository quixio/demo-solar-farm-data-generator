# DEPENDENCIES:
# pip install google-api-python-client
# pip install google-auth
# pip install google-auth-oauthlib
# pip install google-auth-httplib2
# END_DEPENDENCIES

import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def main():
    """
    Test connection to Google Sheets and read 10 sample records.
    """
    try:
        # Get environment variables
        sheet_id = os.environ.get('SHEET_NAME')
        credentials_json_str = os.environ.get('GCP_SHEETS_CREDENTIALS_KEY')
        
        if not sheet_id:
            raise ValueError("SHEET_NAME environment variable is required")
        
        if not credentials_json_str:
            raise ValueError("GCP_SHEETS_CREDENTIALS_KEY environment variable is required")
        
        # Parse credentials JSON
        try:
            credentials_info = json.loads(credentials_json_str)
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON format in GCP_SHEETS_CREDENTIALS_KEY") from e
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        
        # Build the service
        service = build('sheets', 'v4', credentials=credentials)
        
        print(f"Connecting to Google Sheet: {sheet_id}")
        
        # Get sheet metadata to find available sheets
        sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheets = sheet_metadata.get('sheets', [])
        
        if not sheets:
            raise ValueError("No sheets found in the spreadsheet")
        
        # Use the first sheet
        sheet_name = sheets[0]['properties']['title']
        print(f"Reading from sheet: {sheet_name}")
        
        # Define the range to read (first 11 rows to get headers + 10 data rows)
        range_name = f"{sheet_name}!A1:Z11"
        
        # Call the Sheets API
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            print("No data found in the sheet")
            return
        
        print(f"Successfully connected to Google Sheets!")
        print(f"Total rows retrieved: {len(values)}")
        print("-" * 50)
        
        # Print headers if available
        if len(values) > 0:
            headers = values[0]
            print(f"Headers: {headers}")
            print("-" * 50)
        
        # Print up to 10 data rows (excluding header)
        data_rows = values[1:] if len(values) > 1 else values
        rows_to_show = min(10, len(data_rows))
        
        for i, row in enumerate(data_rows[:rows_to_show], 1):
            print(f"Row {i}: {row}")
        
        if len(data_rows) == 0:
            print("No data rows found (only headers present)")
        elif len(data_rows) < 10:
            print(f"Note: Only {len(data_rows)} data rows available (requested 10)")
        
    except HttpError as error:
        if error.resp.status == 403:
            print(f"Permission denied: Make sure the service account has access to the sheet")
        elif error.resp.status == 404:
            print(f"Sheet not found: Check if the SHEET_NAME is correct")
        else:
            print(f"Google Sheets API error: {error}")
    except Exception as e:
        print(f"Connection test failed: {e}")

if __name__ == "__main__":
    main()