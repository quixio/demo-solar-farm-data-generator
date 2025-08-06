# DEPENDENCIES:
# pip install google-api-python-client
# pip install google-auth
# pip install google-auth-oauthlib
# pip install google-auth-httplib2
# END_DEPENDENCIES

import os
import json
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

def test_google_sheets_connection():
    """
    Test connection to Google Sheets and read 10 sample records.
    """
    try:
        # Get credentials from environment variable
        credentials_json = os.environ.get('GCP_SHEETS_CREDENTIALS_KEY')
        if not credentials_json:
            raise ValueError("GCP_SHEETS_CREDENTIALS_KEY environment variable not found")
        
        # Get sheet ID from environment variable
        sheet_id = os.environ.get('SHEET_NAME')
        if not sheet_id:
            raise ValueError("SHEET_NAME environment variable not found")
        
        # Parse the credentials JSON
        try:
            credentials_info = json.loads(credentials_json)
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON format in credentials")
        
        # Create credentials object
        credentials = Credentials.from_service_account_info(
            credentials_info,
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        
        # Build the Sheets API service
        service = build('sheets', 'v4', credentials=credentials)
        
        print(f"Successfully connected to Google Sheets API")
        print(f"Reading from sheet ID: {sheet_id}")
        
        # Get sheet metadata to find available sheets
        sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheets = sheet_metadata.get('sheets', [])
        
        if not sheets:
            print("No sheets found in the spreadsheet")
            return
        
        # Use the first sheet
        first_sheet = sheets[0]['properties']['title']
        print(f"Reading from sheet: {first_sheet}")
        
        # Read data from the first sheet (first 10 rows)
        range_name = f"{first_sheet}!A1:Z10"  # Read first 10 rows, columns A to Z
        
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            print("No data found in the specified range")
            return
        
        print(f"\nSuccessfully read {len(values)} rows from Google Sheets:")
        print("-" * 50)
        
        # Print each row with row number
        for i, row in enumerate(values, 1):
            print(f"Row {i}: {row}")
        
        print("-" * 50)
        print(f"Connection test completed successfully!")
        
    except Exception as e:
        print(f"Error connecting to Google Sheets: {str(e)}")
        raise

if __name__ == "__main__":
    test_google_sheets_connection()