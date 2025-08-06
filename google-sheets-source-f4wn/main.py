# DEPENDENCIES:
# pip install google-auth
# pip install google-auth-oauthlib
# pip install google-auth-httplib2
# pip install google-api-python-client
# END_DEPENDENCIES

import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def test_google_sheets_connection():
    """
    Test connection to Google Sheets and read 10 sample records
    """
    try:
        # Get credentials from environment variable
        credentials_json_str = os.environ.get('GCP_SHEETS_API_KEY_SECRET')
        if not credentials_json_str:
            raise ValueError("GCP_SHEETS_API_KEY_SECRET environment variable not found")
        
        # Parse the JSON credentials
        credentials_dict = json.loads(credentials_json_str)
        
        # Create credentials object
        credentials = Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        
        # Build the service
        service = build('sheets', 'v4', credentials=credentials)
        
        # Get environment variables for the sheet
        document_id = os.environ.get('GOOGLE_SHEETS_DOCUMENT_ID')
        sheet_range = os.environ.get('GOOGLE_SHEETS_RANGE', 'Sheet1!A1:Z')
        
        if not document_id:
            raise ValueError("GOOGLE_SHEETS_DOCUMENT_ID environment variable not found")
        
        print(f"Connecting to Google Sheet: {document_id}")
        print(f"Reading from range: {sheet_range}")
        print("-" * 50)
        
        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=document_id, range=sheet_range).execute()
        values = result.get('values', [])
        
        if not values:
            print("No data found in the sheet.")
            return
        
        print(f"Total rows found: {len(values)}")
        print("Reading first 10 records:")
        print("-" * 50)
        
        # Print exactly 10 records (or fewer if less than 10 exist)
        records_to_show = min(10, len(values))
        
        for i in range(records_to_show):
            row = values[i]
            print(f"Record {i + 1}: {row}")
        
        print("-" * 50)
        print(f"Successfully read {records_to_show} records from Google Sheets")
        
    except HttpError as error:
        print(f"Google Sheets API error: {error}")
        if hasattr(error, 'resp') and hasattr(error.resp, 'status'):
            if error.resp.status == 403:
                print("Access denied. Check if the service account has permission to access the sheet.")
            elif error.resp.status == 404:
                print("Sheet not found. Check the document ID and range.")
        
    except json.JSONDecodeError as error:
        print(f"Error parsing credentials JSON: Invalid JSON format")
        
    except ValueError as error:
        print(f"Configuration error: {error}")
        
    except Exception as error:
        print(f"Unexpected error occurred: {error}")

if __name__ == "__main__":
    test_google_sheets_connection()