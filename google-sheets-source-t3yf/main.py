# DEPENDENCIES:
# pip install google-api-python-client
# pip install google-auth
# pip install google-auth-oauthlib
# pip install google-auth-httplib2
# END_DEPENDENCIES

import os
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def main():
    """
    Connect to Google Sheets and read 10 sample records for testing.
    """
    try:
        # Get environment variables
        sheet_id = os.environ.get('GOOGLE_SHEET_ID')
        access_token_key = os.environ.get('GOOGLE_SHEETS_ACCESS_TOKEN_KEY')
        refresh_token_key = os.environ.get('GOOGLE_SHEETS_REFRESH_TOKEN_KEY') 
        client_id_key = os.environ.get('GOOGLE_API_CLIENT_ID_KEY')
        client_secret_key = os.environ.get('GOOGLE_API_CLIENT_SECRET_KEY')
        
        # Validate required environment variables
        if not sheet_id:
            raise ValueError("GOOGLE_SHEET_ID environment variable is required")
        
        print(f"Connecting to Google Sheet ID: {sheet_id}")
        
        # Get credentials from environment variables
        access_token = os.environ.get(access_token_key) if access_token_key else None
        refresh_token = os.environ.get(refresh_token_key) if refresh_token_key else None
        client_id = os.environ.get(client_id_key) if client_id_key else None
        client_secret = os.environ.get(client_secret_key) if client_secret_key else None
        
        # Try to create credentials using OAuth tokens if available
        creds = None
        if access_token and refresh_token and client_id and client_secret:
            print("Using OAuth credentials...")
            creds = Credentials(
                token=access_token,
                refresh_token=refresh_token,
                client_id=client_id,
                client_secret=client_secret,
                token_uri='https://oauth2.googleapis.com/token'
            )
        else:
            # Try service account credentials from JSON
            service_account_key = os.environ.get('GOOGLE_SHEETS_ACCESS_TOKEN_KEY')
            if service_account_key:
                try:
                    service_account_json = os.environ.get(service_account_key)
                    if service_account_json:
                        print("Using service account credentials...")
                        from google.oauth2 import service_account
                        credentials_info = json.loads(service_account_json)
                        creds = service_account.Credentials.from_service_account_info(
                            credentials_info,
                            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
                        )
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Failed to parse service account JSON: {str(e)}")
        
        if not creds:
            raise ValueError("No valid credentials found. Please provide either OAuth tokens or service account JSON.")
        
        # Build the Google Sheets service
        print("Building Google Sheets service...")
        service = build('sheets', 'v4', credentials=creds)
        
        # Get sheet metadata to find available sheets
        print("Getting sheet metadata...")
        sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheets = sheet_metadata.get('sheets', [])
        
        if not sheets:
            raise ValueError("No sheets found in the spreadsheet")
        
        # Use the first sheet
        first_sheet = sheets[0]
        sheet_name = first_sheet['properties']['title']
        print(f"Reading from sheet: {sheet_name}")
        
        # Define the range to read (first 11 rows to get headers + 10 data rows)
        range_name = f'{sheet_name}!A1:Z11'
        
        # Read data from the sheet
        print("Reading data from Google Sheets...")
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            print("No data found in the sheet")
            return
        
        print(f"Successfully retrieved {len(values)} rows from Google Sheets")
        print("=" * 50)
        
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
            print(f"Note: Only {len(data_rows)} data rows available (less than requested 10)")
        
        print("=" * 50)
        print("Connection test completed successfully!")
        
    except HttpError as error:
        print(f"Google Sheets API error: {error}")
        if error.resp.status == 403:
            print("Access denied. Please check your credentials and permissions.")
        elif error.resp.status == 404:
            print("Sheet not found. Please check the GOOGLE_SHEET_ID.")
    except json.JSONDecodeError as error:
        print(f"JSON parsing error: {error}")
        print("Please check that your credentials are valid JSON.")
    except Exception as error:
        print(f"Unexpected error: {error}")
    finally:
        print("Connection test finished.")

if __name__ == "__main__":
    main()