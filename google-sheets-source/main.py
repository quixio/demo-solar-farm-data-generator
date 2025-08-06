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
    """Test connection to Google Sheets and read sample data."""
    
    try:
        # Get environment variables
        spreadsheet_name = os.environ.get('SPREADSHEET_NAME')
        credentials_json_str = os.environ.get('GOOGLE_SHEETS_CREDS')
        sheet_name = os.environ.get('SHEET_NAME', 'Sheet1')
        api_version = os.environ.get('GOOGLE_SHEETS_API_VERSION', 'v4')
        
        # Validate required environment variables
        if not spreadsheet_name:
            raise ValueError("SPREADSHEET_NAME environment variable is required")
        if not credentials_json_str:
            raise ValueError("GOOGLE_SHEETS_CREDS environment variable is required")
        
        print(f"Connecting to Google Sheets...")
        print(f"Spreadsheet: {spreadsheet_name}")
        print(f"Sheet: {sheet_name}")
        print(f"API Version: {api_version}")
        
        # Parse credentials JSON
        try:
            credentials_info = json.loads(credentials_json_str)
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON in GOOGLE_SHEETS_CREDS") from e
        
        # Create credentials object
        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        credentials = Credentials.from_service_account_info(
            credentials_info, 
            scopes=scopes
        )
        
        # Build the service
        service = build('sheets', api_version, credentials=credentials)
        
        # Get all spreadsheets to find the one we want
        # First, we need to get the spreadsheet ID by name
        print(f"Searching for spreadsheet: {spreadsheet_name}")
        
        # Try to open by name (assuming it's shared with the service account)
        # We'll use the spreadsheet name as ID first, but if that fails,
        # we'll need to search through available spreadsheets
        
        # Define the range to read (first 10 rows)
        range_name = f"{sheet_name}!A1:Z10"
        
        try:
            # Try to get data using spreadsheet name as ID
            result = service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_name,
                range=range_name
            ).execute()
        except Exception:
            # If that fails, the spreadsheet_name might not be the ID
            # In a real scenario, you'd need the actual spreadsheet ID
            raise ValueError(f"Could not access spreadsheet '{spreadsheet_name}'. Make sure it exists and is shared with the service account.")
        
        values = result.get('values', [])
        
        if not values:
            print("No data found in the specified range.")
            return
        
        print(f"\nSuccessfully connected to Google Sheets!")
        print(f"Found {len(values)} rows of data")
        print("\nSample data (first 10 rows):")
        print("-" * 50)
        
        # Print each row
        for i, row in enumerate(values):
            print(f"Row {i + 1}: {row}")
            
        print("-" * 50)
        print(f"Connection test completed successfully!")
        
    except Exception as e:
        print(f"Error connecting to Google Sheets: {str(e)}")
        raise

def main():
    """Main function to run the connection test."""
    test_google_sheets_connection()

if __name__ == "__main__":
    main()