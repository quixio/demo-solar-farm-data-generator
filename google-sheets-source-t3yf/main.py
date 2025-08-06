# DEPENDENCIES:
# pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
# END_DEPENDENCIES

import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


DEFAULT_SHEET_NAME = "Sheet1"
DEFAULT_CELL_RANGE = "A1:Z"        # read first 26 columns

def _build_valid_range(raw: str | None) -> str:
    """
    Make sure the range string sent to the Sheets API is valid.
    Acceptable user inputs:
        Sheet1!A1:B5   (already valid)
        Sheet1         (no '!' → treat as sheet, read default cells)
        A1:B5          (no '!' but starts with column char → treat as cells)
        "" / None      (use defaults)
    """
    if not raw:  # empty → completely default
        return f"{DEFAULT_SHEET_NAME}!{DEFAULT_CELL_RANGE}"

    # If the user forgot the '!'
    if "!" not in raw:
        # Starts with a letter → looks like a pure cell range
        if raw[0].isalpha():
            return f"{DEFAULT_SHEET_NAME}!{raw}"
        # Otherwise treat it as a sheet name
        return f"{raw}!{DEFAULT_CELL_RANGE}"

    # Already contains '!' → assume it's valid
    return raw


def test_google_sheets_connection() -> None:
    """
    Test connection to Google Sheets and print up to 10 rows.
    """
    try:
        # ------------------------------------------------------------------
        # 1. Credentials
        # ------------------------------------------------------------------
        creds_json = os.getenv("GCP_SHEETS_API_KEY_SECRET")
        if not creds_json:
            raise ValueError("GCP_SHEETS_API_KEY_SECRET environment variable not found")

        creds_dict = json.loads(creds_json)
        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )

        # ------------------------------------------------------------------
        # 2. Parameters (document id & range)
        # ------------------------------------------------------------------
        document_id = os.getenv("GOOGLE_SHEETS_DOCUMENT_ID")
        if not document_id:
            raise ValueError("GOOGLE_SHEETS_DOCUMENT_ID environment variable not found")

        raw_range = os.getenv("GOOGLE_SHEETS_RANGE")
        sheet_range = _build_valid_range(raw_range)

        print(f"Connecting to Google Sheet: {document_id}")
        print(f"Reading from range: {sheet_range}")
        print("-" * 50)

        # ------------------------------------------------------------------
        # 3. Call Sheets API
        # ------------------------------------------------------------------
        service = build("sheets", "v4", credentials=credentials)
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=document_id, range=sheet_range)
            .execute()
        )
        values = result.get("values", [])

        if not values:
            print("No data found in the sheet.")
            return

        print(f"Total rows found: {len(values)}")
        print("Reading first 10 records:")
        print("-" * 50)

        for i, row in enumerate(values[:10], start=1):
            print(f"Record {i}: {row}")

        print("-" * 50)
        print(f"Successfully read {min(10, len(values))} record(s) from Google Sheets")

    # ----------------------------------------------------------------------
    # 4. Error handling
    # ----------------------------------------------------------------------
    except HttpError as error:
        print(f"Google Sheets API error: {error}")
        if error.resp is not None:
            if error.resp.status == 403:
                print("Access denied. Check that the service account is shared on the sheet.")
            elif error.resp.status == 404:
                print("Sheet not found. Verify the document ID and range.")
    except json.JSONDecodeError:
        print("Error parsing credentials JSON: invalid JSON format")
    except ValueError as err:
        print(f"Configuration error: {err}")
    except Exception as err:
        print(f"Unexpected error occurred: {err}")


if __name__ == "__main__":
    test_google_sheets_connection()