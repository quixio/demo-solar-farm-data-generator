#!/usr/bin/env python3
"""
Google Cloud Storage connection tester (no compression logic).

Environment variables recognised:
  GCS_BUCKET_NAME        Bucket to read from                        (default: quix-workflow)
  GCS_PROJECT_ID         GCP project ID                             (default: quix-testing-365012)
  GCS_KEY_FILE_PATH      Path to a JSON key OR the raw JSON itself  (required unless ADC is set up)
  GCS_FOLDER_PATH        Folder/prefix inside the bucket            (default: /)
  GCS_FILE_FORMAT        Expected file extension                    (default: csv)
"""

from __future__ import annotations

import json
import os
from io import StringIO

import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account


# ──────────────────────────────────────────────────────────────────────────────
def build_gcs_client(project_id: str, key_file_or_json: str | None) -> storage.Client:
    """Create a GCS client from a key-file path or raw JSON."""
    if key_file_or_json:
        if os.path.exists(key_file_or_json):
            creds = service_account.Credentials.from_service_account_file(key_file_or_json)
            print(f"Using service-account key file: {key_file_or_json}")
            return storage.Client(credentials=creds, project=project_id)

        try:
            creds_dict = json.loads(key_file_or_json)
            creds = service_account.Credentials.from_service_account_info(creds_dict)
            print("Using in-memory service-account JSON")
            return storage.Client(credentials=creds, project=project_id)
        except json.JSONDecodeError as exc:
            raise ValueError("GCS_KEY_FILE_PATH must be a file path or valid JSON") from exc

    print("Using application-default credentials")
    return storage.Client(project=project_id)


# ──────────────────────────────────────────────────────────────────────────────
def test_gcs_connection() -> None:
    """Connect to GCS, list .csv files, and print a sample of the first one."""
    load_dotenv()

    bucket_name  = os.getenv("GCS_BUCKET_NAME",  "quix-workflow")
    project_id   = os.getenv("GCS_PROJECT_ID",   "quix-testing-365012")
    key_data     = os.getenv("GCS_KEY_FILE_PATH")
    folder_path  = os.getenv("GCS_FOLDER_PATH",  "/")
    file_format  = os.getenv("GCS_FILE_FORMAT",  "csv").lower()

    print("=" * 60)
    print("GOOGLE CLOUD STORAGE CONNECTION TEST (no compression)")
    print("=" * 60)
    print(f"Project ID  : {project_id}")
    print(f"Bucket Name : {bucket_name}")
    print(f"Folder Path : {folder_path}")
    print(f"File Format : {file_format}")
    print("-" * 60)

    client: storage.Client | None = None
    try:
        client = build_gcs_client(project_id, key_data)

        # Bucket check
        bucket = client.bucket(bucket_name)
        if not bucket.exists():
            raise RuntimeError(f"Bucket '{bucket_name}' does not exist or is not accessible")
        print(f"✓ Connected to bucket: {bucket_name}")

        # Prefix handling
        prefix = folder_path.strip("/")
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        print(f"Looking for *.{file_format} files under '{folder_path}'")

        blobs = list(bucket.list_blobs(prefix=prefix if prefix else None))
        matching = [b for b in blobs if b.name.lower().endswith(f".{file_format}")]

        if not matching:
            print("⚠ No matching files found. First 10 blobs:")
            for blob in blobs[:10]:
                print("  •", blob.name)
            return

        # Read first matching file
        target = matching[0]
        print(f"Reading: {target.name}")
        print(f"Size         : {target.size} bytes")
        print(f"Last modified: {target.time_created}")

        text = target.download_as_text(encoding="utf-8")

        print("\n" + "=" * 60)
        print("SAMPLE DATA (first 10 records)")
        print("=" * 60)

        if file_format == "csv":
            df = pd.read_csv(StringIO(text))
            print(f"Rows × Cols : {df.shape}")
            print("Columns     :", list(df.columns))
            print("-" * 60)
            for idx, row in df.head(10).iterrows():
                print(f"Record {idx + 1}")
                for col in df.columns:
                    print(f"  {col}: {row[col]}")
                print("-" * 40)
        else:
            lines = text.splitlines()
            for i, line in enumerate(lines[:10], 1):
                print(f"{i}: {line}")
        print("\n✓ Successfully read sample data")

    except Exception as exc:  # pylint: disable=broad-except
        print("\n❌ Error:", type(exc).__name__, "-", exc)
        print("\nTroubleshooting hints:")
        print(" 1. Check the service-account key")
        print(" 2. Verify IAM permissions ('Storage Object Viewer' at minimum)")
        print(" 3. Confirm bucket name, project ID, and folder path")

    finally:
        if client:
            print("\nConnection cleanup completed.")


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    test_gcs_connection()
