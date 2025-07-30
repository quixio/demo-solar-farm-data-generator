# DEPENDENCIES:
# pip install google-cloud-storage
# pip install pandas
# pip install python-dotenv
# END_DEPENDENCIES

"""
Google Cloud Storage connection tester.

This version lets the `GCS_KEY_FILE_PATH` environment variable hold **either**
 • a *path* to a JSON key file, **or**
 • the *raw JSON* for a service-account key.

No temporary files are written—credentials are built in memory when raw JSON is
supplied.
"""

from __future__ import annotations

import gzip
import json
import os
from io import StringIO

import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account


# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def build_gcs_client(project_id: str, key_file_or_json: str | None) -> storage.Client:
    """
    Build a Google Cloud Storage client using either a key-file path or raw JSON.

    Args:
        project_id: Google Cloud project ID.
        key_file_or_json: Path to the key file **or** raw JSON string.

    Returns:
        An authenticated `google.cloud.storage.Client`.
    """
    if key_file_or_json:
        if os.path.exists(key_file_or_json):
            print(f"Using service-account key file: {key_file_or_json}")
            creds = service_account.Credentials.from_service_account_file(key_file_or_json)
            return storage.Client(credentials=creds, project=project_id)

        try:
            creds_dict = json.loads(key_file_or_json)
            print("Using in-memory service-account JSON")
            creds = service_account.Credentials.from_service_account_info(creds_dict)
            return storage.Client(credentials=creds, project=project_id)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "GCS_KEY_FILE_PATH must be a file path or valid service-account JSON"
            ) from exc

    # Fall back to Application Default Credentials
    print("Using application-default credentials")
    return storage.Client(project=project_id)


# ────────────────────────────────────────────────────────────────────────────────
# Main test routine
# ────────────────────────────────────────────────────────────────────────────────
def test_gcs_connection() -> None:
    """Connect to a GCS bucket, list matching files, and print a sample."""
    load_dotenv()  # Load variables from .env (if present)

    bucket_name = os.getenv("GCS_BUCKET_NAME", "quix-workflow")
    project_id = os.getenv("GCS_PROJECT_ID", "quix-testing-365012")
    key_data = os.getenv("GCS_KEY_FILE_PATH")  # path **or** raw JSON
    folder_path = os.getenv("GCS_FOLDER_PATH", "/")
    file_format = os.getenv("GCS_FILE_FORMAT", "csv")
    file_compression = os.getenv("GCS_FILE_COMPRESSION", "gzip")

    print("=" * 60)
    print("GOOGLE CLOUD STORAGE CONNECTION TEST")
    print("=" * 60)
    print(f"Project ID      : {project_id}")
    print(f"Bucket Name     : {bucket_name}")
    print(f"Folder Path     : {folder_path}")
    print(f"File Format     : {file_format}")
    print(f"Compression     : {file_compression}")
    print("-" * 60)

    client: storage.Client | None = None
    try:
        # ── Authenticate ────────────────────────────────────────────────────────
        client = build_gcs_client(project_id, key_data)

        # ── Bucket checks ───────────────────────────────────────────────────────
        bucket = client.bucket(bucket_name)
        if not bucket.exists():
            raise RuntimeError(f"Bucket '{bucket_name}' does not exist or is not accessible")
        print(f"✓ Connected to bucket: {bucket_name}")

        # ── Build prefix for listing ───────────────────────────────────────────
        prefix = folder_path.strip("/")
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        print(f"\nLooking for *.{file_format} files under '{folder_path}'")

        blobs = list(bucket.list_blobs(prefix=prefix if prefix else None))

        def is_match(blob_name: str) -> bool:
            lower = blob_name.lower()
            if file_format.lower() not in lower:
                return False
            if file_compression == "gzip":
                return lower.endswith(".gz")
            return not lower.endswith(".gz")

        matching = [blob for blob in blobs if is_match(blob.name)]

        if not matching:
            print("⚠ No matching files found. First 10 blobs:")
            for blob in blobs[:10]:
                print("  •", blob.name)
            return

        # ── Read first matching file ───────────────────────────────────────────
        target = matching[0]
        print(f"\nReading: {target.name}")
        print(f"Size          : {target.size} bytes")
        print(f"Last modified : {target.time_created}")

        data = target.download_as_bytes()
        if file_compression == "gzip":
            print("Decompressing…")
            data = gzip.decompress(data)

        text = data.decode("utf-8")

        print("\n" + "=" * 60)
        print("SAMPLE DATA (first 10 records)")
        print("=" * 60)

        if file_format.lower() == "csv":
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
            print(f"{len(lines)} total lines")
            print("-" * 60)
            for i, line in enumerate(lines[:10], 1):
                snippet = line[:200] + ("…" if len(line) > 200 else "")
                print(f"Record {i}")
                print("  Content:", snippet)
                print("-" * 40)

        print("\n✓ Successfully read sample data")

    except Exception as exc:  # pylint: disable=broad-except
        print("\n❌ Error:")
        print(type(exc).__name__, "-", exc)
        print("\nTroubleshooting hints:")
        print(" 1. Verify the service-account key (JSON or file) is valid")
        print(" 2. Ensure the account has 'Storage Object Viewer' permissions")
        print(" 3. Check bucket name and project ID")
        print(" 4. Confirm folder path, format, and compression settings")

    finally:
        if client:
            print("\nConnection cleanup completed.")


# ────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":