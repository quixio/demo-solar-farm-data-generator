"""
Secure GCS helper for Quix-injected service-account JSON
Preview now shows first 100 CSV rows instead of first 500 chars.

pip install google-cloud-storage
"""

import os
import json
import base64
import csv
import logging
from io import StringIO
from typing import Dict, Optional

from google.cloud import storage
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(message)s")
LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Load the service-account JSON safely
# ---------------------------------------------------------------------------
def _load_sa_json() -> Dict:
    raw_val: Optional[str] = os.getenv("GS_API_KEY")
    if raw_val is None:
        raise RuntimeError("GS_API_KEY is not set")

    # A. GS_API_KEY already contains raw JSON
    if raw_val.lstrip().startswith("{"):
        return json.loads(raw_val)

    # B. Base-64-encoded JSON
    try:
        decoded = base64.b64decode(raw_val).decode()
        if decoded.lstrip().startswith("{"):
            return json.loads(decoded)
    except Exception:
        pass

    # C. Pointer to another env-var
    pointed = os.getenv(raw_val)
    if pointed is None:
        raise RuntimeError(f"Pointer env-var '{raw_val}' is unset or empty")

    try:
        return json.loads(pointed)
    except json.JSONDecodeError:
        return json.loads(pointed.replace("\\n", "\n"))


# ---------------------------------------------------------------------------
# 2. Build an authenticated Storage client
# ---------------------------------------------------------------------------
def gcs_client() -> storage.Client:
    sa_info = _load_sa_json()
    creds   = service_account.Credentials.from_service_account_info(sa_info)
    project = os.getenv("GS_PROJECT_ID", sa_info.get("project_id"))
    client  = storage.Client(project=project, credentials=creds)
    LOG.info("✓ GCS client initialised for project %s", project)
    return client


# ---------------------------------------------------------------------------
# 3. Prefix helper
# ---------------------------------------------------------------------------
def _prefix() -> Optional[str]:
    path = os.getenv("GS_FOLDER_PATH", "").strip("/")
    return None if path == "" else f"{path}/"


# ---------------------------------------------------------------------------
# 4. Smoke-test: list CSVs and preview first 100 rows
# ---------------------------------------------------------------------------
def smoke_test(max_rows: int = 100) -> None:
    bucket_name = os.getenv("GS_BUCKET")
    if not bucket_name:
        raise RuntimeError("GS_BUCKET must be set")

    client = gcs_client()
    bucket = client.bucket(bucket_name)

    blobs = [
        b for b in bucket.list_blobs(prefix=_prefix())
        if b.name.lower().endswith(".csv")
    ]

    if not blobs:
        LOG.warning("No CSV files under gs://%s/%s",
                    bucket_name, (os.getenv('GS_FOLDER_PATH') or ''))
        return

    blob = blobs[0]
    LOG.info("Found %d CSVs. Previewing %s (%d bytes)",
             len(blobs), blob.name, blob.size)

    # Download entire file as text (UTF-8)
    data = blob.download_as_text()

    # Parse CSV and print up to `max_rows` rows
    reader = csv.reader(StringIO(data))
    headers = next(reader, None)
    print("—" * 60)
    print(f"Columns: {headers}")
    print(f"Showing first {max_rows} rows\n")

    for idx, row in enumerate(reader, 1):
        if idx > max_rows:
            break
        print(f"{idx:>3}: {row}")
    print("—" * 60)


# ---------------------------------------------------------------------------
# 5. Entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    smoke_test()