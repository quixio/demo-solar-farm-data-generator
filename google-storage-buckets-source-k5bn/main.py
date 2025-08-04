"""
Secure GCS helper for Quix-injected service-account JSON
-------------------------------------------------------

Prerequisites:
  pip install google-cloud-storage

Required environment variables
------------------------------
GS_API_KEY       Either:
                   • the *JSON blob itself* (raw, \n-escaped, or base-64), OR
                   • the NAME of another env-var that holds the blob.
GS_PROJECT_ID    Your GCP project id (falls back to key's project_id).
GS_BUCKET        Bucket to read.
GS_FOLDER_PATH   (optional) Prefix/folder to list, e.g. "raw/"  — leave blank
                 to list the bucket root. **Do not** start with "/".
"""

import os
import json
import base64
import logging
from typing import Dict, Optional

from google.cloud import storage                       # ↩︎ client library docs :contentReference[oaicite:3]{index=3}
from google.oauth2 import service_account              # ↩︎ from_service_account_info docs :contentReference[oaicite:4]{index=4}

logging.basicConfig(level=logging.INFO, format="%(message)s")
LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1.  Load the service-account JSON in the safest way possible  --------------
# ---------------------------------------------------------------------------
def _load_sa_json() -> Dict:
    """
    Return a dict parsed from the service-account key supplied by Quix.

    • Accepts raw JSON, base-64 JSON, or a pointer variable.
    • NEVER echoes the secret back to stdout/stderr.
    """
    raw_val: Optional[str] = os.getenv("GS_API_KEY")
    if raw_val is None:
        raise RuntimeError("GS_API_KEY is not set")

    # Case A ─ GS_API_KEY already *is* the JSON blob
    if raw_val.lstrip().startswith("{"):
        return json.loads(raw_val)

    # Case B ─ it might be base-64-encoded JSON
    try:
        decoded = base64.b64decode(raw_val).decode()
        if decoded.lstrip().startswith("{"):
            return json.loads(decoded)
    except Exception:
        pass  # fall through if base-64 decode fails

    # Case C ─ GS_API_KEY is a pointer to a second env-var
    pointed = os.getenv(raw_val)
    if pointed is None:
        raise RuntimeError(f"Pointer env-var '{raw_val}' is unset or empty")

    try:
        return json.loads(pointed)                     # raw JSON in pointer
    except json.JSONDecodeError:
        # Last chance: Quix sometimes stores JSON with literal '\n'
        return json.loads(pointed.replace("\\n", "\n"))


# ---------------------------------------------------------------------------
# 2.  Build a storage client with explicit credentials -----------------------
# ---------------------------------------------------------------------------
def gcs_client() -> storage.Client:
    sa_info = _load_sa_json()
    creds   = service_account.Credentials.from_service_account_info(sa_info)  # :contentReference[oaicite:5]{index=5}
    project = os.getenv("GS_PROJECT_ID", sa_info.get("project_id"))
    client  = storage.Client(project=project, credentials=creds)
    LOG.info("✓ GCS client initialised for project %s", project)
    return client


# ---------------------------------------------------------------------------
# 3.  Prefix helper: None ⇒ list everything at bucket root ------------------
# ---------------------------------------------------------------------------
def _prefix() -> Optional[str]:
    """
    Build a correct prefix for list_blobs():

    • '' or None  -> None  (lists bucket root – no leading slash)   :contentReference[oaicite:6]{index=6}
    • 'my/folder' -> 'my/folder/'
    """
    path = os.getenv("GS_FOLDER_PATH", "").strip("/")
    return None if path == "" else f"{path}/"


# ---------------------------------------------------------------------------
# 4.  Example: list & preview CSV files -------------------------------------
# ---------------------------------------------------------------------------
def smoke_test(max_preview_chars: int = 500) -> None:
    bucket_name = os.getenv("GS_BUCKET")
    if not bucket_name:
        raise RuntimeError("GS_BUCKET must be set")

    client = gcs_client()
    bucket = client.bucket(bucket_name)
    blobs  = [
        b for b in bucket.list_blobs(prefix=_prefix())   # correct prefix usage :contentReference[oaicite:7]{index=7}
        if b.name.lower().endswith(".csv")
    ]

    if not blobs:
        LOG.warning("No CSV files under gs://%s/%s",
                    bucket_name, (os.getenv('GS_FOLDER_PATH') or ''))
        return

    first = blobs[0]
    LOG.info("Found %d CSVs. Previewing %s (%d bytes)",
             len(blobs), first.name, first.size)
    print(first.download_as_text()[:max_preview_chars])  # safe textual preview


# ---------------------------------------------------------------------------
# 5.  Entry-point -----------------------------------------------------------
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    smoke_test()
