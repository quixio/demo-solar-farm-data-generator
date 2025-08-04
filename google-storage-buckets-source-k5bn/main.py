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
