# pip install google-cloud-storage
import os, json, base64, logging
from google.cloud import storage
from google.oauth2 import service_account

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

# ------------------------------------------------------------------
# 1. Pull JSON string from the env-var that Quix wired up
# ------------------------------------------------------------------
def _load_sa_json() -> dict:
    """
    GS_API_KEY points to another env-var whose *value* is the JSON blob.
    Handles raw JSON, \\n-escaped JSON, or base-64-encoded JSON.
    """
    pointer = os.getenv("GS_API_KEY")      # e.g. "GCLOUD_PK_JSON"
    if not pointer:
        raise RuntimeError("GS_API_KEY is unset")

    blob = os.getenv(pointer)
    if not blob:
        raise RuntimeError(f"Env-var {pointer} is unset or empty")

    # --- try raw JSON first ---
    try:
        return json.loads(blob)
    except json.JSONDecodeError:
        pass

    # --- maybe Quix stored base-64? ---
    try:
        decoded = base64.b64decode(blob).decode()
        return json.loads(decoded)
    except Exception:
        # --- or it’s a string with literal '\n' escapes ---
        fixed = blob.replace("\\n", "\n")
        return json.loads(fixed)

# ------------------------------------------------------------------
# 2. Build a Credential and Storage client
# ------------------------------------------------------------------
def gcs_client() -> storage.Client:
    sa_info = _load_sa_json()
    creds    = service_account.Credentials.from_service_account_info(sa_info)   # uses helper ctor :contentReference[oaicite:0]{index=0}
    project  = os.getenv("GS_PROJECT_ID", sa_info.get("project_id"))
    client   = storage.Client(project=project, credentials=creds)
    log.info("✓ GCS client ready (project: %s)", project)
    return client

# ------------------------------------------------------------------
# 3. Tiny smoke-test: list *.csv under GS_FOLDER_PATH
# ------------------------------------------------------------------
def smoke_test():
    bucket_name = os.getenv("GS_BUCKET")
    prefix      = os.getenv("GS_FOLDER_PATH", "").lstrip("/") + "/"
    if not bucket_name:
        raise RuntimeError("GS_BUCKET is required")

    client = gcs_client()
    bucket = client.bucket(bucket_name)
    blobs  = [b for b in bucket.list_blobs(prefix=prefix) if b.name.lower().endswith(".csv")]
    if not blobs:
        log.warning("No CSV files under gs://%s/%s", bucket_name, prefix)
        return
    first = blobs[0]
    log.info("Found %d CSVs, previewing %s (%d bytes)", len(blobs), first.name, first.size)
    print(first.download_as_text()[:500])                                      # Blob.download_as_text :contentReference[oaicite:1]{index=1}

if __name__ == "__main__":
    smoke_test()
