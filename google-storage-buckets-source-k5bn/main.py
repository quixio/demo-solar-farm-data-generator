# pip install google-cloud-storage google-cloud-secret-manager
import os
import json
import logging
from google.cloud import secretmanager, storage
from google.oauth2 import service_account

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

# --------------------------------------------------------------------
# 1. Fetch the service-account key JSON that lives in Secret Manager
# --------------------------------------------------------------------
def _fetch_key_from_secret_manager() -> dict:
    """
    Reads the latest version of the secret whose name is given in
    the GS_API_KEY environment variable and returns it as a dict.
    """
    project_id = os.getenv("GS_PROJECT_ID")
    secret_id  = os.getenv("GS_API_KEY")          # ← e.g. "GCLOUD_PK_JSON"
    if not (project_id and secret_id):
        raise RuntimeError("GS_PROJECT_ID and GS_API_KEY must be set")

    client = secretmanager.SecretManagerServiceClient()              # :contentReference[oaicite:0]{index=0}
    name   = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    payload = client.access_secret_version(request={"name": name}).payload.data.decode("utf-8")  # :contentReference[oaicite:1]{index=1}
    log.info("✓ Retrieved service-account key from Secret Manager")
    return json.loads(payload)

# --------------------------------------------------------------------
# 2. Build a credential object from that JSON
# --------------------------------------------------------------------
def _build_credentials() -> service_account.Credentials:
    key_dict = _fetch_key_from_secret_manager()
    creds    = service_account.Credentials.from_service_account_info(key_dict)   # :contentReference[oaicite:2]{index=2}
    return creds

# --------------------------------------------------------------------
# 3. Make a Storage client that uses those credentials
# --------------------------------------------------------------------
def gcs_client() -> storage.Client:
    """
    Returns an authenticated google.cloud.storage.Client
    """
    creds  = _build_credentials()
    proj   = os.getenv("GS_PROJECT_ID")
    client = storage.Client(project=proj, credentials=creds)          # :contentReference[oaicite:3]{index=3}
    log.info("✓ Connected to Google Cloud Storage")
    return client

# --------------------------------------------------------------------
# 4. Example utility – list *.csv under a prefix and print a preview
# --------------------------------------------------------------------
def sample_list_and_preview():
    bucket_name = os.getenv("GS_BUCKET")
    prefix      = os.getenv("GS_FOLDER_PATH", "").lstrip("/") + "/"
    if not bucket_name:
        raise RuntimeError("GS_BUCKET must be set")

    client = gcs_client()
    bucket = client.bucket(bucket_name)
    blobs  = [b for b in bucket.list_blobs(prefix=prefix)            # :contentReference[oaicite:4]{index=4}
              if b.name.lower().endswith(".csv")]

    if not blobs:
        log.warning("No CSV files found under gs://%s/%s", bucket_name, prefix)
        return

    first = blobs[0]
    log.info("✓ Previewing %s (%d bytes)", first.name, first.size)
    print(first.download_as_text()[:500])                            # :contentReference[oaicite:5]{index=5}

if __name__ == "__main__":
    sample_list_and_preview()
