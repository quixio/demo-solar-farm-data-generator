# pip install google-cloud-storage
import os, json, base64, logging
from google.cloud import storage
from google.oauth2 import service_account

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

def _load_sa_json() -> dict:
    """
    Returns a dict parsed from a service-account key stored in:
      • GS_API_KEY itself   (raw JSON, \\n-escaped JSON, or Base-64)
      • or an env-var whose name is in GS_API_KEY (pointer style)
    NEVER returns or logs the raw secret on failure.
    """
    raw = os.getenv("GS_API_KEY")
    if not raw:
        raise RuntimeError("GS_API_KEY environment variable is not set")

    # 👉 Case A – GS_API_KEY already IS the JSON
    if raw.lstrip().startswith("{"):
        return json.loads(raw)                                #  ✔ raw JSON

    # 👉 Case B – Base-64-encoded JSON in GS_API_KEY
    try:
        decoded = base64.b64decode(raw).decode()
        if decoded.lstrip().startswith("{"):
            return json.loads(decoded)                        #  ✔ base-64 JSON
    except Exception:
        pass  # fall through

    # 👉 Case C – GS_API_KEY points to another variable
    pointed = os.getenv(raw)
    if not pointed:
        raise RuntimeError(f"Pointer env-var '{raw}' is empty or unset")

    try:
        return json.loads(pointed)                            #  ✔ JSON via pointer
    except json.JSONDecodeError:
        # last gasp – maybe Quix stored with literal \n?
        return json.loads(pointed.replace("\\n", "\n"))

def gcs_client() -> storage.Client:
    sa_info = _load_sa_json()
    creds   = service_account.Credentials.from_service_account_info(sa_info)   # official ctor :contentReference[oaicite:3]{index=3}
    project = os.getenv("GS_PROJECT_ID", sa_info.get("project_id"))
    client  = storage.Client(project=project, credentials=creds)              # accepts explicit creds :contentReference[oaicite:4]{index=4}
    LOG.info("✓ GCS client initialised for project %s", project)
    return client

# ---- Smoke-test ----------------------------------------------------
def smoke_test():
    bucket = os.getenv("GS_BUCKET")
    if not bucket:
        raise RuntimeError("GS_BUCKET must be set for smoke-test")

    prefix = os.getenv("GS_FOLDER_PATH", "").lstrip("/") + "/"
    blobs  = [
        b for b in gcs_client().bucket(bucket).list_blobs(prefix=prefix)
        if b.name.lower().endswith(".csv")
    ]
    if not blobs:
        LOG.warning("No CSV files under gs://%s/%s", bucket, prefix); return
    first = blobs[0]
    LOG.info("Found %d CSVs. Previewing %s (%d bytes)", len(blobs),
             first.name, first.size)
    print(first.download_as_text()[:500])                                     # safe preview

if __name__ == "__main__":
    smoke_test()
