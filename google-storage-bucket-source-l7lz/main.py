import os
from quix_integrations.sources.google_storage import GoogleCloudStorageSource


# -----------------------------------------------------------------------------
# Callback handlers
# -----------------------------------------------------------------------------
def on_success():
    print("✅ Connected to Google Cloud Storage bucket")


def on_failure(error: Exception):
    print(f"❌ Failed to connect to bucket: {error}")


# -----------------------------------------------------------------------------
# Build the Source object *before* you use it
# -----------------------------------------------------------------------------
# The values are normally provided through environment variables configured
# in the Quix UI.  Replace or hard-code if you are running locally.
bucket_name      = os.environ["google_cloud_storage__bucket"]
credentials_json = os.environ["google_cloud_storage__credentials_json"]
prefix           = os.getenv("google_cloud_storage__prefix", "")   # optional

source = GoogleCloudStorageSource(
    bucket          = bucket_name,
    credentials_json= credentials_json,
    prefix          = prefix
)

# -----------------------------------------------------------------------------
# Register event handlers
# -----------------------------------------------------------------------------
source.on_client_connect_success = on_success
source.on_client_connect_failure = on_failure

# -----------------------------------------------------------------------------
# Start the connector
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Will block and keep the process alive while it streams data
    source.run()