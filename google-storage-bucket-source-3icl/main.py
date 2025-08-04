"""
Google Storage Bucket Source for Quix Streams

This application reads CSV files from a Google Storage Bucket and streams them to a Kafka
(topic) running on Quix Cloud. It demonstrates proper error handling, connection management
and graceful shutdown.

Key changes (to avoid pickling issues when Quix starts each Source in a spawned process):
• Callback handlers (`on_client_connect_success` / `on_client_connect_failure`) are now
  **methods** on the `GCSSource` class so they are picklable.
• The logging helper used in the streaming dataframe is defined at module level.
• No nested functions or lambdas are attached to the Source instance.
"""

import os
import json
import base64
import csv
import logging
import time
from io import StringIO
from typing import Dict, Optional, List, Any
from dataclasses import dataclass

from google.cloud import storage
from google.oauth2 import service_account
from google.api_core import exceptions as gcs_exceptions

from quixstreams import Application
from quixstreams.sources.base import Source

# ----------------------------------------------------------------------------
# Logging configuration
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------
# Dataclass for configuration
# ----------------------------------------------------------------------------
@dataclass
class GCSConfig:
    """Holds Google Cloud Storage connection settings."""

    bucket: str
    project_id: str
    folder_path: str = "/"
    file_format: str = "csv"
    file_compression: str = "none"
    region: str = "us-central1"
    api_key_env: str = "GS_API_KEY"


# ----------------------------------------------------------------------------
# Quix Source implementation
# ----------------------------------------------------------------------------
class GCSSource(Source):
    """A Quix Streams Source that tails CSV files from a GCS bucket."""

    def __init__(self, config: GCSConfig, name: str = "gcs-source") -> None:
        super().__init__(name=name)
        self.config = config
        self.client: Optional[storage.Client] = None
        self.bucket: Optional[storage.Bucket] = None
        self.processed_files: set[str] = set()
        self.message_count = 0
        self.max_messages = 100  # stop after 100 messages (demo safety‑net)

    # ---------------------------------------------------------------------
    # Picklable connection‑status callbacks (called by Quix SourceManager)
    # ---------------------------------------------------------------------
    def on_client_connect_success(self) -> None:  # type: ignore[override]
        logger.info("✓ GCS connection successful")

    def on_client_connect_failure(self, exc: Exception) -> None:  # type: ignore[override]
        logger.error("✗ GCS connection failed: %s", exc)

    # ---------------------------------------------------------------------
    # Lifecycle helpers
    # ---------------------------------------------------------------------
    def setup(self) -> bool:  # noqa: C901 – longer for clarity
        """Initialises the GCS client and checks bucket access."""
        try:
            logger.info("Setting up GCS client …")

            # 1 Load service‑account credentials --------------------------------
            sa_info = self._load_service_account_json()
            credentials = service_account.Credentials.from_service_account_info(sa_info)

            # 2 Create the client ------------------------------------------------
            project_id = self.config.project_id or sa_info.get("project_id")
            self.client = storage.Client(project=project_id, credentials=credentials)
            self.bucket = self.client.bucket(self.config.bucket)

            # 3 Sanity‑check bucket access --------------------------------------
            if not self.bucket.exists():
                logger.error("Bucket '%s' does not exist or is not accessible", self.config.bucket)
                return False

            logger.info("✓ Connected to bucket: %s (project %s)", self.config.bucket, project_id)
            return True

        except Exception as exc:  # noqa: BLE001 – broad to surface any setup issue
            logger.error("Failed to setup GCS client: %s", exc)
            return False

    def _load_service_account_json(self) -> Dict[str, Any]:  # noqa: C901 – explicit logic
        """Loads the service‑account JSON from env (raw, base64 or pointer)."""
        raw_val = os.getenv(self.config.api_key_env)
        if raw_val is None:
            raise RuntimeError(f"{self.config.api_key_env} is not set")

        # 1 direct JSON --------------------------------------------------------
        if raw_val.lstrip().startswith("{"):
            return json.loads(raw_val)

        # 2 base64 encoded JSON -----------------------------------------------
        try:
            decoded = base64.b64decode(raw_val).decode()
            if decoded.lstrip().startswith("{"):
                return json.loads(decoded)
        except Exception:
            pass  # fall through

        # 3 pointer to another env var ----------------------------------------
        pointed_val = os.getenv(raw_val)
        if pointed_val is None:
            raise RuntimeError(f"Pointer variable '{raw_val}' is not set")
        return json.loads(pointed_val.replace("\\n", "\n"))

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------
    def _get_folder_prefix(self) -> Optional[str]:
        path = self.config.folder_path.strip("/")
        return None if path == "" else f"{path}/"

    def _list_csv_files(self) -> List[storage.Blob]:
        try:
            prefix = self._get_folder_prefix()
            blobs = list(self.bucket.list_blobs(prefix=prefix))  # type: ignore[arg-type]
            return [b for b in blobs if b.name.lower().endswith(".csv") and b.size > 0]
        except gcs_exceptions.GoogleCloudError as exc:
            logger.error("Error listing files from GCS: %s", exc)
            return []

    def _process_csv_file(self, blob: storage.Blob) -> int:  # noqa: C901 – longer for clarity
        rows_processed = 0
        try:
            logger.info("Processing %s (%d bytes)", blob.name, blob.size)
            content = blob.download_as_text(encoding="utf‑8")
            reader = csv.DictReader(StringIO(content))

            for row_num, row in enumerate(reader, 1):
                if not self.running:
                    break
                message_key = f"{blob.name}#{row_num}"
                message_value = {
                    "file_name": blob.name,
                    "file_size": blob.size,
                    "row_number": row_num,
                    "data": row,
                    "processed_at": time.time(),
                }
                try:
                    serialised = self.serialize(key=message_key, value=message_value)
                    self.produce(key=serialised.key, value=serialised.value)
                    rows_processed += 1
                    self.message_count += 1
                    if self.message_count >= self.max_messages:
                        logger.info("Hit message cap of %d — stopping", self.max_messages)
                        break
                except Exception as exc:  # noqa: BLE001 – per‑row log then continue
                    logger.error("Row %d failed in %s: %s", row_num, blob.name, exc)
            logger.info("Completed %s: %d rows", blob.name, rows_processed)
        except (gcs_exceptions.GoogleCloudError, UnicodeDecodeError, csv.Error) as exc:
            logger.error("Failed processing %s: %s", blob.name, exc)
        return rows_processed

    # ---------------------------------------------------------------------
    # Main loop
    # ---------------------------------------------------------------------
    def run(self) -> None:  # type: ignore[override]
        logger.info("Starting GCS Source processing …")
        if not self.setup():
            return

        total_rows = 0
        for blob in self._list_csv_files():
            if not self.running or self.message_count >= self.max_messages:
                break
            if blob.name in self.processed_files:
                continue
            total_rows += self._process_csv_file(blob)
            self.processed_files.add(blob.name)

        logger.info("Run finished: %d rows ⇒ %d messages", total_rows, self.message_count)
        self._cleanup()

    def _cleanup(self) -> None:
        logger.debug("Cleaning up GCS client objects")
        self.client = None
        self.bucket = None

    def stop(self) -> None:  # type: ignore[override]
        logger.info("Stopping GCS Source …")
        super().stop()
        self._cleanup()


# ----------------------------------------------------------------------------
# Helper injected into the streaming dataframe (picklable)
# ----------------------------------------------------------------------------

def log_message(value: Dict[str, Any]) -> Dict[str, Any]:
    """Pass‑through that logs each Kafka message."""
    logger.info("Processing message from %s", value.get("file_name", "<unknown>"))
    return value


# ----------------------------------------------------------------------------
# Configuration factory
# ----------------------------------------------------------------------------

def create_gcs_config() -> GCSConfig:
    """Create GCSConfig from environment variables with sane defaults."""
    return GCSConfig(
        bucket=os.getenv("GS_BUCKET", "quix-workflow"),
        project_id=os.getenv("GS_PROJECT_ID", "quix-testing-365012"),
        folder_path=os.getenv("GS_FOLDER_PATH", "/"),
        file_format=os.getenv("GS_FILE_FORMAT", "csv"),
        file_compression=os.getenv("GS_FILE_COMPRESSION", "none"),
        region=os.getenv("GS_REGION", "us-central1"),
        api_key_env="GS_API_KEY",
    )


# ----------------------------------------------------------------------------
# Application entry‑point
# ----------------------------------------------------------------------------

def main() -> None:
    logger.info("Launching Google Storage Bucket Source Application")

    try:
        app = Application(consumer_group="gcs-source-group", auto_offset_reset="latest")

        # Configure output Kafka topic
        output_topic_name = os.getenv("output", "output")
        topic = app.topic(
            name=output_topic_name,
            value_serializer="json",
            key_serializer="string",
        )

        # Instantiate the Source
        config = create_gcs_config()
        source = GCSSource(config=config, name="google-storage-bucket-source-draft-3icl")

        # Build the streaming pipeline
        sdf = app.dataframe(source=source, topic=topic)
        sdf = sdf.update(log_message)

        logger.info("Application parameters: output_topic=%s, bucket=%s, folder=%s", output_topic_name, config.bucket, config.folder_path)

        app.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user — shutting down")
    except Exception as exc:  # noqa: BLE001 – surface root cause
        logger.error("Fatal error: %s", exc)
        raise
    finally:
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    main()
