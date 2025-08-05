import os
import json
import logging
import tempfile
from datetime import datetime
from typing import Dict, Any, List, Iterator
from io import StringIO

import pandas as pd
from google.cloud import storage
from quixstreams import Application
from quixstreams.sources import Source   # <-- keep this import

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoogleStorageSource(Source):
    """
    Pulls objects from a Google Cloud Storage bucket and publishes each record
    (row or JSON object) to a Quix Streams topic.
    """

    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        credentials_json: str,
        folder_path: str = "/",
        file_format: str = "csv",
        file_compression: str = "none",
        max_messages: int = 100,
        name: str = "gcs-source",
    ):
        # --- Source needs a name ----------------------------------------
        super().__init__(name=name)
        # ----------------------------------------------------------------
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path.lstrip("/")  # normalise
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.max_messages = max_messages

        self.messages_sent = 0
        self.credentials_file: tempfile.NamedTemporaryFile | None = None
        self._client: storage.Client | None = None
        self._bucket: storage.Bucket | None = None

    # ------------------------------------------------------------------ #
    #                            helpers                                 #
    # ------------------------------------------------------------------ #
    def _create_credentials_file(self) -> str:
        """Write the service-account JSON to a temp file and return its path."""
        self.credentials_file = tempfile.NamedTemporaryFile(
            delete=False, suffix=".json"
        )
        self.credentials_file.write(self.credentials_json.encode())
        self.credentials_file.flush()
        return self.credentials_file.name

    def _initialize_client(self) -> None:
        """Lazy-load a GCS client and bucket handle."""
        if self._client:
            return

        creds_path = self._create_credentials_file()
        self._client = storage.Client.from_service_account_json(
            creds_path, project=self.project_id
        )
        self._bucket = self._client.bucket(self.bucket_name)
        logger.info("Connected to bucket %s (project %s)", self.bucket_name, self.project_id)

    def _cleanup(self) -> None:
        """Remove the temporary credentials file if one was created."""
        if self.credentials_file:
            os.unlink(self.credentials_file.name)
            self.credentials_file = None

    def _get_file_blobs(self) -> Iterator[storage.Blob]:
        """Yield blobs under the configured folder path."""
        prefix = f"{self.folder_path.rstrip('/')}/" if self.folder_path else ""
        yield from self._client.list_blobs(self.bucket_name, prefix=prefix)

    # --------------------------- parsing ------------------------------ #
    def _parse_csv_content(self, content: str) -> List[Dict[str, Any]]:
        df = pd.read_csv(StringIO(content))
        return df.to_dict(orient="records")

    def _parse_json_content(self, content: str) -> List[Dict[str, Any]]:
        obj = json.loads(content)
        return obj if isinstance(obj, list) else [obj]

    def _transform_message(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich each record just before publishing."""
        row["_ingested_ts"] = datetime.utcnow().isoformat()
        return row

    # ------------------------------------------------------------------ #
    #                       required abstract method                     #
    # ------------------------------------------------------------------ #
    def run(self) -> None:
        """
        Main loop: iterate over blobs, parse them, publish records, respect
        `max_messages`, and clean up when finished or stopped.
        """
        try:
            self._initialize_client()

            for blob in self._get_file_blobs():
                if not self.running or self.messages_sent >= self.max_messages:
                    break

                content = blob.download_as_text()
                if self.file_format == "csv":
                    records = self._parse_csv_content(content)
                elif self.file_format == "json":
                    records = self._parse_json_content(content)
                else:
                    logger.warning(
                        "Unsupported file format '%s' – skipping blob %s",
                        self.file_format,
                        blob.name,
                    )
                    continue

                for record in records:
                    if not self.running or self.messages_sent >= self.max_messages:
                        break
                    self.produce(value=self._transform_message(record))
                    self.messages_sent += 1

            self.flush()
            logger.info("Produced %d messages", self.messages_sent)
        finally:
            self._cleanup()


def main() -> None:
    try:
        bucket_name = os.environ["GS_BUCKET"]
        project_id = os.environ["GS_PROJECT_ID"]
        credentials_json = os.environ["GS_SECRET_KEY"]
        folder_path = os.environ.get("GS_FOLDER_PATH", "/")
        file_format = os.environ.get("GS_FILE_FORMAT", "csv")
        file_compression = os.environ.get("GS_FILE_COMPRESSION", "none")
        output_topic = os.environ.get("OUTPUT_TOPIC", "output")

        logger.info("Starting Google Storage Bucket source application")
        logger.info("Bucket: %s", bucket_name)
        logger.info("Project ID: %s", project_id)
        logger.info("Folder path: %s", folder_path)
        logger.info("File format: %s", file_format)
        logger.info("Output topic: %s", output_topic)

        app = Application()
        topic = app.topic(output_topic)

        source = GoogleStorageSource(
            bucket_name=bucket_name,
            project_id=project_id,
            credentials_json=credentials_json,
            folder_path=folder_path,
            file_format=file_format,
            file_compression=file_compression,
            max_messages=100,
        )

        sdf = app.dataframe(topic=topic, source=source)
        sdf.print(metadata=True)

        logger.info("Starting Quix Streams application…")
        app.run()

    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception:
        logger.exception("Application error")
        raise


if __name__ == "__main__":
    main()
