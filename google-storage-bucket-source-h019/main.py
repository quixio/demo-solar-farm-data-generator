import os
import json
import io
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd

from quixstreams import Application
from quixstreams.sources import Source
from quixstreams.dataframe import StreamWriter            # helper for type hints

# ---------------------------------------------------------
# logging
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoogleStorageSource(Source):
    """
    Simple Quix Streams source that scans a Google Cloud Storage bucket
    and publishes the content of new CSV / JSON files as pandas
    DataFrames into the pipeline.
    """

    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        credentials_json: str,
        folder_path: str = "/",
        file_format: str = "csv",
        compression: str = "none",
        region: str = "us-central1",
        name: str = "google-storage-source",
    ):
        # quix Source base-class requires a name
        super().__init__(name=name)

        # instance attributes
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path.strip("/")
        self.file_format = file_format.lower()
        self.compression = compression if compression.lower() != "none" else None
        self.region = region

        # will be initialised in configure()
        self.client: Optional[storage.Client] = None
        self.bucket: Optional[storage.bucket.Bucket] = None

        self.processed_files = set()

    # ❶ -----------------------------------------------------
    # Configure is called explicitly in main()
    # -------------------------------------------------------
    def configure(self) -> None:
        logger.info("Configuring GoogleStorageSource …")
        creds_dict = json.loads(self.credentials_json)
        creds = service_account.Credentials.from_service_account_info(creds_dict)
        self.client = storage.Client(project=self.project_id, credentials=creds)
        self.bucket = self.client.bucket(self.bucket_name)
        logger.info("Configuration finished")

    # ❷ -----------------------------------------------------
    # REQUIRED by quixstreams.sources.Source
    # -------------------------------------------------------
    def run(self, writer: "StreamWriter") -> None:
        """
        Scan the bucket for objects, read any new file that matches
        the desired pattern and emit a pandas DataFrame with its data.

        Parameters
        ----------
        writer : quixstreams.dataframe.StreamWriter
            Provided by Quix when the application starts.
            Call writer.write(df) to forward a DataFrame.
        """

        if self.bucket is None:                      # safety net
            self.configure()

        logger.info("Starting source main loop")
        while True:
            # list_blobs returns an iterator – one call per loop keeps it simple
            for blob in self.bucket.list_blobs(prefix=self.folder_path):
                # skip files already sent or wrong format
                if blob.name in self.processed_files:
                    continue
                if not blob.name.lower().endswith(f".{self.file_format}"):
                    continue

                logger.info(f"Processing new file {blob.name}")
                raw_bytes = blob.download_as_bytes()

                # build DataFrame
                if self.file_format == "csv":
                    df = pd.read_csv(
                        io.BytesIO(raw_bytes),
                        compression=self.compression,
                    )
                elif self.file_format == "json":
                    df = pd.read_json(
                        io.BytesIO(raw_bytes),
                        compression=self.compression,
                        lines=True,
                    )
                else:
                    logger.warning(f"Unsupported format {self.file_format}; skipping")
                    continue

                # add some provenance metadata (optional)
                df.attrs["source_file"] = blob.name
                df.attrs["timestamp_utc"] = datetime.utcnow().isoformat()

                # hand over to the Quix pipeline
                writer.write(df)

                # remember we have processed that file
                self.processed_files.add(blob.name)

            # Back-off to avoid hammering the GCS API
            writer.flush()
            writer.sleep(5)   # seconds – change as you like

    # ❸ -----------------------------------------------------
    def cleanup(self) -> None:
        logger.info("Cleaning up GoogleStorageSource resources")
        # nothing to close for GCS, but method required for symmetry


# ------------------------------------------------------------------
# MAIN ENTRY POINT
# ------------------------------------------------------------------
def main():
    try:
        # environment variables
        bucket_name = os.environ["GS_BUCKET"]
        project_id = os.environ["GS_PROJECT_ID"]
        folder_path = os.environ["GS_FOLDER_PATH"]
        file_format = os.environ["GS_FILE_FORMAT"]
        compression = os.environ["GS_FILE_COMPRESSION"]
        credentials_json = os.environ["GS_SECRET_KEY"]
        region = os.environ["GS_REGION"]
        output_topic_name = os.environ["output"]

        logger.info("Starting Google Storage source application")
        logger.info(f"Bucket: {bucket_name}, Project: {project_id}")
        logger.info(f"Folder: {folder_path}, Format: {file_format}")

        # Create Quix Streams application
        app = Application()

        # Create output topic
        output_topic = app.topic(output_topic_name)

        # Build & configure the source
        source = GoogleStorageSource(
            bucket_name=bucket_name,
            project_id=project_id,
            credentials_json=credentials_json,
            folder_path=folder_path,
            file_format=file_format,
            compression=compression,
            region=region,
        )
        source.configure()

        # Create streaming dataframe
        sdf = app.dataframe(topic=output_topic, source=source)

        # Print rows for debug purposes
        sdf.print(metadata=True)

        logger.info("Starting Quix Streams application …")
        app.run()

    except KeyError as e:
        logger.error(f"Missing required environment variable: {e}")
        raise
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        # neat shutdown even on errors
        if "source" in locals():
            source.cleanup()
        logger.info("Application stopped")


if __name__ == "__main__":
    main()