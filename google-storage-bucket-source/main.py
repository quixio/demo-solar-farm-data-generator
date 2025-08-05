import os
import json
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from io import BytesIO
from quixstreams import Application
from quixstreams.sources import Source
import logging
import time
from typing import Optional, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoogleStorageBucketSource(Source):
    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        credentials_json: str,
        folder_path: str = "/",
        file_format: str = "csv",
        compression: str = "none",
        region: str = "us-central1",
        name: str = "google-storage-bucket-source",          # <<< -------- expose name
    ):
        # Supply the name to the parent constructor
        super().__init__(name=name)                          # <<< -------- fixed
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path
        self.file_format = file_format.lower()
        self.compression = compression.lower() if compression else "none"
        self.region = region
        self._client = None
        self._bucket = None
        self._processed_files = set()
        self._messages_produced = 0
        self._max_messages = 100
    # -------------------------------------------
    # (rest of the class is unchanged)
    # -------------------------------------------


def main():
    try:
        app = Application()

        output_topic = app.topic(os.environ['output'])

        bucket_name = os.environ['GS_BUCKET']
        project_id = os.environ['GS_PROJECT_ID']
        credentials_json = os.environ['GS_SECRET_KEY']
        folder_path = os.environ.get('GS_FOLDER_PATH', '/')
        file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
        compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
        region = os.environ.get('GS_REGION', 'us-central1')

        logger.info(f"Initializing Google Storage Bucket source for bucket: {bucket_name}")

        source = GoogleStorageBucketSource(
            bucket_name=bucket_name,
            project_id=project_id,
            credentials_json=credentials_json,
            folder_path=folder_path,
            file_format=file_format,
            compression=compression,
            region=region
        )

        sdf = app.dataframe(topic=output_topic, source=source)

        sdf = sdf.update(lambda row: logger.info(
            f"Processing message from file: {row.get('source_file', 'unknown')}") or row)
        sdf.print(metadata=True)

        logger.info("Starting Quix Streams application...")
        app.run()

    except KeyError as e:
        logger.error(f"Missing required environment variable: {e}")
        raise
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise


if __name__ == "__main__":
    main()