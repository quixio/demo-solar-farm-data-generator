import os
import json
import logging
import tempfile
from datetime import datetime
from typing import Dict, Any
from google.cloud import storage
import pandas as pd
from io import StringIO

from quixstreams import Application
from quixstreams.sources import Source            # <<< use Source
# from quixstreams.sources.base import BaseSource  # <<< remove this import

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoogleStorageSource(Source):                # <<< inherit from Source
    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        credentials_json: str,
        folder_path: str = "/",
        file_format: str = "csv",
        file_compression: str = "none",
        max_messages: int = 100
    ):
        super().__init__()                         # <<< call Source.__init__()
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.max_messages = max_messages
        self.messages_sent = 0
        self.credentials_file = None
        self._client = None
        self._bucket = None

    # ------------------------------------------------------------------
    # all other methods ( _create_credentials_file, _initialize_client,
    # _cleanup, _get_file_blobs, _parse_csv_content, _parse_json_content,
    # _transform_message, run ) stay exactly the same
    # ------------------------------------------------------------------


def main():
    try:
        bucket_name = os.environ.get('GS_BUCKET')
        project_id = os.environ.get('GS_PROJECT_ID')
        credentials_json = os.environ.get('GS_SECRET_KEY')
        folder_path = os.environ.get('GS_FOLDER_PATH', '/')
        file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
        file_compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
        output_topic = os.environ.get('OUTPUT_TOPIC', 'output')  # optional tidy-up

        if not bucket_name:
            raise ValueError("GS_BUCKET environment variable is required")
        if not project_id:
            raise ValueError("GS_PROJECT_ID environment variable is required")
        if not credentials_json:
            raise ValueError("GS_SECRET_KEY environment variable is required")

        logger.info("Starting Google Storage Bucket source application")
        logger.info(f"Bucket: {bucket_name}")
        logger.info(f"Project ID: {project_id}")
        logger.info(f"Folder path: {folder_path}")
        logger.info(f"File format: {file_format}")
        logger.info(f"Output topic: {output_topic}")

        app = Application()
        topic = app.topic(output_topic)

        source = GoogleStorageSource(
            bucket_name=bucket_name,
            project_id=project_id,
            credentials_json=credentials_json,
            folder_path=folder_path,
            file_format=file_format,
            file_compression=file_compression,
            max_messages=100
        )

        sdf = app.dataframe(topic=topic, source=source)
        sdf.print(metadata=True)

        logger.info("Starting Quix Streams application...")
        app.run()

    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        raise


if __name__ == "__main__":
    main()