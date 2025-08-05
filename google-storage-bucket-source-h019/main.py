import os
import json
import io
import logging
from datetime import datetime
from typing import Dict, Any

from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd

from quixstreams import Application
from quixstreams.sources import Source

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageSource(Source):
    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        credentials_json: str,
        folder_path: str = "/",
        file_format: str = "csv",
        compression: str = "none",
        region: str = "us-central1",
        name: str = "google-storage-source"        # <―― added
    ):
        # Pass the required name argument to the base Source class
        super().__init__(name=name)                # <―― fixed
        # original attributes
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path.strip("/")
        self.file_format = file_format.lower()
        self.compression = compression
        self.region = region
        self.client = None
        self.bucket = None
        self.processed_files = set()
        self.message_count = 0
        self.max_messages = 100  # Stop condition for testing
    # --------------- rest of the class unchanged ------------------
    # ...

def main():
    try:
        # Get environment variables
        bucket_name = os.environ['GS_BUCKET']
        project_id = os.environ['GS_PROJECT_ID']
        folder_path = os.environ['GS_FOLDER_PATH']
        file_format = os.environ['GS_FILE_FORMAT']
        compression = os.environ['GS_FILE_COMPRESSION']
        credentials_json = os.environ['GS_SECRET_KEY']
        region = os.environ['GS_REGION']
        output_topic_name = os.environ['output']

        logger.info(f"Starting Google Storage source application")
        logger.info(f"Bucket: {bucket_name}, Project: {project_id}")
        logger.info(f"Folder: {folder_path}, Format: {file_format}")

        # Create Quix Streams application
        app = Application()

        # Create output topic
        output_topic = app.topic(output_topic_name)

        # Create Google Storage source
        source = GoogleStorageSource(
            bucket_name=bucket_name,
            project_id=project_id,
            credentials_json=credentials_json,
            folder_path=folder_path,
            file_format=file_format,
            compression=compression,
            region=region,
            name="google-storage-source"           # <―― explicit but optional
        )

        # Configure source
        source.configure()

        # Create streaming dataframe
        sdf = app.dataframe(topic=output_topic, source=source)

        # Add print statement to see messages being produced
        sdf.print(metadata=True)

        logger.info("Starting Quix Streams application...")

        # Run the application
        try:
            app.run()
        finally:
            # Cleanup
            source.cleanup()
            logger.info("Application stopped and resources cleaned up")

    except KeyError as e:
        logger.error(f"Missing required environment variable: {e}")
        raise
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise

if __name__ == "__main__":
    main()