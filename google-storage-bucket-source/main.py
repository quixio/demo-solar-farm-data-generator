"""
This file defines a GoogleStorageBucketSource class that connects to a Google Cloud Storage bucket,
reads files in specified formats (CSV, JSON, or text), processes their content, and produces messages
to a streaming application. It handles authentication, file retrieval, and message serialization.
"""

import os
import json
import csv
import io
import logging
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoogleStorageBucketSource(Source):
    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        credentials_json: str = None,
        folder_path: str = "/",
        file_format: str = "csv",
        file_compression: str = "none",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path.strip("/")
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.client = None
        self.bucket = None
        self.messages_processed = 0
        self.max_messages = 100

    def _build_client(self):
        """Build a google.cloud.storage.Client with flexible authentication."""
        if self.credentials_json:
            logger.info("Using in-memory JSON credentials")
            credentials_info = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(
                credentials_info
            )
            logger.debug("Credentials loaded from JSON")
            return storage.Client(credentials=credentials, project=self.project_id)

        credentials_path = (
            os.getenv("GS_SECRET_PATH") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        )
        if credentials_path and os.path.exists(credentials_path):
            logger.info("Using credentials file at: %s", credentials_path)
            return storage.Client.from_service_account_json(
                credentials_path, project=self.project_id
            )

        logger.error("No valid credentials found. Please set up Application Default Credentials.")
        raise Exception("No valid credentials found.")

    def setup(self):
        """Setup the Google Cloud Storage client and test connection."""
        try:
            logger.info("Building Google Cloud Storage client")
            self.client = self._build_client()
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test connection
            if self.bucket.exists():
                logger.info("Successfully connected to Google Cloud Storage bucket: %s", self.bucket_name)
            else:
                logger.error("Bucket %s does not exist", self.bucket_name)
                raise Exception("Bucket does not exist")

            if hasattr(self, "on_client_connect_success") and callable(self.on_client_connect_success):
                self.on_client_connect_success()

        except Exception as e:
            logger.error("Failed to connect to Google Cloud Storage: %s", str(e))
            if hasattr(self, "on_client_connect_failure") and callable(self.on_client_connect_failure):
                self.on_client_connect_failure(e)
            raise

    def run(self):
        """Main processing loop to read files from Google Storage bucket."""
        try:
            prefix = self.folder_path + '/' if self.folder_path else ''
            logger.info("Listing blobs in bucket with prefix: %s", prefix)
            blobs = list(self.bucket.list_blobs(prefix=prefix))
            
            if not blobs:
                logger.warning("No files found in folder: %s", self.folder_path)
                return

            # Filter files by format if specified
            target_files = []
            if self.file_format and self.file_format != 'none':
                target_files = [blob for blob in blobs if blob.name.lower().endswith(f'.{self.file_format}')]
            else:
                target_files = blobs

            if not target_files:
                logger.warning("No files with format '%s' found", self.file_format)
                return

            logger.info("Found %d files matching format '%s'", len(target_files), self.file_format)

            for blob in target_files:
                if not self.running or self.messages_processed >= self.max_messages:
                    logger.info("Stopping processing as max messages reached or not running")
                    break
                
                logger.info("Processing file: %s", blob.name)
                self._process_file(blob)

            logger.info("Finished processing. Total messages: %d", self.messages_processed)

        except Exception as e:
            logger.error("Error during processing: %s", str(e))
            raise

    def _process_file(self, blob):
        """Process a single file from the bucket."""
        try:
            logger.info("Downloading content from blob: %s", blob.name)
            content = blob.download_as_bytes()
            
            if self.file_format == 'csv':
                logger.info("Processing CSV content from: %s", blob.name)
                self._process_csv_content(content, blob.name)
            elif self.file_format == 'json':
                logger.info("Processing JSON content from: %s", blob.name)
                self._process_json_content(content, blob.name)
            else:
                logger.info("Processing text content from: %s", blob.name)
                self._process_text_content(content, blob.name)
                
        except Exception as e:
            logger.error("Error processing file %s: %s", blob.name, str(e))

    def _process_csv_content(self, content, filename):
        """Process CSV content and produce messages."""
        try:
            content_str = content.decode('utf-8')
            logger.debug("CSV content decoded for file: %s", filename)
            csv_reader = csv.DictReader(io.StringIO(content_str))
            
            for row in csv_reader:
                if not self.running or self.messages_processed >= self.max_messages:
                    logger.info("Stopping CSV processing as max messages reached or not running")
                    break
                
                # Transform the CSV row based on the schema
                message_value = {
                    "timestamp": row.get("timestamp", ""),
                    "hotend_temperature": float(row.get("hotend_temperature", 0.0)),
                    "bed_temperature": float(row.get("bed_temperature", 0.0)),
                    "ambient_temperature": float(row.get("ambient_temperature", 0.0)),
                    "fluctuated_ambient_temperature": float(row.get("fluctuated_ambient_temperature", 0.0)),
                    "source_file": filename
                }
                
                msg = self.serialize(
                    key=filename,
                    value=message_value,
                )
                
                self.produce(
                    key=msg.key,
                    value=msg.value,
                )
                
                self.messages_processed += 1
                logger.debug("Produced message %d from %s", self.messages_processed, filename)
                
        except Exception as e:
            logger.error("Error processing CSV content from %s: %s", filename, str(e))

    def _process_json_content(self, content, filename):
        """Process JSON content and produce messages."""
        try:
            content_str = content.decode('utf-8')
            logger.debug("JSON content decoded for file: %s", filename)
            
            try:
                # Try to parse as JSON array
                json_data = json.loads(content_str)
                if isinstance(json_data, list):
                    logger.info("Processing JSON array from: %s", filename)
                    for item in json_data:
                        if not self.running or self.messages_processed >= self.max_messages:
                            logger.info("Stopping JSON processing as max messages reached or not running")
                            break
                        self._produce_json_message(item, filename)
                else:
                    logger.info("Processing single JSON object from: %s", filename)
                    self._produce_json_message(json_data, filename)
                    
            except json.JSONDecodeError:
                logger.info("Attempting to parse as JSONL from: %s", filename)
                # Try to parse as JSONL (newline-delimited JSON)
                lines = content_str.strip().split('\n')
                for line in lines:
                    if not self.running or self.messages_processed >= self.max_messages:
                        logger.info("Stopping JSONL processing as max messages reached or not running")
                        break
                    if line.strip():
                        try:
                            json_obj = json.loads(line)
                            self._produce_json_message(json_obj, filename)
                        except json.JSONDecodeError:
                            logger.warning("Skipping invalid JSON line in %s", filename)
                            
        except Exception as e:
            logger.error("Error processing JSON content from %s: %s", filename, str(e))

    def _produce_json_message(self, data, filename):
        """Produce a single JSON message."""
        message_value = {**data, "source_file": filename}
        
        msg = self.serialize(
            key=filename,
            value=message_value,
        )
        
        self.produce(
            key=msg.key,
            value=msg.value,
        )
        
        self.messages_processed += 1
        logger.debug("Produced JSON message %d from %s", self.messages_processed, filename)

    def _process_text_content(self, content, filename):
        """Process text content and produce messages."""
        try:
            content_str = content.decode('utf-8')
            logger.debug("Text content decoded for file: %s", filename)
            lines = content_str.split('\n')
            
            for line in lines:
                if not self.running or self.messages_processed >= self.max_messages:
                    logger.info("Stopping text processing as max messages reached or not running")
                    break
                if line.strip():
                    message_value = {
                        "content": line.strip(),
                        "source_file": filename
                    }
                    
                    msg = self.serialize(
                        key=filename,
                        value=message_value,
                    )
                    
                    self.produce(
                        key=msg.key,
                        value=msg.value,
                    )
                    
                    self.messages_processed += 1
                    logger.debug("Produced text message %d from %s", self.messages_processed, filename)
                    
        except UnicodeDecodeError:
            logger.warning("Binary content detected in %s, skipping", filename)
        except Exception as e:
            logger.error("Error processing text content from %s: %s", filename, str(e))


def main():
    app = Application()

    # Configuration from environment variables
    bucket_name = os.getenv("GS_BUCKET", "quix-workflow")
    project_id = os.getenv("GS_PROJECT_ID", "quix-testing-365012")
    folder_path = os.getenv("GS_FOLDER_PATH", "/")
    file_format = os.getenv("GS_FILE_FORMAT", "csv")
    file_compression = os.getenv("GS_FILE_COMPRESSION", "none")
    
    # Authentication - optional now
    credentials_json = os.getenv("GS_SECRET_KEY")

    # Create output topic
    output_topic = app.topic(os.getenv("output", "output"))

    # Create the Google Storage source
    source = GoogleStorageBucketSource(
        name="google-storage-bucket-source",
        bucket_name=bucket_name,
        project_id=project_id,
        credentials_json=credentials_json,
        folder_path=folder_path,
        file_format=file_format,
        file_compression=file_compression,
    )

    # Setup the source before running
    source.setup()

    # Create streaming dataframe
    sdf = app.dataframe(topic=output_topic, source=source)
    sdf.print(metadata=True)

    logger.info("Starting Google Storage Bucket source application")
    app.run()


if __name__ == "__main__":
    main()
