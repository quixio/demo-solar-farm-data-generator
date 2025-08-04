import os
import json
import logging
from typing import Iterator

from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_PAYLOAD = 900_000

def iter_chunks(lines: Iterator[str], target_bytes: int = MAX_PAYLOAD):
    """
    Yield blocks of lines that are <= target_bytes (utf-8 encoded).
    """
    chunk, size = [], 0
    for line in lines:
        encoded = line.encode("utf-8")
        if size + len(encoded) > target_bytes and chunk:
            yield "".join(chunk)
            chunk, size = [], 0
        chunk.append(line)
        size += len(encoded)

    if chunk:
        yield "".join(chunk)

class GoogleStorageSource(Source):
    def __init__(
        self,
        bucket_name,
        credentials_json,
        project_id,
        folder_path="/",
        file_format="csv",
        compression="none",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.credentials_json = credentials_json
        self.project_id = project_id
        self.folder_path = folder_path.lstrip("/")
        self.file_format = file_format
        self.compression = compression
        self.client = None
        self.bucket = None
        self.processed_count = 0

    def setup(self):
        try:
            if not self.credentials_json:
                raise ValueError("Credentials not found in environment variable")

            credentials_dict = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(
                credentials_dict
            )

            self.client = storage.Client(credentials=credentials, project=self.project_id)
            self.bucket = self.client.bucket(self.bucket_name)
            self.bucket.reload()
            logger.info(f"Successfully connected to Google Storage bucket: {self.bucket_name}")

            if hasattr(self, 'on_client_connect_success') and callable(self.on_client_connect_success):
                self.on_client_connect_success()

        except Exception as exc:
            logger.error(f"Failed to connect to Google Storage")
            if hasattr(self, 'on_client_connect_failure') and callable(self.on_client_connect_failure):
                self.on_client_connect_failure(exc)
            raise

    def run(self):
        self.setup()
        try:
            blobs = self.bucket.list_blobs(prefix=self.folder_path)
            for blob in blobs:
                if not self.running:
                    break
                if blob.name.endswith("/"):
                    continue

                if self.processed_count >= 100:
                    logger.info("Processed 100 messages, stopping for testing")
                    break

                try:
                    self._process_file(blob)
                except Exception as exc:
                    logger.error(f"Error processing file {blob.name}")
                    continue

            logger.info(f"Finished processing. Total chunks produced: {self.processed_count}")

        except Exception as exc:
            logger.error(f"Error during processing")
            raise

    def _process_file(self, blob):
        """
        Read the blob in streaming mode and send it in <= 1 MB chunks.
        """
        with blob.open("r") as fp:
            header = {
                "file_name": blob.name,
                "file_size": blob.size,
                "content_type": blob.content_type,
                "created_time": blob.time_created.isoformat() if blob.time_created else None,
                "updated_time": blob.updated.isoformat() if blob.updated else None,
                "bucket_name": self.bucket_name,
                "file_format": self.file_format,
                "compression": self.compression,
                "record_type": "header",
            }
            self._produce_message(blob.name, header)

            chunk_count = 0
            for idx, chunk_str in enumerate(iter_chunks(fp)):
                if not self.running:
                    break
                    
                chunk_payload = {
                    "file_name": blob.name,
                    "chunk_index": idx,
                    "record_type": "data",
                    "data": chunk_str,
                }
                self._produce_message(f"{blob.name}:{idx}", chunk_payload)
                chunk_count = idx + 1

            footer = {
                "file_name": blob.name,
                "record_type": "footer",
                "total_chunks": chunk_count,
            }
            self._produce_message(f"{blob.name}:footer", footer)

        logger.info(f"Successfully processed file {blob.name} in {chunk_count} chunk(s)")

    def _produce_message(self, key: str, value: dict):
        serialized = self.serialize(key=key, value=value)
        self.produce(key=serialized.key, value=serialized.value)
        self.processed_count += 1

def main():
    app = Application()

    bucket_name = os.environ.get("GS_BUCKET", "quix-workflow")
    credentials_json = os.environ.get("GS_SECRET_KEY")
    project_id = os.environ.get("GS_PROJECT_ID", "quix-testing-365012")
    folder_path = os.environ.get("GS_FOLDER_PATH", "/")
    file_format = os.environ.get("GS_FILE_FORMAT", "csv")
    compression = os.environ.get("GS_FILE_COMPRESSION", "none")
    output_topic_name = os.environ.get("output", "output")

    output_topic = app.topic(output_topic_name)

    source = GoogleStorageSource(
        name="google-storage-source",
        bucket_name=bucket_name,
        credentials_json=credentials_json,
        project_id=project_id,
        folder_path=folder_path,
        file_format=file_format,
        compression=compression,
    )
    app.add_source(source, topic=output_topic)

    logger.info("Starting Google Storage source application")
    app.run()

if __name__ == "__main__":
    main()