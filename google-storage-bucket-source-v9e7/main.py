import os
import json
import csv
from io import StringIO
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources.base import Source


class GoogleStorageSource(Source):
    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        credentials_key: str,
        folder_path: str = "/",
        file_format: str = "csv",
        file_compression: str = "none",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_key = credentials_key
        self.folder_path = folder_path
        self.file_format = file_format
        self.file_compression = file_compression
        self.client = None
        self.bucket = None

    def setup(self):
        credentials_json = os.getenv(self.credentials_key)

        if credentials_json is None and self.credentials_key.strip().startswith("{"):
            credentials_json = self.credentials_key

        if not credentials_json:
            raise ValueError(
                f'Google credentials not provided. Expected JSON in env var '
                f'"{self.credentials_key}", or pass the JSON string directly.'
            )

        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict
        )

        self.client = storage.Client(credentials=credentials, project=self.project_id)
        self.bucket = self.client.bucket(self.bucket_name)

        if hasattr(self, "on_client_connect_success"):
            self.on_client_connect_success()

    def run(self):
        self.setup()

        prefix = (
            self.folder_path.strip("/") + "/"
            if self.folder_path and self.folder_path != "/"
            else ""
        )
        blobs = list(self.bucket.list_blobs(prefix=prefix))

        if self.file_format and self.file_format.lower() != "none":
            blobs = [
                b for b in blobs if b.name.lower().endswith(f".{self.file_format.lower()}")
            ]

        message_count, max_messages = 0, 100

        for blob in blobs:
            if not self.running or message_count >= max_messages:
                break

            try:
                content = blob.download_as_text()

                if self.file_format.lower() == "csv":
                    reader = csv.DictReader(StringIO(content))
                    for row in reader:
                        if not self.running or message_count >= max_messages:
                            break
                        self._produce_row(blob.name, self._process_csv_row(row))
                        message_count += 1

                elif self.file_format.lower() == "json":
                    self._stream_json_content(blob.name, content, message_count, max_messages)

                else:
                    for line in content.strip().split("\n"):
                        if not self.running or message_count >= max_messages:
                            break
                        self._produce_row(blob.name, {"raw_line": line})
                        message_count += 1

            except Exception as e:
                print(f"Error processing file {blob.name}: {e}")
                continue

    def _produce_row(self, key, value):
        msg = self.serialize(key=key, value=value)
        self.produce(key=msg.key, value=msg.value)

    def _stream_json_content(self, blob_name, content, msg_cnt, max_msgs):
        try:
            data = json.loads(content)
            rows = data if isinstance(data, list) else [data]
        except json.JSONDecodeError:
            rows = [
                json.loads(line) if line.strip().startswith("{") else {"raw_line": line}
                for line in content.strip().split("\n")
            ]

        for item in rows:
            if not self.running or msg_cnt >= max_msgs:
                break
            self._produce_row(blob_name, item)
            msg_cnt += 1

    def _process_csv_row(self, row):
        processed = {}
        for key, value in row.items():
            if key == "timestamp":
                processed[key] = value
            elif key in {
                "hotend_temperature",
                "bed_temperature",
                "ambient_temperature",
                "fluctuated_ambient_temperature",
            }:
                try:
                    processed[key] = float(value)
                except (ValueError, TypeError):
                    processed[key] = value
            else:
                processed[key] = value
        return processed


def main():
    app = Application()

    output_topic = app.topic(os.environ["output"])

    credentials_env_var = os.getenv("GCP_CREDENTIALS_KEY", "GCLOUD_PK_JSON")

    if not os.getenv(credentials_env_var) and not credentials_env_var.strip().startswith("{"):
        raise EnvironmentError(
            f"Service-account JSON not found in environment variable '{credentials_env_var}'."
        )

    source = GoogleStorageSource(
        name="google-storage-source",
        bucket_name=os.environ["GCS_BUCKET"],
        project_id=os.environ["GCP_PROJECT_ID"],
        credentials_key=credentials_env_var,
        folder_path=os.getenv("GCS_FOLDER_PATH", "/"),
        file_format=os.getenv("GCS_FILE_FORMAT", "csv"),
        file_compression=os.getenv("GCS_FILE_COMPRESSION", "none"),
    )

    sdf = app.dataframe(topic=output_topic, source=source)
    sdf.print(metadata=True)

    app.run()


if __name__ == "__main__":
    main()