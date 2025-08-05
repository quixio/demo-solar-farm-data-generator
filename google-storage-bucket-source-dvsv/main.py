import os
import json
import io
import csv
import logging
from typing import Dict, Any, Iterator
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account
from quixstreams import Application
from quixstreams.sources import Source

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleStorageBucketSource(Source):
    def __init__(self, bucket_name: str, project_id: str, credentials_json: str, 
                 folder_path: str = "/", file_format: str = "csv", 
                 file_compression: str = "none"):
        super().__init__()
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.credentials_json = credentials_json
        self.folder_path = folder_path.lstrip("/").rstrip("/") + "/" if folder_path != "/" else ""
        self.file_format = file_format.lower()
        self.file_compression = file_compression.lower()
        self.client = None
        self.bucket = None
        self.processed_files = set()
        self.message_count = 0
        self.max_messages = 100

    def configure(self) -> None:
        """Configure the Google Cloud Storage client."""
        try:
            credentials_dict = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            self.client = storage.Client(credentials=credentials, project=self.project_id)
            self.bucket = self.client.bucket(self.bucket_name)
            logger.info(f"Successfully configured Google Storage client for bucket: {self.bucket_name}")
        except Exception as e:
            logger.error(f"Failed to configure Google Storage client: {e}")
            raise

    def run(self) -> Iterator[Dict[str, Any]]:
        """Read data from Google Storage Bucket and yield messages."""
        try:
            # List all files in the specified folder
            blobs = list(self.client.list_blobs(self.bucket, prefix=self.folder_path))
            
            # Filter files by format
            target_files = []
            for blob in blobs:
                if not blob.name.endswith('/'):  # Skip directories
                    if (self.file_format == 'csv' and blob.name.lower().endswith('.csv') or
                        self.file_format == 'json' and blob.name.lower().endswith('.json') or
                        self.file_format == 'txt' and blob.name.lower().endswith('.txt')):
                        target_files.append(blob)

            if not target_files:
                logger.warning(f"No {self.file_format} files found in bucket {self.bucket_name}")
                return

            logger.info(f"Found {len(target_files)} {self.file_format} files to process")

            # Process each file
            for blob in target_files:
                if self.message_count >= self.max_messages:
                    logger.info(f"Reached maximum message limit of {self.max_messages}")
                    break

                if blob.name in self.processed_files:
                    continue

                logger.info(f"Processing file: {blob.name}")
                
                try:
                    # Download file content
                    file_content = blob.download_as_bytes()
                    
                    # Handle compression
                    if self.file_compression == 'gzip':
                        import gzip
                        file_content = gzip.decompress(file_content)

                    # Process content based on format
                    yield from self._process_file_content(file_content, blob.name)
                    
                    self.processed_files.add(blob.name)
                    
                except Exception as e:
                    logger.error(f"Error processing file {blob.name}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error reading from Google Storage Bucket: {e}")
            raise

    def _process_file_content(self, file_content: bytes, filename: str) -> Iterator[Dict[str, Any]]:
        """Process file content and yield individual records."""
        try:
            if self.file_format == 'csv':
                content_str = file_content.decode('utf-8')
                csv_reader = csv.DictReader(io.StringIO(content_str))
                
                for row in csv_reader:
                    if self.message_count >= self.max_messages:
                        break
                    
                    # Transform the row data based on the schema
                    message = self._transform_csv_row(row, filename)
                    if message:
                        self.message_count += 1
                        yield message

            elif self.file_format == 'json':
                content_str = file_content.decode('utf-8')
                try:
                    # Try to parse as JSON array
                    data = json.loads(content_str)
                    if isinstance(data, list):
                        for record in data:
                            if self.message_count >= self.max_messages:
                                break
                            message = self._transform_json_record(record, filename)
                            if message:
                                self.message_count += 1
                                yield message
                    else:
                        # Single JSON object
                        message = self._transform_json_record(data, filename)
                        if message:
                            self.message_count += 1
                            yield message
                except json.JSONDecodeError:
                    # Try to parse as JSONL (one JSON per line)
                    lines = content_str.strip().split('\n')
                    for line in lines:
                        if self.message_count >= self.max_messages:
                            break
                        try:
                            record = json.loads(line)
                            message = self._transform_json_record(record, filename)
                            if message:
                                self.message_count += 1
                                yield message
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON line in {filename}: {line[:100]}...")
                            continue

            elif self.file_format == 'txt':
                content_str = file_content.decode('utf-8')
                lines = content_str.strip().split('\n')
                for i, line in enumerate(lines):
                    if self.message_count >= self.max_messages:
                        break
                    message = self._transform_text_line(line, filename, i + 1)
                    if message:
                        self.message_count += 1
                        yield message

        except Exception as e:
            logger.error(f"Error processing file content from {filename}: {e}")

    def _transform_csv_row(self, row: Dict[str, str], filename: str) -> Dict[str, Any]:
        """Transform CSV row to message format."""
        try:
            # Based on the schema analysis, transform the 3D printer temperature data
            message = {
                "timestamp": row.get("timestamp", ""),
                "hotend_temperature": float(row.get("hotend_temperature", 0)),
                "bed_temperature": float(row.get("bed_temperature", 0)),
                "ambient_temperature": float(row.get("ambient_temperature", 0)),
                "fluctuated_ambient_temperature": float(row.get("fluctuated_ambient_temperature", 0)),
                "_metadata": {
                    "source_file": filename,
                    "processed_at": datetime.utcnow().isoformat(),
                    "record_type": "temperature_reading"
                }
            }
            return message
        except (ValueError, TypeError) as e:
            logger.warning(f"Error transforming CSV row from {filename}: {e}")
            return None

    def _transform_json_record(self, record: Dict[str, Any], filename: str) -> Dict[str, Any]:
        """Transform JSON record to message format."""
        try:
            # Add metadata to the record
            message = dict(record)
            message["_metadata"] = {
                "source_file": filename,
                "processed_at": datetime.utcnow().isoformat(),
                "record_type": "json_record"
            }
            return message
        except Exception as e:
            logger.warning(f"Error transforming JSON record from {filename}: {e}")
            return None

    def _transform_text_line(self, line: str, filename: str, line_number: int) -> Dict[str, Any]:
        """Transform text line to message format."""
        try:
            message = {
                "content": line.strip(),
                "line_number": line_number,
                "_metadata": {
                    "source_file": filename,
                    "processed_at": datetime.utcnow().isoformat(),
                    "record_type": "text_line"
                }
            }
            return message
        except Exception as e:
            logger.warning(f"Error transforming text line from {filename}: {e}")
            return None

def main():
    """Main function to run the Google Storage Bucket source application."""
    
    # Get environment variables
    bucket_name = os.environ.get('GS_BUCKET')
    project_id = os.environ.get('GS_PROJECT_ID')
    credentials_json = os.environ.get('GS_SECRET_KEY')
    folder_path = os.environ.get('GS_FOLDER_PATH', '/')
    file_format = os.environ.get('GS_FILE_FORMAT', 'csv')
    file_compression = os.environ.get('GS_FILE_COMPRESSION', 'none')
    output_topic_name = os.environ.get('output', 'output')

    # Validate required environment variables
    required_vars = {
        'GS_BUCKET': bucket_name,
        'GS_PROJECT_ID': project_id,
        'GS_SECRET_KEY': credentials_json
    }
    
    for var_name, var_value in required_vars.items():
        if not var_value:
            logger.error(f"Required environment variable {var_name} is not set")
            raise ValueError(f"Missing required environment variable: {var_name}")

    logger.info(f"Starting Google Storage Bucket source application")
    logger.info(f"Bucket: {bucket_name}")
    logger.info(f"Project ID: {project_id}")
    logger.info(f"Folder path: {folder_path}")
    logger.info(f"File format: {file_format}")
    logger.info(f"Output topic: {output_topic_name}")

    # Create Quix Streams application
    app = Application()

    # Create output topic
    topic = app.topic(output_topic_name)

    # Create Google Storage Bucket source
    source = GoogleStorageBucketSource(
        bucket_name=bucket_name,
        project_id=project_id,
        credentials_json=credentials_json,
        folder_path=folder_path,
        file_format=file_format,
        file_compression=file_compression
    )

    # Create streaming dataframe
    sdf = app.dataframe(topic=topic, source=source)

    # Print messages for debugging
    sdf.print(metadata=True)

    # Run the application
    try:
        logger.info("Starting application...")
        app.run()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        logger.info("Application shutdown complete")

if __name__ == "__main__":
    main()