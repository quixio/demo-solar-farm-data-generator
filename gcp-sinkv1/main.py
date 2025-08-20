# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
import csv
import io
import logging
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GCPStorageCSVSink(BatchingSink):
    """
    Custom sink that writes solar panel sensor data to a CSV file in GCP Cloud Storage.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self.client = None
        self.bucket = None
        self.project_id = os.environ.get('GCP_PROJECT_ID')
        self.bucket_name = os.environ.get('GCP_BUCKET_NAME')
        self.csv_filename = os.environ.get('CSV_FILENAME', 'solar_data.csv')
        self.credentials_json = os.environ.get('GCP_CREDENTIALS_JSON')
        
        # CSV headers for solar panel data
        self.csv_headers = [
            'timestamp',
            'datetime',
            'panel_id',
            'location_id', 
            'location_name',
            'latitude',
            'longitude',
            'timezone',
            'power_output',
            'unit_power',
            'temperature',
            'unit_temp',
            'irradiance',
            'unit_irradiance',
            'voltage',
            'unit_voltage',
            'current',
            'unit_current',
            'inverter_status'
        ]
        
    def setup(self):
        """Initialize GCP storage client and verify connection"""
        try:
            if not self.credentials_json:
                raise ValueError("GCP_CREDENTIALS_JSON environment variable is required")
            
            # Parse credentials JSON
            credentials_info = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            
            # Initialize storage client
            self.client = storage.Client(credentials=credentials, project=self.project_id)
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test connection by checking if bucket exists
            self.bucket.reload()
            logger.info(f"Successfully connected to GCP bucket: {self.bucket_name}")
            
            # Initialize CSV file with headers if it doesn't exist
            self._initialize_csv_file()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
            
        except Exception as e:
            logger.error(f"Failed to setup GCP storage client: {e}")
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise
    
    def _initialize_csv_file(self):
        """Initialize CSV file with headers if it doesn't exist"""
        try:
            blob = self.bucket.blob(self.csv_filename)
            if not blob.exists():
                # Create CSV with headers
                csv_buffer = io.StringIO()
                writer = csv.writer(csv_buffer)
                writer.writerow(self.csv_headers)
                
                # Upload to GCS
                blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
                logger.info(f"Created CSV file with headers: {self.csv_filename}")
        except Exception as e:
            logger.error(f"Failed to initialize CSV file: {e}")
            raise
    
    def _convert_timestamp_to_datetime(self, timestamp):
        """Convert epoch timestamp to readable datetime"""
        try:
            # The timestamp appears to be in nanoseconds based on the example
            # Convert to seconds first
            timestamp_seconds = timestamp / 1_000_000_000
            return datetime.fromtimestamp(timestamp_seconds).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return "Invalid timestamp"
    
    def _parse_solar_data(self, message):
        """Parse solar panel data from message"""
        try:
            # Debug: print raw message structure
            print(f"Raw message: {message}")
            
            # Extract the value field which contains JSON string
            if hasattr(message, 'value') and isinstance(message.value, str):
                # If value is a JSON string, parse it
                data = json.loads(message.value)
            elif hasattr(message, 'value') and isinstance(message.value, dict):
                # If value is already a dict
                data = message.value
            else:
                logger.warning(f"Unexpected message format: {message}")
                return None
            
            # Convert timestamp to datetime
            timestamp = data.get('timestamp', 0)
            datetime_str = self._convert_timestamp_to_datetime(timestamp)
            
            # Extract all fields according to schema
            row = [
                timestamp,
                datetime_str,
                data.get('panel_id', ''),
                data.get('location_id', ''),
                data.get('location_name', ''),
                data.get('latitude', 0.0),
                data.get('longitude', 0.0),
                data.get('timezone', 0),
                data.get('power_output', 0.0),
                data.get('unit_power', ''),
                data.get('temperature', 0.0),
                data.get('unit_temp', ''),
                data.get('irradiance', 0.0),
                data.get('unit_irradiance', ''),
                data.get('voltage', 0.0),
                data.get('unit_voltage', ''),
                data.get('current', 0.0),
                data.get('unit_current', ''),
                data.get('inverter_status', '')
            ]
            
            return row
            
        except Exception as e:
            logger.error(f"Failed to parse solar data: {e}")
            return None
    
    def write(self, batch: SinkBatch):
        """Write batch of solar data to CSV in GCP Storage"""
        attempts_remaining = 3
        
        while attempts_remaining:
            try:
                # Parse all messages in the batch
                rows_to_write = []
                for item in batch:
                    parsed_row = self._parse_solar_data(item)
                    if parsed_row:
                        rows_to_write.append(parsed_row)
                
                if not rows_to_write:
                    logger.warning("No valid data to write in this batch")
                    return
                
                # Read existing CSV content
                blob = self.bucket.blob(self.csv_filename)
                existing_content = ""
                if blob.exists():
                    existing_content = blob.download_as_text()
                
                # Append new rows
                csv_buffer = io.StringIO()
                csv_buffer.write(existing_content)
                
                writer = csv.writer(csv_buffer)
                for row in rows_to_write:
                    writer.writerow(row)
                
                # Upload updated CSV
                blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
                
                logger.info(f"Successfully wrote {len(rows_to_write)} rows to {self.csv_filename} in bucket {self.bucket_name}")
                logger.info(f"CSV file exists in bucket: {blob.exists()}")
                
                return
                
            except Exception as e:
                logger.error(f"Error writing to GCP Storage: {e}")
                attempts_remaining -= 1
                
                if "timeout" in str(e).lower() or "deadline" in str(e).lower():
                    # Handle timeout with backpressure
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                elif attempts_remaining > 0:
                    # Retry for other errors
                    time.sleep(3)
                else:
                    raise Exception(f"Failed to write to GCP Storage after retries: {e}")


def main():
    """ Here we will set up our Application. """

    # Setup necessary objects
    app = Application(
        consumer_group="solar_data_gcp_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize GCP Storage CSV Sink
    gcp_sink = GCPStorageCSVSink()
    
    # Set up input topic with JSON deserialization
    input_topic = app.topic(name=os.environ["input"], value_deserializer="json")
    sdf = app.dataframe(topic=input_topic)

    # Add debug print to see raw messages
    def debug_message(row):
        print(f"Processing message: {row}")
        return row
    
    sdf = sdf.apply(debug_message)

    # Finish by calling StreamingDataFrame.sink()
    sdf.sink(gcp_sink)

    # With our pipeline defined, now run the Application for testing
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()