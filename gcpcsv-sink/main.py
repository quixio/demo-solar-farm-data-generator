# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
import csv
from io import StringIO
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class GCPCSVSink(BatchingSink):
    """
    A custom sink that writes sensor data to CSV files in Google Cloud Storage.
    
    This sink processes solar sensor data messages, extracts the nested JSON data
    from the 'value' field, and writes structured CSV files to GCP Cloud Storage.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            _on_client_connect_success=on_client_connect_success,
            _on_client_connect_failure=on_client_connect_failure
        )
        self._gcs_client = None
        self._bucket = None
        self._bucket_name = os.environ.get("GCP_BUCKET_NAME")
        self._file_prefix = os.environ.get("GCP_FILE_PREFIX", "solar-data")
        
        # CSV headers based on the schema analysis
        self._csv_headers = [
            "timestamp", "dateTime", "panel_id", "location_id", "location_name", 
            "latitude", "longitude", "timezone", "power_output", "unit_power",
            "temperature", "unit_temp", "irradiance", "unit_irradiance", 
            "voltage", "unit_voltage", "current", "unit_current", "inverter_status"
        ]

    def setup(self):
        """Setup GCS client and test connection"""
        try:
            # Get GCP credentials from environment variable
            credentials_json = os.environ.get("GCP_CREDENTIALS_JSON")
            if not credentials_json:
                raise ValueError("GCP_CREDENTIALS_JSON environment variable is required")
            
            if not self._bucket_name:
                raise ValueError("GCP_BUCKET_NAME environment variable is required")
            
            # Create credentials from JSON string
            credentials_info = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            
            # Initialize GCS client
            self._gcs_client = storage.Client(credentials=credentials)
            self._bucket = self._gcs_client.bucket(self._bucket_name)
            
            # Test connection by checking if bucket exists
            if not self._bucket.exists():
                raise ValueError(f"Bucket '{self._bucket_name}' does not exist or is not accessible")
            
            print(f"Successfully connected to GCS bucket: {self._bucket_name}")
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            print(f"Failed to setup GCS connection: {e}")
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def _parse_message(self, item):
        """Parse the Kafka message and extract sensor data"""
        try:
            # Print raw message for debugging
            print(f"Raw message: {item.value}")
            
            # The message value might already be parsed as dict, or might be a JSON string
            if isinstance(item.value, str):
                message_data = json.loads(item.value)
            else:
                message_data = item.value
            
            # Extract the nested JSON data from the 'value' field
            if 'value' in message_data and isinstance(message_data['value'], str):
                sensor_data = json.loads(message_data['value'])
            else:
                # If no nested value field, assume the data is directly in the message
                sensor_data = message_data
            
            # Prepare row data with fallbacks for missing fields
            row_data = {
                'timestamp': sensor_data.get('timestamp', ''),
                'dateTime': message_data.get('dateTime', ''),
                'panel_id': sensor_data.get('panel_id', ''),
                'location_id': sensor_data.get('location_id', ''),
                'location_name': sensor_data.get('location_name', ''),
                'latitude': sensor_data.get('latitude', ''),
                'longitude': sensor_data.get('longitude', ''),
                'timezone': sensor_data.get('timezone', ''),
                'power_output': sensor_data.get('power_output', ''),
                'unit_power': sensor_data.get('unit_power', ''),
                'temperature': sensor_data.get('temperature', ''),
                'unit_temp': sensor_data.get('unit_temp', ''),
                'irradiance': sensor_data.get('irradiance', ''),
                'unit_irradiance': sensor_data.get('unit_irradiance', ''),
                'voltage': sensor_data.get('voltage', ''),
                'unit_voltage': sensor_data.get('unit_voltage', ''),
                'current': sensor_data.get('current', ''),
                'unit_current': sensor_data.get('unit_current', ''),
                'inverter_status': sensor_data.get('inverter_status', '')
            }
            
            return row_data
            
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"Error parsing message: {e}")
            print(f"Message content: {item.value}")
            # Return None to skip this message
            return None

    def _write_to_gcs(self, data):
        """Write data to GCS as CSV"""
        try:
            # Create CSV content
            csv_buffer = StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=self._csv_headers)
            writer.writeheader()
            
            for row_data in data:
                if row_data:  # Skip None rows (failed parsing)
                    writer.writerow(row_data)
            
            csv_content = csv_buffer.getvalue()
            csv_buffer.close()
            
            # Generate filename with timestamp
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"{self._file_prefix}_{timestamp}.csv"
            
            # Upload to GCS
            blob = self._bucket.blob(filename)
            blob.upload_from_string(csv_content, content_type='text/csv')
            
            print(f"Successfully uploaded {len(data)} records to gs://{self._bucket_name}/{filename}")
            
        except Exception as e:
            print(f"Error writing to GCS: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write batch of messages to GCS as CSV.
        
        This method is called by the Quix Streams framework when a batch of messages
        is ready to be written to the sink destination.
        """
        attempts_remaining = 3
        
        # Parse all messages in the batch
        data = []
        for item in batch:
            parsed_data = self._parse_message(item)
            if parsed_data:
                data.append(parsed_data)
        
        if not data:
            print("No valid data to write in this batch")
            return
        
        while attempts_remaining:
            try:
                return self._write_to_gcs(data)
            except ConnectionError as e:
                # Network connection issues, retry with backoff
                attempts_remaining -= 1
                print(f"Connection error: {e}. Attempts remaining: {attempts_remaining}")
                if attempts_remaining:
                    time.sleep(3)
            except Exception as e:
                # For other errors that might be temporary (rate limits, etc.)
                if "rate" in str(e).lower() or "quota" in str(e).lower():
                    print(f"Rate limit or quota error, implementing backpressure: {e}")
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                else:
                    # For other errors, log and re-raise
                    print(f"Unexpected error writing to GCS: {e}")
                    raise
        
        raise Exception("Failed to write to GCS after multiple attempts")


def main():
    """ Here we will set up our Application. """

    # Setup necessary objects
    app = Application(
        consumer_group="gcp_csv_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize our GCP CSV sink
    gcp_csv_sink = GCPCSVSink()
    
    # Get input topic from environment variable
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Do SDF operations/transformations - keep the data as is for CSV export
    sdf = sdf.apply(lambda row: row).print(metadata=True)

    # Finish by calling StreamingDataFrame.sink()
    sdf.sink(gcp_csv_sink)

    # With our pipeline defined, now run the Application
    # For testing, limit to 10 messages with timeout
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()