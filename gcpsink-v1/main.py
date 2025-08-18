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
from datetime import datetime
from google.cloud import storage

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class GCPCloudStorageSink(BatchingSink):
    """
    A sink that writes sensor data to CSV files in Google Cloud Storage.
    
    This sink processes solar panel sensor data and writes it to CSV format
    in a GCP Cloud Storage bucket with batching for optimal performance.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            _on_client_connect_success=on_client_connect_success,
            _on_client_connect_failure=on_client_connect_failure
        )
        self._client = None
        self._bucket = None
        self._bucket_name = os.environ.get("GCP_BUCKET_NAME")
        self._csv_filename_prefix = os.environ.get("CSV_FILENAME_PREFIX", "sensor_data")
        
        # CSV headers based on the solar panel data schema
        self._csv_headers = [
            'timestamp',
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
            'inverter_status',
            'internal_timestamp'
        ]

    def setup(self):
        """Initialize the GCP Storage client and test the connection"""
        try:
            # Initialize the GCP Storage client using service account credentials from env var
            credentials_json = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
            if not credentials_json:
                raise ValueError("GCP_SERVICE_ACCOUNT_KEY environment variable is required")
            
            # Parse the credentials JSON
            credentials_dict = json.loads(credentials_json)
            
            # Create client using the credentials
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            self._client = storage.Client(credentials=credentials)
            
            # Get the bucket reference
            if not self._bucket_name:
                raise ValueError("GCP_BUCKET_NAME environment variable is required")
                
            self._bucket = self._client.bucket(self._bucket_name)
            
            # Test bucket access
            if not self._bucket.exists():
                raise ValueError(f"Bucket '{self._bucket_name}' does not exist or is not accessible")
            
            print(f"Successfully connected to GCP bucket: {self._bucket_name}")
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            print(f"Failed to setup GCP Storage client: {e}")
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def _extract_sensor_data(self, item_value):
        """Extract and parse sensor data from the message value"""
        try:
            # The schema shows that the actual sensor data is in the 'value' field as a JSON string
            if isinstance(item_value, dict) and 'value' in item_value:
                # Parse the JSON string in the value field
                sensor_data = json.loads(item_value['value'])
                
                # Convert timestamp to readable format if available
                timestamp = item_value.get('dateTime', '')
                if timestamp:
                    # Parse ISO timestamp and convert to readable format
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    formatted_timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    formatted_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Create CSV row data based on schema
                return {
                    'timestamp': formatted_timestamp,
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
                    'inverter_status': sensor_data.get('inverter_status', ''),
                    'internal_timestamp': sensor_data.get('timestamp', '')
                }
            else:
                print(f"Unexpected message structure: {item_value}")
                return None
                
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON from value field: {e}")
            print(f"Raw value: {item_value.get('value') if isinstance(item_value, dict) else item_value}")
            return None
        except Exception as e:
            print(f"Error extracting sensor data: {e}")
            return None

    def _write_csv_to_gcs(self, sensor_records):
        """Write sensor data to CSV format and upload to GCS"""
        try:
            # Create CSV content in memory
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=self._csv_headers)
            
            # Write header
            writer.writeheader()
            
            # Write data rows
            for record in sensor_records:
                if record:  # Only write non-None records
                    writer.writerow(record)
            
            # Generate filename with timestamp
            timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{self._csv_filename_prefix}_{timestamp_str}.csv"
            
            # Upload to GCS
            blob = self._bucket.blob(filename)
            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            
            print(f"Successfully uploaded {len([r for r in sensor_records if r])} records to gs://{self._bucket_name}/{filename}")
            
        except Exception as e:
            print(f"Failed to write CSV to GCS: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write batch of sensor data to GCP Cloud Storage as CSV.
        
        Extracts sensor data from the nested JSON structure and writes it
        to a CSV file in the configured GCS bucket.
        """
        attempts_remaining = 3
        
        print(f"Processing batch of {len(batch)} messages")
        
        # Extract sensor data from all messages in the batch
        sensor_records = []
        for item in batch:
            print(f"Raw message: {item.value}")  # Debug print for message structure
            
            try:
                sensor_data = self._extract_sensor_data(item.value)
                if sensor_data:
                    sensor_records.append(sensor_data)
            except Exception as e:
                print(f"Error processing message: {e}")
                continue
        
        if not sensor_records:
            print("No valid sensor records found in batch, skipping write")
            return
            
        print(f"Extracted {len(sensor_records)} valid sensor records")
        
        while attempts_remaining:
            try:
                return self._write_csv_to_gcs(sensor_records)
            except ConnectionError as e:
                print(f"Connection error: {e}")
                # Maybe we just failed to connect, do a short wait and try again
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
            except Exception as e:
                if "timeout" in str(e).lower() or "deadline" in str(e).lower():
                    # GCS timeout, do a sanctioned extended pause
                    print(f"GCS timeout error: {e}")
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                else:
                    print(f"Unexpected error writing to GCS: {e}")
                    attempts_remaining -= 1
                    if attempts_remaining:
                        time.sleep(5)
                    else:
                        raise
        
        raise Exception("Failed to write to GCP Cloud Storage after multiple attempts")


def main():
    """ Here we will set up our Application. """

    # Setup necessary objects
    app = Application(
        consumer_group="gcp_storage_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    gcs_sink = GCPCloudStorageSink()
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Do SDF operations/transformations
    # Print messages for debugging and pass them to the sink
    sdf = sdf.apply(lambda row: row).print(metadata=True)

    # Finish by calling StreamingDataFrame.sink()
    sdf.sink(gcs_sink)

    # With our pipeline defined, now run the Application
    # Process 10 messages for testing as specified in requirements
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()